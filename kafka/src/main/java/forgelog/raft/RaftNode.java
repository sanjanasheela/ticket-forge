package forgelog.raft;

import forgelog.log.RaftLog;
import forgelog.log.Record;
import forgelog.raft.RaftMessage.AppendRequest;
import forgelog.raft.RaftMessage.AppendResponse;
import forgelog.raft.RaftMessage.VoteRequest;
import forgelog.raft.RaftMessage.VoteResponse;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * One replica of one Raft group: leader election, log replication and
 * commitment, with no I/O of its own.
 *
 * <p>The node is driven by its owner (the broker event loop, or a simulated
 * cluster in tests) through {@link #tick}, {@link #receive}, {@link #propose},
 * {@link #flush} and {@link #onPersisted}; it talks back through a
 * {@link Transport} and a {@link Listener}. Time is passed in as milliseconds
 * from a monotonic clock. Being deterministic given its inputs (and the
 * {@link Random} it is handed), it can be tested exhaustively in simulation.
 *
 * <p>Beyond the basic algorithm it implements:
 * <ul>
 *   <li><b>Pre-vote</b> (thesis §9.6): a node first asks whether it
 *       <em>could</em> win before bumping its term, so a node that was
 *       partitioned away cannot disrupt a healthy cluster when it returns.
 *   <li><b>Leader stickiness</b>: followers that heard from a leader within the
 *       minimum election timeout ignore vote requests.
 *   <li><b>Check-quorum</b>: a leader that has not heard from a majority within
 *       the maximum election timeout steps down, so a partitioned leader stops
 *       accepting writes it can never commit.
 *   <li><b>Fast backtracking</b>: a rejected append carries a hint (the
 *       follower's log end, or the start of the conflicting term).
 *   <li><b>Pipelining</b>: appends are sent optimistically, bounded by
 *       {@link Timing#maxInflightEntries}; heartbeats repair lost messages.
 *   <li><b>Durability ordering</b>: the leader only counts its own vote for an
 *       entry after {@link #onPersisted}; the owner must send
 *       {@link AppendResponse}s only after the log is persisted, and
 *       (non-pre-vote) {@link VoteRequest}s and {@link VoteResponse}s only
 *       after the {@link HardState} is.
 * </ul>
 */
public final class RaftNode {
    public enum Role { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER }

    /**
     * Durable term and vote. The owner must make a {@link #save} durable
     * before delivering any vote message sent after it (the broker defers
     * them to its group commit; the simulation persists immediately).
     */
    public interface HardState {
        long term();

        int votedFor();

        void save(long term, int votedFor);
    }

    public interface Transport {
        void send(int to, RaftMessage m);
    }

    public interface Listener {
        default void onCommit(long commitIndex) {}

        default void onRoleChange(Role role, long term, int leaderId) {}
    }

    /**
     * @param maxAppendBytes upper bound on the entries of one append request
     * @param maxInflightEntries entries sent but not yet acknowledged, per follower
     */
    public record Timing(int electionMinMs, int electionMaxMs, int heartbeatMs, int maxAppendBytes,
            int maxInflightEntries) {
        public static final Timing DEFAULT = new Timing(150, 300, 50, 1 << 20, 100_000);
    }

    private final int group;
    private final int self;
    private final int[] peers;
    private final int quorum;
    private final RaftLog log;
    private final HardState hardState;
    private final Transport transport;
    private final Listener listener;
    private final Timing timing;
    private final Random rng;

    private Role role = Role.FOLLOWER;
    private long term;
    private int votedFor;
    private int leaderId = -1;
    private long commitIndex;
    private long persistedIndex = -1;
    private long leaderStartIndex = Long.MAX_VALUE;

    private long electionDeadline;
    private long nextHeartbeat;
    private long lastLeaderContact = Long.MIN_VALUE / 2;
    private final Set<Integer> votes = new HashSet<>();

    // leader state, indexed like `peers`
    private final long[] nextIndex;
    private final long[] matchIndex;
    private final long[] lastContact;

    /**
     * @param members every replica of the group, including {@code self}
     * @param commitHint a commit index known from before a restart (-1 if none)
     */
    public RaftNode(int group, int self, int[] members, RaftLog log, HardState hardState, Transport transport,
            Listener listener, Timing timing, Random rng, long now, long commitHint) {
        this.group = group;
        this.self = self;
        this.peers = Arrays.stream(members).filter(m -> m != self).distinct().toArray();
        this.quorum = (peers.length + 1) / 2 + 1;
        this.log = log;
        this.hardState = hardState;
        this.transport = transport;
        this.listener = listener;
        this.timing = timing;
        this.rng = rng;
        this.term = hardState.term();
        this.votedFor = hardState.votedFor();
        this.commitIndex = Math.min(Math.max(-1, commitHint), log.lastIndex());
        this.nextIndex = new long[peers.length];
        this.matchIndex = new long[peers.length];
        this.lastContact = new long[peers.length];
        resetElectionTimer(now);
    }

    // -----------------------------------------------------------------------
    // accessors
    // -----------------------------------------------------------------------

    public int group() {
        return group;
    }

    public int self() {
        return self;
    }

    public Role role() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public long term() {
        return term;
    }

    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastIndex() {
        return log.lastIndex();
    }

    /**
     * Leader, and an entry of its own term is committed: its commit index is
     * now current, so reads served from it are linearizable.
     */
    public boolean isReady() {
        return role == Role.LEADER && commitIndex >= leaderStartIndex;
    }

    // -----------------------------------------------------------------------
    // inputs
    // -----------------------------------------------------------------------

    /**
     * Append {@code r} as the leader. Returns its index; it is committed once
     * {@link #commitIndex()} reaches it.
     */
    public long propose(Record r) throws NotLeaderException {
        if (role != Role.LEADER) {
            throw new NotLeaderException(leaderId);
        }
        long index = log.lastIndex() + 1;
        log.append(r.withPosition(index, term));
        if (peers.length == 0) {
            maybeCommit();
        }
        return index;
    }

    /** Send pending entries to followers (call after a batch of proposals). */
    public void flush(long now) {
        if (role != Role.LEADER) {
            return;
        }
        long last = log.lastIndex();
        for (int p = 0; p < peers.length; p++) {
            if (nextIndex[p] <= last && nextIndex[p] - 1 - matchIndex[p] < timing.maxInflightEntries()) {
                sendAppend(p);
            }
        }
    }

    /** The local log is durable up to {@code index}. */
    public void onPersisted(long index) {
        long capped = Math.min(index, log.lastIndex());
        if (capped > persistedIndex) {
            persistedIndex = capped;
        }
        if (role == Role.LEADER) {
            maybeCommit();
        }
    }

    /**
     * When {@link #tick} next has work to do (an election timeout or a
     * heartbeat). Owners should tick at that moment rather than on a coarse
     * period: quantizing the randomized election timeouts makes followers
     * time out together and split the vote.
     */
    public long nextTimeout() {
        return role == Role.LEADER ? nextHeartbeat : electionDeadline;
    }

    public void tick(long now) {
        if (role == Role.LEADER) {
            if (now >= nextHeartbeat) {
                nextHeartbeat = now + timing.heartbeatMs();
                for (int p = 0; p < peers.length; p++) {
                    sendAppend(p);
                }
            }
            if (peers.length > 0) {
                int recent = 1;
                for (int p = 0; p < peers.length; p++) {
                    if (now - lastContact[p] <= timing.electionMaxMs()) {
                        recent++;
                    }
                }
                if (recent < quorum) {
                    becomeFollower(term, -1, now); // check-quorum: we are cut off
                }
            }
        } else if (now >= electionDeadline) {
            startPreVote(now);
        }
    }

    public void receive(RaftMessage m, long now) {
        if (m.group() != group) {
            return;
        }
        switch (m) {
            case VoteRequest v -> onVoteRequest(v, now);
            case VoteResponse v -> onVoteResponse(v, now);
            case AppendRequest a -> onAppendRequest(a, now);
            case AppendResponse a -> onAppendResponse(a, now);
        }
    }

    // -----------------------------------------------------------------------
    // elections
    // -----------------------------------------------------------------------

    private void resetElectionTimer(long now) {
        int span = timing.electionMaxMs() - timing.electionMinMs();
        electionDeadline = now + timing.electionMinMs() + (span > 0 ? rng.nextInt(span + 1) : 0);
    }

    private boolean heardFromLeaderRecently(long now) {
        return role == Role.LEADER || (leaderId != -1 && now - lastLeaderContact < timing.electionMinMs());
    }

    private boolean logUpToDate(long lastTerm, long lastIndex) {
        long myLastTerm = log.termAt(log.lastIndex());
        return lastTerm > myLastTerm || (lastTerm == myLastTerm && lastIndex >= log.lastIndex());
    }

    private void persist() {
        hardState.save(term, votedFor);
    }

    private void setRole(Role r, int leader) {
        Role old = role;
        int oldLeader = leaderId;
        role = r;
        leaderId = leader;
        if (old != r || oldLeader != leader) {
            listener.onRoleChange(r, term, leader);
        }
    }

    private void becomeFollower(long newTerm, int leader, long now) {
        if (newTerm > term) {
            term = newTerm;
            votedFor = -1;
            persist();
        }
        votes.clear();
        leaderStartIndex = Long.MAX_VALUE;
        setRole(Role.FOLLOWER, leader);
        resetElectionTimer(now);
    }

    private void startPreVote(long now) {
        if (peers.length == 0) {
            startElection(now);
            return;
        }
        votes.clear();
        votes.add(self);
        leaderStartIndex = Long.MAX_VALUE;
        setRole(Role.PRE_CANDIDATE, -1);
        resetElectionTimer(now);
        long last = log.lastIndex();
        for (int p : peers) {
            transport.send(p, new VoteRequest(group, self, term + 1, last, log.termAt(last), true));
        }
    }

    private void startElection(long now) {
        term++;
        votedFor = self;
        persist();
        votes.clear();
        votes.add(self);
        setRole(Role.CANDIDATE, -1);
        resetElectionTimer(now);
        if (votes.size() >= quorum) {
            becomeLeader(now);
            return;
        }
        long last = log.lastIndex();
        for (int p : peers) {
            transport.send(p, new VoteRequest(group, self, term, last, log.termAt(last), false));
        }
    }

    private void becomeLeader(long now) {
        long last = log.lastIndex();
        for (int p = 0; p < peers.length; p++) {
            nextIndex[p] = last + 1;
            matchIndex[p] = -1;
            lastContact[p] = now;
        }
        setRole(Role.LEADER, self);
        // A no-op of our own term: committing it commits everything before it
        // and tells us our commit index is current (see isReady).
        leaderStartIndex = last + 1;
        log.append(Record.control().withPosition(leaderStartIndex, term));
        nextHeartbeat = now + timing.heartbeatMs();
        for (int p = 0; p < peers.length; p++) {
            sendAppend(p);
        }
        if (peers.length == 0) {
            maybeCommit();
        }
    }

    private void onVoteRequest(VoteRequest v, long now) {
        if (v.preVote()) {
            boolean grant = v.term() > term && logUpToDate(v.lastTerm(), v.lastIndex())
                    && !heardFromLeaderRecently(now);
            transport.send(v.from(), new VoteResponse(group, self, grant ? v.term() : term, grant, true));
            return;
        }
        if (v.term() > term) {
            if (heardFromLeaderRecently(now)) {
                // Leader stickiness: a live leader exists; don't let this candidate depose it.
                transport.send(v.from(), new VoteResponse(group, self, term, false, false));
                return;
            }
            becomeFollower(v.term(), -1, now);
        }
        boolean grant = v.term() == term && (votedFor == -1 || votedFor == v.from())
                && logUpToDate(v.lastTerm(), v.lastIndex());
        if (grant) {
            votedFor = v.from();
            persist();
            resetElectionTimer(now);
        }
        transport.send(v.from(), new VoteResponse(group, self, term, grant, false));
    }

    private void onVoteResponse(VoteResponse v, long now) {
        if (v.preVote()) {
            if (role == Role.PRE_CANDIDATE && v.granted() && v.term() == term + 1) {
                votes.add(v.from());
                if (votes.size() >= quorum) {
                    startElection(now);
                }
            } else if (!v.granted() && v.term() > term) {
                becomeFollower(v.term(), -1, now);
            }
            return;
        }
        if (v.term() > term) {
            becomeFollower(v.term(), -1, now);
            return;
        }
        if (role == Role.CANDIDATE && v.term() == term && v.granted()) {
            votes.add(v.from());
            if (votes.size() >= quorum) {
                becomeLeader(now);
            }
        }
    }

    // -----------------------------------------------------------------------
    // replication
    // -----------------------------------------------------------------------

    private int peerSlot(int node) {
        for (int p = 0; p < peers.length; p++) {
            if (peers[p] == node) {
                return p;
            }
        }
        return -1;
    }

    private void sendAppend(int p) {
        long prev = nextIndex[p] - 1;
        long last = log.lastIndex();
        List<Record> entries = List.of();
        if (nextIndex[p] <= last && nextIndex[p] - 1 - matchIndex[p] < timing.maxInflightEntries()) {
            long upTo = Math.min(last, matchIndex[p] + timing.maxInflightEntries());
            entries = log.entries(nextIndex[p], upTo, timing.maxAppendBytes());
        }
        transport.send(peers[p], new AppendRequest(group, self, term, prev, log.termAt(prev), commitIndex, entries));
        if (!entries.isEmpty()) {
            nextIndex[p] = entries.getLast().offset + 1; // optimistic: pipeline the next batch
        }
    }

    private void onAppendRequest(AppendRequest a, long now) {
        if (a.term() < term) {
            transport.send(a.from(), new AppendResponse(group, self, term, false, -1, log.lastIndex() + 1));
            return;
        }
        if (a.term() > term || role != Role.FOLLOWER || leaderId != a.from()) {
            becomeFollower(a.term(), a.from(), now);
        }
        lastLeaderContact = now;
        resetElectionTimer(now);

        long last = log.lastIndex();
        if (a.prevIndex() > last) {
            transport.send(a.from(), new AppendResponse(group, self, term, false, -1, last + 1));
            return;
        }
        if (a.prevIndex() >= 0 && log.termAt(a.prevIndex()) != a.prevTerm()) {
            long hint = Math.max(commitIndex + 1, log.firstIndexOfTerm(a.prevIndex()));
            transport.send(a.from(), new AppendResponse(group, self, term, false, -1, hint));
            return;
        }
        for (Record e : a.entries()) {
            if (e.offset <= log.lastIndex()) {
                if (log.termAt(e.offset) == e.term) {
                    continue; // already have it
                }
                if (e.offset <= commitIndex) {
                    throw new IllegalStateException("group " + group + ": leader " + a.from()
                            + " conflicts with committed entry " + e.offset);
                }
                log.truncateFrom(e.offset);
                persistedIndex = Math.min(persistedIndex, e.offset - 1);
            }
            log.append(e);
        }
        long matched = a.prevIndex() + a.entries().size();
        if (a.leaderCommit() > commitIndex) {
            long nc = Math.min(a.leaderCommit(), matched);
            if (nc > commitIndex) {
                commitIndex = nc;
                listener.onCommit(commitIndex);
            }
        }
        transport.send(a.from(), new AppendResponse(group, self, term, true, matched, matched + 1));
    }

    private void onAppendResponse(AppendResponse r, long now) {
        if (r.term() > term) {
            becomeFollower(r.term(), -1, now);
            return;
        }
        if (role != Role.LEADER || r.term() != term) {
            return;
        }
        int p = peerSlot(r.from());
        if (p < 0) {
            return;
        }
        lastContact[p] = now;
        if (r.success()) {
            if (r.matchIndex() > matchIndex[p]) {
                matchIndex[p] = r.matchIndex();
                maybeCommit();
            }
            nextIndex[p] = Math.max(nextIndex[p], r.matchIndex() + 1);
            if (nextIndex[p] <= log.lastIndex() && nextIndex[p] - 1 - matchIndex[p] < timing.maxInflightEntries()) {
                sendAppend(p);
            }
        } else {
            nextIndex[p] = Math.max(matchIndex[p] + 1, Math.min(r.hintIndex(), log.lastIndex() + 1));
            sendAppend(p);
        }
    }

    private void maybeCommit() {
        long[] match = new long[peers.length + 1];
        System.arraycopy(matchIndex, 0, match, 0, peers.length);
        match[peers.length] = persistedIndex;
        Arrays.sort(match);
        long n = match[match.length - quorum]; // highest index a quorum has
        if (n > commitIndex && log.termAt(n) == term) {
            commitIndex = n;
            listener.onCommit(commitIndex);
        }
    }

    @Override
    public String toString() {
        return "raft[g" + group + " n" + self + " " + role + " t" + term + " leader=" + leaderId + " last="
                + log.lastIndex() + " commit=" + commitIndex + "]";
    }
}
