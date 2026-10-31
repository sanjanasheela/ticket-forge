package forgeraft;

import forgeraft.Msg.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Raft (Ongaro & Ousterhout): leader election, log replication, commit by majority of the current term,
 * and a leader check-quorum so a leader cut off from the majority steps down (no stale leader accepts writes).
 * State is in memory only: a restarted node rejoins empty and is caught up by the leader.
 */
public final class Node {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public static final class NotLeader extends RuntimeException { public final String hint; NotLeader(String h) { super("not leader, try " + h); hint = h; } }

    final String id; final List<String> peers; final Transport net; final Consumer<byte[]> apply;
    final int electionMinMs, electionMaxMs, heartbeatMs;
    final Object lock = new Object();
    final ExecutorService rpc = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
    final Random rnd = new Random();
    volatile boolean running;

    long term; String votedFor; Role role = Role.FOLLOWER; String leaderId;
    final List<Entry> log = new ArrayList<>(List.of(new Entry(0, new byte[0])));  // index 0 is a sentinel
    long commitIndex, lastApplied;
    final Map<String, Long> next = new HashMap<>(), match = new HashMap<>(), lastContact = new HashMap<>();
    final Map<Long, CompletableFuture<Long>> waiting = new HashMap<>();
    long deadline;

    public Node(String id, List<String> peers, Transport net, Consumer<byte[]> apply, int electionMinMs, int heartbeatMs) {
        this.id = id; this.peers = peers; this.net = net; this.apply = apply;
        this.electionMinMs = electionMinMs; this.electionMaxMs = electionMinMs * 2; this.heartbeatMs = heartbeatMs;
    }

    public void start() {
        running = true; resetDeadline();
        Thread t = new Thread(this::tickLoop, "raft-" + id); t.setDaemon(true); t.start();
    }
    public void stop() { running = false; }

    void resetDeadline() { deadline = System.nanoTime() + (electionMinMs + rnd.nextInt(electionMaxMs - electionMinMs)) * 1_000_000L; }

    void tickLoop() {
        while (running) {
            try { Thread.sleep(heartbeatMs / 2 + 1); } catch (InterruptedException e) { return; }
            synchronized (lock) {
                if (role == Role.LEADER) {
                    broadcastAppend();
                    if (!hasQuorumContact()) stepDown(term, null);          // check-quorum
                } else if (System.nanoTime() >= deadline) startElection();
            }
        }
    }

    boolean hasQuorumContact() {
        long now = System.nanoTime(), win = electionMinMs * 1_000_000L; int alive = 1;
        for (String p : peers) if (now - lastContact.getOrDefault(p, 0L) < win) alive++;
        return alive * 2 > peers.size() + 1;
    }

    // ---- election ----
    void startElection() {
        role = Role.CANDIDATE; term++; votedFor = id; leaderId = null; resetDeadline();
        final long myTerm = term, li = lastIndex(), lt = log.get((int) li).term();
        int[] votes = {1};
        for (String p : peers) rpc.execute(() -> {
            try {
                Msg r = net.call(id, p, new RequestVote(myTerm, id, li, lt));
                synchronized (lock) {
                    VoteResp v = (VoteResp) r;
                    if (v.term() > term) { stepDown(v.term(), null); return; }
                    if (role == Role.CANDIDATE && term == myTerm && v.granted() && ++votes[0] * 2 > peers.size() + 1) becomeLeader();
                }
            } catch (IOException ignored) {}
        });
        if (peers.isEmpty()) becomeLeader();
    }

    void becomeLeader() {
        role = Role.LEADER; leaderId = id;
        long now = System.nanoTime();
        for (String p : peers) { next.put(p, lastIndex() + 1); match.put(p, 0L); lastContact.put(p, now); }
        append(new byte[0]);                 // no-op in the new term so earlier entries can commit
        broadcastAppend();
    }

    void stepDown(long newTerm, String leader) {
        if (newTerm > term) { term = newTerm; votedFor = null; }
        if (role == Role.LEADER) { for (var f : waiting.values()) f.completeExceptionally(new NotLeader(String.valueOf(leader))); waiting.clear(); }
        role = Role.FOLLOWER; if (leader != null) leaderId = leader; resetDeadline();
    }

    long lastIndex() { return log.size() - 1; }

    // ---- replication ----
    long append(byte[] data) { log.add(new Entry(term, data)); return lastIndex(); }

    /** Replicates data; returns its log index once committed by a majority. Blocks. */
    public long propose(byte[] data, long timeoutMs) throws Exception {
        CompletableFuture<Long> f = new CompletableFuture<>();
        synchronized (lock) {
            if (role != Role.LEADER) throw new NotLeader(String.valueOf(leaderId));
            long idx = append(data); waiting.put(idx, f); broadcastAppend();
            if (peers.isEmpty()) advanceCommit();
        }
        try { return f.get(timeoutMs, TimeUnit.MILLISECONDS); }
        catch (ExecutionException e) { throw e.getCause() instanceof RuntimeException re ? re : e; }
    }

    void broadcastAppend() {
        for (String p : peers) {
            final long myTerm = term;
            long ni = next.getOrDefault(p, lastIndex() + 1);
            long prev = ni - 1;
            List<Entry> es = new ArrayList<>(log.subList((int) ni, (int) Math.min(log.size(), ni + 256)));
            Append a = new Append(myTerm, id, prev, log.get((int) prev).term(), es, commitIndex);
            rpc.execute(() -> {
                try {
                    AppendResp r = (AppendResp) net.call(id, p, a);
                    synchronized (lock) {
                        if (r.term() > term) { stepDown(r.term(), null); return; }
                        if (role != Role.LEADER || term != myTerm) return;
                        lastContact.put(p, System.nanoTime());
                        if (r.success()) {
                            match.put(p, Math.max(match.getOrDefault(p, 0L), r.matchIndex()));
                            next.put(p, match.get(p) + 1);
                            advanceCommit();
                        } else next.put(p, Math.max(1, Math.min(next.get(p) - 1, r.matchIndex() + 1)));
                    }
                } catch (IOException ignored) {}
            });
        }
    }

    void advanceCommit() {
        for (long n = lastIndex(); n > commitIndex; n--) {
            if (log.get((int) n).term() != term) break;          // only commit entries of the current term by counting
            int c = 1; for (String p : peers) if (match.getOrDefault(p, 0L) >= n) c++;
            if (c * 2 > peers.size() + 1) { commitIndex = n; break; }
        }
        applyCommitted();
    }

    void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            byte[] d = log.get((int) lastApplied).data();
            if (d.length > 0) apply.accept(d);
            var f = waiting.remove(lastApplied); if (f != null) f.complete(lastApplied);
        }
    }

    // ---- RPC handlers (called by the transport) ----
    public Msg handle(Msg m) {
        synchronized (lock) {
            if (m instanceof RequestVote rv) {
                if (rv.term() > term) stepDown(rv.term(), null);
                long li = lastIndex(), lt = log.get((int) li).term();
                boolean upToDate = rv.lastTerm() > lt || (rv.lastTerm() == lt && rv.lastIndex() >= li);
                boolean grant = rv.term() == term && upToDate && (votedFor == null || votedFor.equals(rv.candidate()));
                if (grant) { votedFor = rv.candidate(); resetDeadline(); }
                return new VoteResp(term, grant);
            }
            Append a = (Append) m;
            if (a.term() < term) return new AppendResp(term, false, 0);
            stepDown(a.term(), a.leader());                        // also resets the election timer
            if (a.prevIndex() > lastIndex() || log.get((int) a.prevIndex()).term() != a.prevTerm())
                return new AppendResp(term, false, Math.min(lastIndex(), Math.max(0, a.prevIndex() - 1)));
            long idx = a.prevIndex();
            for (Entry e : a.entries()) {
                idx++;
                if (idx <= lastIndex()) {
                    if (log.get((int) idx).term() == e.term()) continue;
                    while (log.size() > idx) log.remove(log.size() - 1);   // conflict: truncate
                }
                log.add(e);
            }
            if (a.leaderCommit() > commitIndex) { commitIndex = Math.min(a.leaderCommit(), a.prevIndex() + a.entries().size()); applyCommitted(); }
            return new AppendResp(term, true, a.prevIndex() + a.entries().size());
        }
    }

    // ---- introspection ----
    public String id() { return id; }
    public Role role() { synchronized (lock) { return role; } }
    public long term() { synchronized (lock) { return term; } }
    public String leader() { synchronized (lock) { return leaderId; } }
    public long commitIndex() { synchronized (lock) { return commitIndex; } }
    public List<byte[]> committedData() {
        synchronized (lock) { List<byte[]> r = new ArrayList<>(); for (int i = 1; i <= commitIndex; i++) if (log.get(i).data().length > 0) r.add(log.get(i).data()); return r; }
    }
}
