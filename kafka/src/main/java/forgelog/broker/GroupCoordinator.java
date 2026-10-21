package forgelog.broker;

import forgelog.protocol.Errors;
import forgelog.protocol.Messages.HeartbeatRequest;
import forgelog.protocol.Messages.JoinGroupRequest;
import forgelog.protocol.Messages.JoinGroupResponse;
import forgelog.protocol.Messages.LeaveGroupRequest;
import forgelog.protocol.TopicPartition;
import forgelog.util.Log;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Consumer-group membership, run by the leader of the offsets group.
 *
 * <p>The protocol is Kafka's eager rebalance, with the assignment computed
 * here instead of by a group leader:
 * <ol>
 *   <li>A join, leave or expired session starts a rebalance. Heartbeats then
 *       answer {@code REBALANCE_IN_PROGRESS}, telling members to stop
 *       consuming and rejoin.
 *   <li>{@code JOIN_GROUP} is a long poll: it is answered when every member
 *       has rejoined, or when the rebalance timeout (the longest session
 *       timeout) runs out, at which point members that didn't rejoin are
 *       dropped.
 *   <li>The generation is bumped and every member gets its partitions.
 * </ol>
 *
 * <p>Offset commits carry the member's generation and are refused once it is
 * stale, so a consumer that lost its partitions in a rebalance cannot
 * overwrite the new owner's progress. Membership is soft state: if the
 * coordinator moves, members get {@code NOT_COORDINATOR} and rejoin the new
 * one; committed offsets live in the Raft log and are unaffected.
 */
final class GroupCoordinator {
    private record PendingJoin(ClientConn conn, int correlationId) {}

    private static final class Member {
        final String id;
        List<String> topics = List.of();
        int sessionTimeoutMs;
        long lastHeartbeat;
        PendingJoin join;
        List<TopicPartition> assignment = List.of();

        Member(String id) {
            this.id = id;
        }
    }

    private static final class Group {
        final String id;
        int generation;
        boolean rebalancing;
        long rebalanceDeadline;
        final Map<String, Member> members = new LinkedHashMap<>();

        Group(String id) {
            this.id = id;
        }
    }

    private final Broker broker;
    private final Map<String, Group> groups = new HashMap<>();
    private boolean active;
    private long memberSeq;

    GroupCoordinator(Broker broker) {
        this.broker = broker;
    }

    boolean active() {
        return active;
    }

    /** This broker became (or stopped being) the coordinator. */
    void setActive(boolean on) {
        if (active == on) {
            return;
        }
        active = on;
        if (!on) {
            for (Group g : groups.values()) {
                for (Member m : g.members.values()) {
                    if (m.join != null) {
                        respond(m.join, new JoinGroupResponse(Errors.NOT_COORDINATOR, -1, -1, m.id, List.of()));
                    }
                }
            }
            groups.clear();
        }
        Log.info("group coordinator " + (on ? "active" : "inactive") + " on broker " + broker.config().nodeId());
    }

    private void respond(PendingJoin j, JoinGroupResponse r) {
        broker.respond(j.conn(), j.correlationId(), r::encode);
    }

    void join(ClientConn conn, int corr, JoinGroupRequest req, long now) {
        Group g = groups.computeIfAbsent(req.group(), Group::new);
        Member m;
        if (req.memberId() == null || req.memberId().isEmpty()) {
            m = new Member("member-" + broker.config().nodeId() + "-" + (++memberSeq) + "-"
                    + Long.toHexString(System.nanoTime() & 0xFFFFFF));
            g.members.put(m.id, m);
        } else {
            m = g.members.get(req.memberId());
            if (m == null) {
                broker.respond(conn, corr, new JoinGroupResponse(Errors.UNKNOWN_MEMBER_ID, -1, -1, req.memberId(),
                        List.of())::encode);
                return;
            }
        }
        if (m.join != null) {
            respond(m.join, new JoinGroupResponse(Errors.REBALANCE_IN_PROGRESS, -1, -1, m.id, List.of()));
        }
        m.topics = List.copyOf(req.topics());
        m.sessionTimeoutMs = Math.max(100, req.sessionTimeoutMs());
        m.lastHeartbeat = now;
        m.join = new PendingJoin(conn, corr);
        if (!g.rebalancing) {
            Log.info("group " + g.id + ": " + m.id + " joined at generation " + g.generation + ", rebalancing");
            startRebalance(g, now);
        }
        maybeComplete(g, now);
    }

    Errors heartbeat(HeartbeatRequest req, long now) {
        Group g = groups.get(req.group());
        Member m = g == null ? null : g.members.get(req.memberId());
        if (m == null) {
            return Errors.UNKNOWN_MEMBER_ID;
        }
        m.lastHeartbeat = now;
        if (g.rebalancing) {
            return Errors.REBALANCE_IN_PROGRESS;
        }
        return req.generation() == g.generation ? Errors.NONE : Errors.ILLEGAL_GENERATION;
    }

    Errors leave(LeaveGroupRequest req, long now) {
        Group g = groups.get(req.group());
        Member m = g == null ? null : g.members.remove(req.memberId());
        if (m == null) {
            return Errors.UNKNOWN_MEMBER_ID;
        }
        if (m.join != null) {
            respond(m.join, new JoinGroupResponse(Errors.UNKNOWN_MEMBER_ID, -1, -1, m.id, List.of()));
        }
        membershipChanged(g, now);
        return Errors.NONE;
    }

    /** May this member commit offsets for this group? */
    Errors checkCommit(String group, String memberId, int generation) {
        Group g = groups.get(group);
        if (generation < 0 && (memberId == null || memberId.isEmpty())) {
            // Standalone consumer: fine as long as no group is using these offsets.
            return g == null || g.members.isEmpty() ? Errors.NONE : Errors.ILLEGAL_GENERATION;
        }
        Member m = g == null ? null : g.members.get(memberId);
        if (m == null) {
            return Errors.UNKNOWN_MEMBER_ID;
        }
        return generation == g.generation ? Errors.NONE : Errors.ILLEGAL_GENERATION;
    }

    /** A connection went away: joins waiting on it can no longer be answered. */
    void onConnectionClosed(ClientConn conn) {
        for (Group g : groups.values()) {
            for (Member m : g.members.values()) {
                if (m.join != null && m.join.conn() == conn) {
                    m.join = null;
                }
            }
        }
    }

    /** Expire sessions and finish rebalances whose timeout ran out. */
    void tick(long now) {
        for (Group g : groups.values()) {
            boolean removed = false;
            for (Iterator<Member> it = g.members.values().iterator(); it.hasNext();) {
                Member m = it.next();
                if (m.join == null && now - m.lastHeartbeat > m.sessionTimeoutMs) {
                    Log.info("group " + g.id + ": member " + m.id + " session expired");
                    it.remove();
                    removed = true;
                }
            }
            if (removed) {
                membershipChanged(g, now);
            }
            maybeComplete(g, now);
        }
    }

    private void membershipChanged(Group g, long now) {
        if (g.members.isEmpty()) {
            g.rebalancing = false;
            g.generation++;
            return;
        }
        if (!g.rebalancing) {
            startRebalance(g, now);
        }
        maybeComplete(g, now);
    }

    private void startRebalance(Group g, long now) {
        int timeout = 0;
        for (Member m : g.members.values()) {
            timeout = Math.max(timeout, m.sessionTimeoutMs);
        }
        g.rebalancing = true;
        g.rebalanceDeadline = now + timeout;
        Log.debug("group " + g.id + ": rebalance started (" + g.members.size() + " members)");
    }

    private void maybeComplete(Group g, long now) {
        if (!g.rebalancing) {
            return;
        }
        boolean all = true;
        for (Member m : g.members.values()) {
            all &= m.join != null;
        }
        if (!all && now < g.rebalanceDeadline) {
            return;
        }
        g.members.values().removeIf(m -> m.join == null); // didn't rejoin in time
        g.generation++;
        g.rebalancing = false;
        assign(g);
        for (Member m : g.members.values()) {
            PendingJoin j = m.join;
            m.join = null;
            m.lastHeartbeat = now;
            respond(j, new JoinGroupResponse(Errors.NONE, broker.config().nodeId(), g.generation, m.id,
                    m.assignment));
        }
        Log.info("group " + g.id + ": generation " + g.generation + " with " + g.members.size() + " member(s)");
    }

    /** Round-robin each subscribed topic's partitions over the members subscribed to it. */
    private void assign(Group g) {
        Map<String, List<TopicPartition>> out = new HashMap<>();
        TreeSet<String> topics = new TreeSet<>();
        for (Member m : g.members.values()) {
            out.put(m.id, new ArrayList<>());
            topics.addAll(m.topics);
        }
        for (String topic : topics) {
            List<String> subscribers = new ArrayList<>();
            for (Member m : g.members.values()) {
                if (m.topics.contains(topic)) {
                    subscribers.add(m.id);
                }
            }
            subscribers.sort(null);
            int partitions = broker.partitionCount(topic);
            for (int p = 0; p < partitions; p++) {
                out.get(subscribers.get(p % subscribers.size())).add(new TopicPartition(topic, p));
            }
        }
        for (Member m : g.members.values()) {
            m.assignment = List.copyOf(out.get(m.id));
        }
    }

    /** For STATUS-style introspection and tests: group → member → assignment. */
    Map<String, Map<String, List<TopicPartition>>> describe() {
        Map<String, Map<String, List<TopicPartition>>> d = new TreeMap<>();
        for (Group g : groups.values()) {
            Map<String, List<TopicPartition>> m = new TreeMap<>();
            for (Member mem : g.members.values()) {
                m.put(mem.id, mem.assignment);
            }
            d.put(g.id, m);
        }
        return d;
    }
}
