package forgelog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Consumer-group membership for one (group, topic). Members join and heartbeat; a member that stays silent for
 * {@code sessionMs} is dropped. Any change in membership bumps the generation and re-deals the partitions
 * round-robin over the sorted member ids. A consumer that sees a different generation in a heartbeat reply
 * re-joins and picks up its new share. (Same shape as Kafka's group protocol, without the multi-phase sync.)
 */
final class Coordinator {

    private final int partitions;
    private final long sessionMs;
    private final Map<String, Long> lastSeen = new TreeMap<>();
    private final Map<String, List<Integer>> assignment = new HashMap<>();
    private int generation;

    Coordinator(int partitions, long sessionMs) {
        this.partitions = partitions;
        this.sessionMs = sessionMs;
    }

    synchronized int join(String member, long now) {
        expire(now);
        if (lastSeen.put(member, now) == null) {
            rebalance();
        }
        return generation;
    }

    /** Returns the current generation, or -1 if this member is unknown (it must join again). */
    synchronized int heartbeat(String member, long now) {
        if (!lastSeen.containsKey(member)) {
            return -1;
        }
        lastSeen.put(member, now);
        expire(now);
        return generation;
    }

    synchronized void leave(String member) {
        if (lastSeen.remove(member) != null) {
            rebalance();
        }
    }

    synchronized List<Integer> assigned(String member) {
        return assignment.getOrDefault(member, List.of());
    }

    private void expire(long now) {
        if (lastSeen.values().removeIf(t -> now - t > sessionMs)) {
            rebalance();
        }
    }

    private void rebalance() {
        generation++;
        assignment.clear();
        List<String> members = new ArrayList<>(lastSeen.keySet());
        for (int p = 0; p < partitions && !members.isEmpty(); p++) {
            assignment.computeIfAbsent(members.get(p % members.size()), m -> new ArrayList<>()).add(p);
        }
    }
}
