package forgeraft;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** In-process network with injectable partitions and node crashes, for deterministic tests and chaos demos. */
public final class MemNet implements Transport {
    final Map<String, Node> nodes = new ConcurrentHashMap<>();
    final Set<String> down = ConcurrentHashMap.newKeySet();
    final Set<String> cut = ConcurrentHashMap.newKeySet();      // "a|b" both directions

    public void register(String id, Node n) { nodes.put(id, n); }
    public void crash(String id) { down.add(id); }
    public boolean isDown(String id) { return down.contains(id); }
    public void recover(String id) { down.remove(id); }
    public void partition(Collection<String> a, Collection<String> b) { for (String x : a) for (String y : b) { cut.add(x + "|" + y); cut.add(y + "|" + x); } }
    public void heal() { cut.clear(); }

    @Override public Msg call(String from, String to, Msg m) throws IOException {
        if (down.contains(from) || down.contains(to) || cut.contains(from + "|" + to)) throw new IOException("unreachable");
        Node n = nodes.get(to);
        if (n == null) throw new IOException("no node");
        return n.handle(m);
    }
}
