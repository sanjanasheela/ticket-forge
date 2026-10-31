package forgekv.storage;

import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.HashMap;

/** Set of keys with a TTL supporting O(1) insert, remove and random access. */
final class ExpireIndex {
    final ArrayList<Bytes> keys = new ArrayList<>();
    final HashMap<Bytes, Integer> pos = new HashMap<>();

    void insert(Bytes key) {
        if (pos.containsKey(key)) {
            return;
        }
        pos.put(key, keys.size());
        keys.add(key);
    }

    void remove(Bytes key) {
        Integer idx = pos.remove(key);
        if (idx == null) {
            return;
        }
        // swap_remove
        int last = keys.size() - 1;
        Bytes moved = keys.get(last);
        keys.set(idx, moved);
        keys.remove(last);
        if (idx < keys.size()) {
            pos.put(moved, idx);
        }
    }

    int size() {
        return keys.size();
    }

    void clear() {
        keys.clear();
        pos.clear();
    }
}
