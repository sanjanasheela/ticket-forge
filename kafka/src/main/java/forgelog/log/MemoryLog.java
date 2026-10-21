package forgelog.log;

import java.util.ArrayList;
import java.util.List;

/** An in-memory {@link RaftLog} (Raft simulation tests). */
public final class MemoryLog implements RaftLog {
    private final List<Record> entries = new ArrayList<>();

    @Override
    public long lastIndex() {
        return entries.size() - 1;
    }

    @Override
    public long termAt(long index) {
        if (index == -1) {
            return 0;
        }
        if (index < 0 || index > lastIndex()) {
            return -1;
        }
        return entries.get((int) index).term;
    }

    @Override
    public long firstIndexOfTerm(long index) {
        long t = termAt(index);
        long i = index;
        while (i > 0 && termAt(i - 1) == t) {
            i--;
        }
        return i;
    }

    @Override
    public void append(Record r) {
        if (r.offset != entries.size()) {
            throw new IllegalArgumentException("append at " + r.offset + ", expected " + entries.size());
        }
        entries.add(r);
    }

    @Override
    public void truncateFrom(long index) {
        while (entries.size() > Math.max(0, index)) {
            entries.removeLast();
        }
    }

    @Override
    public List<Record> entries(long from, long toInclusive, int maxBytes) {
        List<Record> out = new ArrayList<>();
        int bytes = 0;
        for (long i = Math.max(0, from); i <= Math.min(toInclusive, lastIndex()); i++) {
            Record r = entries.get((int) i);
            if (!out.isEmpty() && bytes + r.encodedSize() > maxBytes) {
                break;
            }
            bytes += r.encodedSize();
            out.add(r);
        }
        return out;
    }

    public Record get(long index) {
        return entries.get((int) index);
    }
}
