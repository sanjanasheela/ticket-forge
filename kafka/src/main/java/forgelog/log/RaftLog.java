package forgelog.log;

import java.util.List;

/**
 * The log a Raft group replicates. Indexes start at 0 and are dense; index
 * {@code -1} is the empty prefix (term 0). There is no compaction: the first
 * index is always 0.
 */
public interface RaftLog {
    /** Index of the last entry, -1 if the log is empty. */
    long lastIndex();

    /** Term of the entry at {@code index}; 0 for index -1, -1 past the end. */
    long termAt(long index);

    /** First index of the run of entries that share {@code termAt(index)}. */
    long firstIndexOfTerm(long index);

    /** Append {@code r}, whose offset must be {@code lastIndex() + 1}. */
    void append(Record r);

    /** Remove every entry from {@code index} on. */
    void truncateFrom(long index);

    /**
     * Entries {@code from..toInclusive}, stopping early after about
     * {@code maxBytes} (at least one entry is returned if any exists).
     */
    List<Record> entries(long from, long toInclusive, int maxBytes);
}
