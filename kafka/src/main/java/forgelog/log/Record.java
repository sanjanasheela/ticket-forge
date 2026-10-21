package forgelog.log;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

/**
 * One log entry. The same bytes are used on disk, in Raft replication and in
 * fetch responses (which are sent straight from the segment file).
 *
 * <pre>
 * int32  length      bytes after this field
 * int32  crc32c      of every byte after this field
 * int64  offset      == Raft index
 * int64  term        Raft term the entry was created in
 * int8   type        DATA or CONTROL (a leader's no-op)
 * int64  timestamp   producer wall clock, ms
 * int64  producerId  idempotent producer, -1 if none
 * int32  sequence    per (producer, partition), -1 if none
 * int32  keyLength   -1 = null
 * bytes  key
 * int32  valueLength -1 = null
 * bytes  value
 * </pre>
 */
public final class Record {
    public static final byte DATA = 0;
    /** Written by a new Raft leader to commit its term; never returned to consumers. */
    public static final byte CONTROL = 1;

    /** Fixed part of an encoded record (everything but key and value bytes). */
    public static final int OVERHEAD = 4 + 4 + 8 + 8 + 1 + 8 + 8 + 4 + 4 + 4;
    /** Largest accepted encoded record. */
    public static final int MAX_SIZE = 16 * 1024 * 1024;

    public final long offset;
    public final long term;
    public final byte type;
    public final long timestamp;
    public final long producerId;
    public final int sequence;
    public final byte[] key;
    public final byte[] value;

    public Record(long offset, long term, byte type, long timestamp, long producerId, int sequence, byte[] key,
            byte[] value) {
        this.offset = offset;
        this.term = term;
        this.type = type;
        this.timestamp = timestamp;
        this.producerId = producerId;
        this.sequence = sequence;
        this.key = key;
        this.value = value;
    }

    /** A record whose offset and term are assigned when it is appended. */
    public static Record data(long timestamp, long producerId, int sequence, byte[] key, byte[] value) {
        return new Record(-1, -1, DATA, timestamp, producerId, sequence, key, value);
    }

    public static Record data(byte[] value) {
        return data(System.currentTimeMillis(), -1, -1, null, value);
    }

    public static Record control() {
        return new Record(-1, -1, CONTROL, System.currentTimeMillis(), -1, -1, null, null);
    }

    public Record withPosition(long newOffset, long newTerm) {
        return new Record(newOffset, newTerm, type, timestamp, producerId, sequence, key, value);
    }

    public boolean isData() {
        return type == DATA;
    }

    public int encodedSize() {
        return OVERHEAD + (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
    }

    /** Append the encoded record to {@code out} (which must have room). */
    public void encode(ByteBuffer out) {
        int start = out.position();
        out.putInt(encodedSize() - 4);
        out.putInt(0); // crc placeholder
        out.putLong(offset);
        out.putLong(term);
        out.put(type);
        out.putLong(timestamp);
        out.putLong(producerId);
        out.putInt(sequence);
        putBytes(out, key);
        putBytes(out, value);
        int end = out.position();
        CRC32C crc = new CRC32C();
        ByteBuffer body = out.duplicate();
        body.position(start + 8).limit(end);
        crc.update(body);
        out.putInt(start + 4, (int) crc.getValue());
    }

    public ByteBuffer toBuffer() {
        ByteBuffer b = ByteBuffer.allocate(encodedSize());
        encode(b);
        return b.flip();
    }

    private static void putBytes(ByteBuffer out, byte[] b) {
        if (b == null) {
            out.putInt(-1);
        } else {
            out.putInt(b.length);
            out.put(b);
        }
    }

    /**
     * Decode one record at {@code in}'s position (advancing past it), or
     * return {@code null} if {@code in} holds only part of a record.
     *
     * @throws CorruptRecordException if the bytes are not a valid record
     */
    public static Record decode(ByteBuffer in) throws CorruptRecordException {
        if (in.remaining() < 4) {
            return null;
        }
        int start = in.position();
        int len = in.getInt(start);
        if (len < OVERHEAD - 4 || len > MAX_SIZE) {
            throw new CorruptRecordException("invalid record length " + len + " at " + start);
        }
        if (in.remaining() < 4 + len) {
            return null;
        }
        int stored = in.getInt(start + 4);
        CRC32C crc = new CRC32C();
        ByteBuffer body = in.duplicate();
        body.position(start + 8).limit(start + 4 + len);
        crc.update(body);
        if ((int) crc.getValue() != stored) {
            throw new CorruptRecordException("CRC mismatch at " + start);
        }
        in.position(start + 8);
        long offset = in.getLong();
        long term = in.getLong();
        byte type = in.get();
        long ts = in.getLong();
        long pid = in.getLong();
        int seq = in.getInt();
        int end = start + 4 + len;
        byte[] key = getBytes(in, end);
        byte[] value = getBytes(in, end);
        if (in.position() != end) {
            throw new CorruptRecordException("record length mismatch at " + start);
        }
        return new Record(offset, term, type, ts, pid, seq, key, value);
    }

    private static byte[] getBytes(ByteBuffer in, int end) throws CorruptRecordException {
        if (in.position() + 4 > end) {
            throw new CorruptRecordException("truncated record field");
        }
        int n = in.getInt();
        if (n == -1) {
            return null;
        }
        if (n < 0 || in.position() + n > end) {
            throw new CorruptRecordException("invalid field length " + n);
        }
        byte[] b = new byte[n];
        in.get(b);
        return b;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Record r && r.offset == offset && r.term == term && r.type == type
                && r.timestamp == timestamp && r.producerId == producerId && r.sequence == sequence
                && Arrays.equals(r.key, key) && Arrays.equals(r.value, value);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset) * 31 + Long.hashCode(term);
    }

    @Override
    public String toString() {
        return "Record{offset=" + offset + ", term=" + term + ", type=" + type + ", pid=" + producerId + ", seq="
                + sequence + ", value=" + (value == null ? "null" : value.length + "B") + "}";
    }
}
