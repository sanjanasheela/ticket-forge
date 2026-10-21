package forgelog.broker;

import forgelog.raft.RaftNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * A Raft group's term and vote in {@code raft.state}, replaced atomically
 * (temp file + fsync + rename + directory fsync). Changes are group-committed
 * with the logs, so a broker running many elections at once (all the
 * partitions a dead broker led) doesn't stall its event loop on one fsync
 * per election.
 *
 * <p>Also keeps {@code commit.hint}: the last commit index seen, written
 * without fsync. A stale (lower) hint is always safe; it only lets a
 * restarted broker serve and replay its committed data before it hears from
 * the leader.
 */
final class FileHardState implements RaftNode.HardState {
    private final Path dir;
    private long term;
    private int votedFor = -1;
    private boolean dirty;

    private FileHardState(Path dir) {
        this.dir = dir;
    }

    static FileHardState open(Path dir) throws IOException {
        FileHardState s = new FileHardState(dir);
        try {
            String[] parts = Files.readString(dir.resolve("raft.state")).trim().split("\\s+");
            s.term = Long.parseLong(parts[0]);
            s.votedFor = Integer.parseInt(parts[1]);
        } catch (NoSuchFileException e) {
            // fresh group
        } catch (RuntimeException e) {
            throw new IOException("corrupt " + dir.resolve("raft.state"), e);
        }
        return s;
    }

    @Override
    public long term() {
        return term;
    }

    @Override
    public int votedFor() {
        return votedFor;
    }

    /**
     * Record a new term/vote. It reaches the disk at the broker's next group
     * commit ({@link #flush}); the broker holds back every message that
     * reveals it (vote requests and grants) until then.
     */
    @Override
    public void save(long newTerm, int newVote) {
        term = newTerm;
        votedFor = newVote;
        dirty = true;
    }

    boolean isDirty() {
        return dirty;
    }

    /** Persist the latest term/vote atomically and durably. Safe to call off the event loop while it waits. */
    void flush() throws IOException {
        long t = term;
        int v = votedFor;
        Path tmp = dir.resolve("raft.state.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(ByteBuffer.wrap((t + " " + v + "\n").getBytes(StandardCharsets.US_ASCII)));
            ch.force(true);
        }
        Files.move(tmp, dir.resolve("raft.state"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException e) {
            // not supported everywhere; best effort
        }
    }

    void markFlushed() {
        dirty = false;
    }

    long readCommitHint() {
        try {
            return Long.parseLong(Files.readString(dir.resolve("commit.hint")).trim());
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
    }

    void writeCommitHint(long commitIndex) {
        Path tmp = dir.resolve("commit.hint.tmp");
        try {
            Files.writeString(tmp, Long.toString(commitIndex));
            Files.move(tmp, dir.resolve("commit.hint"), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // a hint; losing it only slows down the next restart
        }
    }
}
