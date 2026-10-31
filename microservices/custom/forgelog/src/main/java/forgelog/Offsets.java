package forgelog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Committed consumer offsets: a map in memory, an append-only text log on disk (last line for a key wins).
 * Commits are not fsynced: losing one only makes a consumer re-read a few records, which consumers tolerate.
 */
final class Offsets {

    private final Map<String, Long> committed = new HashMap<>();
    private final Path file;

    Offsets(Path dir) throws IOException {
        Files.createDirectories(dir);
        file = dir.resolve("__offsets");
        if (Files.exists(file)) {
            for (String line : List.copyOf(Files.readAllLines(file))) {
                String[] f = line.split("\t");
                if (f.length == 4) {
                    committed.put(key(f[0], f[1], Integer.parseInt(f[2])), Long.parseLong(f[3]));
                }
            }
        }
    }

    private static String key(String group, String topic, int partition) {
        return group + "\t" + topic + "\t" + partition;
    }

    synchronized long get(String group, String topic, int partition) {
        return committed.getOrDefault(key(group, topic, partition), -1L);
    }

    synchronized void commit(String group, String topic, int partition, long offset) throws IOException {
        String k = key(group, topic, partition);
        committed.put(k, offset);
        Files.writeString(file, k + "\t" + offset + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
