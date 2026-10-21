package forgelog.log;

import java.nio.channels.FileChannel;

/**
 * A run of whole, encoded records inside one segment file, ready to be sent
 * to a socket with {@link FileChannel#transferTo} (sendfile: the bytes never
 * enter the JVM heap).
 *
 * @param endOffset offset of the first record after this region
 */
public record Region(FileChannel channel, long position, int length, long endOffset) {}
