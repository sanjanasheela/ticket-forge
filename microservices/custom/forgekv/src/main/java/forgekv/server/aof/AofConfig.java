package forgekv.server.aof;

import java.nio.file.Path;

/**
 * @param path base name of the AOF family (see {@link AofLog} for the layout)
 */
public record AofConfig(Path path, FsyncPolicy fsync, IoBackend backend) {
    public AofConfig(Path path, FsyncPolicy fsync) {
        this(path, fsync, IoBackend.AUTO);
    }
}
