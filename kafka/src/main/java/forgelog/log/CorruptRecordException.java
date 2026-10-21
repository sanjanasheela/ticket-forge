package forgelog.log;

import java.io.IOException;

/** Bytes that should hold a record don't (bad length, CRC mismatch...). */
public final class CorruptRecordException extends IOException {
    public CorruptRecordException(String message) {
        super(message);
    }
}
