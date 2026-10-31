package forgekv.resp;

/** A protocol violation. The connection should be closed after reporting it. */
public final class ProtocolException extends Exception {
    public ProtocolException(String message) {
        super(message, null, false, false);
    }
}
