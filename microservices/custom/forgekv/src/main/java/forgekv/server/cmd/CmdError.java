package forgekv.server.cmd;

import forgekv.resp.Frame;

/** An error reply raised by a command handler (no stack trace: it's control flow). */
public final class CmdError extends Exception {
    public final transient Frame frame;

    public CmdError(Frame frame) {
        super(null, null, false, false);
        this.frame = frame;
    }
}
