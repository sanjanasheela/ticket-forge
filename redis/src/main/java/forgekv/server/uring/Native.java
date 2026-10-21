package forgekv.server.uring;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;

/**
 * The handful of libc calls the io_uring backend needs, bound with the
 * Foreign Function &amp; Memory API (no JNI, no native library of our own).
 *
 * <p>Every call captures {@code errno}; failures surface as
 * {@link NativeException} carrying it.
 */
final class Native {
    static final long SYS_IO_URING_SETUP = 425;
    static final long SYS_IO_URING_ENTER = 426;
    static final long SYS_IO_URING_REGISTER = 427;

    static final int PROT_READ = 1;
    static final int PROT_WRITE = 2;
    static final int MAP_SHARED = 1;
    static final int MAP_POPULATE = 0x8000;
    static final int O_WRONLY = 1;
    static final int O_CREAT = 0x40;
    static final int O_CLOEXEC = 0x80000;
    static final int EFD_CLOEXEC = 0x80000;
    static final int EINTR = 4;
    static final int EAGAIN = 11;

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBC = LINKER.defaultLookup();
    private static final StructLayout CAPTURE = Linker.Option.captureStateLayout();
    private static final VarHandle ERRNO = CAPTURE.varHandle(MemoryLayout.PathElement.groupElement("errno"));
    private static final Linker.Option ERRNO_OPT = Linker.Option.captureCallState("errno");

    private static final ValueLayout.OfLong L = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt I = ValueLayout.JAVA_INT;
    private static final ValueLayout A = ValueLayout.ADDRESS;

    // syscall(long nr, ...): every variadic argument is passed as a long.
    private static final MethodHandle SYSCALL3 = variadicSyscall(2);
    private static final MethodHandle SYSCALL5 = variadicSyscall(4);
    private static final MethodHandle SYSCALL7 = variadicSyscall(6);
    private static final MethodHandle MMAP = bind("mmap", FunctionDescriptor.of(A, A, L, I, I, I, L));
    private static final MethodHandle MUNMAP = bind("munmap", FunctionDescriptor.of(I, A, L));
    private static final MethodHandle CLOSE = bind("close", FunctionDescriptor.of(I, I));
    private static final MethodHandle EVENTFD = bind("eventfd", FunctionDescriptor.of(I, I, I));
    private static final MethodHandle READ = bind("read", FunctionDescriptor.of(L, I, A, L));
    private static final MethodHandle WRITE = bind("write", FunctionDescriptor.of(L, I, A, L));
    private static final MethodHandle OPEN = LINKER.downcallHandle(
            LIBC.find("open").orElseThrow(),
            FunctionDescriptor.of(I, A, I, I),
            Linker.Option.firstVariadicArg(2),
            ERRNO_OPT);

    private Native() {}

    private static MethodHandle bind(String name, FunctionDescriptor fd) {
        return LINKER.downcallHandle(LIBC.find(name).orElseThrow(), fd, ERRNO_OPT);
    }

    private static MethodHandle variadicSyscall(int nargs) {
        ValueLayout[] args = new ValueLayout[nargs + 1];
        java.util.Arrays.fill(args, L);
        return LINKER.downcallHandle(
                LIBC.find("syscall").orElseThrow(),
                FunctionDescriptor.of(L, args),
                Linker.Option.firstVariadicArg(1),
                ERRNO_OPT);
    }

    /** An OS error from a native call. */
    static final class NativeException extends IOException {
        final int errno;

        NativeException(String call, int errno) {
            super(call + " failed: errno " + errno + " (" + describe(errno) + ")");
            this.errno = errno;
        }

        private static String describe(int errno) {
            return switch (errno) {
                case 1 -> "EPERM";
                case 4 -> "EINTR";
                case 9 -> "EBADF";
                case 11 -> "EAGAIN";
                case 12 -> "ENOMEM";
                case 14 -> "EFAULT";
                case 22 -> "EINVAL";
                case 24 -> "EMFILE";
                case 28 -> "ENOSPC";
                case 38 -> "ENOSYS";
                default -> "?";
            };
        }
    }

    private static int errno(MemorySegment state) {
        return (int) ERRNO.get(state, 0L);
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException r) {
            return r;
        }
        if (t instanceof Error e) {
            throw e;
        }
        return new IllegalStateException(t);
    }

    static long syscall(long nr, long a, long b) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            long r = (long) SYSCALL3.invokeExact(st, nr, a, b);
            if (r < 0) {
                throw new NativeException("syscall " + nr, errno(st));
            }
            return r;
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static long syscall(long nr, long a, long b, long c, long d) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            long r = (long) SYSCALL5.invokeExact(st, nr, a, b, c, d);
            if (r < 0) {
                throw new NativeException("syscall " + nr, errno(st));
            }
            return r;
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static long syscall(long nr, long a, long b, long c, long d, long e, long f) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            long r = (long) SYSCALL7.invokeExact(st, nr, a, b, c, d, e, f);
            if (r < 0) {
                throw new NativeException("syscall " + nr, errno(st));
            }
            return r;
        } catch (NativeException ex) {
            throw ex;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static MemorySegment mmap(int fd, long len, long offset) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            MemorySegment p = (MemorySegment) MMAP.invokeExact(
                    st, MemorySegment.NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, offset);
            if (p.address() == -1L) {
                throw new NativeException("mmap", errno(st));
            }
            return p.reinterpret(len);
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static void munmap(MemorySegment seg) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            int r = (int) MUNMAP.invokeExact(st, seg, seg.byteSize());
            assert r == 0;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static void close(int fd) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            int r = (int) CLOSE.invokeExact(st, fd);
            assert r == 0 || r == -1;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /** A blocking eventfd (a watcher thread blocks on it, see {@link Ring}). */
    static int eventfd() throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            int fd = (int) EVENTFD.invokeExact(st, 0, EFD_CLOEXEC);
            if (fd < 0) {
                throw new NativeException("eventfd", errno(st));
            }
            return fd;
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /** Read an eventfd counter (blocks until it is non-zero). */
    static long readEventfd(int fd) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            MemorySegment v = arena.allocate(8, 8);
            while (true) {
                long n = (long) READ.invokeExact(st, fd, v, 8L);
                if (n == 8) {
                    return v.get(ValueLayout.JAVA_LONG, 0);
                }
                int err = errno(st);
                if (n < 0 && err == EINTR) {
                    continue;
                }
                throw new NativeException("read(eventfd)", err);
            }
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /** Add {@code value} to an eventfd counter, waking a blocked reader. */
    static void signalEventfd(int fd, long value) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            MemorySegment v = arena.allocate(8, 8);
            v.set(ValueLayout.JAVA_LONG, 0, value);
            long n = (long) WRITE.invokeExact(st, fd, v, 8L);
            if (n != 8) {
                throw new NativeException("write(eventfd)", errno(st));
            }
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /** {@code open(path, O_WRONLY|O_CREAT|O_CLOEXEC, 0644)}. */
    static int openForWrite(String path) throws NativeException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment st = arena.allocate(CAPTURE);
            byte[] p = path.getBytes(StandardCharsets.UTF_8);
            MemorySegment cpath = arena.allocate(p.length + 1L);
            cpath.copyFrom(MemorySegment.ofArray(p));
            cpath.set(ValueLayout.JAVA_BYTE, p.length, (byte) 0);
            int fd = (int) OPEN.invokeExact(st, cpath, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
            if (fd < 0) {
                throw new NativeException("open " + path, errno(st));
            }
            return fd;
        } catch (NativeException e) {
            throw e;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
