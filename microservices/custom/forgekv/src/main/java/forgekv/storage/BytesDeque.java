package forgekv.storage;

import forgekv.util.Bytes;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A ring-buffer deque of {@link Bytes} with O(1) push/pop at both ends and
 * O(1) random access, like Rust's {@code VecDeque} (Java's
 * {@code ArrayDeque} has no indexed access, which {@code LRANGE}/{@code LINDEX} need).
 */
public final class BytesDeque implements Iterable<Bytes> {
    private Bytes[] items = new Bytes[8];
    private int head;
    private int size;

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    private void grow() {
        Bytes[] next = new Bytes[items.length * 2];
        for (int i = 0; i < size; i++) {
            next[i] = items[(head + i) & (items.length - 1)];
        }
        items = next;
        head = 0;
    }

    public void addFirst(Bytes b) {
        if (size == items.length) {
            grow();
        }
        head = (head - 1) & (items.length - 1);
        items[head] = b;
        size++;
    }

    public void addLast(Bytes b) {
        if (size == items.length) {
            grow();
        }
        items[(head + size) & (items.length - 1)] = b;
        size++;
    }

    /** Remove and return the first element, or {@code null} if empty. */
    public Bytes pollFirst() {
        if (size == 0) {
            return null;
        }
        Bytes b = items[head];
        items[head] = null;
        head = (head + 1) & (items.length - 1);
        size--;
        return b;
    }

    /** Remove and return the last element, or {@code null} if empty. */
    public Bytes pollLast() {
        if (size == 0) {
            return null;
        }
        int idx = (head + size - 1) & (items.length - 1);
        Bytes b = items[idx];
        items[idx] = null;
        size--;
        return b;
    }

    public Bytes get(int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException(i);
        }
        return items[(head + i) & (items.length - 1)];
    }

    public BytesDeque copy() {
        BytesDeque d = new BytesDeque();
        for (Bytes b : this) {
            d.addLast(b);
        }
        return d;
    }

    @Override
    public Iterator<Bytes> iterator() {
        return new Iterator<>() {
            int i;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public Bytes next() {
                if (i >= size) {
                    throw new NoSuchElementException();
                }
                return get(i++);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BytesDeque d) || d.size != size) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (!get(i).equals(d.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 1;
        for (Bytes b : this) {
            h = 31 * h + b.hashCode();
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(get(i));
        }
        return sb.append(']').toString();
    }
}
