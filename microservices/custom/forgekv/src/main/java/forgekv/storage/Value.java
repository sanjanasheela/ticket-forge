package forgekv.storage;

import forgekv.util.Bytes;
import java.util.HashMap;
import java.util.HashSet;

/** A stored value. Aggregates are mutated in place by their commands. */
public sealed interface Value {
    record Str(Bytes value) implements Value {}

    record ListV(BytesDeque items) implements Value {}

    record HashV(HashMap<Bytes, Bytes> map) implements Value {}

    record SetV(HashSet<Bytes> set) implements Value {}

    static Value str(Bytes b) {
        return new Str(b);
    }

    /** Name reported by the {@code TYPE} command. */
    default String typeName() {
        return switch (this) {
            case Str s -> "string";
            case ListV l -> "list";
            case HashV h -> "hash";
            case SetV s -> "set";
        };
    }
}
