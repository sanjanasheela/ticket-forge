package forgelog;

/** One record as stored and as delivered. {@code offset} is its position in the partition (0, 1, 2, ...). */
public record Rec(long offset, long timestamp, byte[] key, String type, String eventId, byte[] value) {

    public String valueString() {
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }
}
