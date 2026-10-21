package forgelog.protocol;

public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {
    @Override
    public int compareTo(TopicPartition o) {
        int c = topic.compareTo(o.topic);
        return c != 0 ? c : Integer.compare(partition, o.partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
