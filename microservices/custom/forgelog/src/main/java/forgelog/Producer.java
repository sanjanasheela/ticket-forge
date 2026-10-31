package forgelog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Asynchronous sender. {@link #send} returns at once; one background thread takes whatever has queued up (up to
 * 500 records), sends it as one request per topic and completes the futures. Batching happens naturally under
 * load (the more that queues while a request is in flight, the bigger the next batch) with no artificial delay.
 */
public final class Producer implements Closeable {

    private record Pending(Client.Msg msg, CompletableFuture<Long> done) {
    }

    private final Client client;
    private final BlockingQueue<Pending> queue = new LinkedBlockingQueue<>();
    private final Thread sender;
    private volatile boolean closed;

    public Producer(String bootstrap) {
        this.client = new Client(bootstrap);
        this.sender = Thread.ofPlatform().name("forgelog-producer").daemon(true).start(this::loop);
    }

    public CompletableFuture<Long> send(Client.Msg msg) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        if (closed) {
            f.completeExceptionally(new IOException("producer closed"));
        } else {
            queue.add(new Pending(msg, f));
        }
        return f;
    }

    private void loop() {
        while (!closed) {
            try {
                List<Pending> batch = new ArrayList<>();
                batch.add(queue.take());
                queue.drainTo(batch, 499);
                Map<String, List<Pending>> byTopic = new LinkedHashMap<>();
                for (Pending p : batch) {
                    byTopic.computeIfAbsent(p.msg().topic(), t -> new ArrayList<>()).add(p);
                }
                byTopic.forEach(this::sendTopic);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void sendTopic(String topic, List<Pending> pending) {
        try {
            long[] offsets = client.produce(topic, pending.stream().map(Pending::msg).toList());
            for (int i = 0; i < pending.size(); i++) {
                pending.get(i).done().complete(offsets[i]);
            }
        } catch (IOException | RuntimeException e) {
            pending.forEach(p -> p.done().completeExceptionally(e));
        }
    }

    @Override
    public void close() {
        closed = true;
        sender.interrupt();
        client.close();
        queue.forEach(p -> p.done().completeExceptionally(new IOException("producer closed")));
    }
}
