package forgegate;

import static org.junit.jupiter.api.Assertions.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GateTests {
    @Test void singleflightCollapsesConcurrentCalls() throws Exception {
        Singleflight<Integer> sf = new Singleflight<>();
        AtomicInteger loads = new AtomicInteger();
        ExecutorService ex = Executors.newFixedThreadPool(50);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Integer>> fs = new ArrayList<>();
        for (int i = 0; i < 50; i++) fs.add(ex.submit(() -> { go.await(); return sf.call("k", () -> { loads.incrementAndGet(); sleep(300); return 7; }); }));
        go.countDown();
        for (var f : fs) assertEquals(7, f.get());
        assertTrue(loads.get() <= 3, "loads=" + loads.get());   // one leader; a late arrival can start a second flight
        ex.shutdown();
    }

    @Test void limiterShrinksWhenLatencyRisesAndRecovers() {
        GradientLimiter l = new GradientLimiter(100, 5, 1000, 1.5, 0.2, 10);
        for (int i = 0; i < 200; i++) { l.tryAcquire(); l.release(1_000_000, false); }   // 1 ms baseline
        int healthy = l.limit();
        for (int i = 0; i < 400; i++) { l.tryAcquire(); l.release(50_000_000, false); }  // 50 ms: backend queueing
        int degraded = l.limit();
        assertTrue(degraded < healthy, healthy + " -> " + degraded);
        assertTrue(degraded >= 5);
    }

    @Test void limiterRefusesBeyondLimit() {
        GradientLimiter l = new GradientLimiter(3, 3, 3, 1.5, 0.2, 10);
        assertTrue(l.tryAcquire()); assertTrue(l.tryAcquire()); assertTrue(l.tryAcquire());
        assertFalse(l.tryAcquire());
    }

    @Test void breakerOpensAndHalfOpens() throws Exception {
        CircuitBreaker cb = new CircuitBreaker(3, 100);
        for (int i = 0; i < 3; i++) cb.failure();
        assertFalse(cb.allow());
        Thread.sleep(150);
        assertTrue(cb.allow());          // probe
        assertFalse(cb.allow());         // only one probe
        cb.success();
        assertTrue(cb.allow());
    }

    @Test void endToEndCoalescesAndSheds() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer up = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        up.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        up.createContext("/", e -> { hits.incrementAndGet(); sleep(300); byte[] b = "{\"ok\":1}".getBytes(); e.sendResponseHeaders(200, b.length); e.getResponseBody().write(b); e.close(); });
        up.start();
        String u = "http://127.0.0.1:" + up.getAddress().getPort();
        Gateway g = new Gateway(new GradientLimiter(1000, 5, 1000, 1.5, .2, 20), u, u, u, u);
        HttpServer gs = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        gs.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        gs.createContext("/", g::handle); gs.start();
        String base = "http://127.0.0.1:" + gs.getAddress().getPort();
        HttpClient c = HttpClient.newHttpClient();
        List<CompletableFuture<HttpResponse<String>>> rs = new ArrayList<>();
        for (int i = 0; i < 100; i++) rs.add(c.sendAsync(HttpRequest.newBuilder(URI.create(base + "/shows/1")).build(), HttpResponse.BodyHandlers.ofString()));
        for (var r : rs) assertEquals(200, r.get().statusCode());
        assertTrue(hits.get() < 20, "upstream hits=" + hits.get());
        // non-coalesced route with a tiny limit sheds
        Gateway g2 = new Gateway(new GradientLimiter(2, 2, 2, 1.5, .2, 20), u, u, u, u);
        HttpServer gs2 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        gs2.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        gs2.createContext("/", g2::handle); gs2.start();
        String base2 = "http://127.0.0.1:" + gs2.getAddress().getPort();
        List<CompletableFuture<HttpResponse<String>>> ps = new ArrayList<>();
        for (int i = 0; i < 20; i++) ps.add(c.sendAsync(HttpRequest.newBuilder(URI.create(base2 + "/auth/login")).POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString()));
        int s429 = 0; for (var r : ps) if (r.get().statusCode() == 429) s429++;
        assertTrue(s429 > 0, "expected sheds");
        gs.stop(0); gs2.stop(0); up.stop(0);
    }

    static void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException e) { throw new RuntimeException(e); } }
}
