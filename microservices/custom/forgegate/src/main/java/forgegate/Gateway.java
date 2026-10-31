package forgegate;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/** Phase 3A gateway. Routes by path, sheds load with an adaptive limit, coalesces identical GETs, breaks circuits. */
public final class Gateway {
    record Route(Pattern path, String method, String upstream, boolean coalesce, boolean stream) {}
    record Resp(int status, byte[] body, String contentType) {}

    final List<Route> routes = new ArrayList<>();
    final Map<String, CircuitBreaker> breakers = new HashMap<>();
    final GradientLimiter limiter;
    final Singleflight<Resp> flight = new Singleflight<>();
    final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .executor(Executors.newVirtualThreadPerTaskExecutor()).connectTimeout(Duration.ofSeconds(2)).build();
    final java.util.concurrent.atomic.LongAdder shed = new java.util.concurrent.atomic.LongAdder(),
            broken = new java.util.concurrent.atomic.LongAdder(), proxied = new java.util.concurrent.atomic.LongAdder();

    public Gateway(GradientLimiter limiter, String auth, String catalog, String reservation, String booking) {
        this.limiter = limiter;
        r("^/auth/.*", null, auth, false, false);
        r("^/admin/seed/loadtest-users$", null, auth, false, false);
        r("^/shows/[^/]+/seats/stream$", "GET", reservation, false, true);
        r("^/shows/[^/]+/queue/stream$", "GET", reservation, false, true);
        r("^/shows/[^/]+/(holds|seats|queue)(/.*)?$", null, reservation, false, false);
        r("^/holds/[^/]+/checkout$", null, booking, false, false);
        r("^/holds/.*", null, reservation, false, false);
        r("^/me/.*", null, booking, false, false);
        r("^/admin/(payment-gateway)/.*", null, booking, false, false);
        r("^/admin/inventory/.*", null, reservation, false, false);
        r("^/(events|shows)(/.*)?$", "GET", catalog, true, false);
        r("^/admin/.*", null, catalog, false, false);
        for (String u : new String[]{auth, catalog, reservation, booking}) breakers.put(u, new CircuitBreaker(20, 3000));
    }

    private void r(String re, String m, String up, boolean co, boolean st) { routes.add(new Route(Pattern.compile(re), m, up, co, st)); }

    Route match(String method, String path) {
        for (Route r : routes) if (r.path.matcher(path).matches() && (r.method == null || r.method.equals(method))) return r;
        return null;
    }

    public void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath(), method = ex.getRequestMethod();
        if (path.equals("/gate/stats")) { send(ex, 200, stats().getBytes(), "application/json"); return; }
        if (path.equals("/gate/health")) { send(ex, 200, "ok".getBytes(), "text/plain"); return; }
        Route route = match(method, path);
        if (route == null) { send(ex, 404, "{\"error\":\"no route\"}".getBytes(), "application/json"); return; }
        CircuitBreaker cb = breakers.get(route.upstream);
        if (!cb.allow()) { broken.increment(); send(ex, 503, "{\"error\":\"circuit open\"}".getBytes(), "application/json"); return; }

        if (route.stream) { stream(ex, route, cb); return; }

        if (!limiter.tryAcquire()) {
            shed.increment();
            ex.getResponseHeaders().add("Retry-After", "1");
            send(ex, 429, "{\"error\":\"overloaded\"}".getBytes(), "application/json");
            return;
        }
        long t0 = System.nanoTime(); boolean dropped = false;
        try {
            byte[] reqBody = ex.getRequestBody().readAllBytes();
            String uri = route.upstream + ex.getRequestURI().toString();
            Resp resp;
            if (route.coalesce) {
                String key = uri + "|" + Objects.toString(ex.getRequestHeaders().getFirst("Authorization"), "");
                resp = flight.call(key, () -> forward(method, uri, ex.getRequestHeaders(), reqBody));
            } else resp = forward(method, uri, ex.getRequestHeaders(), reqBody);
            dropped = resp.status >= 500;
            if (dropped) cb.failure(); else cb.success();
            proxied.increment();
            send(ex, resp.status, resp.body, resp.contentType);
        } catch (Exception e) {
            dropped = true; cb.failure();
            send(ex, 502, "{\"error\":\"bad gateway\"}".getBytes(), "application/json");
        } finally {
            limiter.release(System.nanoTime() - t0, dropped);
        }
    }

    Resp forward(String method, String uri, Headers h, byte[] body) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofSeconds(30))
                    .method(method, body.length == 0 && !method.equals("POST") && !method.equals("PUT")
                            ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
            copyHeaders(h, b);
            HttpResponse<byte[]> r = client.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
            return new Resp(r.statusCode(), r.body(), r.headers().firstValue("content-type").orElse("application/json"));
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e);
        } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static final Set<String> SKIP = Set.of("host", "content-length", "connection", "transfer-encoding", "upgrade", "expect");
    static void copyHeaders(Headers h, HttpRequest.Builder b) {
        for (var e : h.entrySet()) if (!SKIP.contains(e.getKey().toLowerCase())) for (String v : e.getValue()) b.header(e.getKey(), v);
    }

    void stream(HttpExchange ex, Route route, CircuitBreaker cb) throws IOException {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(route.upstream + ex.getRequestURI())).GET();
            copyHeaders(ex.getRequestHeaders(), b);
            HttpResponse<InputStream> r = client.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
            ex.getResponseHeaders().add("Content-Type", r.headers().firstValue("content-type").orElse("text/event-stream"));
            ex.sendResponseHeaders(r.statusCode(), 0);
            try (InputStream in = r.body(); OutputStream out = ex.getResponseBody()) {
                byte[] buf = new byte[4096]; int n;
                while ((n = in.read(buf)) > 0) { out.write(buf, 0, n); out.flush(); }
            }
        } catch (Exception e) { cb.failure(); try { ex.close(); } catch (Exception ignore) {} }
    }

    static void send(HttpExchange ex, int status, byte[] body, String ct) throws IOException {
        ex.getResponseHeaders().set("Content-Type", ct);
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) try (OutputStream o = ex.getResponseBody()) { o.write(body); }
        ex.close();
    }

    public String stats() {
        return "{\"limit\":" + limiter.limit() + ",\"inflight\":" + limiter.inflight() + ",\"shed\":" + shed.sum()
                + ",\"circuit_rejected\":" + broken.sum() + ",\"proxied\":" + proxied.sum()
                + ",\"singleflight_leaders\":" + flight.leaders.sum() + ",\"singleflight_followers\":" + flight.followers.sum() + "}";
    }

    public static void main(String[] a) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        Gateway g = new Gateway(new GradientLimiter(), env("AUTH", 8081), env("CATALOG", 8082), env("RESERVATION", 8083), env("BOOKING", 8084));
        HttpServer s = HttpServer.create(new java.net.InetSocketAddress(port), 4096);
        s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        s.createContext("/", g::handle);
        Map<String, String> probes = new LinkedHashMap<>();
        probes.put("auth", env("AUTH", 8081) + "/actuator/health"); probes.put("catalog", env("CATALOG", 8082) + "/actuator/health");
        probes.put("reservation", env("RESERVATION", 8083) + "/actuator/health"); probes.put("booking", env("BOOKING", 8084) + "/actuator/health");
        probes.put("cache", "tcp://127.0.0.1:" + System.getenv().getOrDefault("REDIS_PORT", "6380"));
        MissionControl mc = new MissionControl(g, probes, System.getenv().getOrDefault("CHAOS_SCRIPT", "scripts/mc-chaos.sh"));
        s.createContext("/mc", mc::handle);
        s.start();
        System.out.println("forgegate on :" + port);
    }
    static String env(String k, int def) { return System.getenv().getOrDefault(k + "_URL", "http://localhost:" + def); }
}
