package com.ticketforge;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import com.ticketforge.contracts.VenueLayout;

import tools.jackson.databind.json.JsonMapper;

/**
 * Stands in for catalog-service's {@code /internal/venues/{id}/layout}. Tests register layouts, count the
 * calls, and can switch it "down" to prove the listener retries instead of dropping the event.
 */
public class StubCatalog implements AutoCloseable {

    private final HttpServer server;
    private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();
    private final Map<Long, VenueLayout> layouts = new ConcurrentHashMap<>();
    private final Map<Long, AtomicInteger> calls = new ConcurrentHashMap<>();
    private final AtomicBoolean down = new AtomicBoolean();

    StubCatalog() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/internal/venues/", exchange -> {
            String[] parts = exchange.getRequestURI().getPath().split("/");   // "", internal, venues, {id}, layout
            long venueId = Long.parseLong(parts[3]);
            calls.computeIfAbsent(venueId, k -> new AtomicInteger()).incrementAndGet();
            VenueLayout layout = layouts.get(venueId);
            int status = down.get() ? 503 : layout == null ? 404 : 200;
            byte[] body = status == 200 ? json.writeValueAsString(layout).getBytes(StandardCharsets.UTF_8) : new byte[0];
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public void register(VenueLayout layout) {
        layouts.put(layout.venueId(), layout);
    }

    public int callsFor(long venueId) {
        AtomicInteger n = calls.get(venueId);
        return n == null ? 0 : n.get();
    }

    public void setDown(boolean value) {
        down.set(value);
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
