package forgegate;

import com.sun.net.httpserver.*;
import forgeraft.MemNet;
import forgeraft.Node;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Phase 4 "Mission Control": a live topology page (Server-Sent Events, not WebSocket) with chaos buttons.
 * Services/cache are probed for real. The Raft panel is an in-process 5-node demo cluster (MemNet), not the broker.
 */
public final class MissionControl {
    final Gateway gw; final Map<String, String> health = new LinkedHashMap<>(); final String chaosScript;
    final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();
    final MemNet net = new MemNet(); final Map<String, Node> raft = new LinkedHashMap<>();
    final List<String> raftIds = List.of("r1", "r2", "r3", "r4", "r5");
    volatile String last = "";

    MissionControl(Gateway gw, Map<String, String> probes, String chaosScript) {
        this.gw = gw; this.health.putAll(probes); this.chaosScript = chaosScript;
        for (String id : raftIds) {
            List<String> peers = new ArrayList<>(raftIds); peers.remove(id);
            Node n = new Node(id, peers, net, d -> {}, 300, 60); raft.put(id, n); net.register(id, n);
        }
        raft.values().forEach(Node::start);
        Thread t = new Thread(() -> { for (;;) { try { Thread.sleep(500); last = snapshot(); } catch (Exception ignored) {} } });
        t.setDaemon(true); t.start();
        Thread w = new Thread(() -> { for (;;) { try { Thread.sleep(200); Node l = leader(); if (l != null) l.propose("tick".getBytes(), 150); } catch (Exception ignored) {} } });
        w.setDaemon(true); w.start();
    }

    Node leader() { for (Node n : raft.values()) if (n.role() == Node.Role.LEADER && !net.isDown(n.id())) return n; return null; }

    boolean up(String url) {
        try {
            if (url.startsWith("tcp://")) { URI u = URI.create(url); try (Socket s = new Socket()) { s.connect(new InetSocketAddress(u.getHost(), u.getPort()), 300); return true; } }
            return http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofMillis(800)).build(), HttpResponse.BodyHandlers.discarding()).statusCode() < 500;
        } catch (Exception e) { return false; }
    }

    String snapshot() {
        StringBuilder sb = new StringBuilder("{\"gate\":").append(gw.stats()).append(",\"nodes\":[");
        boolean f = true;
        for (var e : health.entrySet()) { if (!f) sb.append(','); f = false; sb.append("{\"name\":\"").append(e.getKey()).append("\",\"up\":").append(up(e.getValue())).append('}'); }
        sb.append("],\"raft\":[");
        f = true;
        for (Node n : raft.values()) {
            if (!f) sb.append(','); f = false;
            boolean down = net.isDown(n.id());
            sb.append("{\"id\":\"").append(n.id()).append("\",\"role\":\"").append(down ? "DOWN" : n.role()).append("\",\"term\":").append(n.term()).append(",\"commit\":").append(n.commitIndex()).append('}');
        }
        return sb.append("]}").toString();
    }

    void handle(HttpExchange ex) throws IOException {
        String p = ex.getRequestURI().getPath();
        try {
            if (p.equals("/mc")) { byte[] b = page().getBytes(StandardCharsets.UTF_8); Gateway.send(ex, 200, b, "text/html; charset=utf-8"); return; }
            if (p.equals("/mc/events")) {
                ex.getResponseHeaders().add("Content-Type", "text/event-stream"); ex.getResponseHeaders().add("Cache-Control", "no-cache");
                ex.sendResponseHeaders(200, 0);
                try (OutputStream o = ex.getResponseBody()) { for (;;) { o.write(("data: " + last + "\n\n").getBytes()); o.flush(); Thread.sleep(500); } }
            }
            if (p.startsWith("/mc/chaos/") && ex.getRequestMethod().equals("POST")) {
                String a = p.substring("/mc/chaos/".length());
                String r = chaos(a);
                Gateway.send(ex, 200, ("{\"action\":\"" + a + "\",\"result\":\"" + r + "\"}").getBytes(), "application/json"); return;
            }
            Gateway.send(ex, 404, "{}".getBytes(), "application/json");
        } catch (InterruptedException | IOException e) { try { ex.close(); } catch (Exception ignore) {} }
    }

    String chaos(String a) {
        switch (a) {
            case "kill-raft-leader": { Node l = leader(); if (l == null) return "no leader"; net.crash(l.id()); return "crashed " + l.id(); }
            case "partition-raft-leader": { Node l = leader(); if (l == null) return "no leader"; List<String> o = new ArrayList<>(raftIds); o.remove(l.id()); net.partition(List.of(l.id()), o); return "isolated " + l.id(); }
            case "heal-raft": net.heal(); for (String id : raftIds) net.recover(id); return "healed";
        }
        if (a.matches("(kill|start)-(cache|auth|catalog|reservation|booking)")) {
            try {
                Process pr = new ProcessBuilder("bash", chaosScript, a).redirectErrorStream(true).start();
                String out = new String(pr.getInputStream().readAllBytes()).trim().replace("\"", "'").replace("\n", " ");
                pr.waitFor(10, TimeUnit.SECONDS); return out.isEmpty() ? "ok" : out;
            } catch (Exception e) { return "failed: " + e.getMessage(); }
        }
        return "unknown action";
    }

    static String page() {
        return """
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TicketForge Mission Control</title>
<style>:root{--bg:#0e1116;--fg:#e6edf3;--card:#161b22;--ok:#2ea043;--bad:#f85149;--gold:#d4a72c;--blue:#388bfd;--mut:#8b949e}
*{box-sizing:border-box}body{margin:0;padding:16px;background:var(--bg);color:var(--fg);font:14px system-ui,sans-serif}
h1{font-size:18px;margin:0 0 12px}h2{font-size:13px;color:var(--mut);margin:0 0 8px;text-transform:uppercase;letter-spacing:.05em}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}.card{background:var(--card);border-radius:8px;padding:12px}
.node{display:inline-block;margin:4px;padding:8px 12px;border-radius:6px;border:2px solid var(--ok);min-width:96px;text-align:center}
.node.bad{border-color:var(--bad);color:var(--bad)}.node.LEADER{border-color:var(--gold);color:var(--gold)}.node.FOLLOWER{border-color:var(--blue)}.node.CANDIDATE{border-color:#a371f7}.node.DOWN{border-color:var(--bad);color:var(--bad);opacity:.6}
.node small{display:block;color:var(--mut)}.pulse{animation:p 1s infinite}@keyframes p{50%{box-shadow:0 0 10px currentColor}}
button{background:#21262d;color:var(--fg);border:1px solid #30363d;border-radius:6px;padding:8px 10px;margin:3px;cursor:pointer;min-height:36px}button:hover{border-color:var(--bad)}
.kv{display:flex;justify-content:space-between;padding:2px 0}#log{font:12px monospace;color:var(--mut);white-space:pre-wrap;max-height:120px;overflow:auto}</style></head><body>
<h1>TicketForge Mission Control</h1><div class="grid">
<div class="card"><h2>Gateway</h2><div id="gate"></div></div>
<div class="card"><h2>Live services (probed)</h2><div id="nodes"></div></div>
<div class="card"><h2>Raft demo cluster (in-process, 5 nodes)</h2><div id="raft"></div></div>
<div class="card"><h2>Chaos</h2>
<button onclick="c('kill-raft-leader')">Kill Raft leader</button><button onclick="c('partition-raft-leader')">Partition Raft leader</button><button onclick="c('heal-raft')">Heal Raft</button><br>
<button onclick="c('kill-cache')">Kill cache</button><button onclick="c('start-cache')">Start cache</button>
<button onclick="c('kill-catalog')">Kill catalog</button><button onclick="c('start-catalog')">Start catalog</button>
<div id="log"></div></div></div>
<script>
const $=id=>document.getElementById(id);
const es=new EventSource('/mc/events');
es.onmessage=e=>{if(!e.data)return;const d=JSON.parse(e.data);
$('gate').innerHTML=Object.entries(d.gate).map(([k,v])=>`<div class=kv><span>${k}</span><b>${v}</b></div>`).join('');
$('nodes').innerHTML=d.nodes.map(n=>`<span class="node ${n.up?'pulse':'bad'}">${n.name}<small>${n.up?'UP':'DOWN'}</small></span>`).join('');
$('raft').innerHTML=d.raft.map(n=>`<span class="node ${n.role}">${n.id}<small>${n.role} t${n.term} c${n.commit}</small></span>`).join('');};
async function c(a){const r=await fetch('/mc/chaos/'+a,{method:'POST'});const j=await r.json();$('log').textContent=new Date().toLocaleTimeString()+' '+j.action+': '+j.result+'\\n'+$('log').textContent}
</script></body></html>""";
    }
}
