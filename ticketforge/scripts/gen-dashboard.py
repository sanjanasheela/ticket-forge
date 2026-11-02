"""Generates docker/grafana/dashboards/ticketforge.json. Run: python3 scripts/gen-dashboard.py"""
import json

panels, y = [], 0
pid = 0


def row(title):
    global y, pid
    pid += 1
    panels.append({"type": "row", "title": title, "id": pid, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}})
    y += 1


def ts(title, exprs, unit="short", x=0, w=8, h=8, stack=False):
    global pid
    pid += 1
    panels.append({
        "type": "timeseries", "title": title, "id": pid,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "prometheus", "uid": "${ds}"},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "stacking": {"mode": "normal" if stack else "none"}, "fillOpacity": 15 if stack else 5}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [{"expr": e, "legendFormat": l, "refId": chr(65 + i)} for i, (e, l) in enumerate(exprs)],
    })


def stat(title, expr, x, w=4, h=4, unit="short", red_above=None):
    global pid
    pid += 1
    thresholds = {"mode": "absolute", "steps": [{"color": "green", "value": None}]}
    if red_above is not None:
        thresholds["steps"].append({"color": "red", "value": red_above})
    panels.append({
        "type": "stat", "title": title, "id": pid,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "prometheus", "uid": "${ds}"},
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": thresholds}, "overrides": []},
        "options": {"colorMode": "background", "reduceOptions": {"calcs": ["lastNotNull"]}},
        "targets": [{"expr": expr, "refId": "A"}],
    })


row("Correctness")
stat("Invariant violations (must be 0)", "sum(ticketforge_invariant_violations) or vector(0)", 0, w=6, red_above=1)
stat("Holds/s (success)", 'sum(rate(ticketforge_hold_claim_seconds_count{outcome="success"}[30s]))', 6, unit="reqps")
stat("Hold p99", 'histogram_quantile(0.99, sum(rate(ticketforge_hold_claim_seconds_bucket[30s])) by (le))', 10, unit="s")
stat("DB pool waiting", "sum(hikaricp_connections_pending)", 14, red_above=1)
stat("Late-payment refunds", "sum(ticketforge_late_payment_refunds_total) or vector(0)", 18, w=6)
y += 4

row("Booking hot path")
ts("Hold attempts/s by outcome",
   [('sum(rate(ticketforge_hold_claim_seconds_count[30s])) by (outcome)', "{{outcome}}")], "reqps", 0, stack=True)
ts("Hold claim latency by strategy",
   [('histogram_quantile(0.5, sum(rate(ticketforge_hold_claim_seconds_bucket[30s])) by (le, strategy))', "p50 {{strategy}}"),
    ('histogram_quantile(0.99, sum(rate(ticketforge_hold_claim_seconds_bucket[30s])) by (le, strategy))', "p99 {{strategy}}")],
   "s", 8)
ts("Retries & lock failures/s",
   [('sum(rate(ticketforge_hold_retries_total[30s])) by (reason)', "{{reason}}"),
    ('sum(rate(ticketforge_gap_rule_races_total[30s]))', "gap-rule races")], "short", 16)
y += 8
ts("Checkout outcomes/s", [('sum(rate(ticketforge_checkout_seconds_count[30s])) by (outcome)', "{{outcome}}")],
   "reqps", 0, stack=True)
ts("Holds expired/s", [('sum(rate(ticketforge_holds_expired_total[30s]))', "expired")], "short", 8)
ts("Idempotency", [('sum(rate(ticketforge_idempotency_total[30s])) by (outcome)', "{{outcome}}")], "short", 16)
y += 8

row("HTTP & database pool")
ts("HTTP requests/s by status",
   [('sum(rate(http_server_requests_seconds_count{uri!~"/actuator.*"}[30s])) by (status)', "{{status}}")], "reqps", 0,
   stack=True)
ts("HTTP p99 by endpoint",
   [('histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket{uri!~"/actuator.*"}[30s])) by (le, uri))',
     "{{uri}}")], "s", 8)
ts("Hikari pool", [("sum(hikaricp_connections_active)", "active"), ("sum(hikaricp_connections_pending)", "waiting"),
                   ("sum(hikaricp_connections_max)", "max")], "short", 16)
y += 8

row("Waiting room & live updates")
ts("Queue", [("sum(ticketforge_queue_waiting)", "waiting"),
             ("sum(rate(ticketforge_queue_admitted_total[30s]))", "admitted/s")], "short", 0)
ts("SSE connections", [("sum(ticketforge_sse_connections) by (stream)", "{{stream}}")], "short", 8)
ts("SSE fan-out backlog", [("sum(ticketforge_sse_fanout_backlog)", "queued events")], "short", 16)
y += 8

row("Resources (containers, JVM, Postgres)")
ts("Container CPU (cores)",
   [('sum(rate(container_cpu_usage_seconds_total{name=~".+"}[30s])) by (name)', "{{name}}")], "short", 0)
ts("Container memory",
   [('sum(container_memory_working_set_bytes{name=~".+"}) by (name)', "{{name}}")], "bytes", 8)
ts("JVM", [('sum(jvm_memory_used_bytes{area="heap"})', "heap used"),
           ('sum(jvm_threads_live_threads)', "platform threads")], "short", 16)
y += 8
ts("Postgres commits & rollbacks/s",
   [('sum(rate(pg_stat_database_xact_commit{datname="ticketforge"}[30s]))', "commits"),
    ('sum(rate(pg_stat_database_xact_rollback{datname="ticketforge"}[30s]))', "rollbacks")], "short", 0)
ts("Postgres locks by mode", [('sum(pg_locks_count{datname="ticketforge"}) by (mode)', "{{mode}}")], "short", 8)
ts("Postgres deadlocks", [('sum(increase(pg_stat_database_deadlocks{datname="ticketforge"}[1m]))', "deadlocks/min")],
   "short", 16)
y += 8

dashboard = {
    "title": "TicketForge - Phase 1 Monolith", "uid": "ticketforge-p1", "schemaVersion": 39, "version": 1,
    "refresh": "5s", "time": {"from": "now-15m", "to": "now"}, "tags": ["ticketforge"],
    "templating": {"list": [{"name": "ds", "type": "datasource", "query": "prometheus", "current": {}}]},
    "panels": panels,
}
with open("docker/grafana/dashboards/ticketforge.json", "w") as f:
    json.dump(dashboard, f, indent=2)
print(f"{len(panels)} panels")
