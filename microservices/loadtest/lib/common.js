// Shared helpers for every scenario. Load-test users have deterministic ids (see
// UserProvisioning.loadTestUserId), and k6 signs their access JWTs itself with the dev secret,
// so a run measures booking, not bcrypt logins.
import http from 'k6/http';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import { Counter, Trend } from 'k6/metrics';

export const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const SECRET = __ENV.JWT_SECRET || 'dev-only-secret-change-me-0123456789abcdef';

export function userId(i) {
  return `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`;
}

export function token(i) {
  const now = Math.floor(Date.now() / 1000);
  const header = encoding.b64encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' }), 'rawurl');
  const payload = encoding.b64encode(JSON.stringify({
    iss: 'ticketforge', sub: userId(i), iat: now, exp: now + 3600, typ: 'access', roles: ['USER'],
  }), 'rawurl');
  const signature = crypto.hmac('sha256', SECRET, `${header}.${payload}`, 'base64rawurl');
  return `${header}.${payload}.${signature}`;
}

export function params(tok, headers = {}, tags = {}) {
  return { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${tok}`, ...headers }, tags };
}

export function uuid() {
  const b = crypto.randomBytes(16);
  const hex = Array.from(new Uint8Array(b), x => x.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

// ---- setup ---------------------------------------------------------------------------------

export function adminToken() {
  const r = http.post(`${BASE}/auth/login`, JSON.stringify({ email: 'admin@ticketforge.local', password: 'admin12345' }),
    { headers: { 'Content-Type': 'application/json' } });
  if (r.status !== 200) throw new Error(`admin login failed: ${r.status} ${r.body}`);
  return r.json('accessToken');
}

/** Puts the server in a known state for the run and returns the admin token. */
export function configure({ strategy = __ENV.STRATEGY || 'ATOMIC_CONDITIONAL', gapRule = true, users = 1000,
  gateway = { latencyMs: 150, jitterMs: 100, failureRate: 0.05, timeoutRate: 0 } } = {}) {
  const admin = adminToken();
  const put = (path, body) => {
    const r = http.put(`${BASE}${path}`, JSON.stringify(body), params(admin));
    if (r.status !== 200) throw new Error(`${path}: ${r.status} ${r.body}`);
  };
  put('/admin/inventory/config', { strategy, gapRuleEnabled: gapRule });
  put('/admin/payment-gateway/config', gateway);
  const seeded = http.post(`${BASE}/admin/seed/loadtest-users?count=${users}`, null, Object.assign(params(admin), { timeout: '300s' }));
  if (seeded.status !== 200) throw new Error(`seeding users failed: ${seeded.status}`);
  return admin;
}

export function freshConcertShow(admin, { queue = false, holdTtlSec, maxHoldsPerUser, gaShards = 0 } = {}) {
  const q = [`queue=${queue}`, `gaShards=${gaShards}`];
  if (holdTtlSec) q.push(`holdTtlSec=${holdTtlSec}`);
  if (maxHoldsPerUser !== undefined) q.push(`maxHoldsPerUser=${maxHoldsPerUser}`);
  const r = http.post(`${BASE}/admin/seed/concert-show?${q.join('&')}`, null, Object.assign(params(admin), { timeout: '120s' }));
  if (r.status !== 200) throw new Error(`concert show: ${r.status} ${r.body}`);
  const showId = r.json('showId');
  const details = http.get(`${BASE}/shows/${showId}`).json();
  const tiers = Object.fromEntries(details.tiers.map(t => [t.name, t.id]));
  return { showId, tiers };
}

/** All seats of one stadium stand, as rows of seat ids ordered by seat number. */
export function standRows(showId, section) {
  const map = http.get(`${BASE}/shows/${showId}/seats?section=${section}`).json();
  const rows = {};
  for (const s of map.seats) (rows[s.row] = rows[s.row] || []).push(s.id);
  return rows;
}

// ---- outcome accounting --------------------------------------------------------------------

export const holdOk = new Counter('holds_ok');
export const holdConflict = new Counter('holds_conflict');       // 409: lost the race / sold out
export const holdRejected = new Counter('holds_rejected');       // 4xx business rule (gap rule, limits)
export const holdOverloaded = new Counter('holds_overloaded');   // 503: contention or DB pool exhausted
export const holdError = new Counter('holds_error');             // anything else
export const holdLatency = new Trend('hold_latency', true);

export function recordHold(res) {
  holdLatency.add(res.timings.duration);
  if (res.status === 201) holdOk.add(1);
  else if (res.status === 409) holdConflict.add(1);
  else if (res.status === 503) holdOverloaded.add(1);
  else if (res.status >= 400 && res.status < 500) holdRejected.add(1);
  else holdError.add(1);
  return res.status === 201;
}
