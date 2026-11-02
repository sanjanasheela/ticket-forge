// #4 Realistic movie mix and #10 Friday evening (PROFILE=friday): browse, look at the seat map,
// hold (half pick seats, half "best available"), think, then pay (80%) or walk away (20%).
// The gateway declines 10% of payments.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE, adminToken, configure, params, recordHold, token, uuid } from '../lib/common.js';

const FRIDAY = __ENV.PROFILE === 'friday';
const SHOWS = Number(__ENV.SHOWS || (FRIDAY ? 200 : 20));
const VUS = Number(__ENV.VUS || (FRIDAY ? 1500 : 300));
const paid = new Counter('checkouts_confirmed');
const declined = new Counter('checkouts_declined');
const lost = new Counter('checkouts_failed_other');

export const options = {
  setupTimeout: '600s',
  scenarios: {
    mix: FRIDAY
      ? { executor: 'constant-vus', vus: VUS, duration: __ENV.DURATION || '3m' }
      : { executor: 'ramping-vus', stages: [{ target: VUS, duration: '30s' }, { target: VUS, duration: '90s' }] },
  },
  thresholds: { holds_error: ['count==0'] },
};

export function setup() {
  configure({ users: 400000, gateway: { latencyMs: 150, jitterMs: 100, failureRate: 0.1, timeoutRate: 0 } });
  const r = http.post(`${BASE}/admin/seed/movie-shows?count=${SHOWS}`, null,
    Object.assign(params(adminToken()), { timeout: '600s' }));
  return { shows: r.json('showIds') };
}

export default function (data) {
  const user = 300000 + exec.scenario.iterationInTest;  // movie users don't collide with concert users
  const tok = token(user);
  const show = data.shows[Math.floor(Math.random() * data.shows.length)];
  http.get(`${BASE}/shows/${show}`, { tags: { name: 'show' } });
  const map = http.get(`${BASE}/shows/${show}/seats`, { tags: { name: 'seatmap' } }).json();
  sleep(1 + Math.random() * 2);

  let body;
  if (Math.random() < 0.5) {
    const tiers = [...new Set(map.seats.map(s => s.tier))];
    body = { tierId: tiers[Math.floor(Math.random() * tiers.length)], bestAvailable: 2 };
  } else {
    const free = map.seats.filter((s, i, all) => s.st === 'A' && !s.ar && all[i + 1] && all[i + 1].st === 'A' && all[i + 1].row === s.row);
    if (!free.length) return;
    const s = free[Math.floor(Math.random() * free.length)];
    body = { seatIds: [s.id, s.id + 1] };
  }
  const res = http.post(`${BASE}/shows/${show}/holds`, JSON.stringify(body), params(tok, { 'Idempotency-Key': uuid() }, { name: 'hold' }));
  if (!recordHold(res)) return;
  sleep(2 + Math.random() * 3);
  if (Math.random() < 0.2) return; // walks away; the hold expires after 7 min

  const co = http.post(`${BASE}/holds/${res.json('holdId')}/checkout`, null,
    params(tok, { 'Idempotency-Key': uuid() }, { name: 'checkout' }));
  if (co.status === 201) paid.add(1);
  else if (co.status === 402) declined.add(1);
  else lost.add(1);
}
