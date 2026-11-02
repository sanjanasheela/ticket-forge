// #8 Concert on-sale (QUEUE=true) and #9 the same without the waiting room (QUEUE=false).
// A fixed pool of VUS concurrent fans; each iteration is a new fan who (with the queue) joins,
// polls until admitted, then: 40% picks two seats in the front stands S1-S4, 30% asks for the
// best two in a random tier, 30% buys 1-4 pit tickets. 25% abandon the hold (3-minute TTL churn).
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE, configure, freshConcertShow, params, recordHold, standRows, token, uuid } from '../lib/common.js';

const QUEUE = __ENV.QUEUE !== 'false';
const VUS = Number(__ENV.VUS || 1500);
const queueWait = new Trend('queue_wait', true);
const paid = new Counter('checkouts_confirmed');
const gaveUp = new Counter('queue_gave_up');

export const options = {
  setupTimeout: '600s',
  scenarios: { onsale: { executor: 'ramping-vus', stages: [{ target: VUS, duration: '20s' }, { target: VUS, duration: __ENV.DURATION || '160s' }] } },
  thresholds: { holds_error: ['count==0'] },
};

export function setup() {
  const admin = configure({ users: 400000, gateway: { latencyMs: 150, jitterMs: 100, failureRate: 0.05, timeoutRate: 0 } });
  const show = freshConcertShow(admin, { queue: QUEUE });
  const front = [];
  for (const stand of ['S1', 'S2', 'S3', 'S4']) {
    const rows = standRows(show.showId, stand);
    for (let r = 1; r <= 10; r++) front.push(rows[String(r)]);
  }
  return { ...show, front };
}

function admitted(data, tok) {
  const t0 = Date.now();
  http.post(`${BASE}/shows/${data.showId}/queue`, null, params(tok, {}, { name: 'queue_join' }));
  for (let i = 0; i < 90; i++) {
    sleep(2);
    const s = http.get(`${BASE}/shows/${data.showId}/queue`, params(tok, {}, { name: 'queue_poll' }));
    if (s.status === 200 && s.json('admitted')) {
      queueWait.add(Date.now() - t0);
      return http.get(`${BASE}/shows/${data.showId}/queue/access`, params(tok, {}, { name: 'queue_access' })).json('token');
    }
  }
  gaveUp.add(1);
  return null;
}

export default function (data) {
  const user = 1 + exec.scenario.iterationInTest;
  const tok = token(user);
  const headers = { 'Idempotency-Key': uuid() };
  if (QUEUE) {
    const access = admitted(data, tok);
    if (!access) return;
    headers['X-Queue-Token'] = access;
  }

  let body;
  const roll = Math.random();
  if (roll < 0.4) {
    const row = data.front[Math.floor(Math.random() * data.front.length)];
    const start = Math.floor(Math.random() * (row.length - 1));
    body = { seatIds: [row[start], row[start + 1]] };
  } else if (roll < 0.7) {
    const tiers = [data.tiers.Platinum, data.tiers.Gold, data.tiers.Silver];
    body = { tierId: tiers[Math.floor(Math.random() * 3)], bestAvailable: 2 };
  } else {
    body = { tierId: data.tiers.Pit, quantity: 1 + Math.floor(Math.random() * 4) };
  }
  const res = http.post(`${BASE}/shows/${data.showId}/holds`, JSON.stringify(body), params(tok, headers, { name: 'hold' }));
  if (!recordHold(res)) return;

  sleep(2 + Math.random() * 4);
  if (Math.random() < 0.25) return; // abandoned: the sweeper returns it after 3 minutes
  const co = http.post(`${BASE}/holds/${res.json('holdId')}/checkout`, null,
    params(tok, { 'Idempotency-Key': uuid() }, { name: 'checkout' }));
  if (co.status === 201) paid.add(1);
}
