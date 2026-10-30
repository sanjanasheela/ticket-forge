// #6 Retry storm: every request is sent 3 times at once with the same Idempotency-Key.
// Must produce one hold and one charge per buyer, never duplicates.
import http from 'k6/http';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE, configure, freshConcertShow, token, uuid } from '../lib/common.js';

const duplicateHolds = new Counter('duplicate_holds');
const holdsCreated = new Counter('holds_created');
const confirmed = new Counter('checkouts_confirmed');

export const options = {
  setupTimeout: '300s',
  scenarios: { storm: { executor: 'shared-iterations', vus: Number(__ENV.VUS || 200), iterations: Number(__ENV.ITER || 3000), maxDuration: '300s' } },
  thresholds: { duplicate_holds: ['count==0'] },
};

export function setup() {
  const admin = configure({ users: 400000, gateway: { latencyMs: 100, jitterMs: 50, failureRate: 0, timeoutRate: 0 } });
  return freshConcertShow(admin, { maxHoldsPerUser: 0 });
}

const req = (url, body, tok, key) => ({
  method: 'POST', url, body,
  params: { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${tok}`, 'Idempotency-Key': key } },
});

export default function (data) {
  const tok = token(50000 + exec.scenario.iterationInTest);
  const key = uuid();
  const url = `${BASE}/shows/${data.showId}/holds`;
  const body = JSON.stringify({ tierId: data.tiers.Gold, bestAvailable: 2 });
  const holds = http.batch([req(url, body, tok, key), req(url, body, tok, key), req(url, body, tok, key)]);
  const ids = new Set(holds.filter(r => r.status === 201).map(r => r.json('holdId')));
  if (ids.size > 1) duplicateHolds.add(1);
  if (ids.size === 0) return;
  holdsCreated.add(1);
  const holdId = [...ids][0];
  const payKey = uuid();
  const pays = http.batch([0, 1, 2].map(() => req(`${BASE}/holds/${holdId}/checkout`, null, tok, payKey)));
  if (pays.some(r => r.status === 201)) confirmed.add(1);
}
