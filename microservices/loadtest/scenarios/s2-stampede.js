// #2 Same-seat stampede: VUS different users try to hold the same seat at the same instant.
// Exactly one may win. Run with STRATEGY=PESSIMISTIC|OPTIMISTIC|ATOMIC_CONDITIONAL.
import http from 'k6/http';
import { BASE, configure, freshConcertShow, params, recordHold, standRows, token } from '../lib/common.js';

const VUS = Number(__ENV.VUS || 1000);

export const options = {
  setupTimeout: '300s',
  scenarios: { stampede: { executor: 'per-vu-iterations', vus: VUS, iterations: 1, maxDuration: '120s' } },
  thresholds: { holds_ok: ['count==1'], holds_error: ['count==0'] },
};

export function setup() {
  const admin = configure({ users: VUS });
  const { showId } = freshConcertShow(admin, { maxHoldsPerUser: 0 });
  const seat = standRows(showId, 'S1')['1'][11]; // S1 row 1 seat 12: the "A12" everyone wants
  return { showId, seat };
}

export default function (data) {
  const res = http.post(`${BASE}/shows/${data.showId}/holds`, JSON.stringify({ seatIds: [data.seat] }),
    params(token(__VU), {}, { name: 'hold' }));
  recordHold(res);
}
