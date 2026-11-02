// #3 Hot-row GA flash sale: sell the 10,000-ticket pit, one ticket per buyer, as fast as possible.
// The single counter row is the hot spot; GA_SHARDS=8 spreads it over 8 rows.
import http from 'k6/http';
import exec from 'k6/execution';
import { BASE, configure, freshConcertShow, params, recordHold, token } from '../lib/common.js';

const VUS = Number(__ENV.VUS || 500);
const ATTEMPTS = Number(__ENV.ATTEMPTS || 12000);

export const options = {
  setupTimeout: '300s',
  scenarios: { sellout: { executor: 'shared-iterations', vus: VUS, iterations: ATTEMPTS, maxDuration: '300s' } },
  thresholds: { holds_ok: ['count<=10000'], holds_error: ['count==0'] },
};

export function setup() {
  const admin = configure({ users: ATTEMPTS });
  const { showId, tiers } = freshConcertShow(admin, { gaShards: Number(__ENV.GA_SHARDS || 0) });
  return { showId, pit: tiers.Pit };
}

export default function (data) {
  const user = exec.scenario.iterationInTest + 1;
  const res = http.post(`${BASE}/shows/${data.showId}/holds`, JSON.stringify({ tierId: data.pit, quantity: 1 }),
    params(token(user), {}, { name: 'hold' }));
  recordHold(res);
}

export function teardown(data) {
  const ga = http.get(`${BASE}/shows/${data.showId}/seats?section=none`).json('ga');
  console.log(`pit after the sale: ${JSON.stringify(ga)}`);
}
