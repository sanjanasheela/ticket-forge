// #7 Waiting-room surge: 20,000 users join one concert's queue within 5 seconds, before the
// sale opens. Fairness (admission strictly in join order) is checked in SQL afterwards.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { BASE, configure, freshConcertShow, params, token } from '../lib/common.js';

const USERS = Number(__ENV.USERS || 20000);

export const options = {
  setupTimeout: '300s',
  scenarios: {
    surge: {
      executor: 'constant-arrival-rate', rate: USERS / 5, timeUnit: '1s', duration: '5s',
      preAllocatedVUs: 500, maxVUs: 2000,
    },
  },
};

export function setup() {
  const admin = configure({ users: USERS });
  const r = http.post(`${BASE}/admin/seed/concert-show?queue=true&saleOpensInSec=20`, null, params(admin));
  console.log(`queue show id: ${r.json('showId')}`);
  return { showId: r.json('showId') };
}

export default function (data) {
  const res = http.post(`${BASE}/shows/${data.showId}/queue`, null, params(token(exec.scenario.iterationInTest + 1), {}, { name: 'join' }));
  check(res, { joined: r => r.status === 200 });
}
