// #1 Catalog read baseline: ramp read traffic to find the read-path knee.
import http from 'k6/http';
import { check } from 'k6';
import { BASE } from '../lib/common.js';

const RATE = Number(__ENV.RATE || 2000);

export const options = {
  scenarios: {
    reads: {
      executor: 'ramping-arrival-rate', startRate: 100, timeUnit: '1s',
      preAllocatedVUs: 200, maxVUs: Number(__ENV.MAX_VUS || 1000),
      stages: [{ target: RATE, duration: '40s' }, { target: RATE, duration: '20s' }],
    },
  },
};

export function setup() {
  const movie = http.get(`${BASE}/events?category=MOVIE`).json()[0].id;
  return { shows: http.get(`${BASE}/events/${movie}/shows`).json().map(s => s.id) };
}

export default function (data) {
  const show = data.shows[Math.floor(Math.random() * data.shows.length)];
  const roll = Math.random();
  const res = roll < 0.3 ? http.get(`${BASE}/events`, { tags: { name: 'events' } })
    : roll < 0.6 ? http.get(`${BASE}/shows/${show}`, { tags: { name: 'show' } })
      : http.get(`${BASE}/shows/${show}/seats`, { tags: { name: 'seatmap' } });
  check(res, { 'status 200': r => r.status === 200 });
}
