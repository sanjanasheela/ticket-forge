'use strict';

// ---- api & auth -----------------------------------------------------------------------------

const auth = {
  get() {
    try { return JSON.parse(localStorage.getItem('tf.auth')) || null; } catch { return null; }
  },
  set(v) {
    try { v ? localStorage.setItem('tf.auth', JSON.stringify(v)) : localStorage.removeItem('tf.auth'); } catch { /* private mode */ }
    renderAccount();
  },
};

async function api(path, { method = 'GET', body, headers = {}, authed = false } = {}) {
  const send = () => {
    const h = { ...headers };
    const a = auth.get();
    if (a) h.Authorization = 'Bearer ' + a.accessToken;
    if (body !== undefined) h['Content-Type'] = 'application/json';
    return fetch(path, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body) });
  };
  if (authed && !auth.get()) await requireLogin();
  let res = await send();
  if (res.status === 401 && auth.get()) {
    if (await refresh()) res = await send();
  }
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok && res.status !== 202) {
    const err = new Error((data && data.message) || res.statusText);
    err.status = res.status; err.code = data && data.code; err.data = data;
    throw err;
  }
  return data;
}

async function refresh() {
  const a = auth.get();
  try {
    const r = await fetch('/auth/refresh', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refreshToken: a.refreshToken }),
    });
    if (!r.ok) { auth.set(null); return false; }
    const t = await r.json();
    auth.set({ ...a, ...t });
    return true;
  } catch { return false; }
}

function requireLogin() {
  return new Promise(resolve => {
    const dlg = document.getElementById('login');
    const form = document.getElementById('login-form');
    const errorEl = document.getElementById('login-error');
    errorEl.textContent = '';
    form.onsubmit = async ev => {
      ev.preventDefault();
      const action = ev.submitter ? ev.submitter.value : 'login';
      let email = form.email.value, password = form.password.value;
      if (action === 'guest') {
        email = `guest-${crypto.randomUUID().slice(0, 8)}@demo.local`;
        password = crypto.randomUUID();
      }
      try {
        const t = await api(action === 'login' ? '/auth/login' : '/auth/signup', { method: 'POST', body: { email, password } });
        auth.set({ ...t, email });
        dlg.close();
        resolve();
      } catch (e) {
        errorEl.textContent = e.message;
      }
    };
    dlg.showModal();
  });
}

function renderAccount() {
  const el = document.getElementById('account');
  const a = auth.get();
  el.replaceChildren();
  if (a) {
    el.append(a.email || 'Signed in');
    const out = button('Sign out', 'secondary', async () => {
      try { await api('/auth/logout', { method: 'POST', body: { refreshToken: a.refreshToken } }); } catch { /* ignore */ }
      auth.set(null);
    });
    el.append(out);
  } else {
    el.append(button('Sign in', 'secondary', () => requireLogin()));
  }
}

// ---- tiny DOM helpers -----------------------------------------------------------------------

function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else if (v !== false && v != null) el.setAttribute(k, v === true ? '' : v);
  }
  el.append(...children.flat().filter(c => c != null));
  return el;
}
const button = (label, cls, onclick) => h('button', { class: cls, type: 'button', onclick }, label);
const money = (minor, cur = 'INR') => new Intl.NumberFormat('en-IN', { style: 'currency', currency: cur, maximumFractionDigits: 0 }).format(minor / 100);
const when = iso => new Date(iso).toLocaleString([], { weekday: 'short', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
const view = () => document.getElementById('view');

let toastTimer;
function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.add('show');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('show'), 3500);
}

// Close streams/timers when navigating away from a page.
let cleanups = [];
function onLeave(fn) { cleanups.push(fn); }
function leave() { cleanups.forEach(f => { try { f(); } catch { /* ignore */ } }); cleanups = []; }

// ---- pages ----------------------------------------------------------------------------------

async function eventsPage() {
  let category = sessionStorage.getItem('tf.cat') || 'MOVIE';
  const list = h('div', { class: 'grid' });
  const tabs = h('div', { class: 'tabs' });
  const draw = async () => {
    tabs.replaceChildren(...['MOVIE', 'CONCERT'].map(c => h('button', {
      type: 'button', 'aria-pressed': String(c === category),
      onclick: () => { category = c; sessionStorage.setItem('tf.cat', c); draw(); },
    }, c === 'MOVIE' ? 'Movies' : 'Concerts')));
    const events = await api('/events?category=' + category);
    list.replaceChildren(...events.map(e => h('a', { class: 'card', href: `#/event/${e.id}` },
      h('div', { class: 'tag' }, e.genre), h('h3', {}, e.title), h('div', { class: 'muted' }, `${e.durationMin} min`))));
    if (!events.length) {
      list.append(h('p', { class: 'muted' }, 'Nothing here yet. An admin can load demo data with POST /admin/seed/demo.'));
    }
  };
  view().replaceChildren(h('h1', {}, 'What would you like to see?'), tabs, list);
  await draw();
}

async function eventPage(id) {
  const shows = await api(`/events/${id}/shows`);
  const title = shows[0] ? shows[0].eventTitle : 'Shows';
  view().replaceChildren(
    h('a', { href: '#/', class: 'muted' }, '← All events'),
    h('h1', {}, title),
    h('div', { class: 'grid' }, shows.map(s => h('a', { class: 'card', href: `#/show/${s.id}` },
      h('div', { class: 'tag' }, s.venueName), h('h3', {}, when(s.startsAt)), h('div', { class: 'muted' }, s.city)))),
  );
}

async function showPage(id) {
  const show = await api(`/shows/${id}`);
  const header = h('div', {},
    h('a', { href: `#/event/${show.show.eventId}`, class: 'muted' }, '← All shows'),
    h('h1', {}, show.show.eventTitle),
    h('p', { class: 'muted' }, `${show.show.venueName} · ${when(show.show.startsAt)} · seats held for ${Math.round(show.holdTtlSec / 60)} min`));
  const body = h('div');
  view().replaceChildren(header, body);

  let queueToken = null;
  if (show.queueEnabled) {
    await requireLoginIfNeeded();
    queueToken = await waitingRoom(show, body);
  }
  await seatSelection(show, body, queueToken);
}

async function requireLoginIfNeeded() {
  if (!auth.get()) await requireLogin();
}

/** Resolves with an access token once the waiting room admits this user. */
async function waitingRoom(show, container) {
  const big = h('div', { class: 'big' }, '…');
  const bar = h('div', { style: 'width:0%' });
  const eta = h('p', { class: 'muted' }, 'Joining the queue');
  container.replaceChildren(h('div', { class: 'panel queue' },
    h('h2', {}, 'You are in the waiting room'),
    h('p', { class: 'muted' }, 'High demand: fans get in strictly in the order they arrived. Keep this page open.'),
    h('div', { class: 'muted' }, 'People ahead of you'), big, h('div', { class: 'progress' }, bar), eta));

  const first = await api(`/shows/${show.show.id}/queue`, { method: 'POST', authed: true });
  const startAhead = Math.max(first.ahead, 1);
  const paint = s => {
    big.textContent = s.ahead.toLocaleString();
    bar.style.width = `${Math.min(100, 100 * (1 - s.ahead / startAhead))}%`;
    eta.textContent = s.admitted ? 'You are in!' : `Estimated wait: ${formatSecs(s.estimatedWaitSec)}`;
  };
  paint(first);
  if (!first.admitted) {
    await new Promise(resolve => {
      const es = new EventSource(`/shows/${show.show.id}/queue/stream?access_token=${encodeURIComponent(auth.get().accessToken)}`);
      onLeave(() => es.close());
      es.addEventListener('queue', ev => {
        const s = JSON.parse(ev.data);
        paint(s);
        if (s.admitted) { es.close(); resolve(); }
      });
      es.onerror = () => { /* EventSource reconnects by itself */ };
    });
  }
  const access = await api(`/shows/${show.show.id}/queue/access`, { authed: true });
  return access.token;
}

function formatSecs(s) {
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m ${s % 60}s`;
}

const TIER_COLORS = ['#7048e8', '#1c7ed6', '#0ca678', '#f08c00', '#d6336c', '#495057'];

async function seatSelection(show, container, queueToken) {
  const tiers = new Map(show.tiers.map((t, i) => [t.id, { ...t, color: TIER_COLORS[i % TIER_COLORS.length] }]));
  const state = { seats: new Map(), ga: new Map(), selected: new Set(), mine: new Set(), section: null, hold: null };
  const mapEl = h('div', { class: 'seatmap' });
  const gaEl = h('div');
  const side = h('div', { class: 'panel sticky' });
  const sectionTabs = h('div', { class: 'tabs' });

  const legend = h('div', { class: 'legend' },
    [...tiers.values()].map(t => h('span', {}, h('i', { style: `border-color:${t.color}` }), `${t.name} ${money(t.priceMinor, show.currency)}`)),
    h('span', {}, h('i', { style: 'background:var(--seat-held)' }), 'Held'),
    h('span', {}, h('i', { style: 'background:var(--seat-booked)' }), 'Sold'),
    h('span', {}, h('i', { style: 'background:var(--accent)' }), 'Selected'));

  container.replaceChildren(h('div', { class: 'layout' },
    h('div', { class: 'panel' }, legend, sectionTabs, mapEl, gaEl), side));

  // Live updates: subscribe first, buffer until the snapshot is in, then apply in order.
  let buffer = [];
  let live = false;
  const es = new EventSource(`/shows/${show.show.id}/seats/stream`);
  onLeave(() => es.close());
  const apply = ev => {
    if (ev.type === 'seats') {
      const st = { HELD: 'H', RELEASED: 'A', BOOKED: 'B' }[ev.data.change];
      for (const id of ev.data.seatIds) {
        const s = state.seats.get(id);
        if (!s) continue;
        s.st = st;
        if (st !== 'A') state.selected.delete(id);
        paintSeat(s);
      }
      drawSide();
    } else if (ev.type === 'ga') {
      const g = state.ga.get(ev.data.tierId);
      if (g) { g.available = ev.data.available; drawGa(); }
    }
  };
  for (const type of ['seats', 'ga']) {
    es.addEventListener(type, e => {
      const ev = { type, data: JSON.parse(e.data) };
      live ? apply(ev) : buffer.push(ev);
    });
  }

  async function load(section) {
    state.section = section;
    const map = await api(`/shows/${show.show.id}/seats${section ? `?section=${encodeURIComponent(section)}` : ''}`);
    if (!section && map.sections.length > 1) return load(map.sections[0]);
    state.seats = new Map(map.seats.map(s => [s.id, s]));
    state.ga = new Map(map.ga.map(g => [g.tierId, g]));
    sectionTabs.replaceChildren(...(map.sections.length > 1 ? map.sections.map(sec => h('button', {
      type: 'button', 'aria-pressed': String(sec === state.section), onclick: () => load(sec),
    }, sec)) : []));
    drawMap();
    drawGa();
    live = true;
    buffer.forEach(apply);
    buffer = [];
  }

  function drawMap() {
    const rows = new Map();
    for (const s of state.seats.values()) {
      const key = `${s.sec}|${s.row}`;
      if (!rows.has(key)) rows.set(key, []);
      rows.get(key).push(s);
    }
    const nodes = [];
    if (show.show.category === 'MOVIE' && state.seats.size) nodes.push(h('div', { class: 'screen' }, 'SCREEN THIS WAY'));
    let lastTier = null;
    // movies list rows front-to-back with the screen on top; seat map rows come in row order
    for (const seats of rows.values()) {
      const tier = tiers.get(seats[0].tier);
      if (tier && tier.id !== lastTier) {
        nodes.push(h('div', { class: 'tier-label' }, `${tier.name} · ${money(tier.priceMinor, show.currency)}`));
        lastTier = tier.id;
      }
      nodes.push(h('div', { class: 'seatrow' }, h('span', { class: 'rowlabel' }, seats[0].row),
        seats.map(s => {
          s.el = h('button', {
            type: 'button', class: 'seat', title: `${s.sec} ${s.row}-${s.n}`, style: `--tier:${tiers.get(s.tier).color}`,
            onclick: () => toggle(s),
          }, String(s.n));
          paintSeat(s);
          return s.el;
        })));
    }
    mapEl.replaceChildren(...nodes);
  }

  function paintSeat(s) {
    if (!s.el) return;
    const mine = state.mine.has(s.id);
    s.el.className = 'seat' + (s.ar ? ' aisle' : '') + (mine ? ' mine' : state.selected.has(s.id) ? ' selected' : s.st !== 'A' ? ` ${s.st}` : '');
    s.el.disabled = !mine && s.st !== 'A';
  }

  function toggle(s) {
    if (state.hold) { toast('Finish or release your current hold first'); return; }
    if (state.selected.has(s.id)) state.selected.delete(s.id);
    else if (state.selected.size >= show.maxSeatsPerHold) { toast(`At most ${show.maxSeatsPerHold} tickets per booking`); return; }
    else state.selected.add(s.id);
    paintSeat(s);
    drawSide();
  }

  function drawGa() {
    const ga = [...state.ga.values()];
    gaEl.replaceChildren(...(ga.length ? [h('h2', {}, 'Standing')] : []), ...ga.map(g => {
      const tier = tiers.get(g.tierId);
      const qty = h('input', { type: 'number', min: 1, max: Math.min(show.maxSeatsPerHold, g.available), value: 1 });
      return h('div', { class: 'ga' },
        h('div', {}, h('strong', {}, tier.name), ' ', money(tier.priceMinor, show.currency),
          h('div', { class: 'muted' }, `${g.available.toLocaleString()} of ${g.capacity.toLocaleString()} left`)),
        h('div', { class: 'row' }, qty, button('Hold', 'primary', () => createHold({ tierId: g.tierId, quantity: Number(qty.value) }))));
    }));
  }

  function drawSide() {
    if (state.hold) return drawHold();
    const picked = [...state.selected].map(id => state.seats.get(id)).filter(Boolean);
    const total = picked.reduce((sum, s) => sum + tiers.get(s.tier).priceMinor, 0);
    side.replaceChildren(...[
      h('h2', {}, 'Your selection'),
      picked.length ? h('div', {}, picked.map(s => h('div', { class: 'summary-line' },
        h('span', {}, `${s.row}-${s.n}`), h('span', {}, money(tiers.get(s.tier).priceMinor, show.currency)))))
        : h('p', { class: 'muted' }, 'Pick seats on the map.'),
      picked.length ? h('div', { class: 'summary-line total' }, h('span', {}, 'Total'), h('span', {}, money(total, show.currency))) : null,
      h('div', { class: 'row', style: 'margin-top:12px' },
        h('button', { class: 'primary', type: 'button', disabled: !picked.length, onclick: () => createHold({ seatIds: [...state.selected] }) },
          picked.length ? `Hold ${picked.length} seat${picked.length > 1 ? 's' : ''}` : 'Hold seats')),
    ].filter(Boolean));
  }

  async function createHold(body) {
    try {
      const headers = { 'Idempotency-Key': crypto.randomUUID() };
      if (queueToken) headers['X-Queue-Token'] = queueToken;
      state.hold = await api(`/shows/${show.show.id}/holds`, { method: 'POST', body, headers, authed: true });
      state.selected.clear();
      state.hold.items.filter(i => i.seatId).forEach(i => state.mine.add(i.seatId));
      state.mine.forEach(id => paintSeat(state.seats.get(id) || {}));
      drawHold();
    } catch (e) {
      if (e.code === 'GAP_RULE_VIOLATION') {
        (e.data.details.orphanSeatIds || []).forEach(id => {
          const s = state.seats.get(id);
          if (s && s.el) { s.el.classList.add('flash'); setTimeout(() => s.el.classList.remove('flash'), 2000); }
        });
        toast('That would leave a single empty seat. Try shifting your selection.');
      } else {
        toast(e.message);
      }
    }
  }

  let timer;
  onLeave(() => clearInterval(timer));
  function drawHold() {
    const hold = state.hold;
    const clock = h('div', { class: 'timer' });
    const tick = () => {
      const left = Math.max(0, Math.round((new Date(hold.expiresAt) - Date.now()) / 1000));
      clock.textContent = `${Math.floor(left / 60)}:${String(left % 60).padStart(2, '0')}`;
      clock.classList.toggle('low', left < 60);
      if (left === 0) { clearInterval(timer); endHold('Your hold expired and the seats went back on sale.'); }
    };
    clearInterval(timer);
    timer = setInterval(tick, 1000);
    tick();
    const status = h('p', { class: 'error' });
    const pay = h('button', { class: 'primary', type: 'button', onclick: () => checkout(pay, status) }, `Pay ${money(hold.totalMinor, hold.currency)}`);
    side.replaceChildren(
      h('h2', {}, 'Seats held for you'),
      h('p', { class: 'muted' }, 'Complete payment before the timer runs out.'),
      clock,
      h('div', {}, hold.items.map(i => h('div', { class: 'summary-line' },
        h('span', {}, i.seatId ? i.label : `${tiers.get(i.tierId).name} × ${i.quantity}`), h('span', {}, money(i.priceMinor * i.quantity, hold.currency))))),
      h('div', { class: 'summary-line total' }, h('span', {}, 'Total'), h('span', {}, money(hold.totalMinor, hold.currency))),
      status,
      h('div', { class: 'row' }, pay, button('Release', 'secondary', async () => {
        try { await api(`/holds/${hold.holdId}`, { method: 'DELETE', authed: true }); } catch { /* already gone */ }
        endHold('Seats released.');
      })));
  }

  async function checkout(payButton, statusEl) {
    payButton.disabled = true;
    statusEl.textContent = 'Processing payment…';
    try {
      const r = await api(`/holds/${state.hold.holdId}/checkout`, { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() }, authed: true });
      if (r.outcome === 'CONFIRMED') {
        clearInterval(timer);
        side.replaceChildren(h('h2', {}, 'You are going! 🎉'), h('p', {}, h('span', { class: 'badge ok' }, 'Confirmed')),
          h('p', { class: 'muted' }, `Booking ${r.bookingId.slice(0, 8)}`), h('a', { href: '#/bookings' }, 'See my bookings'));
        state.hold = null;
      } else if (r.outcome === 'PAYMENT_PENDING') {
        statusEl.textContent = 'Payment is being confirmed. Check My bookings in a moment.';
      }
    } catch (e) {
      statusEl.textContent = e.data && e.data.message ? e.data.message : e.message;
      payButton.disabled = e.data && e.data.outcome === 'HOLD_EXPIRED_REFUNDED';
      if (!payButton.disabled) payButton.textContent = 'Try again';
    }
  }

  function endHold(message) {
    clearInterval(timer);
    state.mine.clear();
    state.hold = null;
    state.seats.forEach(paintSeat);
    drawSide();
    toast(message);
  }

  drawSide();
  // Only fetch the snapshot once the stream is open, so no change can fall between the two.
  await new Promise(resolve => {
    es.addEventListener('hello', resolve, { once: true });
    setTimeout(resolve, 3000); // stream blocked (proxy?): show the map anyway, just not live
  });
  await load(null);
}

async function bookingsPage() {
  const list = await api('/me/bookings', { authed: true });
  view().replaceChildren(h('h1', {}, 'My bookings'), list.length
    ? h('div', { class: 'grid' }, list.map(b => h('div', { class: 'card' },
      h('div', { class: 'tag' }, b.status), h('h3', {}, money(b.totalMinor, b.currency)),
      h('div', { class: 'muted' }, when(b.createdAt)),
      h('div', {}, b.items.map(i => i.label + (i.quantity > 1 ? ` × ${i.quantity}` : '')).join(', ')))))
    : h('p', { class: 'muted' }, 'No bookings yet.'));
}

// ---- router ---------------------------------------------------------------------------------

async function route() {
  leave();
  const [, page, id] = location.hash.split('/');
  try {
    if (page === 'event') await eventPage(id);
    else if (page === 'show') await showPage(id);
    else if (page === 'bookings') await bookingsPage();
    else await eventsPage();
  } catch (e) {
    view().replaceChildren(h('p', { class: 'error' }, e.message));
  }
}

window.addEventListener('hashchange', route);
renderAccount();
route();
