'use strict';
// Shared helpers (files starting with "_" are not exposed as routes on Vercel).
const crypto = require('crypto');
const R_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '';
const R_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '';
const mem = globalThis.__sdMem || (globalThis.__sdMem = new Map());

// ---- tiny in-memory Redis (local testing only) ----
function memCmd(c) {
  const now = Date.now();
  const raw = k => { const v = mem.get(k); if (!v) return null; if (v.exp && v.exp < now) { mem.delete(k); return null; } return v; };
  const get = k => { const v = raw(k); return v && v.t === 's' ? v.val : null; };
  const zs = k => { let v = raw(k); if (!v) { v = { t: 'z', val: new Map(), exp: 0 }; mem.set(k, v); } return v.val; };
  const zsorted = k => { const v = raw(k); if (!v) return []; return [...v.val.entries()].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)); };
  const [op, k, ...a] = c;
  switch (op) {
    case 'GET': return get(k);
    case 'MGET': return [k, ...a].map(get);
    case 'SET': { const nx = a.includes('NX'); if (nx && raw(k)) return null; const exi = a.indexOf('EX'); mem.set(k, { t: 's', val: a[0], exp: exi >= 0 ? now + a[exi + 1] * 1000 : 0 }); return 'OK'; }
    case 'DEL': return mem.delete(k) ? 1 : 0;
    case 'INCR': case 'INCRBY': { const v = raw(k); const n = (v ? +v.val : 0) + (op === 'INCR' ? 1 : +a[0]); mem.set(k, { t: 's', val: String(n), exp: v ? v.exp : 0 }); return n; }
    case 'EXPIRE': { const v = raw(k); if (v) v.exp = now + a[0] * 1000; return v ? 1 : 0; }
    case 'ZINCRBY': { const z = zs(k); const n = (z.get(a[1]) || 0) + (+a[0]); z.set(a[1], n); return String(n); }
    case 'ZADD': { const z = zs(k); z.set(a[1], +a[0]); return 1; }
    case 'ZSCORE': { const v = raw(k); const s = v && v.val.get(a[0]); return s == null ? null : String(s); }
    case 'ZREVRANK': { const i = zsorted(k).findIndex(e => e[0] === a[0]); return i < 0 ? null : i; }
    case 'ZREVRANGE': { const arr = zsorted(k).slice(+a[0], +a[1] + 1); return a.includes('WITHSCORES') ? arr.flatMap(e => [e[0], String(e[1])]) : arr.map(e => e[0]); }
    case 'ZCARD': { const v = raw(k); return v ? v.val.size : 0; }
  }
  return null;
}
async function redis(cmds) {
  if (!R_URL) return cmds.map(memCmd);
  const r = await fetch(R_URL.replace(/\/$/, '') + '/pipeline', {
    method: 'POST', headers: { Authorization: 'Bearer ' + R_TOKEN, 'Content-Type': 'application/json' }, body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  const j = await r.json();
  return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}

const cleanPid = p => String(p || '').replace(/[^a-zA-Z0-9]/g, '').slice(0, 32);
const uidOf = pid => crypto.createHash('sha256').update('sd|' + pid).digest('hex').slice(0, 16);
const cleanName = n => String(n || '').replace(/[\u0000-\u001f​-‏‪-‮<>"'`]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16);

function body(req) { let b = req.body; if (typeof b === 'string') { try { b = JSON.parse(b); } catch (e) { b = {}; } } return b || {}; }

async function getProfile(uid) {
  const [raw] = await redis([['GET', 'sd:u:' + uid]]);
  try { return raw ? JSON.parse(raw) : null; } catch (e) { return null; }
}
async function getProfiles(uids) {
  if (!uids.length) return [];
  const [arr] = await redis([['MGET', ...uids.map(u => 'sd:u:' + u)]]);
  return arr.map(x => { try { return x ? JSON.parse(x) : null; } catch (e) { return null; } });
}

// ---- leaderboard periods, Israel time; the week starts on Sunday ----
function periodKeys(ts) {
  const f = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Jerusalem', year: 'numeric', month: '2-digit', day: '2-digit', weekday: 'short' });
  const p = Object.fromEntries(f.formatToParts(new Date(ts || Date.now())).map(x => [x.type, x.value]));
  const y = +p.year, m = +p.month, d = +p.day;
  const wd = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(p.weekday);
  const sun = new Date(Date.UTC(y, m - 1, d - wd));
  const pad = n => String(n).padStart(2, '0');
  return {
    day: 'd' + y + pad(m) + pad(d),
    week: 'w' + sun.getUTCFullYear() + pad(sun.getUTCMonth() + 1) + pad(sun.getUTCDate()),
    month: 'm' + y + pad(m),
    all: 'all'
  };
}
const PERIOD_TTL = { day: 3 * 86400, week: 16 * 86400, month: 70 * 86400, all: 0 };
const PERIODS = ['day', 'week', 'month', 'all'];

async function recordResult(winUid, loseUid, tie) {
  const pk = periodKeys();
  const cmds = [];
  for (const per of PERIODS) {
    const base = 'sd:lb:' + pk[per];
    if (tie) { cmds.push(['ZINCRBY', base + ':t', 1, winUid], ['ZINCRBY', base + ':t', 1, loseUid]); }
    else { cmds.push(['ZINCRBY', base + ':w', 1, winUid], ['ZINCRBY', base + ':l', 1, loseUid], ['ZINCRBY', base + ':w', 0, loseUid], ['ZINCRBY', base + ':l', 0, winUid]); }
    if (PERIOD_TTL[per]) for (const s of ['w', 'l', 't', 'r']) cmds.push(['EXPIRE', base + ':' + s, PERIOD_TTL[per]]);
  }
  const hk = h2hKey(winUid, loseUid);
  cmds.push(tie ? ['INCR', hk + ':t'] : ['INCR', hk + ':' + winUid]);
  await redis(cmds);
  // recompute rank score: wins first, then fewer losses
  const q = [];
  for (const per of PERIODS) { const base = 'sd:lb:' + pk[per]; for (const u of [winUid, loseUid]) q.push(['ZSCORE', base + ':w', u], ['ZSCORE', base + ':l', u]); }
  const res = await redis(q);
  const set = []; let i = 0;
  for (const per of PERIODS) {
    const base = 'sd:lb:' + pk[per];
    for (const u of [winUid, loseUid]) { const w = +res[i++] || 0, l = +res[i++] || 0; set.push(['ZADD', base + ':r', w * 1000 + (999 - Math.min(l, 999)), u]); }
    if (PERIOD_TTL[per]) set.push(['EXPIRE', base + ':r', PERIOD_TTL[per]]);
  }
  await redis(set);
}

// head-to-head record between two players (kept forever)
const h2hKey = (a, b) => 'sd:h2h:' + (a < b ? a + ':' + b : b + ':' + a);
const h2hCmd = (a, b) => { const k = h2hKey(a, b); return ['MGET', k + ':' + a, k + ':' + b, k + ':t']; };
const h2hParse = r => ({ a: +(r && r[0]) || 0, b: +(r && r[1]) || 0, t: +(r && r[2]) || 0 });

// long-lived first-party cookie with the device key (survives Safari's 7-day storage cleanup)
function cookies(req) { const o = {}; String(req.headers && req.headers.cookie || '').split(';').forEach(p => { const i = p.indexOf('='); if (i > 0) o[p.slice(0, i).trim()] = decodeURIComponent(p.slice(i + 1).trim()); }); return o; }
function setPidCookie(res, pid) { try { res.setHeader('Set-Cookie', 'sd_pid=' + pid + '; Max-Age=34560000; Path=/; HttpOnly; Secure; SameSite=Lax'); } catch (e) {} }


// Duel rooms (Vercel serverless). Players poll "sync" about once a second, like CrowdHunt.

const TTL = 60 * 60 * 3;
const ONLINE_MS = 9000, ABANDON_MS = 25000;
const DURS = [60, 90, 120, 150, 180];
const CODE_CHARS = 'abcdefghjkmnpqrstuvwxyz23456789';
const rnd = n => { let s = ''; for (let i = 0; i < n; i++) s += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]; return s; };
const cleanCode = c => String(c || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8);
const num = (v, max) => Math.max(0, Math.min(max, Math.floor(+v || 0)));
const mkey = c => 'sd:r:' + c, pkey = (c, p) => 'sd:r:' + c + ':' + p;
const PLAN_TYPES = ['hue', 'flip', 'vflip', 'rot', 'clone'];
const clampN = (v, a, b) => { v = +v; return Number.isFinite(v) ? Math.max(a, Math.min(b, Math.round(v * 1000) / 1000)) : a; };
function cleanPlans(p) {
  if (!Array.isArray(p) || p.length > 5) return null;
  const out = p.map(r => !Array.isArray(r) ? null : r.slice(0, 10).map(d => ({
    t: PLAN_TYPES.includes(d && d.t) ? d.t : 'flip', x: clampN(d.x, 0, 400), y: clampN(d.y, 0, 250), r: clampN(d.r, 6, 40),
    p: clampN(d.p, 0, 1), ox: clampN(d.ox, -200, 200), oy: clampN(d.oy, -200, 200)
  })));
  return JSON.stringify(out).length > 12000 ? null : out;
}
const touch = (code, pid, extra) => ['SET', pkey(code, pid), JSON.stringify({ seen: Date.now(), ...(extra || {}) }), 'EX', TTL];

async function loadStates(meta) {
  const keys = meta.players.map(p => pkey(meta.code, p.pid));
  const cmds = [];
  if (keys.length) cmds.push(['MGET', ...keys]);
  if (meta.gid) cmds.push(['GET', 'sd:res:' + meta.gid]);
  const two = meta.players.length === 2;
  if (two) cmds.push(h2hCmd(meta.players[0].uid, meta.players[1].uid));
  const out = cmds.length ? await redis(cmds) : [];
  const states = (keys.length ? out[0] : []).map(x => { try { return JSON.parse(x || '{}') || {}; } catch (e) { return {}; } });
  let result = null; if (meta.gid) { try { result = JSON.parse(out[keys.length ? 1 : 0] || 'null'); } catch (e) {} }
  const h2h = two ? h2hParse(out[out.length - 1]) : null;
  return { states, result, h2h };
}
function view(meta, pid, states, result, withPlans, h2h) {
  const now = Date.now();
  const meI = meta.players.findIndex(p => p.pid === pid);
  return {
    h2h: h2h && meI >= 0 ? { me: meI === 0 ? h2h.a : h2h.b, rival: meI === 0 ? h2h.b : h2h.a, ties: h2h.t } : null,
    ok: true, now, code: meta.code, state: meta.state, gid: meta.gid, seed: meta.seed, startAt: meta.startAt, dur: meta.dur,
    amHost: meta.host === pid,
    plans: withPlans && meta.state === 'playing' ? (meta.plans || null) : undefined,
    result: result ? { winner: result.winner, tie: !!result.tie, abandon: !!result.abandon } : null,
    players: meta.players.map((p, i) => {
      const s = states[i] || {};
      return {
        me: p.pid === pid, host: p.pid === meta.host, uid: p.uid, name: p.name, av: p.av, v: p.v,
        online: !!s.seen && now - s.seen < ONLINE_MS,
        gid: s.gid || null, score: s.score || 0, round: s.round || 0, found: s.found || 0, nd: s.nd || 0,
        done: !!s.done, ready: s.ready || null
      };
    })
  };
}
async function maybeFinish(meta, pid, states, result) {
  if (result || meta.state !== 'playing' || !meta.gid || meta.players.length < 2) return result;
  const now = Date.now();
  const [a, b] = meta.players, [sa, sb] = states;
  const done = s => s && s.gid === meta.gid && s.done;
  let res = null;
  if (done(sa) && done(sb)) {
    if (sa.score === sb.score) res = { tie: true, winner: null };
    else res = { winner: sa.score > sb.score ? a.uid : b.uid };
  } else if (now > meta.startAt + 20000) {
    const meI = meta.players.findIndex(p => p.pid === pid), other = 1 - meI;
    const so = states[other];
    if (meI >= 0 && done(states[meI]) && !done(so) && (!so || !so.seen || now - so.seen > ABANDON_MS)) res = { winner: meta.players[meI].uid, abandon: true };
  }
  if (!res) return null;
  const [ok] = await redis([['SET', 'sd:res:' + meta.gid, JSON.stringify(res), 'EX', TTL, 'NX']]);
  if (ok) {
    const loser = res.tie ? b.uid : (res.winner === a.uid ? b.uid : a.uid);
    await recordResult(res.tie ? a.uid : res.winner, loser, !!res.tie);
  }
  return res;
}
async function playerFromProfile(pid) {
  const uid = uidOf(pid); const p = await getProfile(uid);
  if (!p) return null;
  return { pid, uid, name: p.name, av: p.av, v: p.v };
}

const H_duel = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') { res.status(405).json({ ok: false, error: 'method' }); return; }
  const b = body(req), pid = cleanPid(b.player);
  if (pid.length < 12) { res.status(400).json({ ok: false, error: 'player' }); return; }
  try {
    const action = b.action;
    const send = async meta => { const { states, result, h2h } = await loadStates(meta); res.json(view(meta, pid, states, result, true, h2h)); };
    if (action === 'create') {
      const me = await playerFromProfile(pid); if (!me) { res.status(400).json({ ok: false, error: 'profile' }); return; }
      const dur = DURS.includes(+b.dur) ? +b.dur : 90;
      for (let i = 0; i < 6; i++) {
        const code = rnd(5);
        const meta = { code, host: pid, players: [me], state: 'lobby', gid: null, seed: null, startAt: 0, dur };
        const [ok] = await redis([['SET', mkey(code), JSON.stringify(meta), 'EX', TTL, 'NX'], touch(code, pid)]);
        if (ok) { await send(meta); return; }
      }
      throw new Error('codes');
    }
    const code = cleanCode(b.code);
    const [raw] = await redis([['GET', mkey(code)]]);
    if (!raw) { res.status(404).json({ ok: false, error: 'gone' }); return; }
    const meta = JSON.parse(raw);
    const idx = meta.players.findIndex(p => p.pid === pid);
    const save = (...extra) => redis([['SET', mkey(code), JSON.stringify(meta), 'EX', TTL], ...extra]);

    if (action === 'peek') { await send(meta); return; }
    if (action === 'join') {
      const me = await playerFromProfile(pid); if (!me) { res.status(400).json({ ok: false, error: 'profile' }); return; }
      if (idx >= 0) meta.players[idx] = me;
      else {
        if (meta.players.length >= 2) {
          const { states } = await loadStates(meta), now = Date.now();
          const free = meta.players.findIndex((p, i) => p.pid !== meta.host && !(states[i].seen && now - states[i].seen < ONLINE_MS));
          if (free < 0) { res.status(409).json({ ok: false, error: 'full' }); return; }
          meta.players.splice(free, 1);
        }
        meta.players.push(me);
      }
      await save(touch(code, pid));
      await send(meta); return;
    }
    if (idx < 0) { res.status(403).json({ ok: false, error: 'notin' }); return; }
    if (action === 'config') {
      if (meta.host !== pid) { res.status(403).json({ ok: false, error: 'host' }); return; }
      if (DURS.includes(+b.dur)) meta.dur = +b.dur;
      await save(); await send(meta); return;
    }
    if (action === 'start') {
      if (meta.host !== pid) { res.status(403).json({ ok: false, error: 'host' }); return; }
      if (meta.players.length < 2) { res.status(409).json({ ok: false, error: 'alone' }); return; }
      if (DURS.includes(+b.dur)) meta.dur = +b.dur;
      meta.state = 'playing'; meta.gid = rnd(6); meta.seed = /^[a-z0-9]{6,16}$/.test(b.seed || '') ? b.seed : rnd(10); meta.plans = cleanPlans(b.plans); meta.startAt = Date.now() + 4500;
      await save(touch(code, pid, { gid: meta.gid }));
      await send(meta); return;
    }
    if (action === 'sync') {
      const st = {
        gid: typeof b.gid === 'string' ? b.gid.slice(0, 12) : null,
        score: num(b.score, 999999), round: num(b.round, 9), found: num(b.found, 20), nd: num(b.nd, 20),
        done: !!b.done, ready: typeof b.ready === 'string' ? b.ready.slice(0, 12) : null
      };
      await redis([touch(code, pid, st)]);
      let { states, result, h2h } = await loadStates(meta);
      const fin = await maybeFinish(meta, pid, states, result);
      if (fin && !result) h2h = (await loadStates(meta)).h2h;
      res.json(view(meta, pid, states, fin, st.gid !== meta.gid, h2h)); return;
    }
    res.status(400).json({ ok: false, error: 'action' });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'server' });
  }
};

// Player profile: unique username + avatar (preset or small uploaded photo). No accounts: identity is a random device key.
const PRESETS = 16;

const H_profile = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') { res.status(405).json({ ok: false, error: 'method' }); return; }
  const b = body(req);
  if (b.action === 'restore') { const c = cleanPid(cookies(req).sd_pid); res.json({ ok: true, pid: c.length >= 12 ? c : null }); return; }
  const pid = cleanPid(b.player);
  if (pid.length < 12) { res.status(400).json({ ok: false, error: 'player' }); return; }
  const uid = uidOf(pid);
  setPidCookie(res, pid);
  try {
    if (b.action === 'get') { const p = await getProfile(uid); res.json({ ok: true, uid, profile: p }); return; }
    // single-player progress backup (levels, stars, coins)
    if (b.action === 'loadprog') { const [raw] = await redis([['GET', 'sd:prog:' + uid]]); let prog = null; try { prog = raw ? JSON.parse(raw) : null; } catch (e) {} const prof = await getProfile(uid); res.json({ ok: true, uid, prog, profile: prof }); return; }
    if (b.action === 'saveprog') {
      const g = b.prog || {}, num = (v, mx) => Math.max(0, Math.min(mx, Math.floor(+v || 0)));
      const stars = {}, chal = {};
      if (g.stars && typeof g.stars === 'object') for (const k in g.stars) { const n = +k; if (n >= 1 && n <= 200) stars[n] = num(g.stars[k], 3); }
      if (g.chal && typeof g.chal === 'object') for (const k in g.chal) { const n = +k; if (n >= 1 && n <= 40 && g.chal[k]) chal[n] = true; }
      const prog = { unlocked: Math.max(1, num(g.unlocked, 201)), stars, chal, coins: num(g.coins, 1e7), total: num(g.total, 1e10), t: num(g.t, 9e15) };
      await redis([['SET', 'sd:prog:' + uid, JSON.stringify(prog)]]);
      res.json({ ok: true }); return;
    }
    if (b.action === 'set') {
      const name = cleanName(b.name);
      if (name.length < 2) { res.status(400).json({ ok: false, error: 'short' }); return; }
      const old = await getProfile(uid);
      const lower = name.toLowerCase();
      if (!old || old.name.toLowerCase() !== lower) {
        const [ok] = await redis([['SET', 'sd:n:' + lower, uid, 'NX']]);
        if (!ok) { const [owner] = await redis([['GET', 'sd:n:' + lower]]); if (owner !== uid) { res.status(409).json({ ok: false, error: 'taken' }); return; } }
        if (old) { const [owner] = await redis([['GET', 'sd:n:' + old.name.toLowerCase()]]); if (owner === uid) await redis([['DEL', 'sd:n:' + old.name.toLowerCase()]]); }
      }
      let av = String(b.av || 'p:0'), v = (old && old.v) || 0;
      const cmds = [];
      if (av === 'img') {
        if (typeof b.img === 'string') {
          if (!/^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(b.img) || b.img.length > 60000) { res.status(400).json({ ok: false, error: 'img' }); return; }
          cmds.push(['SET', 'sd:img:' + uid, b.img]); v++;
        } else if (!old || old.av !== 'img') av = 'p:0';
      } else { const n = +av.slice(2); av = /^p:\d+$/.test(av) && n >= 0 && n < PRESETS ? av : 'p:0'; }
      const prof = { name, av, v, since: (old && old.since) || Date.now() };
      cmds.push(['SET', 'sd:u:' + uid, JSON.stringify(prof)]);
      await redis(cmds);
      res.json({ ok: true, uid, profile: prof }); return;
    }
    res.status(400).json({ ok: false, error: 'action' });
  } catch (e) { res.status(500).json({ ok: false, error: 'server' }); }
};

// Wins/losses tables: daily, weekly, monthly (Israel time)

const H_leaderboard = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const b = req.method === 'POST' ? body(req) : (req.query || {});
  const per = ['day', 'week', 'month', 'all'].includes(b.period) ? b.period : 'week';
  const base = 'sd:lb:' + periodKeys()[per];
  try {
    const [flat] = await redis([['ZREVRANGE', base + ':r', 0, 29]]);
    const uids = flat || [];
    const pid = cleanPid(b.player), me = pid.length >= 12 ? uidOf(pid) : null;
    const q = [];
    uids.forEach(u => q.push(['ZSCORE', base + ':w', u], ['ZSCORE', base + ':l', u], ['ZSCORE', base + ':t', u]));
    if (me) q.push(['ZREVRANK', base + ':r', me], ['ZSCORE', base + ':w', me], ['ZSCORE', base + ':l', me], ['ZSCORE', base + ':t', me]);
    const sc = q.length ? await redis(q) : [];
    const profs = await getProfiles(me ? [...uids, me] : uids);
    const rows = uids.map((u, i) => ({ uid: u, name: (profs[i] && profs[i].name) || 'שחקן', av: (profs[i] && profs[i].av) || 'p:0', v: (profs[i] && profs[i].v) || 0,
      wins: +sc[i * 3] || 0, losses: +sc[i * 3 + 1] || 0, ties: +sc[i * 3 + 2] || 0, me: u === me }));
    let mine = null;
    if (me) { const o = uids.length * 3, p = profs[uids.length]; mine = { uid: me, rank: sc[o] == null ? null : +sc[o] + 1, wins: +sc[o + 1] || 0, losses: +sc[o + 2] || 0, ties: +sc[o + 3] || 0, name: p && p.name, av: p && p.av, v: p && p.v }; }
    res.json({ ok: true, period: per, rows, mine });
  } catch (e) { res.status(500).json({ ok: false, error: 'server' }); }
};

// GET /api/avatar?u=<uid>  -> the uploaded profile photo
const H_avatar = async (req, res) => {
  const u = String((req.query && req.query.u) || new URL(req.url, 'http://x').searchParams.get('u') || '').replace(/[^a-f0-9]/g, '').slice(0, 16);
  try {
    const [img] = u ? await redis([['GET', 'sd:img:' + u]]) : [null];
    const m = img && /^data:(image\/(?:jpeg|png|webp));base64,(.+)$/.exec(img);
    if (!m) { res.statusCode = 404; res.end(); return; }
    res.setHeader('Content-Type', m[1]);
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.end(Buffer.from(m[2], 'base64'));
  } catch (e) { res.statusCode = 500; res.end(); }
};

module.exports=async(req,res)=>{const route=(req.query&&req.query.route)||((req.url||'').match(/\/api\/([a-z]+)/)||[])[1];const fn={duel:H_duel,profile:H_profile,leaderboard:H_leaderboard,avatar:H_avatar}[route];if(!fn){res.statusCode=404;res.setHeader('Content-Type','application/json');res.end('{"error":"not_found"}');return}return fn(req,res)};
