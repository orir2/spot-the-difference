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
    case 'GETDEL': { const v = get(k); mem.delete(k); return v; }
    case 'EXISTS': return raw(k) ? 1 : 0;
    case 'SADD': { let v = raw(k); if (!v) { v = { t: 'set', val: new Set(), exp: 0 }; mem.set(k, v); } let n = 0; a.forEach(x => { if (!v.val.has(x)) { v.val.add(x); n++; } }); return n; }
    case 'SREM': { const v = raw(k); if (!v) return 0; let n = 0; a.forEach(x => { if (v.val.delete(x)) n++; }); return n; }
    case 'SMEMBERS': { const v = raw(k); return v ? [...v.val] : []; }
    case 'SISMEMBER': { const v = raw(k); return v && v.val.has(a[0]) ? 1 : 0; }
    case 'SCARD': { const v = raw(k); return v ? v.val.size : 0; }
    case 'RPUSH': { let v = raw(k); if (!v) { v = { t: 'list', val: [], exp: 0 }; mem.set(k, v); } v.val.push(...a); return v.val.length; }
    case 'LRANGE': { const v = raw(k); if (!v) return []; const L = v.val.length, st = +a[0] < 0 ? L + +a[0] : +a[0], en = +a[1] < 0 ? L + +a[1] : +a[1]; return v.val.slice(Math.max(0, st), en + 1); }
    case 'LTRIM': { const v = raw(k); if (!v) return 'OK'; const L = v.val.length, st = +a[0] < 0 ? Math.max(0, L + +a[0]) : +a[0], en = +a[1] < 0 ? L + +a[1] : +a[1]; v.val = v.val.slice(st, en + 1); return 'OK'; }
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


const ICONS = {
  '192': 'iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAMAAABlApw1AAABgFBMVEVloZPejbAxbVmcz+lXzJKfqLbWy900kGO3oWJeUFqpO0+jeznbwWwlHzmobY1vVzu9scMgIEA/0H9xMEZbknp6zN+EWHa92rXAP1L3nMEmIToAAACO2/7+yU/+2Gf+0lk7znm15/79TF3+vTmA4KhwbX3+ocb9/v7+4HbHqFn/wD7Gm0MwM0zHljtTcpBzq8vou1JVSkbHrGJYRjs2o2kuSE2CjJ0lIDlIV3JvdIFI0I4tKkMlIDk+TGdpWEUlIDklIDmReEokIDh3ZUlx19AjHThumLVGOTx5ttdUKkFPaIRjLUSGyuvX8v8kHjhy16yniU1wVjqEbEar2tElHzlCJj5Z06m1llKr2u/G1vE7MjyGZzobGzYkIDhMY3Rliqfmym8iIi5HO03XslfQt2foqzeadDeYhFbV2Y/ottfG2qYyVlBPTmNZVExWh3RdhKJ92eOUuM+oprDTQ1bcozfJ7v8lGzZPeWx+wOKLnbCMrcS0hzjo5+oAAEA4unJxhJtusckyAAAAgHRSTlP/////////////////jP///wj///////////4A////////////////////////////////////0f////+w//9wkP9P//8u//////////9P//////9u/////////w40////EP//////////////////////////Gv///////wT//wmtAggAAA3HSURBVHja7Z35XxrJEsDHmGNz7e6735sFgkRXQMIRw0JAOQWJTwVNNrreR9ScmvvcJP/6zgzMTN8HMwP6+VD5KYlIfauqq2t6uquVX864KAOAAcAAYABwtgBqiw9KU5mM6rpkMlOlB4s1LwFqiyUPFMdASlIUwgDvS2rvJFN87zJAZUrtsUxV3APYeKD2RYobrgBslNS+SWnDOcADta9SdAiwmVH7LJmaE4Ciegqk2DXALjn1rKUK1fLRym2aRG5H5GXlqFwtpNbICel9dwAVwu/aK98OhMPhgCdy48YNf2R5lvC1tW4AcP1TR9fCuvb6Hy/01wk02S+nsK9elAfAsk91J9wRz+xvSbIqPBBoACWa+l7b30QoCBJQAK4j6l8Lh3tn/04kIQglGQDY/tVHtva9sT8RoSQOAKf/H8Lh3tu/LSuQIg9E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',
  '512': 'iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAMAAADDpiTIAAABgFBMVEWSmalUW2mfeTmf0uwvcFebN0wODkDij7NojJe2oGFYjXicbophypAOADBnUDrFrGTWnjZ+vuDdwmyessQ1nWd+wOKGWXj7nsLZzds/0H8/0IAlHzlzb4DAfJ0mIToAAACO2/7+yU7+2Gj+0lk8znq15/7+vTn+TF1wbX2A4Kj+ocb+/v7+4Hb/wD4lIDlWSkRXRTsqKUI+TmmKdU4tSUuGbkU0N02HaDqjyN11rJMlIDnMp1IlIDlyqcpI0I5JWHJPaYZplrQ2o2lIOjtx19AkIDnGnUgkIDgkHjh4ZUgjIDg6MjyDx+p6t9nouE4kHTdpWEVWd5Vw1q6oiEq0lVElFzcaGjRPKEA0lWRpLkR2MEat2s5Z06lxVzlmi6hKZHbS2ZJGJz7LljfPtWclHzmahVXnqzezhjjW8v/ptdbpyWshIS9dhaPJ2O8yjmHbs1RGOkh92ePVyebE2ahUVE6q2u7QQlXG7f+62rgyVlBud4tst5DKxc/o6OolHzmTts7AzbudAAAAgHRSTlP///////8F////////Bf/////////////////////+AP//////////////////x///////////////sf+Q//////////9w/09N/zD/////Lv//////FAz/////////////////cf///////xH///////////////////////+S//XomoIAACk6SURBVHja7Z2JW1RX0ocbtzGJmjhJvpn5lturIi5A09iO3Q0NAaTZmp2YCOgoYqIYTeKu0X/9a1Z7OUvVOafO3U7xJM8zE6Dprrd+tZy69yaSzmJtCfcROACcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCcOQCobHV6slabmNiqVCqlhvVGyHbfT+NtbU1M1GqT06sOgHbX17YqJS9GVqps1dYcAA17MjlR6vViar2licnVGAMwHWPft1AwHz8A1moTJef7ppQwMbkWGwCeTFZc4LOkoDL5JPoArG4554sgmFiNMgBrNaf78mwweSqiAEw67wOtUoseANMV51eMfZyOFAAu+BVSwXRkAJh0dZ9aRTgZBQDWJpz7NZqCtZADsOpSv25BuBpiAFzlZ6QenA8pANOu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',
  'm512': 'iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAMAAADDpiTIAAABgFBMVEXY19uaprZgVWJmkpKd1PBgLEO1n2GucpLUnTfhj7IqYmQzkGK2PVF/weOLYGzFrGVclX1cz4/5nMHcwWwkHzg/0IB+v+G9u8PDfZ8/0H9/YDgVorcmITqO2/7+yU7+2Gj+0lk8znq15/7+vTn9TF1wbX2A4Kj+ocb+/v7+4Hb/wD4dYXlMZoMrSUzkuFFXTEcrN08WmK3lxmkad40pKkNCJj5wdIDkqztUcpBI0I5IVnCOdktYRzyEyOoeV24YhJqoiUtpWEVx19DOp1AXjaJrl7UySmMubFV4tdZHOjtZ06nU2Y+zlVNwp8cbbIM7Mjxt1q/muNlki6mr2tBdhKJvVjp82eOJaDmEbEZ4ZUjPtWeYhFaq2u7MmDdVVE3H2PDDm0jW8v+1hzdJY3pFOUrYxePirkA0V1M2p2ptupGmfDfG2qbI7f9JTWS82rbVRFbaslU4uHF1yptlp4eLNEqYcjeTts2qqLKizeQfTmZOdmtUhHNagp1iTTqLiJaKrMN5jPSiAAAAgHRSTlP//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////xUHpOoAAB0aSURBVHja7Z33X9vm2sad0Tbd65yevi+yjQkhIWBbxj4JYAxmlg0lk6xmtWlWM5uOpO2/fmzwkvTsR5YeSdfFT/0kdbDu7z2fodQAlGil8AgAAAQAIAAAAQAIAEAAAAIAEACAAAAEACAAAAEACABAAAACABAAgAAABAAgAAABAAgAQAAAAgAQAIAAAAQAIAAAAQAIAEAAAAIAEACAAAAEACAAAAEACABAAAACABAAgAAABAAgAAABAAgAQAAAAgAQAIAAAAQAIAAAAQAIAEAAAAIAEACAAAAEACAAAAEACABAAAACABAAgAAABAAgAAABAAgAQAAAAgAQAIAAAAQAIAAAAQAIAEAAAAIAEACAAAAEACAAAAEACAAAADwCAAABAAgAQAAAAgAQAIAAAAQAIABgqkqlpblyeXvMIG2Xy3NLpRIA6Kvd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'
};
// Duel rooms (Vercel serverless). Players poll "sync" about once a second, like CrowdHunt.

const TTL = 60 * 60 * 3;
const ONLINE_MS = 9000, ABANDON_MS = 25000;
const DURS = [60, 90, 120, 150, 180];
const CODE_CHARS = 'abcdefghjkmnpqrstuvwxyz23456789';
const rnd = n => { let s = ''; for (let i = 0; i < n; i++) s += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]; return s; };
const cleanCode = c => String(c || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8);
const num = (v, max) => Math.max(0, Math.min(max, Math.floor(+v || 0)));
const MM_MODES = { quick: 90, bo5: 90, king: 60 };
const ROUNDS = { friend: 3, quick: 3, bo5: 5, king: 3 };
const qkey = m => 'sd:mq:' + m;
// best-of-N: a round goes to whoever scored more on that picture
function series(ra, rb, n) {
  let wa = 0, wb = 0; const m = Math.min(ra.length, rb.length, n);
  for (let i = 0; i < m; i++) { if (ra[i] > rb[i]) wa++; else if (rb[i] > ra[i]) wb++; }
  const need = Math.floor(n / 2) + 1;
  return { wa, wb, dec: wa >= need ? 'a' : wb >= need ? 'b' : null };
}
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
    ok: true, now, code: meta.code, mode: meta.mode || 'friend', state: meta.state, gid: meta.gid, seed: meta.seed, startAt: meta.startAt, dur: meta.dur,
    amHost: meta.host === pid,
    plans: withPlans && meta.state === 'playing' ? (meta.plans || null) : undefined,
    result: result ? { winner: result.winner, tie: !!result.tie, abandon: !!result.abandon } : null,
    players: meta.players.map((p, i) => {
      const s = states[i] || {};
      return {
        me: p.pid === pid, host: p.pid === meta.host, uid: p.uid, name: p.name, av: p.av, v: p.v,
        online: !!s.seen && now - s.seen < ONLINE_MS,
        gid: s.gid || null, score: s.score || 0, round: s.round || 0, found: s.found || 0, nd: s.nd || 0,
        done: !!s.done, ready: s.ready || null, rs: Array.isArray(s.rs) ? s.rs : []
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
  const cur = s => s && s.gid === meta.gid;
  const bo = meta.mode === 'bo5';
  const ser = bo && cur(sa) && cur(sb) ? series(sa.rs || [], sb.rs || [], ROUNDS.bo5) : null;
  if (ser && ser.dec) res = { winner: ser.dec === 'a' ? a.uid : b.uid };
  else if (done(sa) && done(sb)) {
    if (ser && ser.wa !== ser.wb) res = { winner: ser.wa > ser.wb ? a.uid : b.uid };
    else if (sa.score === sb.score) res = { tie: true, winner: null };
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
        const meta = { code, mode: 'friend', host: pid, players: [me], state: 'lobby', gid: null, seed: null, startAt: 0, dur };
        const [ok] = await redis([['SET', mkey(code), JSON.stringify(meta), 'EX', TTL, 'NX'], touch(code, pid)]);
        if (ok) { await send(meta); return; }
      }
      throw new Error('codes');
    }
    if (action === 'match') {
      // random opponent: take whoever is waiting in this mode's queue, otherwise wait there ourselves
      const mode = MM_MODES[b.mode] ? b.mode : 'quick';
      const me = await playerFromProfile(pid); if (!me) { res.status(400).json({ ok: false, error: 'profile' }); return; }
      const now = Date.now();
      let mine = cleanCode(b.code), myMeta = null;
      if (mine) { const [r] = await redis([['GET', mkey(mine)]]); try { myMeta = r && JSON.parse(r); } catch (e) {} if (myMeta && (myMeta.host !== pid || myMeta.mode !== mode)) myMeta = null; }
      if (myMeta && myMeta.players.length >= 2) { await redis([touch(mine, pid)]); await send(myMeta); return; }
      const [q] = await redis([['GETDEL', qkey(mode)]]);
      let qe = null; try { qe = JSON.parse(q || 'null'); } catch (e) {}
      if (qe && qe.pid !== pid && qe.code !== mine && now - qe.t < 15000) {
        const [r] = await redis([['GET', mkey(qe.code)]]);
        let m = null; try { m = r && JSON.parse(r); } catch (e) {}
        if (m && m.state === 'lobby' && m.players.length === 1 && m.mode === mode && m.players[0].pid !== pid) {
          m.players.push(me);
          const cmds = [['SET', mkey(qe.code), JSON.stringify(m), 'EX', TTL], touch(qe.code, pid)];
          if (myMeta) cmds.push(['DEL', mkey(mine)]);
          await redis(cmds); await send(m); return;
        }
      }
      if (!myMeta) {
        for (let i = 0; i < 6 && !myMeta; i++) {
          const code = rnd(5);
          const meta = { code, mode, mm: true, host: pid, players: [me], state: 'lobby', gid: null, seed: null, startAt: 0, dur: MM_MODES[mode] };
          const [ok] = await redis([['SET', mkey(code), JSON.stringify(meta), 'EX', TTL, 'NX']]);
          if (ok) { myMeta = meta; mine = code; }
        }
        if (!myMeta) throw new Error('codes');
      }
      await redis([['SET', qkey(mode), JSON.stringify({ code: mine, pid, t: now }), 'EX', 15], touch(mine, pid)]);
      await send(myMeta); return;
    }
    if (action === 'cancel') {
      const mode = MM_MODES[b.mode] ? b.mode : 'quick', mine = cleanCode(b.code);
      const [q, r] = await redis([['GET', qkey(mode)], ['GET', mkey(mine)]]);
      let qe = null, m = null; try { qe = JSON.parse(q || 'null'); m = r && JSON.parse(r); } catch (e) {}
      const cmds = [];
      if (qe && qe.code === mine && qe.pid === pid) cmds.push(['DEL', qkey(mode)]);
      if (m && m.host === pid && m.players.length < 2) cmds.push(['DEL', mkey(mine)]);
      if (cmds.length) await redis(cmds);
      res.json({ ok: true }); return;
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
      if (DURS.includes(+b.dur) && (meta.mode || 'friend') === 'friend') meta.dur = +b.dur;
      await save(); await send(meta); return;
    }
    if (action === 'start') {
      if (meta.host !== pid) { res.status(403).json({ ok: false, error: 'host' }); return; }
      if (meta.players.length < 2) { res.status(409).json({ ok: false, error: 'alone' }); return; }
      if (DURS.includes(+b.dur) && (meta.mode || 'friend') === 'friend') meta.dur = +b.dur;
      meta.state = 'playing'; meta.gid = rnd(6); meta.seed = /^[a-z0-9]{6,16}$/.test(b.seed || '') ? b.seed : rnd(10); meta.plans = cleanPlans(b.plans); meta.startAt = Date.now() + 4500;
      await save(touch(code, pid, { gid: meta.gid }));
      await send(meta); return;
    }
    if (action === 'sync') {
      const st = {
        gid: typeof b.gid === 'string' ? b.gid.slice(0, 12) : null,
        score: num(b.score, 999999), round: num(b.round, 9), found: num(b.found, 20), nd: num(b.nd, 20),
        done: !!b.done, ready: typeof b.ready === 'string' ? b.ready.slice(0, 12) : null,
        rs: Array.isArray(b.rs) ? b.rs.slice(0, 5).map(v => num(v, 99999)) : []
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
    // ---- friends: mutual list, online presence, direct challenges ----
    const cleanUid = v => String(v || '').replace(/[^a-f0-9]/g, '').slice(0, 16);
    if (b.action === 'friends') {
      const [ids] = await redis([['SMEMBERS', 'sd:fr:' + uid], ['SET', 'sd:on:' + uid, '1', 'EX', 30]]);
      const list = (ids || []).slice(0, 100);
      if (!list.length) { res.json({ ok: true, friends: [] }); return; }
      const profs = await getProfiles(list);
      const on = await redis(list.map(f => ['EXISTS', 'sd:on:' + f]));
      const friends = list.map((f, i) => profs[i] ? { uid: f, name: profs[i].name, av: profs[i].av, v: profs[i].v || 0, online: !!on[i] } : null).filter(Boolean)
        .sort((a, b2) => (b2.online - a.online) || a.name.localeCompare(b2.name));
      res.json({ ok: true, friends }); return;
    }
    if (b.action === 'addfriend') {
      const me = await getProfile(uid); if (!me) { res.status(400).json({ ok: false, error: 'profile' }); return; }
      let fu = cleanUid(b.fuid);
      if (!fu) { const nm = cleanName(b.name).toLowerCase(); if (nm.length < 2) { res.status(400).json({ ok: false, error: 'short' }); return; } const [o2] = await redis([['GET', 'sd:n:' + nm]]); fu = o2 || ''; }
      if (!fu) { res.status(404).json({ ok: false, error: 'nouser' }); return; }
      if (fu === uid) { res.status(400).json({ ok: false, error: 'self' }); return; }
      const fp = await getProfile(fu); if (!fp) { res.status(404).json({ ok: false, error: 'nouser' }); return; }
      const [cnt] = await redis([['SCARD', 'sd:fr:' + uid]]); if (+cnt >= 100) { res.status(400).json({ ok: false, error: 'many' }); return; }
      await redis([['SADD', 'sd:fr:' + uid, fu], ['SADD', 'sd:fr:' + fu, uid]]);
      res.json({ ok: true, friend: { uid: fu, name: fp.name, av: fp.av, v: fp.v || 0 } }); return;
    }
    if (b.action === 'delfriend') { const fu = cleanUid(b.fuid); if (fu) await redis([['SREM', 'sd:fr:' + uid, fu], ['SREM', 'sd:fr:' + fu, uid]]); res.json({ ok: true }); return; }
    if (b.action === 'invite') {
      const fu = cleanUid(b.fuid), code = String(b.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
      const [isF] = await redis([['SISMEMBER', 'sd:fr:' + uid, fu]]);
      if (!+isF || !code) { res.status(400).json({ ok: false, error: 'notfriend' }); return; }
      await redis([['RPUSH', 'sd:inv:' + fu, JSON.stringify({ from: uid, code, t: Date.now() })], ['LTRIM', 'sd:inv:' + fu, -5, -1], ['EXPIRE', 'sd:inv:' + fu, 180]]);
      res.json({ ok: true }); return;
    }
    if (b.action === 'inbox') {
      const [raw] = await redis([['LRANGE', 'sd:inv:' + uid, 0, -1], ['DEL', 'sd:inv:' + uid], ['SET', 'sd:on:' + uid, '1', 'EX', 30]]);
      const now = Date.now(), inv = (raw || []).map(x => { try { return JSON.parse(x); } catch (e) { return null; } }).filter(x => x && now - x.t < 150000);
      const profs = await getProfiles(inv.map(x => x.from));
      res.json({ ok: true, invites: inv.map((x, i) => profs[i] ? { code: x.code, from: x.from, name: profs[i].name, av: profs[i].av, v: profs[i].v || 0 } : null).filter(Boolean) }); return;
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

// GET /api/app?f=manifest|sw|icon&s=192|512|m512|assetlinks – everything that makes the site an installable app
const MANIFEST = {
  id: '/', name: 'מצא את ההבדלים – דו־קרב', short_name: 'מצא הבדלים',
  description: 'משחק מצא את ההבדלים צבעוני – 200 שלבים, דו־קרבות אונליין וטורנירים.',
  start_url: '/?src=app', scope: '/', display: 'standalone', orientation: 'portrait',
  background_color: '#16a3b8', theme_color: '#0f8597', lang: 'he', dir: 'rtl', categories: ['games'],
  icons: [
    { src: '/api/app?f=icon&s=192', sizes: '192x192', type: 'image/png', purpose: 'any' },
    { src: '/api/app?f=icon&s=512', sizes: '512x512', type: 'image/png', purpose: 'any' },
    { src: '/api/app?f=icon&s=m512', sizes: '512x512', type: 'image/png', purpose: 'maskable' }
  ]
};
const SW = `const C='sd-shell-v1';
self.addEventListener('install',e=>{self.skipWaiting();e.waitUntil(caches.open(C).then(c=>c.addAll(['/'])).catch(()=>{}))});
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
self.addEventListener('fetch',e=>{const r=e.request;if(r.method!=='GET'||r.mode!=='navigate')return;
  e.respondWith(fetch(r).then(res=>{if(res.ok){const cp=res.clone();caches.open(C).then(c=>c.put('/',cp))}return res}).catch(()=>caches.match('/')))});`;
const ASSETLINKS = [];
const H_app = async (req, res) => {
  const q = req.query || Object.fromEntries(new URL(req.url, 'http://x').searchParams);
  const f = String(q.f || '');
  if (f === 'manifest') { res.setHeader('Content-Type', 'application/manifest+json'); res.setHeader('Cache-Control', 'public, max-age=3600'); res.end(JSON.stringify(MANIFEST)); return; }
  if (f === 'sw') { res.setHeader('Content-Type', 'text/javascript'); res.setHeader('Service-Worker-Allowed', '/'); res.setHeader('Cache-Control', 'no-cache'); res.end(SW); return; }
  if (f === 'assetlinks') { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(ASSETLINKS)); return; }
  if (f === 'icon') {
    const b = ICONS[String(q.s || '512')] || ICONS['512'];
    res.setHeader('Content-Type', 'image/png'); res.setHeader('Cache-Control', 'public, max-age=604800');
    res.end(Buffer.from(b, 'base64')); return;
  }
  res.statusCode = 404; res.end();
};

module.exports=async(req,res)=>{const route=(req.query&&req.query.route)||((req.url||'').match(/\/api\/([a-z]+)/)||[])[1];const fn={duel:H_duel,profile:H_profile,leaderboard:H_leaderboard,avatar:H_avatar,app:H_app}[route];if(!fn){res.statusCode=404;res.setHeader('Content-Type','application/json');res.end('{"error":"not_found"}');return}return fn(req,res)};
