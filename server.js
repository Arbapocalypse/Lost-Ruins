'use strict';
// LOST RUIN persistent multiplayer server: account auth, presence, chat and shared-world storage.
// Requires Node.js 18+ and npm dependencies from package.json.
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');
const { createClient } = require('@supabase/supabase-js');

const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 8080);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 150);
const MAX_MESSAGE_BYTES = 256 * 1024;
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DATA_FILE = path.join(DATA_DIR, 'lost-ruin-data.json');
const clients = new Map();
const accounts = new Map();
const sharedWorlds = new Map();
const tradeOffers = new Map();
const guilds = new Map();
const parties = new Map();
const guildInvites = new Map();
const partyInvites = new Map();
const playerParty = new Map();

// ============================================================
// STORAGE: Supabase is primary when configured; local JSON is a
// migration source and best-effort backup. Designed for one server.
// ============================================================
let storageMode = 'local';
let supabase = null;
let persistenceQueue = Promise.resolve();
const persistedKeys = {
  accounts: new Set(),
  worlds: new Set(),
  guilds: new Set()
};

function readLocalStore() {
  try {
    const data = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    return {
      accounts: data.accounts && typeof data.accounts === 'object' ? data.accounts : {},
      worlds: data.worlds && typeof data.worlds === 'object' ? data.worlds : {},
      guilds: data.guilds && typeof data.guilds === 'object' ? data.guilds : {}
    };
  } catch (e) {
    if (e.code !== 'ENOENT') console.error('Could not read local backup:', e.message);
    return { accounts: {}, worlds: {}, guilds: {} };
  }
}

function takeSnapshot() {
  return {
    accounts: Object.fromEntries(accounts),
    worlds: Object.fromEntries(sharedWorlds),
    guilds: Object.fromEntries(guilds)
  };
}

function saveLocalBackup(snapshot) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const temp = DATA_FILE + '.tmp';
    fs.writeFileSync(temp, JSON.stringify(snapshot));
    fs.renameSync(temp, DATA_FILE);
  } catch (e) {
    console.error('Local backup failed:', e.message);
  }
}

async function fetchAllRows(table, keyColumn, dataColumn) {
  const rows = [];
  const pageSize = 500;
  for (let offset = 0; ; offset += pageSize) {
    const { data, error } = await supabase
      .from(table)
      .select(`${keyColumn},${dataColumn}`)
      .range(offset, offset + pageSize - 1);
    if (error) throw new Error(`Supabase read failed for ${table}: ${error.message}`);
    rows.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return rows;
}

async function upsertRows(table, rows, conflictColumn) {
  const batchSize = 100;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    if (!batch.length) continue;
    const { error } = await supabase.from(table).upsert(batch, { onConflict: conflictColumn });
    if (error) throw new Error(`Supabase save failed for ${table}: ${error.message}`);
  }
}

async function deleteMissingRows(table, keyColumn, previousKeys, currentKeys) {
  const missing = [...previousKeys].filter(key => !currentKeys.has(key));
  for (let i = 0; i < missing.length; i += 100) {
    const batch = missing.slice(i, i + 100);
    if (!batch.length) continue;
    const { error } = await supabase.from(table).delete().in(keyColumn, batch);
    if (error) throw new Error(`Supabase cleanup failed for ${table}: ${error.message}`);
  }
}

async function writeSupabaseSnapshot(snapshot) {
  const accountRows = Object.entries(snapshot.accounts).map(([username, account]) => ({ username: username.toLowerCase(), account_data: account }));
  const worldRows = Object.entries(snapshot.worlds).map(([world_id, world_data]) => ({ world_id, world_data }));
  const guildRows = Object.entries(snapshot.guilds).map(([guild_id, guild_data]) => ({ guild_id, guild_data }));

  await upsertRows('game_server_accounts', accountRows, 'username');
  await upsertRows('game_server_worlds', worldRows, 'world_id');
  await upsertRows('game_server_guilds', guildRows, 'guild_id');

  const currentAccountKeys = new Set(Object.keys(snapshot.accounts).map(k => k.toLowerCase()));
  const currentWorldKeys = new Set(Object.keys(snapshot.worlds));
  const currentGuildKeys = new Set(Object.keys(snapshot.guilds));

  await deleteMissingRows('game_server_accounts', 'username', persistedKeys.accounts, currentAccountKeys);
  await deleteMissingRows('game_server_worlds', 'world_id', persistedKeys.worlds, currentWorldKeys);
  await deleteMissingRows('game_server_guilds', 'guild_id', persistedKeys.guilds, currentGuildKeys);

  persistedKeys.accounts = currentAccountKeys;
  persistedKeys.worlds = currentWorldKeys;
  persistedKeys.guilds = currentGuildKeys;
}

async function initializeStorage() {
  const local = readLocalStore();
  const projectUrl = process.env.SUPABASE_URL;
  const secretKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SECRET_KEY;

  if (!projectUrl || !secretKey) {
    if (process.env.RENDER || process.env.NODE_ENV === 'production') {
      throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_SECRET_KEY) must be set in production.');
    }
    storageMode = 'local';
    for (const [k, v] of Object.entries(local.accounts)) accounts.set(k.toLowerCase(), v);
    for (const [k, v] of Object.entries(local.worlds)) sharedWorlds.set(k, v);
    for (const [k, v] of Object.entries(local.guilds)) guilds.set(k, v);
    console.warn('Supabase credentials missing; using local JSON storage for development.');
    return;
  }

  supabase = createClient(projectUrl, secretKey, {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false }
  });

  const [cloudAccounts, cloudWorlds, cloudGuilds] = await Promise.all([
    fetchAllRows('game_server_accounts', 'username', 'account_data'),
    fetchAllRows('game_server_worlds', 'world_id', 'world_data'),
    fetchAllRows('game_server_guilds', 'guild_id', 'guild_data')
  ]);

  for (const row of cloudAccounts) {
    if (row.username && row.account_data) {
      accounts.set(String(row.username).toLowerCase(), row.account_data);
      persistedKeys.accounts.add(String(row.username).toLowerCase());
    }
  }
  for (const row of cloudWorlds) {
    if (row.world_id && row.world_data) {
      sharedWorlds.set(row.world_id, row.world_data);
      persistedKeys.worlds.add(row.world_id);
    }
  }
  for (const row of cloudGuilds) {
    if (row.guild_id && row.guild_data) {
      guilds.set(row.guild_id, row.guild_data);
      persistedKeys.guilds.add(row.guild_id);
    }
  }

  // Import local records only when the cloud has no record with that key.
  for (const [key, value] of Object.entries(local.accounts)) {
    const normalized = key.toLowerCase();
    if (!accounts.has(normalized)) accounts.set(normalized, value);
  }
  for (const [key, value] of Object.entries(local.worlds)) {
    if (!sharedWorlds.has(key)) sharedWorlds.set(key, value);
  }
  for (const [key, value] of Object.entries(local.guilds)) {
    if (!guilds.has(key)) guilds.set(key, value);
  }

  storageMode = 'supabase';
  const snapshot = takeSnapshot();
  await writeSupabaseSnapshot(snapshot);
  saveLocalBackup(snapshot);
  console.log(`Storage mode: Supabase | Accounts: ${accounts.size} | Worlds: ${sharedWorlds.size} | Guilds: ${guilds.size}`);
}

function persist() {
  const snapshot = takeSnapshot();
  saveLocalBackup(snapshot);
  if (storageMode !== 'supabase') return Promise.resolve(true);
  // Keep the queue moving after a failed write, but return the individual
  // operation's result so account creation can wait for a confirmed save.
  const write = persistenceQueue.then(() => writeSupabaseSnapshot(snapshot));
  persistenceQueue = write.catch(error => {
    console.error('Supabase persistence failed:', error.message);
  });
  return write.then(() => true).catch(() => false);
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: true, service: 'lost-ruin-multiplayer', storage: storageMode, clients: clients.size, accounts: accounts.size, worlds: sharedWorlds.size }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
  res.end('Lost Ruin multiplayer server is running. WebSocket endpoint: /\nHealth: /health\n');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE_BYTES, perMessageDeflate: false });
const cleanText = (v, max) => String(v ?? '').replace(/[<>\u0000-\u001f\u007f]/g, '').trim().slice(0, max);
const validCoord = v => Number.isFinite(v) ? Math.max(-1_000_000, Math.min(1_000_000, Math.floor(v))) : 0;
const validUsername = v => /^[a-zA-Z0-9_]{3,24}$/.test(v);
const validCharacter = v => /^[\p{L}0-9 _'-]{2,24}$/u.test(v);
const safeRace = v => ['human', 'orc', 'goblin', 'elf', 'dragonkin', 'dwarf'].includes(v) ? v : 'human';
function send(ws, msg) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); }
function fail(ws, message) { send(ws, { type: 'error', message }); }
function roomMembers(room) { return [...clients.values()].filter(c => c.joined && c.roomId === room && c.ws.readyState === WebSocket.OPEN); }
function publicPlayer(c) {
  const p = c.profile || {}, privacy = p.privacy || {}, visible = {};
  for (const key of ['name', 'alias', 'age', 'title', 'nicknames', 'mainProfession', 'subProfession']) {
    if (privacy[key] !== false) { const value = key === 'name' ? (p.name || c.name) : p[key]; if (value !== undefined && value !== null && value !== '') visible[key] = value; }
  }
  const gear = {};
  if (privacy.equipment !== false && c.equipment && typeof c.equipment === 'object') {
    for (const [k, v] of Object.entries(c.equipment)) {
      if (v && typeof v === 'object') gear[k] = { id: String(v.id || v.itemId || ''), name: String(v.name || v.id || v.itemId || 'Equipment').slice(0, 80) };
      else if (v) gear[k] = { name: String(v).slice(0, 80) };
    }
  }
  const partyId = playerParty.get(c.id) || null, party = partyId ? parties.get(partyId) : null, guild = c.accountKey ? guildForAccount(c.accountKey) : null;
  return { id: c.id, name: c.name, race: c.race, x: c.x, y: c.y, level: c.level || 0, publicProfile: visible, publicEquipment: gear, partyId, partyStrategy: party?.strategy || 'balanced', guildId: guild?.id || null, guildName: guild?.name || null, guildRole: guild?.members?.[c.accountKey] || null, scout: /scout/i.test(String(p.mainProfession || p.subProfession || '')) };
}
function broadcastPlayers(room) { const players = roomMembers(room).map(publicPlayer); for (const c of roomMembers(room)) send(c.ws, { type: 'players', players }); }
function worldFor(id) { if (!sharedWorlds.has(id)) sharedWorlds.set(id, { tileData: {}, groundLoot: {}, bonfires: {}, structures: {}, updatedAt: Date.now() }); return sharedWorlds.get(id); }
function sendWorld(c) { send(c.ws, { type: 'worldState', state: worldFor(c.roomId) }); }
function applyPatch(target, patch) { if (!patch || typeof patch !== 'object' || Array.isArray(patch)) return; for (const [key, value] of Object.entries(patch)) { if (!/^-?\d+,-?\d+$/.test(key) && key.length > 100) continue; if (value === null) delete target[key]; else target[key] = value; } }
function validPatch(patch) { if (!patch || typeof patch !== 'object' || Array.isArray(patch)) return {}; const out = {}; for (const [k, v] of Object.entries(patch).slice(0, 15000)) { if (k.length > 100) continue; const raw = JSON.stringify(v); if (raw && raw.length < 120000) out[k] = v === null ? null : v; } return out; }
function guildForAccount(key) { for (const g of guilds.values()) if (g.members && g.members[key]) return g; return null; }
function sendGuild(c, text = '') {
  const g = c.accountKey ? guildForAccount(c.accountKey) : null;
  if (!g) { send(c.ws, { type: 'guildState', guild: null, text }); return; }
  const members = Object.entries(g.members).map(([key, role]) => { const acc = accounts.get(key); const online = [...clients.values()].find(p => p.joined && p.accountKey === key); return { id: online?.id || key, name: online?.name || acc?.characterName || acc?.username || key, role, online: !!online }; });
  const leaderOnline = [...clients.values()].find(p => p.joined && p.accountKey === g.leaderKey);
  send(c.ws, { type: 'guildState', guild: { id: g.id, name: g.name, leaderId: leaderOnline?.id || g.leaderKey, leaderName: leaderOnline?.name || g.leaderName, members }, text });
}
function broadcastGuild(g, text = '') { for (const c of clients.values()) if (c.joined && c.accountKey && g.members[c.accountKey]) sendGuild(c, text); }
function sendParty(p, text = '') {
  if (!p) return;
  const members = p.members.map(id => clients.get(id)).filter(Boolean).map(c => ({ id: c.id, name: c.name, race: c.race, level: c.level || 0, position: p.formation?.[c.id] || '', profession: c.profile?.mainProfession || c.profile?.subProfession || '', scout: /scout/i.test(String(c.profile?.mainProfession || c.profile?.subProfession || '')), guildMate: !!(guildForAccount(c.accountKey) && p.members.some(id => { const q = clients.get(id); return q && guildForAccount(q.accountKey)?.id === guildForAccount(c.accountKey)?.id; })) }));
  for (const id of p.members) { const c = clients.get(id); if (c) send(c.ws, { type: 'partyState', party: { id: p.id, leaderId: p.leaderId, leaderName: clients.get(p.leaderId)?.name || p.leaderName, strategy: p.strategy, members }, text }); }
}
function partyOf(c) { const id = playerParty.get(c.id); return id ? parties.get(id) : null; }
function removePartyMember(c) {
  const p = partyOf(c); if (!p) return;
  p.members = p.members.filter(id => id !== c.id); playerParty.delete(c.id); if (p.formation) delete p.formation[c.id];
  if (p.members.length < 2) { for (const id of p.members) playerParty.delete(id); for (const id of p.members) { const peer = clients.get(id); if (peer) send(peer.ws, { type: 'partyState', party: null, text: 'Party disbanded.' }); } parties.delete(p.id); }
  else { if (p.leaderId === c.id) { p.leaderId = p.members[0]; p.leaderName = clients.get(p.leaderId)?.name || 'Survivor'; } sendParty(p, c.name + ' left the party.'); }
}
function handleGuildAction(c, msg) {
  const action = cleanText(msg.action, 20); let g = c.accountKey ? guildForAccount(c.accountKey) : null;
  if (action === 'state') { sendGuild(c); return; }
  if (action === 'create') {
    const name = cleanText(msg.name, 32); if (g) { sendGuild(c, 'You already belong to a guild.'); return; }
    if (name.length < 3) { fail(c.ws, 'Guild name must be at least 3 characters.'); return; }
    if ([...guilds.values()].some(x => x.name.toLowerCase() === name.toLowerCase())) { fail(c.ws, 'That guild name is already taken.'); return; }
    g = { id: crypto.randomUUID(), name, leaderId: c.id, leaderKey: c.accountKey, leaderName: c.name, members: { [c.accountKey]: 'guild leader' }, createdAt: Date.now() };
    guilds.set(g.id, g); persist(); sendGuild(c, 'Guild created successfully.'); return;
  }
  if (action === 'invite') {
    if (!g || g.leaderKey !== c.accountKey) { fail(c.ws, 'Only the guild leader can invite members in this version.'); return; }
    const target = clients.get(cleanText(msg.targetId, 80)); if (!target || !target.joined || target.roomId !== c.roomId) { fail(c.ws, 'That player is not online.'); return; }
    if (guildForAccount(target.accountKey)) { fail(c.ws, 'That player already belongs to a guild.'); return; }
    const inviteId = crypto.randomUUID(); guildInvites.set(inviteId, { guildId: g.id, fromKey: c.accountKey, toKey: target.accountKey, expires: Date.now() + 120000 });
    send(target.ws, { type: 'guildInvite', inviteId, guildName: g.name, fromName: c.name }); send(c.ws, { type: 'system', text: 'Guild invitation sent.' }); return;
  }
  if (action === 'accept') {
    const inv = guildInvites.get(cleanText(msg.inviteId, 80)); if (!inv || inv.toKey !== c.accountKey || inv.expires < Date.now()) { fail(c.ws, 'Guild invitation expired.'); return; }
    g = guilds.get(inv.guildId); if (!g || guildForAccount(c.accountKey)) { guildInvites.delete(inv.inviteId); fail(c.ws, 'Guild is unavailable or you already joined a guild.'); return; }
    g.members[c.accountKey] = 'member'; guildInvites.delete(msg.inviteId); persist(); broadcastGuild(g, c.name + ' joined the guild.'); return;
  }
  if (action === 'decline') { const inv = guildInvites.get(cleanText(msg.inviteId, 80)); if (inv && inv.toKey === c.accountKey) guildInvites.delete(cleanText(msg.inviteId, 80)); return; }
  if (action === 'role') {
    if (!g || g.leaderKey !== c.accountKey) { fail(c.ws, 'Only the guild leader can assign positions.'); return; }
    const member = clients.get(cleanText(msg.memberId, 80)); const key = member?.accountKey || cleanText(msg.memberId, 80); const role = cleanText(msg.role, 32);
    const allowed = ['vice guild leader', 'senior member', 'junior member', 'elite member', 'member'];
    if (!allowed.includes(role) || !g.members[key] || key === g.leaderKey) { fail(c.ws, 'Invalid member or position.'); return; }
    g.members[key] = role; persist(); broadcastGuild(g, 'Guild position updated.'); return;
  }
  if (action === 'leave') {
    if (!g) return;
    if (c.accountKey === g.leaderKey) { for (const key of Object.keys(g.members)) if (key !== g.leaderKey) { g.members[key] = 'guild leader'; g.leaderKey = key; g.leaderName = accounts.get(key)?.characterName || 'Survivor'; break; } if (Object.keys(g.members).length === 1) guilds.delete(g.id); else delete g.members[c.accountKey]; }
    else delete g.members[c.accountKey];
    persist(); sendGuild(c, 'You left the guild.'); if (guilds.has(g.id)) broadcastGuild(g); return;
  }
  fail(c.ws, 'Unknown guild action.');
}
function handlePartyAction(c, msg) {
  const action = cleanText(msg.action, 20); let p = partyOf(c);
  if (action === 'state') { if (p) sendParty(p); else send(c.ws, { type: 'partyState', party: null, text: 'You are not in a party.' }); return; }
  if (action === 'create') { if (p) { sendParty(p, 'You are already in a party.'); return; } p = { id: crypto.randomUUID(), leaderId: c.id, leaderName: c.name, members: [c.id], strategy: 'balanced', formation: { [c.id]: 'front-center' }, roomId: c.roomId }; parties.set(p.id, p); playerParty.set(c.id, p.id); sendParty(p, 'Party created.'); return; }
  if (action === 'invite') {
    if (!p) { p = { id: crypto.randomUUID(), leaderId: c.id, leaderName: c.name, members: [c.id], strategy: 'balanced', formation: { [c.id]: 'front-center' }, roomId: c.roomId }; parties.set(p.id, p); playerParty.set(c.id, p.id); }
    if (p.leaderId !== c.id) { fail(c.ws, 'Only the party leader can invite players.'); return; }
    if (p.members.length >= 10) { fail(c.ws, 'A party can have at most 10 members.'); return; }
    const target = clients.get(cleanText(msg.targetId, 80)); if (!target || !target.joined || target.roomId !== c.roomId || playerParty.has(target.id)) { fail(c.ws, 'That player is unavailable or already in a party.'); return; }
    const inviteId = crypto.randomUUID(); partyInvites.set(inviteId, { partyId: p.id, toId: target.id, expires: Date.now() + 120000 }); send(target.ws, { type: 'partyInvite', inviteId, fromName: c.name }); send(c.ws, { type: 'system', text: 'Party invitation sent.' }); return;
  }
  if (action === 'accept') {
    const inv = partyInvites.get(cleanText(msg.inviteId, 80)); if (!inv || inv.toId !== c.id || inv.expires < Date.now()) { fail(c.ws, 'Party invitation expired.'); return; }
    p = parties.get(inv.partyId); if (!p || p.members.length >= 10 || playerParty.has(c.id)) { fail(c.ws, 'Party is full (10 members maximum) or you are already in a party.'); return; }
    p.members.push(c.id); playerParty.set(c.id, p.id); p.formation = p.formation || {}; const occupied = new Set(Object.values(p.formation)); p.formation[c.id] = ['front-left','front-center','front-right','middle-left','middle-center','middle-right','back-left','back-center','back-right','reserve'].find(pos => !occupied.has(pos)) || 'reserve'; partyInvites.delete(cleanText(msg.inviteId, 80)); const leader = clients.get(p.leaderId);
    if (leader) { c.x = leader.x; c.y = leader.y; send(c.ws, { type: 'partyFollow', leaderId: leader.id, x: leader.x, y: leader.y }); }
    sendParty(p, c.name + ' joined the party.'); return;
  }
  if (action === 'decline') { const inv = partyInvites.get(cleanText(msg.inviteId, 80)); if (inv && inv.toId === c.id) partyInvites.delete(cleanText(msg.inviteId, 80)); return; }
  if (action === 'formation') {
    if (!p || p.leaderId !== c.id) { fail(c.ws, 'Only the party leader can arrange formation.'); return; }
    const memberId = cleanText(msg.memberId, 80), position = cleanText(msg.position, 24);
    const allowed = ['front-left','front-center','front-right','middle-left','middle-center','middle-right','back-left','back-center','back-right','reserve'];
    if (!p.members.includes(memberId) || !allowed.includes(position)) { fail(c.ws, 'Invalid party member or formation slot.'); return; }
    p.formation = p.formation || {};
    const occupant = Object.keys(p.formation).find(id => id !== memberId && p.formation[id] === position);
    if (occupant) p.formation[occupant] = p.formation[memberId] || 'reserve';
    p.formation[memberId] = position; sendParty(p, 'Party formation updated.'); return;
  }
  if (action === 'strategy') { if (!p || p.leaderId !== c.id) { fail(c.ws, 'Only the party leader can change strategy.'); return; } const strategy = ['offensive', 'balanced', 'defensive'].includes(msg.strategy) ? msg.strategy : 'balanced'; p.strategy = strategy; sendParty(p, 'Party strategy changed to ' + strategy + '.'); return; }
  if (action === 'leave') { removePartyMember(c); send(c.ws, { type: 'partyState', party: null, text: 'You left the party.' }); return; }
  fail(c.ws, 'Unknown party action.');
}
async function handleAuth(c, msg) {
  const mode = ['login', 'register', 'resume'].includes(msg.mode) ? msg.mode : 'register';
  const username = cleanText(msg.username, 24), password = String(msg.password || '');
  if (!validUsername(username)) { fail(c.ws, 'Username must be 3–24 characters: letters, numbers, underscore.'); return false; }
  if (mode !== 'resume' && (password.length < 8 || password.length > 128)) { fail(c.ws, 'Password must be 8–128 characters.'); return false; }
  let account = accounts.get(username.toLowerCase());
  if (mode === 'resume') {
    const token = String(msg.token || ''), tokenHash = crypto.createHash('sha256').update(token).digest('hex');
    if (!account || !token || !Array.isArray(account.sessionTokens) || !account.sessionTokens.includes(tokenHash)) { fail(c.ws, 'Saved session expired. Please log in again.'); return false; }
  } else if (mode === 'register') {
    const characterName = cleanText(msg.characterName, 24), race = safeRace(cleanText(msg.race, 20));
    if (!validCharacter(characterName)) { fail(c.ws, 'Character name must be 2–24 characters.'); return false; }
    if (account) { fail(c.ws, 'That username already exists. Please log in instead.'); return false; }
    const salt = crypto.randomBytes(16).toString('hex'), hash = crypto.scryptSync(password, salt, 64).toString('hex');
    account = { username, characterName, race, salt, hash, createdAt: Date.now(), level: 0 };
    accounts.set(username.toLowerCase(), account);
  } else if (mode === 'login') {
    if (!account) { fail(c.ws, 'Account not found. Check your username or create an account.'); return false; }
    const hash = crypto.scryptSync(password, account.salt, 64).toString('hex');
    const savedHash = Buffer.from(String(account.hash || ''), 'hex');
    const submittedHash = Buffer.from(hash, 'hex');
    if (savedHash.length !== submittedHash.length || !crypto.timingSafeEqual(submittedHash, savedHash)) { fail(c.ws, 'Incorrect username or password.'); return false; }
  }
  if (!Array.isArray(account.sessionTokens)) account.sessionTokens = [];
  let sessionToken = '';
  if (mode === 'resume') sessionToken = String(msg.token || '');
  else {
    sessionToken = crypto.randomBytes(32).toString('base64url');
    const tokenHash = crypto.createHash('sha256').update(sessionToken).digest('hex');
    account.sessionTokens = account.sessionTokens.filter(t => t !== tokenHash); account.sessionTokens.push(tokenHash);
    if (account.sessionTokens.length > 8) account.sessionTokens = account.sessionTokens.slice(-8);
    accounts.set(username.toLowerCase(), account);
  }
  // Do not tell the client registration/login succeeded until the latest
  // account record, including its session token hash, has been saved.
  const saved = await persist();
  if (!saved) {
    if (mode === 'register') {
      accounts.delete(username.toLowerCase());
      saveLocalBackup(takeSnapshot());
    }
    fail(c.ws, 'The server could not save your account to the database. Please try again shortly.');
    return false;
  }
  c.accountKey = username.toLowerCase(); c.account = account; c.name = account.characterName; c.race = account.race; c.level = Number(account.level) || 0;
  c.roomId = cleanText(msg.worldId, 48) || 'lost-ruin-main'; c.x = validCoord(Number(msg.x)); c.y = validCoord(Number(msg.y)); c.joined = true; clients.set(c.id, c); const existingGuild = guildForAccount(c.accountKey); if (existingGuild && existingGuild.leaderKey === c.accountKey) { existingGuild.leaderId = c.id; existingGuild.leaderName = c.name; }
  const players = roomMembers(c.roomId).map(publicPlayer);
  send(c.ws, { type: 'welcome', id: c.id, name: c.name, serverName: 'Lost Ruin Online', sessionToken, account: { username: account.username, characterName: account.characterName, race: account.race, gameSave: account.gameSave || null }, players, worldState: worldFor(c.roomId) });
  for (const peer of roomMembers(c.roomId)) if (peer.id !== c.id) send(peer.ws, { type: 'system', text: `${c.name} joined the world.` });
  broadcastPlayers(c.roomId); sendGuild(c); send(c.ws, { type: 'partyState', party: null }); return true;
}
function channelRule(channel) { return ({ tile: { level: 0, cooldown: 3000 }, area: { level: 1, cooldown: 10000 }, region: { level: 20, cooldown: 30000 } })[channel] || null; }

wss.on('connection', (ws) => {
  if (clients.size >= MAX_CLIENTS) { send(ws, { type: 'error', message: 'Server is full. Try again later.' }); ws.close(1013, 'Server full'); return; }
  const c = { ws, id: crypto.randomUUID(), name: 'Survivor', race: 'human', roomId: null, x: 0, y: 0, joined: false, chatAt: {}, account: null, accountKey: null };
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { fail(ws, 'Invalid message format.'); return; }
    if (!msg || typeof msg.type !== 'string') { fail(ws, 'Missing message type.'); return; }
    if (msg.type === 'auth') { if (c.joined) { fail(ws, 'Already authenticated.'); return; } handleAuth(c, msg).catch(error => { console.error('Authentication/storage error:', error.message); fail(ws, 'The server encountered a storage error while processing your account. Please try again later.'); }); return; }
    if (msg.type === 'ping') { send(ws, { type: 'pong', at: Number(msg.at) || Date.now() }); return; }
    if (!c.joined || !c.roomId) { fail(ws, 'Create an account or log in before entering the world.'); return; }
    if (msg.type === 'location') {
      const oldX = c.x, oldY = c.y; c.x = validCoord(Number(msg.x)); c.y = validCoord(Number(msg.y));
      c.level = Math.max(0, Math.min(100000, Math.floor(Number(msg.level) || c.level || 0)));
      if (msg.profile && typeof msg.profile === 'object') c.profile = msg.profile;
      if (msg.equipment && typeof msg.equipment === 'object') c.equipment = msg.equipment;
      const p = partyOf(c);
      if (p && p.leaderId === c.id && (oldX !== c.x || oldY !== c.y)) {
        for (const id of p.members) { if (id === c.id) continue; const peer = clients.get(id); if (peer) { peer.x = c.x; peer.y = c.y; send(peer.ws, { type: 'partyFollow', leaderId: c.id, x: c.x, y: c.y }); } }
      }
      broadcastPlayers(c.roomId); return;
    }
    if (msg.type === 'accountSave') {
      if (!c.account || !c.accountKey) { fail(ws, 'Log in before saving your character.'); return; }
      const incoming = msg.gameSave;
      if (!incoming || typeof incoming !== 'object' || !incoming.player || typeof incoming.player !== 'object') { fail(ws, 'Invalid account save data.'); return; }
      let serialized; try { serialized = JSON.stringify(incoming); } catch { fail(ws, 'Account save could not be encoded.'); return; }
      if (serialized.length > 230000) { fail(ws, 'Account save is too large. Explore less before saving again.'); return; }
      c.account.gameSave = incoming;
      c.account.updatedAt = Date.now();
      accounts.set(c.accountKey, c.account);
      persist().then(ok => { if (!ok) send(ws, { type: 'saveError', message: 'The server could not persist your latest account save. It remains saved on this device.' }); });
      return;
    }
    if (msg.type === 'guildAction') { handleGuildAction(c, msg); return; }
    if (msg.type === 'partyAction') { handlePartyAction(c, msg); return; }
    if (msg.type === 'worldRequest') { sendWorld(c); return; }
    if (msg.type === 'worldSync') {
      const incoming = msg.patch && typeof msg.patch === 'object' ? msg.patch : null;
      if (!incoming) { fail(ws, 'Invalid world update.'); return; }
      const world = worldFor(c.roomId);
      for (const key of ['tileData', 'groundLoot', 'bonfires', 'structures']) { if (!world[key]) world[key] = {}; applyPatch(world[key], validPatch(incoming[key])); }
      world.updatedAt = Date.now(); persist(); for (const peer of roomMembers(c.roomId)) send(peer.ws, { type: 'worldState', state: world }); return;
    }
    if (msg.type === 'chat') {
      const channel = cleanText(msg.channel, 12) || 'tile', rule = channelRule(channel);
      if (!rule) { fail(ws, 'That chat channel is unavailable.'); return; }
      const level = Math.max(0, Math.floor(Number(msg.level) || 0)); if (level < rule.level) { fail(ws, `Reach Level ${rule.level} to use ${channel} chat.`); return; }
      const now = Date.now(), last = c.chatAt[channel] || 0; if (now - last < rule.cooldown) { fail(ws, `Please wait ${Math.ceil((rule.cooldown - (now - last)) / 1000)} seconds before chatting again.`); return; }
      const text = cleanText(msg.text, 240); if (!text) { fail(ws, 'Chat message cannot be empty.'); return; }
      c.chatAt[channel] = now; c.x = validCoord(Number(msg.x)); c.y = validCoord(Number(msg.y));
      for (const peer of roomMembers(c.roomId)) {
        const visible = channel === 'tile' ? (peer.x === c.x && peer.y === c.y) : channel === 'area' ? (Math.abs(peer.x - c.x) <= 5 && Math.abs(peer.y - c.y) <= 5) : channel === 'region' ? (Math.floor(peer.x / 50) === Math.floor(c.x / 50) && Math.floor(peer.y / 50) === Math.floor(c.y / 50)) : false;
        if (visible) send(peer.ws, { type: 'chat', channel, id: c.id, name: c.name, text, at: now });
      }
      return;
    }
    if (msg.type === 'privateChat') {
      const toId = cleanText(msg.toId, 80), target = clients.get(toId), text = cleanText(msg.text, 240);
      if (!target || !target.joined || target.roomId !== c.roomId || target.id === c.id || target.x !== c.x || target.y !== c.y) { fail(ws, 'Private chat requires another player on your exact tile.'); return; }
      if (!text) { fail(ws, 'Private message cannot be empty.'); return; }
      const packet = { type: 'privateChat', fromId: c.id, toId: target.id, fromName: c.name, text, at: Date.now() }; send(c.ws, packet); send(target.ws, packet); return;
    }
    if (msg.type === 'tradeOffer') {
      const toId = cleanText(msg.toId, 80), target = clients.get(toId), itemId = cleanText(msg.itemId, 80), itemName = cleanText(msg.itemName, 80), quantity = Math.floor(Number(msg.quantity) || 0);
      if (!target || !target.joined || target.roomId !== c.roomId || target.id === c.id) { fail(ws, 'That player is no longer available for trading.'); return; }
      if ((itemId && (quantity < 1 || quantity > 999)) || (!itemId && quantity !== 0)) { fail(ws, 'Invalid trade item or quantity.'); return; }
      const coins = Math.max(0, Math.min(1000000000, Math.floor(Number(msg.coins) || 0))), spiritType = ['low', 'medium', 'high', 'supreme'].includes(msg.spiritType) ? msg.spiritType : '', spiritQty = Math.max(0, Math.min(999999, Math.floor(Number(msg.spiritQty) || 0)));
      if (!itemId && !coins && !spiritQty) { fail(ws, 'Offer must include an item, coins, or spirit stones.'); return; }
      const offer = { id: crypto.randomUUID(), fromId: c.id, fromName: c.name, toId: target.id, toName: target.name, itemId, itemName, quantity, itemFrom: { itemId, itemName, quantity, coins, spiritType, spiritQty }, itemTo: { itemId: '', itemName: '', quantity: 0, coins: 0, spiritType: '', spiritQty: 0 }, coins, spiritType, spiritQty, stage: 'proposed', confirmed: {}, createdAt: Date.now() };
      tradeOffers.set(offer.id, offer); send(target.ws, { type: 'tradeOffer', offer }); send(ws, { type: 'tradeUpdate', offerId: offer.id, text: 'Trade offer sent to ' + target.name + '.' }); return;
    }
    if (msg.type === 'tradeRespond') {
      const offer = tradeOffers.get(cleanText(msg.offerId, 80)); if (!offer || offer.toId !== c.id || offer.stage !== 'proposed') { fail(ws, 'That trade offer is no longer available.'); return; }
      const sender = clients.get(offer.fromId);
      if (!sender || !sender.joined) { tradeOffers.delete(offer.id); send(ws, { type: 'tradeUpdate', offerId: offer.id, text: 'The other player disconnected.' }); return; }
      if (msg.accept !== true) { tradeOffers.delete(offer.id); send(sender.ws, { type: 'tradeUpdate', offerId: offer.id, text: c.name + ' rejected the trade offer.' }); send(ws, { type: 'tradeUpdate', offerId: offer.id, text: 'Trade offer rejected.' }); return; }
      const itemId = cleanText(msg.itemId, 80), itemName = cleanText(msg.itemName, 80), quantity = Math.max(0, Math.min(999, Math.floor(Number(msg.quantity) || 0))), coins = Math.max(0, Math.min(1000000000, Math.floor(Number(msg.coins) || 0))), spiritType = ['low', 'medium', 'high', 'supreme'].includes(msg.spiritType) ? msg.spiritType : '', spiritQty = Math.max(0, Math.min(999999, Math.floor(Number(msg.spiritQty) || 0)));
      offer.itemTo = { itemId, itemName, quantity, coins, spiritType, spiritQty }; offer.stage = 'confirm'; offer.confirmed = {}; tradeOffers.set(offer.id, offer);
      const packet = { type: 'tradeReview', offer }; send(sender.ws, packet); send(c.ws, packet); return;
    }
    if (msg.type === 'tradeConfirm') {
      const offer = tradeOffers.get(cleanText(msg.offerId, 80)); if (!offer || offer.stage !== 'confirm' || ![offer.fromId, offer.toId].includes(c.id)) { fail(ws, 'This trade is no longer awaiting confirmation.'); return; }
      const otherId = c.id === offer.fromId ? offer.toId : offer.fromId, other = clients.get(otherId), sender = clients.get(offer.fromId);
      if (msg.confirm !== true) { tradeOffers.delete(offer.id); for (const id of [offer.fromId, offer.toId]) { const peer = clients.get(id); if (peer) send(peer.ws, { type: 'tradeUpdate', offerId: offer.id, text: 'Trade cancelled by ' + c.name + '.' }); } return; }
      offer.confirmed[c.id] = true; tradeOffers.set(offer.id, offer);
      if (!offer.confirmed[offer.fromId] || !offer.confirmed[offer.toId]) { const packet = { type: 'tradeReview', offer }; send(c.ws, packet); if (other) send(other.ws, packet); return; }
      tradeOffers.delete(offer.id);
      const payload = { type: 'tradeAccepted', text: 'Both players confirmed the trade.', offerId: offer.id, fromId: offer.fromId, toId: offer.toId, transfers: [{ fromId: offer.fromId, toId: offer.toId, ...offer.itemFrom }, { fromId: offer.toId, toId: offer.fromId, ...offer.itemTo }] };
      if (sender) send(sender.ws, payload); if (other && other !== sender) send(other.ws, payload); return;
    }
    fail(ws, 'Unknown message type.');
  });
  ws.on('close', () => {
    if (c.joined) { removePartyMember(c); clients.delete(c.id); const g = c.accountKey ? guildForAccount(c.accountKey) : null; if (g) broadcastGuild(g); for (const peer of roomMembers(c.roomId)) send(peer.ws, { type: 'system', text: `${c.name} left the world.` }); broadcastPlayers(c.roomId); }
  });
  ws.on('error', () => {
    if (c.joined) { removePartyMember(c); clients.delete(c.id); broadcastPlayers(c.roomId); const g = c.accountKey ? guildForAccount(c.accountKey) : null; if (g) broadcastGuild(g); }
  });
});

initializeStorage()
  .then(() => {
    server.listen(PORT, HOST, () => {
      console.log(`Lost Ruin multiplayer listening on ${HOST}:${PORT}`);
      console.log('Storage mode:', storageMode);
    });
  })
  .catch(error => {
    console.error('CRITICAL: Storage initialization failed:', error.message);
    console.error('Server will not start until storage is working.');
    process.exitCode = 1;
  });

async function shutdown() {
  for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
  try { await persistenceQueue; } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 3000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
