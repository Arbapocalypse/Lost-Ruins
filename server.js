#!/usr/bin/env node
'use strict';

// LOST RUIN multiplayer prototype.
// Requires Node.js and: npm install ws
// Set ACCOUNT_FILE to persistent storage on your host. Do not use a temporary filesystem for real accounts.
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');

const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || 8080);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 100);
const MAX_MESSAGE_BYTES = 8 * 1024;
const CHAT_LIMIT = 8;
const CHAT_WINDOW_MS = 10_000;
const SERVER_NAME = process.env.SERVER_NAME || 'Lost Ruin · Main Survival';
const ACCOUNT_FILE = process.env.ACCOUNT_FILE || path.join(__dirname, 'accounts.json');
const clients = new Map();
const accounts = new Map();
const pendingTrades = new Map();
const validRaces = new Set(['human', 'orc', 'goblin', 'elf', 'dragonkin', 'dwarf']);

function loadAccounts() {
  try {
    const data = JSON.parse(fs.readFileSync(ACCOUNT_FILE, 'utf8'));
    for (const [username, account] of Object.entries(data || {})) {
      if (account && account.passwordHash && account.salt) accounts.set(username, account);
    }
  } catch (err) {
    if (err.code !== 'ENOENT') console.error('Could not load accounts:', err.message);
  }
}
function saveAccounts() {
  const temp = ACCOUNT_FILE + '.tmp';
  const obj = Object.fromEntries(accounts.entries());
  fs.writeFileSync(temp, JSON.stringify(obj, null, 2), { mode: 0o600 });
  fs.renameSync(temp, ACCOUNT_FILE);
}
loadAccounts();

const server = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: true, service: 'lost-ruin-multiplayer', serverName: SERVER_NAME,
      clients: clients.size, accounts: accounts.size, rooms: new Set([...clients.values()].map(c => c.roomId)).size }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
  res.end(`Lost Ruin multiplayer server: ${SERVER_NAME}\nWebSocket endpoint: /\nHealth: /health\n`);
});
const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE_BYTES, perMessageDeflate: false });
const cleanText = (value, max) => String(value ?? '').replace(/[<>\u0000-\u001f\u007f]/g, '').trim().slice(0, max);
const validCoord = value => Number.isFinite(value) ? Math.max(-1_000_000, Math.min(1_000_000, Math.floor(value))) : 0;
const usernameValid = value => /^[a-zA-Z0-9_]{3,24}$/.test(value);
const nameValid = value => /^[\p{L}0-9 _'-]{2,24}$/u.test(value);
function send(ws, message) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message)); }
function fail(ws, message) { send(ws, { type: 'error', message }); }
function roomMembers(roomId) { return [...clients.values()].filter(c => c.roomId === roomId && c.ws.readyState === WebSocket.OPEN); }
function publicPlayer(c) { return { id: c.id, name: c.characterName, username: c.username, race: c.race, x: c.x, y: c.y, day: c.day, hour: c.hour, terrain: c.terrain }; }
function broadcastPlayers(roomId) {
  const players = roomMembers(roomId).map(publicPlayer);
  for (const c of roomMembers(roomId)) send(c.ws, { type: 'players', players });
}
function broadcast(roomId, message, exceptId = null) {
  for (const c of roomMembers(roomId)) if (c.id !== exceptId) send(c.ws, message);
}
function leaveRoom(client) {
  if (!client.roomId) return;
  const oldRoom = client.roomId;
  clients.delete(client.id);
  client.roomId = null;
  broadcast(oldRoom, { type: 'system', text: `${client.characterName} left the world.` });
  broadcastPlayers(oldRoom);
  for (const [id, offer] of pendingTrades) {
    if (offer.fromId === client.id || offer.toId === client.id) {
      pendingTrades.delete(id);
      const other = clients.get(offer.fromId === client.id ? offer.toId : offer.fromId);
      if (other) send(other.ws, { type: 'tradeUpdate', offerId: id, text: 'Trade cancelled because a player disconnected.' });
    }
  }
}
function passwordHash(password, salt) {
  return crypto.scryptSync(password, salt, 64).toString('hex');
}
function safeAccount(username, account) {
  return { username, characterName: account.characterName, race: account.race };
}
function joinAuthenticated(state, account, username, roomId, x, y, day, hour, terrain) {
  // A single account can have only one active session.
  for (const other of clients.values()) {
    if (other.username === username) {
      send(other.ws, { type: 'error', message: 'This account is already online. Log out from the other session first.' });
      other.ws.close(4001, 'Account already active');
    }
  }
  state.username = username;
  state.characterName = account.characterName;
  state.race = account.race;
  state.roomId = cleanText(roomId, 48) || 'lost-ruin-main';
  state.x = validCoord(Number(x)); state.y = validCoord(Number(y));
  state.day = Math.max(1, Math.floor(Number(day) || 1));
  state.hour = Math.max(0, Math.min(23, Math.floor(Number(hour) || 8)));
  state.terrain = cleanText(terrain, 20) || 'unknown';
  state.joined = true;
  clients.set(state.id, state);
  send(state.ws, { type: 'welcome', id: state.id, name: state.characterName, account: safeAccount(username, account), serverName: SERVER_NAME,
    players: roomMembers(state.roomId).map(publicPlayer) });
  broadcast(state.roomId, { type: 'system', text: `${state.characterName} (${state.race}) joined the world.` }, state.id);
  broadcastPlayers(state.roomId);
}
function itemIdValid(value) { return /^[a-zA-Z0-9_ -]{1,64}$/.test(String(value || '')); }

wss.on('connection', ws => {
  if (clients.size >= MAX_CLIENTS) {
    send(ws, { type: 'error', message: 'Server is full. Try again later.' });
    ws.close(1013, 'Server full');
    return;
  }
  const state = { ws, id: crypto.randomUUID(), username: '', characterName: 'Survivor', race: 'human',
    roomId: null, x: 0, y: 0, day: 1, hour: 8, terrain: 'unknown', joined: false, chatTimes: [] };
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { fail(ws, 'Invalid message format.'); return; }
    if (!msg || typeof msg.type !== 'string') { fail(ws, 'Missing message type.'); return; }

    if (msg.type === 'auth') {
      if (state.joined || state.authAttempted) { fail(ws, 'This connection has already authenticated.'); return; }
      state.authAttempted = true;
      if (Number(msg.version) !== 2) { fail(ws, 'Client/server version mismatch. Use the updated HTML and server.js together.'); ws.close(1002, 'Version mismatch'); return; }
      const username = String(msg.username || '').trim();
      const password = String(msg.password || '');
      const mode = msg.mode === 'login' ? 'login' : 'register';
      if (!usernameValid(username)) { fail(ws, 'Username must be 3–24 characters: letters, numbers, underscore.'); return; }
      if (password.length < 8 || password.length > 128) { fail(ws, 'Password must be 8–128 characters.'); return; }
      let account = accounts.get(username);
      if (mode === 'register') {
        if (account) { fail(ws, 'That username already exists. Please log in.'); return; }
        const characterName = cleanText(msg.characterName, 24);
        const race = String(msg.race || 'human');
        if (!nameValid(characterName)) { fail(ws, 'Character name must be 2–24 letters, numbers, spaces, apostrophes, underscores, or hyphens.'); return; }
        if (!validRaces.has(race)) { fail(ws, 'Choose a valid race.'); return; }
        const salt = crypto.randomBytes(16).toString('hex');
        account = { salt, passwordHash: passwordHash(password, salt), characterName, race, createdAt: new Date().toISOString() };
        accounts.set(username, account);
        try { saveAccounts(); } catch (err) { accounts.delete(username); fail(ws, 'Account could not be saved on the server. Check persistent storage permissions.'); return; }
      } else {
        if (!account || !crypto.timingSafeEqual(Buffer.from(passwordHash(password, account.salt), 'hex'), Buffer.from(account.passwordHash, 'hex'))) {
          fail(ws, 'Incorrect username or password.'); return;
        }
      }
      joinAuthenticated(state, account, username, msg.worldId, msg.x, msg.y, msg.day, msg.hour, msg.terrain);
      return;
    }
    if (msg.type === 'ping') { send(ws, { type: 'pong', at: Number(msg.at) || Date.now() }); return; }
    if (!state.joined || !state.roomId) { fail(ws, 'Create an account or log in first.'); return; }

    if (msg.type === 'location') {
      const oldX = state.x, oldY = state.y;
      state.x = validCoord(Number(msg.x)); state.y = validCoord(Number(msg.y));
      state.day = Math.max(1, Math.floor(Number(msg.day) || state.day));
      state.hour = Math.max(0, Math.min(23, Math.floor(Number(msg.hour) || 0)));
      state.terrain = cleanText(msg.terrain, 20) || state.terrain;
      if (oldX !== state.x || oldY !== state.y) {
        broadcast(state.roomId, { type: 'worldEvent', text: `${state.characterName} traveled to ${state.terrain} at (${state.x}, ${state.y}).` }, state.id);
        broadcastPlayers(state.roomId);
      }
      return;
    }
    if (msg.type === 'worldAction') {
      const action = cleanText(msg.action, 80);
      if (!action) return;
      broadcast(state.roomId, { type: 'worldEvent', text: `${state.characterName} ${action} at (${state.x}, ${state.y}).` }, state.id);
      return;
    }
    if (msg.type === 'chat') {
      const now = Date.now();
      state.chatTimes = state.chatTimes.filter(t => now - t < CHAT_WINDOW_MS);
      if (state.chatTimes.length >= CHAT_LIMIT) { fail(ws, 'Chat rate limit reached. Please wait a moment.'); return; }
      const text = cleanText(msg.text, 240);
      if (!text) { fail(ws, 'Chat message cannot be empty.'); return; }
      state.chatTimes.push(now);
      broadcast(state.roomId, { type: 'chat', id: state.id, name: state.characterName, text, at: now }, state.id);
      return;
    }
    if (msg.type === 'tradeOffer') {
      const to = clients.get(String(msg.toId || ''));
      const itemId = String(msg.itemId || '');
      const itemName = cleanText(msg.itemName, 64);
      const quantity = Math.floor(Number(msg.quantity));
      if (!to || to.roomId !== state.roomId || to.id === state.id) { fail(ws, 'That player is not online in this server.'); return; }
      if (Math.abs(to.x - state.x) + Math.abs(to.y - state.y) > 1) { fail(ws, 'Move next to the player before trading.'); return; }
      if (!itemIdValid(itemId) || !itemName || quantity < 1 || quantity > 999) { fail(ws, 'Invalid trade item or quantity.'); return; }
      const id = crypto.randomUUID();
      const offer = { id, fromId: state.id, fromName: state.characterName, toId: to.id, toName: to.characterName, itemId, itemName, quantity, createdAt: Date.now() };
      pendingTrades.set(id, offer);
      send(to.ws, { type: 'tradeOffer', offer });
      send(ws, { type: 'system', text: `Trade offer sent to ${to.characterName}.` });
      return;
    }
    if (msg.type === 'tradeRespond') {
      const offer = pendingTrades.get(String(msg.offerId || ''));
      if (!offer || offer.toId !== state.id) { fail(ws, 'That trade offer is no longer available.'); return; }
      pendingTrades.delete(offer.id);
      const from = clients.get(offer.fromId);
      if (!from || from.roomId !== state.roomId) { fail(ws, 'The other player disconnected; trade cancelled.'); return; }
      if (msg.accept !== true) {
        send(from.ws, { type: 'tradeUpdate', offerId: offer.id, text: `${state.characterName} rejected the trade.` });
        send(ws, { type: 'tradeUpdate', offerId: offer.id, text: 'You rejected the trade.' });
        return;
      }
      if (Math.abs(from.x - state.x) + Math.abs(from.y - state.y) > 1) {
        send(from.ws, { type: 'tradeUpdate', offerId: offer.id, text: 'Trade cancelled: players are no longer nearby.' });
        send(ws, { type: 'tradeUpdate', offerId: offer.id, text: 'Trade cancelled: players are no longer nearby.' });
        return;
      }
      const payload = {
        type: 'tradeAccepted', offerId: offer.id,
        give: { fromId: from.id, toId: state.id, itemId: offer.itemId, itemName: offer.itemName, quantity: offer.quantity },
        receive: { fromId: from.id, toId: state.id, itemId: offer.itemId, itemName: offer.itemName, quantity: offer.quantity },
        text: `${from.characterName} and ${state.characterName} completed a trade for ${offer.quantity} × ${offer.itemName}.`
      };
      send(from.ws, payload); send(state.ws, payload);
      broadcast(state.roomId, { type: 'worldEvent', text: `${from.characterName} traded with ${state.characterName}.` });
      return;
    }
    fail(ws, 'Unknown message type.');
  });
  ws.on('close', () => { if (state.joined) leaveRoom(state); });
  ws.on('error', () => { if (state.joined) leaveRoom(state); });
});
server.listen(PORT, HOST, () => {
  console.log(`Lost Ruin multiplayer server "${SERVER_NAME}" listening on http://${HOST}:${PORT}`);
  console.log(`WebSocket URL: ws://${HOST}:${PORT}`);
  console.log(`Health check: http://${HOST}:${PORT}/health`);
  console.log(`Accounts stored in: ${ACCOUNT_FILE}`);
});
function shutdown() {
  for (const client of wss.clients) client.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 3000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
