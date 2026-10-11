'use strict';

// Lost Ruin multiplayer foundation: guest presence + world chat.
// This is a development scaffold, not a production-secure game backend.
const http = require('node:http');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const HOST = process.env.HOST || '127.0.0.1';
const PORT = Number(process.env.PORT || 8080);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 100);
const MAX_MESSAGE_BYTES = 8 * 1024;
const CHAT_LIMIT = 8;
const CHAT_WINDOW_MS = 10_000;
const clients = new Map();

const server = http.createServer((req, res) => {
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: true, service: 'lost-ruin-multiplayer', clients: clients.size, rooms: new Set([...clients.values()].map(c => c.roomId)).size }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
  res.end('Lost Ruin multiplayer server is running. WebSocket endpoint: /\nHealth: /health\n');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE_BYTES, perMessageDeflate: false });
const cleanText = (value, max) => String(value ?? '').replace(/[<>\u0000-\u001f\u007f]/g, '').trim().slice(0, max);
const validCoord = value => Number.isFinite(value) ? Math.max(-1_000_000, Math.min(1_000_000, Math.floor(value))) : 0;

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
}
function roomMembers(roomId) {
  return [...clients.values()].filter(c => c.roomId === roomId && c.ws.readyState === WebSocket.OPEN);
}
function publicPlayer(client) {
  return { id: client.id, name: client.name, x: client.x, y: client.y };
}
function broadcastPlayers(roomId) {
  const players = roomMembers(roomId).map(publicPlayer);
  for (const client of roomMembers(roomId)) send(client.ws, { type: 'players', players });
}
function leaveRoom(client) {
  if (!client.roomId) return;
  const oldRoom = client.roomId;
  clients.delete(client.id);
  client.roomId = null;
  broadcastPlayers(oldRoom);
}
function fail(ws, message) { send(ws, { type: 'error', message }); }

wss.on('connection', (ws, req) => {
  if (clients.size >= MAX_CLIENTS) {
    send(ws, { type: 'error', message: 'Server is full. Try again later.' });
    ws.close(1013, 'Server full');
    return;
  }
  const state = { ws, id: crypto.randomUUID(), name: 'Survivor', roomId: null, x: 0, y: 0, joined: false, chatTimes: [] };
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { fail(ws, 'Invalid message format.'); return; }
    if (!msg || typeof msg.type !== 'string') { fail(ws, 'Missing message type.'); return; }

    if (msg.type === 'join') {
      if (state.joined) { fail(ws, 'You already joined this session.'); return; }
      const version = Number(msg.version);
      if (version !== 1) { fail(ws, 'Client/server protocol mismatch. Refresh the game and try again.'); return; }
      const worldId = cleanText(msg.worldId, 48) || 'lost-ruin-main';
      state.roomId = worldId;
      state.name = cleanText(msg.name, 24) || 'Survivor';
      state.x = validCoord(Number(msg.x));
      state.y = validCoord(Number(msg.y));
      state.joined = true;
      clients.set(state.id, state);
      const players = roomMembers(state.roomId).map(publicPlayer);
      send(ws, { type: 'welcome', id: state.id, name: state.name, players });
      for (const peer of roomMembers(state.roomId)) {
        if (peer.id !== state.id) send(peer.ws, { type: 'system', text: `${state.name} joined the world.` });
      }
      broadcastPlayers(state.roomId);
      return;
    }

    if (msg.type === 'ping') { send(ws, { type: 'pong', at: Number(msg.at) || Date.now() }); return; }
    if (!state.joined || !state.roomId) { fail(ws, 'Join the world before sending this message.'); return; }

    if (msg.type === 'location') {
      state.x = validCoord(Number(msg.x));
      state.y = validCoord(Number(msg.y));
      broadcastPlayers(state.roomId);
      return;
    }
    if (msg.type === 'chat') {
      const now = Date.now();
      state.chatTimes = state.chatTimes.filter(t => now - t < CHAT_WINDOW_MS);
      if (state.chatTimes.length >= CHAT_LIMIT) { fail(ws, 'Chat rate limit reached. Please wait a moment.'); return; }
      const text = cleanText(msg.text, 240);
      if (!text) { fail(ws, 'Chat message cannot be empty.'); return; }
      state.chatTimes.push(now);
      for (const peer of roomMembers(state.roomId)) {
        if (peer.id !== state.id) send(peer.ws, { type: 'chat', id: state.id, name: state.name, text, at: now });
      }
      return;
    }
    if (msg.type === 'trade' || msg.type === 'encounter') {
      fail(ws, 'This interaction is not implemented yet. Trading and PvP require server-authoritative game rules.');
      return;
    }
    fail(ws, 'Unknown message type.');
  });

  ws.on('close', () => { if (state.joined) leaveRoom(state); });
  ws.on('error', () => { if (state.joined) leaveRoom(state); });
});

server.listen(PORT, HOST, () => {
  console.log(`Lost Ruin multiplayer server listening on http://${HOST}:${PORT}`);
  console.log(`WebSocket URL: ws://${HOST}:${PORT}`);
  console.log(`Health check: http://${HOST}:${PORT}/health`);
  if (HOST === '127.0.0.1') console.log('Local development only. Set HOST=0.0.0.0 only when you understand your network/firewall setup.');
});

function shutdown() {
  for (const client of wss.clients) client.close(1001, 'Server shutting down');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 3000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
