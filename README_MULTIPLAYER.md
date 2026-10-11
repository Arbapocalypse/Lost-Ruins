# Lost Ruin — Multiplayer Foundation (v0.1)

This milestone adds a separate Node.js WebSocket server and a Multiplayer screen inside the existing game.

## Included
- Guest connection with a display name
- Online player presence and coarse map tile coordinates
- Shared world chat between connected clients
- Connection status, disconnect button, chat rate limiting, message length limits, and a health endpoint
- Existing local game save key (`lostRuinAutoSave`) remains unchanged; online name/server preferences use separate keys
- The single-player game can still be played without connecting

## Not included yet
- Real account registration, authentication, or persistent player profiles
- Server-authoritative world state, movement, inventory, combat, PvP, or player-to-player item transfers
- Database, moderation/report tools, deployment automation, TLS termination, or production security review

Do not treat client-reported coordinates as trusted. This scaffold is for local development and protocol work, not an internet-ready public game server.

## Requirements
- Node.js 20 or newer
- npm

## Run locally
1. Keep `index.html`, `server.js`, and `package.json` in the same folder.
2. Open a terminal in that folder and run:
   ```sh
   npm install
   npm start
   ```
3. Open `index.html` in your browser.
4. Open **More → Multiplayer**.
5. Use `ws://localhost:8080`, enter a display name, and click **Connect**.
6. Open the game in another browser window/tab and connect again with a different display name to test shared presence and chat.

Health check: `http://127.0.0.1:8080/health`

## Hosting notes
For two people on different computers, the server must be deployed on a reachable host and exposed over a secure WebSocket URL (`wss://...`). The server currently defaults to `127.0.0.1` intentionally so it only listens on the local machine. Hosting it publicly requires a deliberate bind/firewall/reverse-proxy setup and TLS. Do not simply expose this development scaffold to the internet.
