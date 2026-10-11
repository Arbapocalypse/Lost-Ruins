LOST RUIN — LOGIN, PLAYER INTERACTIONS, TRADE & CURRENCY UPDATE

FILES
- LOST_RUIN_home_auth_update.html : updated single-file mobile game
- server.js : matching multiplayer WebSocket backend
- package.json : Node server dependency manifest

IMPORTANT: LOGIN FIX / DEPLOYMENT
The HTML alone cannot repair login on an old or different live server. The website's configured multiplayer address must point to this matching server.js version. Deploy/redeploy server.js and package.json to your Node hosting service, then set persistent disk storage with DATA_DIR pointing to that disk. Without persistent storage, accounts may disappear after a host restart/redeploy. An account created on a different server or on a server whose database was reset will not exist on this server; it must be recreated there.

Server setup
1. Upload server.js and package.json to your Node hosting service.
2. Run `npm install` and start with `npm start` (or `node server.js`).
3. Set DATA_DIR to a persistent disk/folder on the hosting provider. Do not rely on an ephemeral deployment filesystem for account storage.
4. Set the multiplayer WebSocket URL in the HTML's MP_SERVERS object and server selector to the deployed secure WebSocket URL (wss://...).
5. Test /health on the host. It should return JSON with `ok:true`.

FEATURES IN THIS UPDATE
- Password show/hide eye control.
- Login form remembers the last username on the same device and no longer erases it just by switching to Login.
- Old screen-level “Back to Home” buttons now return to the game map; the dedicated Home navigation still opens mode selection.
- Same-tile players get Inspect, Private Chat, and Trade actions.
- Public profile fields respect the character's visibility settings, including visible equipment.
- Player trade proposals show both sides; the recipient can add an item, copper-value coins, or spirit stones in return. Both players must separately confirm before the server announces completion.
- Currency uses copper-value internally: 100 copper = 1 silver, 100 silver = 1 gold. The wallet displays gold/silver/copper breakdown.
- Spirit stones have four grades (low, medium, high, supreme), can be traded, and have prototype consume/recovery and XP-absorption actions.

IMPORTANT PROTOTYPE LIMITATION
The current game inventory and currency are still stored client-side. The server relays the two-person trade confirmation but does not yet authoritatively own or validate inventory, balances, or spirit stones. This is suitable for testing UI flow, not a cheat-resistant public economy. Before a public launch, inventory, currency, equipment, spirit stones, and trade settlement should all be stored and validated on the server. Spirit-stone upgrade/alchemy/training recipes are not yet integrated into every game system.

Password notes
- Passwords are stored server-side as salted scrypt hashes, not plain text.
- Do not use a real-life password for this prototype.
