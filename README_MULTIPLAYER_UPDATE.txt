LOST RUIN - Shared World Multiplayer Update

Files
- LOST_RUIN_home_auth_update.html: updated game client
- server.js: Node.js multiplayer server with account login/registration and persistent shared-world state

Client fixes/features
- Reloading always opens the Single Player / Multiplayer home screen.
- Multiplayer account creation shows a success alert after the server confirms success.
- A welcome toast appears after successful login/registration and fades after four seconds.
- Switching to Login clears the account-creation fields.
- Removed the separate World Chat section from the Multiplayer screen and moved player trade offers to the Trade screen.
- Chat header is simply “Chat”; World chat is removed. Region, Area and Tile remain.
- Shared-world sync sends changed tile records, dropped loot, bonfires and settlement/building/territory records to the server.

Deploying the server
1. Deploy server.js as the backend entry point in your Node.js hosting service.
2. Install dependency: npm install ws
3. Start command: node server.js
4. Configure DATA_DIR to a persistent disk directory. Without a persistent disk, account/world data may be lost when the host restarts or redeploys.
5. Configure the HTML server URL to the WebSocket URL of this deployment if it differs from the existing server.

Important limitations
- The shared-world backend stores world records in persistent JSON and broadcasts changes, but this is an early multiplayer foundation. For a production game, add transactional database storage, validation of every game action, anti-cheat checks, rate limits, and server-authoritative inventory/building/territory rules.
- Player trade offers are relayed and client-side inventory changes remain prototype-level; they are not secure against cheating.
- The existing hosted endpoint will not gain shared-world support until this server update is deployed there. Updating the HTML alone cannot change the remote backend.
