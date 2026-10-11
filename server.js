'use strict';
// LOST RUIN persistent multiplayer server: account auth, presence, chat and shared-world storage.
// Requires Node.js 18+ and `npm install ws`. Store DATA_DIR on a persistent disk in production.
const http = require('node:http');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');
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
function readStore(){try{const data=JSON.parse(fs.readFileSync(DATA_FILE,'utf8'));for(const [k,v] of Object.entries(data.accounts||{}))accounts.set(k,v);for(const [k,v] of Object.entries(data.worlds||{}))sharedWorlds.set(k,v);}catch(e){if(e.code!=='ENOENT')console.error('Could not read persistent data:',e.message);}}
function persist(){try{fs.mkdirSync(DATA_DIR,{recursive:true});const temp=DATA_FILE+'.tmp';fs.writeFileSync(temp,JSON.stringify({accounts:Object.fromEntries(accounts),worlds:Object.fromEntries(sharedWorlds)}));fs.renameSync(temp,DATA_FILE);}catch(e){console.error('Persistence failed:',e.message);}}
readStore();
const server=http.createServer((req,res)=>{if(req.url==='/health'&&req.method==='GET'){res.writeHead(200,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});res.end(JSON.stringify({ok:true,service:'lost-ruin-multiplayer',clients:clients.size,accounts:accounts.size,worlds:sharedWorlds.size}));return;}res.writeHead(200,{'content-type':'text/plain; charset=utf-8','cache-control':'no-store'});res.end('Lost Ruin multiplayer server is running. WebSocket endpoint: /\nHealth: /health\n');});
const wss=new WebSocketServer({server,maxPayload:MAX_MESSAGE_BYTES,perMessageDeflate:false});
const cleanText=(v,max)=>String(v??'').replace(/[<>\u0000-\u001f\u007f]/g,'').trim().slice(0,max);
const validCoord=v=>Number.isFinite(v)?Math.max(-1_000_000,Math.min(1_000_000,Math.floor(v))):0;
const validUsername=v=>/^[a-zA-Z0-9_]{3,24}$/.test(v);
const validCharacter=v=>/^[\p{L}0-9 _'-]{2,24}$/u.test(v);
const safeRace=v=>['human','orc','goblin','elf','dragonkin','dwarf'].includes(v)?v:'human';
function send(ws,msg){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));}
function fail(ws,message){send(ws,{type:'error',message});}
function roomMembers(room){return [...clients.values()].filter(c=>c.joined&&c.roomId===room&&c.ws.readyState===WebSocket.OPEN);}
function publicPlayer(c){return{id:c.id,name:c.name,race:c.race,x:c.x,y:c.y,level:c.level||0};}
function broadcastPlayers(room){const players=roomMembers(room).map(publicPlayer);for(const c of roomMembers(room))send(c.ws,{type:'players',players});}
function worldFor(id){if(!sharedWorlds.has(id))sharedWorlds.set(id,{tileData:{},groundLoot:{},bonfires:{},structures:{},updatedAt:Date.now()});return sharedWorlds.get(id);}
function sendWorld(c){send(c.ws,{type:'worldState',state:worldFor(c.roomId)});}
function applyPatch(target,patch){if(!patch||typeof patch!=='object'||Array.isArray(patch))return;for(const [key,value] of Object.entries(patch)){if(!/^-?\d+,-?\d+$/.test(key)&&key.length>100)continue;if(value===null)delete target[key];else target[key]=value;}}
function validPatch(patch){if(!patch||typeof patch!=='object'||Array.isArray(patch))return {};const out={};for(const [k,v] of Object.entries(patch).slice(0,15000)){if(k.length>100)continue;const raw=JSON.stringify(v);if(raw&&raw.length<120000)out[k]=v===null?null:v;}return out;}
function handleAuth(c,msg){
 const mode=msg.mode==='login'?'login':'register',username=cleanText(msg.username,24),password=String(msg.password||'');
 if(!validUsername(username)){fail(c.ws,'Username must be 3–24 characters: letters, numbers, underscore.');return false;}
 if(password.length<8||password.length>128){fail(c.ws,'Password must be 8–128 characters.');return false;}
 let account=accounts.get(username.toLowerCase());
 if(mode==='register'){
  const characterName=cleanText(msg.characterName,24),race=safeRace(cleanText(msg.race,20));
  if(!validCharacter(characterName)){fail(c.ws,'Character name must be 2–24 characters.');return false;}
  if(account){fail(c.ws,'That username already exists. Please log in instead.');return false;}
  const salt=crypto.randomBytes(16).toString('hex');const hash=crypto.scryptSync(password,salt,64).toString('hex');
  account={username,characterName,race,salt,hash,createdAt:Date.now(),level:0};accounts.set(username.toLowerCase(),account);persist();
 }else{
  if(!account){fail(c.ws,'Account not found. Check your username or create an account.');return false;}
  const hash=crypto.scryptSync(password,account.salt,64).toString('hex');if(!crypto.timingSafeEqual(Buffer.from(hash,'hex'),Buffer.from(account.hash,'hex'))){fail(c.ws,'Incorrect username or password.');return false;}
 }
 c.accountKey=username.toLowerCase();c.account=account;c.name=account.characterName;c.race=account.race;c.level=Number(account.level)||0;c.roomId=cleanText(msg.worldId,48)||'lost-ruin-main';c.x=validCoord(Number(msg.x));c.y=validCoord(Number(msg.y));c.joined=true;clients.set(c.id,c);
 const players=roomMembers(c.roomId).map(publicPlayer);send(c.ws,{type:'welcome',id:c.id,name:c.name,serverName:'Lost Ruin Online',account:{username:account.username,characterName:account.characterName,race:account.race},players,worldState:worldFor(c.roomId)});
 for(const peer of roomMembers(c.roomId))if(peer.id!==c.id)send(peer.ws,{type:'system',text:`${c.name} joined the world.`});broadcastPlayers(c.roomId);return true;
}
function channelRule(channel){return ({tile:{level:0,cooldown:3000},area:{level:1,cooldown:10000},region:{level:20,cooldown:30000}})[channel]||null;}
wss.on('connection',(ws)=>{
 if(clients.size>=MAX_CLIENTS){send(ws,{type:'error',message:'Server is full. Try again later.'});ws.close(1013,'Server full');return;}
 const c={ws,id:crypto.randomUUID(),name:'Survivor',race:'human',roomId:null,x:0,y:0,joined:false,chatAt:{},account:null,accountKey:null};
 ws.on('message',raw=>{let msg;try{msg=JSON.parse(raw.toString());}catch{fail(ws,'Invalid message format.');return;}if(!msg||typeof msg.type!=='string'){fail(ws,'Missing message type.');return;}
  if(msg.type==='auth'){if(c.joined){fail(ws,'Already authenticated.');return;}handleAuth(c,msg);return;}
  if(msg.type==='ping'){send(ws,{type:'pong',at:Number(msg.at)||Date.now()});return;}
  if(!c.joined||!c.roomId){fail(ws,'Create an account or log in before entering the world.');return;}
  if(msg.type==='location'){c.x=validCoord(Number(msg.x));c.y=validCoord(Number(msg.y));c.level=Math.max(0,Math.min(100000,Math.floor(Number(msg.level)||c.level||0)));broadcastPlayers(c.roomId);return;}
  if(msg.type==='worldRequest'){sendWorld(c);return;}
  if(msg.type==='worldSync'){
   const incoming=msg.patch&&typeof msg.patch==='object'?msg.patch:null;
   if(!incoming){fail(ws,'Invalid world update.');return;}
   const world=worldFor(c.roomId);for(const key of ['tileData','groundLoot','bonfires','structures']){if(!world[key])world[key]={};applyPatch(world[key],validPatch(incoming[key]));}
   world.updatedAt=Date.now();persist();for(const peer of roomMembers(c.roomId))send(peer.ws,{type:'worldState',state:world});return;
  }
  if(msg.type==='chat'){
   const channel=cleanText(msg.channel,12)||'tile',rule=channelRule(channel);if(!rule){fail(ws,'That chat channel is unavailable.');return;}const level=Math.max(0,Math.floor(Number(msg.level)||0));if(level<rule.level){fail(ws,`Reach Level ${rule.level} to use ${channel} chat.`);return;}const now=Date.now(),last=c.chatAt[channel]||0;if(now-last<rule.cooldown){fail(ws,`Please wait ${Math.ceil((rule.cooldown-(now-last))/1000)} seconds before chatting again.`);return;}const text=cleanText(msg.text,240);if(!text){fail(ws,'Chat message cannot be empty.');return;}c.chatAt[channel]=now;c.x=validCoord(Number(msg.x));c.y=validCoord(Number(msg.y));
   for(const peer of roomMembers(c.roomId)){let visible=channel==='tile'?(peer.x===c.x&&peer.y===c.y):channel==='area'?(Math.abs(peer.x-c.x)<=5&&Math.abs(peer.y-c.y)<=5):channel==='region'?(Math.floor(peer.x/50)===Math.floor(c.x/50)&&Math.floor(peer.y/50)===Math.floor(c.y/50)):false;if(visible)send(peer.ws,{type:'chat',channel,id:c.id,name:c.name,text,at:now});}return;
  }
  if(msg.type==='tradeOffer'){
   const toId=cleanText(msg.toId,80),target=clients.get(toId),itemId=cleanText(msg.itemId,80),itemName=cleanText(msg.itemName,80),quantity=Math.floor(Number(msg.quantity)||0);
   if(!target||!target.joined||target.roomId!==c.roomId||target.id===c.id){fail(ws,'That player is no longer available for trading.');return;}
   if(!itemId||quantity<1||quantity>999){fail(ws,'Invalid trade item or quantity.');return;}
   const offer={id:crypto.randomUUID(),fromId:c.id,fromName:c.name,toId:target.id,toName:target.name,itemId,itemName,quantity,createdAt:Date.now()};tradeOffers.set(offer.id,offer);send(target.ws,{type:'tradeOffer',offer});send(ws,{type:'tradeUpdate',offerId:offer.id,text:'Trade offer sent to '+target.name+'.'});return;
  }
  if(msg.type==='tradeRespond'){
   const offer=tradeOffers.get(cleanText(msg.offerId,80));if(!offer||offer.toId!==c.id){fail(ws,'That trade offer is no longer available.');return;}tradeOffers.delete(offer.id);const sender=clients.get(offer.fromId);
   if(!sender||!sender.joined){send(ws,{type:'tradeUpdate',offerId:offer.id,text:'The other player disconnected.'});return;}
   if(msg.accept===true){const payload={type:'tradeAccepted',text: c.name+' accepted the trade offer.',offerId:offer.id,give:{fromId:offer.fromId,toId:offer.toId,itemId:offer.itemId,itemName:offer.itemName,quantity:offer.quantity},receive:{fromId:offer.fromId,toId:offer.toId,itemId:offer.itemId,itemName:offer.itemName,quantity:offer.quantity}};send(sender.ws,payload);send(ws,payload);}
   else{send(sender.ws,{type:'tradeUpdate',offerId:offer.id,text:c.name+' rejected the trade offer.'});send(ws,{type:'tradeUpdate',offerId:offer.id,text:'Trade offer rejected.'});}return;
  }
  fail(ws,'Unknown message type.');
 });
 ws.on('close',()=>{if(c.joined){clients.delete(c.id);for(const peer of roomMembers(c.roomId))send(peer.ws,{type:'system',text:`${c.name} left the world.`});broadcastPlayers(c.roomId);}});
 ws.on('error',()=>{if(c.joined){clients.delete(c.id);broadcastPlayers(c.roomId);}});
});
server.listen(PORT,HOST,()=>console.log(`Lost Ruin multiplayer listening on ${HOST}:${PORT}; data file: ${DATA_FILE}`));
function shutdown(){for(const ws of wss.clients)ws.close(1001,'Server shutting down');server.close(()=>process.exit(0));setTimeout(()=>process.exit(1),3000).unref();}
process.on('SIGINT',shutdown);process.on('SIGTERM',shutdown);
