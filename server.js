/**
 * Animal Hospital — multiplayer relay server
 * ---------------------------------------------------------------
 * This is a "dumb pipe": it doesn't run any game logic itself. It just
 * keeps track of who's in which room and relays messages between them
 * (over a normal WebSocket, which travels over HTTPS/port 443 once
 * deployed — this is what makes it work on strict networks like school
 * wifi, unlike WebRTC/PeerJS).
 *
 * Lobby flow:
 *   - A client sends "createRoom" to get a fresh, unique room code. The
 *     creator becomes that room's host.
 *   - A client sends "joinRoom" with a code someone shared with them.
 *     The server checks the room exists, isn't full (max 4 players),
 *     and hasn't already started, then adds them and tells everyone
 *     else in the room that a new player joined (so clients can show a
 *     "(Name) has joined!" notification).
 *   - Only the host can send "startGame", which is broadcast to everyone
 *     in the room so all clients leave the waiting room together.
 *
 * Once the game has started, one connected player (the host) runs the
 * actual game loop and broadcasts the full game state; everyone else
 * just receives it and sends back their actions (which the host
 * applies). This server only relays those messages and remembers the
 * latest snapshot so late joiners/rejoiners catch up. If the host
 * disconnects, the server promotes whichever remaining player joined
 * earliest and tells the room who the new host is.
 *
 * Run locally:   npm install   then   npm start
 * Deploy free:   push this folder to GitHub, then create a new
 *                "Web Service" on https://render.com pointing at it.
 *                Render auto-detects Node, runs `npm start`, and gives
 *                you a URL like wss://your-app.onrender.com
 */
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;
const ROOM_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O/1/I to avoid ambiguity
const wss = new WebSocketServer({ port: PORT });

// roomCode -> room object: { hostUid, started, clients: Map(uid->ws), players: Map(uid->playerData), state }
const rooms = new Map();

function randomCode() {
  let s = "";
  for (let i = 0; i < 5; i++) s += ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)];
  return s;
}
function generateUniqueCode() {
  let code;
  do { code = randomCode(); } while (rooms.has(code));
  return code;
}
function makeRoom(hostUid) {
  return { hostUid, started: false, clients: new Map(), players: new Map(), state: null };
}
function playersObj(room) {
  return Object.fromEntries(room.players);
}
function sendTo(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}
function broadcast(room, msg, exceptUid) {
  const data = JSON.stringify(msg);
  for (const [uid, client] of room.clients) {
    if (uid === exceptUid) continue;
    if (client.readyState === WebSocket.OPEN) client.send(data);
  }
}
function promoteNextHost(room) {
  let best = null, bestTs = Infinity;
  for (const [uid, p] of room.players) {
    const ts = (p && p.joinedAt) || Infinity;
    if (ts < bestTs) { bestTs = ts; best = uid; }
  }
  room.hostUid = best;
  return best;
}

wss.on("connection", (ws) => {
  let myRoomCode = null;
  let myUid = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    // ---- create a brand-new room; sender becomes host ----
    if (msg.type === "createRoom") {
      const uid = String(msg.uid || "").slice(0, 40);
      const name = String(msg.name || "Player").slice(0, 20);
      if (!uid) return;

      const code = generateUniqueCode();
      const room = makeRoom(uid);
      rooms.set(code, room);
      room.clients.set(uid, ws);
      room.players.set(uid, { name, joinedAt: Date.now() });

      myRoomCode = code; myUid = uid;
      sendTo(ws, { type: "roomCreated", room: code, hostUid: uid, players: playersObj(room) });
      return;
    }

    // ---- join an existing room by code ----
    if (msg.type === "joinRoom") {
      const code = String(msg.room || "").toUpperCase().slice(0, 12);
      const uid = String(msg.uid || "").slice(0, 40);
      const name = String(msg.name || "Player").slice(0, 20);
      if (!uid) return;

      const room = rooms.get(code);
      if (!room) { sendTo(ws, { type: "joinError", reason: "not_found", room: code }); return; }

      const alreadyInRoom = room.players.has(uid);
      if (!alreadyInRoom && room.players.size >= MAX_PLAYERS) {
        sendTo(ws, { type: "joinError", reason: "full", room: code }); return;
      }
      if (room.started && !alreadyInRoom) {
        sendTo(ws, { type: "joinError", reason: "already_started", room: code }); return;
      }

      room.clients.set(uid, ws);
      if (!alreadyInRoom) room.players.set(uid, { name, joinedAt: Date.now() });

      myRoomCode = code; myUid = uid;
      sendTo(ws, {
        type: "roomJoined", room: code, hostUid: room.hostUid, started: room.started,
        players: playersObj(room), state: room.state,
      });
      if (!alreadyInRoom) broadcast(room, { type: "playerJoined", uid, name }, uid);
      return;
    }

    // Everything below requires an established room membership.
    if (!myRoomCode || !myUid) return;
    const room = rooms.get(myRoomCode);
    if (!room) return;

    if (msg.type === "startGame") {
      if (room.hostUid !== myUid || room.started) return; // only the host can start, once
      room.started = true;
      broadcast(room, { type: "gameStarted" }, null);

    } else if (msg.type === "playerUpdate") {
      const existing = room.players.get(myUid) || { joinedAt: Date.now() };
      const data = Object.assign({}, existing, msg.data, { joinedAt: existing.joinedAt });
      room.players.set(myUid, data);
      broadcast(room, { type: "playerUpdate", uid: myUid, data }, myUid);

    } else if (msg.type === "state") {
      room.state = msg.data;
      broadcast(room, { type: "state", data: msg.data }, myUid);

    } else if (msg.type === "action") {
      broadcast(room, { type: "action", action: msg.action }, myUid);
    }
  });

  ws.on("close", () => {
    if (!myRoomCode || !myUid) return;
    const room = rooms.get(myRoomCode);
    if (!room) return;

    room.clients.delete(myUid);
    room.players.delete(myUid);

    if (room.clients.size === 0) {
      rooms.delete(myRoomCode);
      return;
    }
    if (room.hostUid === myUid) {
      const newHost = promoteNextHost(room);
      broadcast(room, { type: "hostChanged", hostUid: newHost }, null);
    }
    broadcast(room, { type: "playerLeft", uid: myUid }, null);
  });
});

console.log("Animal Hospital relay server listening on port " + PORT);
