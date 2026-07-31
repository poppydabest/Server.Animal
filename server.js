/**
 * Animal Hospital — multiplayer relay server
 * ---------------------------------------------------------------
 * This is a "dumb pipe": it doesn't run any game logic itself. It just
 * keeps track of who's in which room and relays messages between them
 * (over a normal WebSocket, which travels over HTTPS/port 443 once
 * deployed — this is what makes it work on strict networks like school
 * wifi, unlike WebRTC/PeerJS).
 *
 * One connected player (whoever joined a room first) acts as the "host"
 * — their browser runs the actual game loop and broadcasts the full
 * game state; everyone else just receives it and sends back their
 * actions (which the host applies). This server only relays those
 * messages and remembers the latest snapshot so late joiners catch up.
 *
 * Run locally:   npm install   then   npm start
 * Deploy free:   push this folder to GitHub, then create a new
 *                "Web Service" on https://render.com pointing at it.
 *                Render auto-detects Node, runs `npm start`, and gives
 *                you a URL like wss://your-app.onrender.com
 */
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// roomCode -> Map(uid -> { ws, name })
const rooms = new Map();
// roomCode -> Map(uid -> playerData)   (latest known player record per room)
const roomPlayers = new Map();
// roomCode -> latest full game state object broadcast by the host
const roomState = new Map();
// roomCode -> Map(uid -> serverAssignedJoinTimestamp)  (keeps host-election consistent)
const roomJoinTimes = new Map();

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, new Map());
  return rooms.get(code);
}
function getJoinTimes(code) {
  if (!roomJoinTimes.has(code)) roomJoinTimes.set(code, new Map());
  return roomJoinTimes.get(code);
}
function getPlayersSnap(code) {
  if (!roomPlayers.has(code)) roomPlayers.set(code, new Map());
  return roomPlayers.get(code);
}

function broadcast(code, msg, exceptUid) {
  const room = rooms.get(code);
  if (!room) return;
  const data = JSON.stringify(msg);
  for (const [uid, client] of room) {
    if (uid === exceptUid) continue;
    if (client.ws.readyState === WebSocket.OPEN) client.ws.send(data);
  }
}
function mapToObj(map) {
  const out = {};
  for (const [k, v] of map) out[k] = v;
  return out;
}

wss.on("connection", (ws) => {
  let joinedRoom = null;
  let joinedUid = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === "join") {
      joinedRoom = String(msg.room || "LOBBY").toUpperCase().slice(0, 12);
      joinedUid = String(msg.uid || "").slice(0, 40);
      if (!joinedUid) return;

      getRoom(joinedRoom).set(joinedUid, { ws, name: msg.name || "Player" });
      const joinTimes = getJoinTimes(joinedRoom);
      if (!joinTimes.has(joinedUid)) joinTimes.set(joinedUid, Date.now());

      ws.send(JSON.stringify({
        type: "snapshot",
        players: mapToObj(getPlayersSnap(joinedRoom)),
        state: roomState.get(joinedRoom) || null,
        myJoinedAt: joinTimes.get(joinedUid),
      }));
      return;
    }

    if (!joinedRoom || !joinedUid) return; // must join before anything else

    if (msg.type === "playerUpdate") {
      const joinTimes = getJoinTimes(joinedRoom);
      const joinedAt = joinTimes.get(joinedUid) || Date.now();
      const data = Object.assign({}, msg.data, { joinedAt });
      getPlayersSnap(joinedRoom).set(joinedUid, data);
      broadcast(joinedRoom, { type: "playerUpdate", uid: joinedUid, data }, joinedUid);

    } else if (msg.type === "state") {
      roomState.set(joinedRoom, msg.data);
      broadcast(joinedRoom, { type: "state", data: msg.data }, joinedUid);

    } else if (msg.type === "action") {
      broadcast(joinedRoom, { type: "action", action: msg.action }, joinedUid);
    }
  });

  ws.on("close", () => {
    if (!joinedRoom || !joinedUid) return;
    const room = rooms.get(joinedRoom);
    if (room) {
      room.delete(joinedUid);
      if (room.size === 0) {
        rooms.delete(joinedRoom);
        roomState.delete(joinedRoom);
        roomPlayers.delete(joinedRoom);
        roomJoinTimes.delete(joinedRoom);
      }
    }
    const playersSnap = roomPlayers.get(joinedRoom);
    if (playersSnap) playersSnap.delete(joinedUid);
    broadcast(joinedRoom, { type: "playerLeft", uid: joinedUid });
  });
});

console.log("Animal Hospital relay server listening on port " + PORT);
