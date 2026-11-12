"use strict";

const assert = require("assert/strict");
const { EventEmitter } = require("events");
const { createRequire } = require("module");
const path = require("path");
const observeSocketClose = require("./observe_socket_close.cjs");
const root = process.env.HB_SERVER_ROOT || process.env.STORM_SERVER_ROOT || "F:/Colyseus_SDET_Project/colyseus-unity-demo/colyseus-game-client/Server";
const req = createRequire(path.join(root, "package.json"));
const { Room } = req("colyseus/lib/Room");

(async () => {
  const socket = new EventEmitter();
  let nativeCloses = 0, roomLeaves = 0, observations = 0;
  socket.on("close", () => nativeCloses++); // ws server's existing listener
  socket.once("close", () => roomLeaves++); // room's existing cleanup listener
  const originalListeners = socket.listeners("close");
  observeSocketClose(socket, (code, reason) => {
    assert.equal(code, 1000);
    assert.equal(reason.toString(), "");
    observations++;
  });
  assert.deepEqual(socket.listeners("close"), originalListeners);
  assert.equal(socket.emit("unhandled_event"), false);
  const room = { async _onLeave() { roomLeaves++; } };
  const client = { ref: socket, leave() { socket.emit("close", 1000, Buffer.alloc(0)); } };
  // Exercise the installed framework's real index-sensitive cleanup code.
  Room.prototype._forciblyCloseClient.call(room, client, 4000);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(roomLeaves, 1, "onLeave must not run twice");
  assert.equal(nativeCloses, 1);
  assert.equal(observations, 1, "normal LEAVE_ROOM must still be observed");
  console.log("PASS: real Colyseus LEAVE_ROOM preserves cleanup and close observation");
})().catch(error => { console.error(error); process.exitCode = 1; });
