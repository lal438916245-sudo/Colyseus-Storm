"use strict";
// Decode the actual reflected Colyseus Schema, not a substring/MessagePack test.
const { createRequire } = require("module"), path = require("path"), readline = require("readline");
const req = createRequire(path.join(process.env.STORM_SERVER_ROOT, "package.json"));
const { Reflection } = req("@colyseus/schema");
readline.createInterface({ input: process.stdin }).on("line", line => {
  try {
    const request = JSON.parse(line);
    const state = Reflection.decode(Buffer.from(request.handshake, "hex"));
    state.decode(Buffer.from(request.state, "hex"));
    const user = state.networkedUsers.get(request.session_id);
    if (!user || user.id !== request.session_id || user.username !== request.username) throw Error("initial state identity mismatch");
    process.stdout.write(JSON.stringify({ ok: true, session_id: user.id, username: user.username }) + "\n");
  } catch (error) { process.stdout.write(JSON.stringify({ ok: false, error: error.message }) + "\n"); }
});
