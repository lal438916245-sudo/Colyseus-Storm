"use strict";
// Real game handlers; observation and 1 Hz sampled application acknowledgements.
const fs = require("fs"), path = require("path"), http = require("http");
const { createRequire } = require("module");
const { performance, monitorEventLoopDelay } = require("perf_hooks");
const observeClose = require("./observe_socket_close.cjs");
const root = path.resolve(process.env.STEP_SERVER_ROOT), req = createRequire(path.join(root, "package.json"));
const name = process.env.STEP_DATABASE, uri = process.env.STEP_MONGO_URI;
const runId = process.env.STEP_RUN_ID, token = process.env.STEP_TOKEN;
if (!/^storm_step_[a-zA-Z0-9_]+$/.test(name) || !["127.0.0.1", "localhost"].includes(new URL(uri).hostname)) throw Error("Unsafe database");
const out = fs.createWriteStream(process.env.STEP_EVENTS, { flags: "a" });
function log(event, fields = {}) { out.write(JSON.stringify({ event, wall_ms: Date.now(), mono_ms: performance.now(), run_id: runId, ...fields }) + "\n"); }
const consoleLog = console.log.bind(console);
console.log = (...a) => consoleLog(...a.map(v => typeof v === "string" ? v.replace(/Encrypting password: .*/, "Encrypting password: [redacted]") : v));
process.env.NODE_ARENA = "true"; process.env.DEMO_DATABASE = uri;
req("ts-node").register({ transpileOnly: true, project: path.join(root, "tsconfig.json") });
const totals = { opens: 0, closes: 0, updates: 0, acks: 0, pings: 0, pongs: 0, terminations: 0, bytes: 0, frames: 0, violations: 0 };
const sessions = new Map(), rooms = new Map(), clients = new Map();
function stat(id) {
  if (!sessions.has(id)) sessions.set(id, { session_id: id, open: false, updates: 0, acks: 0, pings: 0, pongs: 0, terminations: 0 });
  return sessions.get(id);
}
let heartbeat;
const { WebSocketTransport } = req("colyseus/lib/transport/WebSocket/WebSocketTransport");
const auto = WebSocketTransport.prototype.autoTerminateUnresponsiveClients;
WebSocketTransport.prototype.autoTerminateUnresponsiveClients = function (ms, retries) {
  heartbeat = { interval_ms: ms, retries }; return auto.apply(this, arguments);
};
const connect = WebSocketTransport.prototype.onConnection;
WebSocketTransport.prototype.onConnection = function (socket, request) {
  const id = new URL(request.url, "http://127.0.0.1").searchParams.get("sessionId"), s = stat(id);
  s.open = true; totals.opens++;
  log("connected", { session_id: id });
  const ping = socket.ping, terminate = socket.terminate, send = socket.send;
  socket.ping = function () { s.pings++; totals.pings++; return ping.apply(this, arguments); };
  socket.terminate = function () {
    s.terminations++; totals.terminations++;
    log("transport_terminate", { session_id: id, ping_count: this.pingCount });
    return terminate.apply(this, arguments);
  };
  socket.send = function (bytes) {
    totals.bytes += bytes.length || bytes.byteLength || 0; totals.frames++;
    return send.apply(this, arguments);
  };
  socket.on("pong", () => { s.pongs++; totals.pongs++; });
  socket.on("error", e => log("socket_error", { session_id: id, message: e.message }));
  observeClose(socket, (code, reason) => {
    s.open = false; s.close_wall_ms = Date.now(); s.close_code = code; totals.closes++;
    log("closed", { ...s, reason: reason.toString() });
  });
  return connect.apply(this, arguments);
};
const options = req(path.join(root, "src/config/mikro-orm.config.ts")).default;
options.dbName = name; options.clientUrl = uri;
const database = req(path.join(root, "src/config/database.config.ts"));
const { MMORoom } = req(path.join(root, "src/rooms/MMORoom.ts"));
const onCreate = MMORoom.prototype.onCreate, onDispose = MMORoom.prototype.onDispose;
MMORoom.prototype.onCreate = function () {
  const result = onCreate.apply(this, arguments); rooms.set(this.roomId, this);
  log("room_created", { room_id: this.roomId, max_clients: this.maxClients, patch_ms: this.patchRate });
  return result;
};
MMORoom.prototype.onDispose = function () { rooms.delete(this.roomId); return onDispose.apply(this, arguments); };
const onAuth = MMORoom.prototype.onAuth, onJoin = MMORoom.prototype.onJoin;
MMORoom.prototype.onAuth = async function (client) {
  const user = await onAuth.apply(this, arguments);
  Object.assign(stat(client.sessionId), { username: user.username, room_id: this.roomId, auth_ok: true });
  log("auth", { session_id: client.sessionId, username: user.username, room_id: this.roomId });
  return user;
};
MMORoom.prototype.onJoin = async function (client) {
  await onJoin.apply(this, arguments); clients.set(client.sessionId, client);
};
const update = MMORoom.prototype.onEntityUpdate;
MMORoom.prototype.onEntityUpdate = function (id, data) {
  const result = update.apply(this, arguments), entity = this.state.networkedUsers.get(String(data[0]));
  const s = stat(id);
  if (!entity || data[0] !== id || entity.xPos !== data[2] || entity.zPos !== data[6]) {
    totals.violations++; log("business_violation", { session_id: id }); return result;
  }
  totals.updates++; s.updates++; s.last_x = entity.xPos; s.last_z = entity.zPos;
  const seq = Math.round(data[2] * 1000);
  if (seq === 1 || seq % 10 === 0) {
    // A probe response after the ORIGINAL handler applied the coordinates.
    clients.get(id).send(99, [seq, id, s.username, entity.xPos, entity.zPos, this.roomId]);
    totals.acks++; s.acks++;
  }
  return result;
};
let patchTimes = [];
const patch = MMORoom.prototype.broadcastPatch;
MMORoom.prototype.broadcastPatch = function () {
  const start = performance.now();
  try { return patch.apply(this, arguments); }
  finally { patchTimes.push(performance.now() - start); }
};
function percentile(a, p) { if (!a.length) return 0; a.sort((a, b) => a - b); return a[Math.ceil(a.length * p) - 1]; }
function snapshot(details = false) {
  const active = [...sessions.values()].filter(s => s.open);
  const roomRows = [...rooms.values()].map(r => ({ room_id: r.roomId, clients: r.clients.length, entities: r.state.networkedUsers.size, max_clients: r.maxClients }));
  return { wall_ms: Date.now(), totals: { ...totals }, open: active.length, rooms: roomRows,
    ...(details ? { sessions: [...sessions.values()] } : {}) };
}
let game, monitor, closing = false, profiling = false;
async function shutdown() {
  if (closing) return; closing = true; clearInterval(monitor);
  if (game) await game.gracefullyShutdown(false);
  if (database.DI.orm) await database.DI.orm.close(true);
  log("stopped", snapshot(true));
  await new Promise(resolve => out.end(resolve)); process.exit(0);
}
async function profile(seconds) {
  const { Session } = require("inspector"), session = new Session(); session.connect();
  const post = (method, params = {}) => new Promise((resolve, reject) => session.post(method, params, (e, r) => e ? reject(e) : resolve(r)));
  try {
    await post("Profiler.enable"); await post("Profiler.setSamplingInterval", { interval: 1000 }); await post("Profiler.start");
    log("profile_start", { seconds }); await new Promise(r => setTimeout(r, seconds * 1000));
    const result = await post("Profiler.stop");
    const file = path.join(path.dirname(process.env.STEP_EVENTS), `server-${Date.now()}.cpuprofile`);
    fs.writeFileSync(file, JSON.stringify(result.profile)); log("profile_end", { file });
    return file;
  } finally { session.disconnect(); profiling = false; }
}
async function main() {
  await database.connect();
  if (database.DI.orm.config.get("dbName") !== name) throw Error("Database mismatch");
  const express = req("express"), { RequestContext } = req("@mikro-orm/core"), app = express();
  app.use(express.json());
  app.use((r, s, next) => RequestContext.create(database.DI.orm.em, next));
  app.use("/users", req(path.join(root, "src/routes/userRoutes.ts")).default);
  app.get("/__step/health", (r, s) => s.json({ run_id: runId, database: name, heartbeat, pid: process.pid,
    colyseus: req("colyseus/package.json").version, ws: req("ws/package.json").version }));
  app.get("/__step/snapshot", (r, s) => s.json(snapshot(r.query.details === "1")));
  app.use("/__step", (r, s, next) => r.headers.authorization === `Bearer ${token}` ? next() : s.sendStatus(403));
  app.post("/__step/shutdown", (r, s) => { s.json({ closing: true }); setImmediate(() => shutdown().catch(e => { console.error(e); process.exit(1); })); });
  app.post("/__step/profile", async (r, s) => {
    if (profiling) return s.sendStatus(409); profiling = true;
    try { s.json({ file: await profile(Math.min(20, Math.max(1, Number(r.body.seconds) || 10))) }); }
    catch (e) { s.status(500).json({ error: e.message }); }
  });
  game = new (req("colyseus").Server)({ server: http.createServer(app), gracefullyShutdown: false });
  game.define("lobby_room", MMORoom).filterBy(["progress"]);
  await game.listen(Number(process.env.STEP_PORT), "127.0.0.1");
  const lag = monitorEventLoopDelay({ resolution: 10 }); lag.enable();
  let cpu = process.cpuUsage(), at = performance.now(), previousLoop = performance.eventLoopUtilization();
  monitor = setInterval(() => {
    const now = performance.now(), used = process.cpuUsage(cpu); cpu = process.cpuUsage();
    const loop = performance.eventLoopUtilization(previousLoop); previousLoop = performance.eventLoopUtilization();
    log("sample", { ...snapshot(), cpu_one_core_pct: (used.user + used.system) / ((now - at) * 10), rss: process.memoryUsage().rss,
      loop_utilization: loop.utilization, loop_active_ms: loop.active, loop_idle_ms: loop.idle,
      loop_p95_ms: lag.percentile(95) / 1e6, loop_p99_ms: lag.percentile(99) / 1e6, loop_max_ms: lag.max / 1e6,
      patch_count: patchTimes.length, patch_total_ms: patchTimes.reduce((a, b) => a + b, 0), patch_p95_ms: percentile(patchTimes, .95) });
    at = now; lag.reset(); patchTimes = [];
  }, 1000);
  log("ready", { pid: process.pid, heartbeat, database: name }); console.log("STEP_SERVER_READY");
}
main().catch(e => { console.error(e); log("fatal", { message: e.message, stack: e.stack }); out.end(() => process.exit(1)); });
