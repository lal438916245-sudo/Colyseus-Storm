"use strict";
const fs = require("fs"), http = require("http"), path = require("path"), readline = require("readline");
const { createRequire } = require("module"), { performance, monitorEventLoopDelay } = require("perf_hooks");
const { LagHistogram, sortedQuantile } = require("./step_statistics.cjs");
const req = createRequire(path.join(process.env.STEP_SERVER_ROOT, "package.json"));
const WS = req("ws"), pack = req("notepack.io"), index = Number(process.env.STEP_WORKER);
const base = new URL(process.env.STEP_BASE), run = process.env.STEP_RUN_ID;
const journal = fs.createWriteStream(process.env.STEP_EVENTS, { flags: "a" });
const accounts = fs.createWriteStream(process.env.STEP_ACCOUNTS, { flags: "a" });
let logPressure = 0;
function log(event, fields = {}) {
  if (!journal.write(JSON.stringify({ event, worker: index, wall_ms: Date.now(), mono_ms: performance.now(), ...fields }) + "\n")) logPressure++;
}
const agent = new http.Agent({ keepAlive: true, maxSockets: 8 });
function signup(account) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ ...account, password: "Step_Experiment_Only_2026!" }), start = performance.now();
    const r = http.request({ hostname: base.hostname, port: base.port, path: "/users/signup", method: "POST", agent,
      headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) } }, response => {
      let text = ""; response.on("data", b => text += b);
      response.on("end", () => {
        log("signup", { username: account.username, status: response.statusCode, ms: performance.now() - start, body: text });
        try { if (response.statusCode !== 200) throw Error(`signup ${response.statusCode}: ${text}`); resolve(JSON.parse(text).output); }
        catch (e) { reject(e); }
      });
    });
    r.setTimeout(20000, () => r.destroy(Error("signup timeout"))); r.on("error", reject); r.end(body);
  });
}
const bots = [], phases = new Map();
let current = null, ordinal = 0, closing = false;
const counters = { sent: 0, received_frames: 0, received_bytes: 0, acked: 0, errors: 0, closes: 0, ping_received: 0, skipped: 0 };
function coordinates(seq, id) { return { x: seq / 1000, z: (((seq * 1543 + id * 157) % 20000) - 10000) / 100 }; }
function send(bot) {
  const seq = ++bot.seq, c = coordinates(seq, bot.id), phase = current;
  const bytes = Buffer.concat([Buffer.from([13]), pack.encode("entityUpdate"), pack.encode([bot.sid, "xPos", c.x, "yPos", 0, "zPos", c.z])]);
  if (seq === 1 || seq % 10 === 0) {
    bot.pending.set(seq, { at: performance.now(), phase, ...c });
    if (phase) phases.get(phase).expected_samples++;
  }
  bot.ws.send(bytes, { binary: true }, error => {
    if (error) { counters.errors++; if (phase) phases.get(phase).errors++; log("send_error", { session_id: bot.sid, phase, seq, message: error.message }); }
  });
  counters.sent++; bot.sent++; if (phase) phases.get(phase).sent++;
}
async function addBot() {
  const id = ++ordinal, username = `SL${run.slice(-8)}w${index}u${id}`;
  const account = { username, email: username.toLowerCase() + "@step.invalid" };
  accounts.write(JSON.stringify(account) + "\n");
  log("account_attempt", account);
  const output = await signup(account), seat = output.seatReservation;
  if (output.user.username !== username || !seat.sessionId || !seat.room.roomId) throw Error("signup identity/seat mismatch");
  const bot = { id, username, sid: seat.sessionId, room: seat.room.roomId, seq: 0, sent: 0, acked: 0, pending: new Map(), ready: false, closed: false, next: 0 };
  bots.push(bot);
  await new Promise((resolve, reject) => {
    let join = false, initial = false, settled = false;
    const timer = setTimeout(() => { if (!settled) { settled = true; reject(Error("join/business precondition timeout")); bot.ws.terminate(); } }, 20000);
    const ws = bot.ws = new WS(`ws://${base.host}/${seat.room.processId}/${bot.room}?sessionId=${bot.sid}`, { perMessageDeflate: false, handshakeTimeout: 15000 });
    ws.on("ping", () => counters.ping_received++);
    ws.on("message", bytes => {
      counters.received_frames++; counters.received_bytes += bytes.length;
      try {
        if (bytes[0] === 10 && !join) { join = true; ws.send(Buffer.from([10])); }
        else if (bytes[0] === 14 && !initial) {
          if (!join) throw Error("initial state before JOIN"); initial = true; send(bot);
          log("initial_state", { session_id: bot.sid, room_id: bot.room, bytes: bytes.length });
        } else if (bytes[0] === 11) throw Error(`server error frame ${bytes.toString("hex")}`);
        else if (bytes[0] === 13 && bytes[1] === 99) {
          const [seq, sid, name, x, z, room] = pack.decode(bytes.subarray(2)), pending = bot.pending.get(seq);
          if (!pending || sid !== bot.sid || name !== username || room !== bot.room || x !== pending.x || z !== pending.z) throw Error("sample ACK identity/position/sequence mismatch");
          const rtt = performance.now() - pending.at; bot.pending.delete(seq); counters.acked++; bot.acked++;
          if (pending.phase) { const p = phases.get(pending.phase); p.acked++; p.rtts.push(rtt); }
          log("business_ack", { phase: pending.phase, session_id: sid, seq, x, z, sent_mono_ms: pending.at, rtt_ms: rtt });
          if (!settled && seq === 1) {
            settled = true; clearTimeout(timer); bot.ready = true; bot.next = performance.now() + (id * 17 % 100);
            log("ready", { session_id: sid, username, room_id: room }); resolve();
          }
        }
      } catch (e) {
        counters.errors++; if (current) phases.get(current).errors++;
        log("protocol_error", { session_id: bot.sid, message: e.message });
        if (!settled) { settled = true; clearTimeout(timer); reject(e); }
        ws.terminate();
      }
    });
    ws.on("error", e => { counters.errors++; if (current) phases.get(current).errors++; log("socket_error", { session_id: bot.sid, message: e.message }); if (!settled) { settled = true; clearTimeout(timer); reject(e); } });
    ws.on("close", (code, reason) => {
      bot.closed = true; counters.closes++; if (current && !closing) phases.get(current).drops++;
      log("closed", { session_id: bot.sid, code, reason: reason.toString(), phase: current, cleanup: closing });
      if (!settled) { settled = true; clearTimeout(timer); reject(Error(`closed before ready ${code}`)); }
    });
  });
}
const tick = setInterval(() => {
  const now = performance.now();
  for (const bot of bots) {
    if (!bot.ready || bot.closed || bot.ws.readyState !== WS.OPEN || now < bot.next) continue;
    const late = Math.max(0, now - bot.next), skipped = Math.floor(late / 100);
    counters.skipped += skipped;
    if (current) { const p = phases.get(current); p.lateness.add(late); p.skipped += skipped; p.buffer_max = Math.max(p.buffer_max, bot.ws.bufferedAmount); }
    try { send(bot); } catch (e) { counters.errors++; if (current) phases.get(current).errors++; log("send_exception", { session_id: bot.sid, message: e.message }); }
    bot.next = skipped ? now + 100 : bot.next + 100;
  }
}, 5);
const lag = monitorEventLoopDelay({ resolution: 10 }); lag.enable();
let cpu = process.cpuUsage(), at = performance.now();
const monitor = setInterval(() => {
  const now = performance.now(), used = process.cpuUsage(cpu); cpu = process.cpuUsage();
  log("sample", { live: bots.filter(b => b.ready && !b.closed).length, counters: { ...counters },
    cpu_one_core_pct: (used.user + used.system) / ((now - at) * 10), rss: process.memoryUsage().rss,
    loop_p99_ms: lag.percentile(99) / 1e6, log_backpressure_events: logPressure });
  at = now; lag.reset();
}, 1000);
async function command(c) {
  if (c.op === "ramp") {
    const failures = [], pending = new Set();
    while (bots.filter(b => b.ready && !b.closed).length + pending.size < c.target) {
      if (["STOP_REQUESTED", "cancel.requested"].some(name => fs.existsSync(path.join(path.dirname(process.env.STEP_EVENTS), name)))) {
        failures.push("resource guard requested stop during admission"); break;
      }
      const task = addBot().catch(e => { failures.push(e.message); log("admission_error", { message: e.message }); }).finally(() => pending.delete(task));
      pending.add(task); await new Promise(r => setTimeout(r, 1000 / c.rate));
      if (pending.size >= 8) await Promise.race(pending);
      if (failures.length) break;
    }
    await Promise.all(pending);
    return { live: bots.filter(b => b.ready && !b.closed).length, failures };
  }
  if (c.op === "phase_start") {
    current = c.phase;
    phases.set(current, { phase: current, start_wall_ms: Date.now(), start_mono_ms: performance.now(), sent: 0, expected_samples: 0,
      acked: 0, rtts: [], lateness: new LagHistogram(), skipped: 0, drops: 0, errors: 0, buffer_max: 0 });
    log("phase_start", { phase: current }); return { started: true };
  }
  if (c.op === "phase_end") {
    const p = phases.get(current); p.end_wall_ms = Date.now(); p.seconds = (performance.now() - p.start_mono_ms) / 1000;
    log("phase_end", { phase: current, seconds: p.seconds }); current = null; return { ended: true };
  }
  if (c.op === "report") {
    const p = phases.get(c.phase), { rtts, lateness, ...rest } = p;
    const sorted = rtts.slice().sort((a, b) => a - b);
    const result = { ...rest, live: bots.filter(b => b.ready && !b.closed).length, counters: { ...counters },
      rtts, lateness_p95_ms: lateness.percentile(.95), rtt_p50_ms: sortedQuantile(sorted, .5), rtt_p95_ms: sortedQuantile(sorted, .95), rtt_p99_ms: sortedQuantile(sorted, .99),
      clients: bots.filter(b => b.ready).map(b => ({ session_id: b.sid, room_id: b.room, username: b.username, closed: b.closed, sent: b.sent, acked: b.acked })) };
    log("phase_report", { phase: c.phase, acked: result.acked, sent: result.sent });
    // Raw ACKs and returned report preserve data; release completed cohorts'
    // large arrays so recording prior levels does not inflate later RSS.
    p.rtts = []; p.lateness = new LagHistogram();
    return result;
  }
  if (c.op === "stop") {
    closing = true; current = null; clearInterval(tick); clearInterval(monitor);
    for (const b of bots) if (b.ws && b.ws.readyState === WS.OPEN) { b.ws.send(Buffer.from([12])); b.ws.close(1000); }
    await new Promise(r => setTimeout(r, 1500));
    for (const b of bots) if (b.ws && b.ws.readyState !== WS.CLOSED) b.ws.terminate();
    agent.destroy(); log("stopped", { counters, accounts: ordinal });
    await Promise.all([new Promise(r => accounts.end(r)), new Promise(r => journal.end(r))]);
    setTimeout(() => process.exit(0), 30); return { stopped: true };
  }
  throw Error(`unknown operation ${c.op}`);
}
readline.createInterface({ input: process.stdin }).on("line", async line => {
  let c;
  try { c = JSON.parse(line); const result = await command(c); process.stdout.write(JSON.stringify({ id: c.id, ok: true, result }) + "\n"); }
  catch (e) { process.stdout.write(JSON.stringify({ id: c && c.id, ok: false, error: e.stack }) + "\n"); }
});
log("started", { pid: process.pid, worker: index });
