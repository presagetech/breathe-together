import test from "node:test";
import assert from "node:assert/strict";
import WebSocket from "ws";
import { startServer, type RunningServer } from "../src/server/index.js";
import { FROZEN_DEFAULT_SETTINGS, patchSettings } from "../src/config.js";
import { SimClock } from "../src/sim/harness.js";
import type { ServerMessage } from "../src/server/protocol.js";
import { encodeFrameHeader } from "../src/server/protocol.js";

const settings = patchSettings(FROZEN_DEFAULT_SETTINGS, {
  server: { port: 0, pacerUpdateHz: 10 },
});

async function startTestServer(clock: SimClock): Promise<RunningServer> {
  return startServer({
    port: 0,
    clock,
    settings,
    signalSource: "mock",
    driver: "manual",
  });
}

interface Client {
  ws: WebSocket;
  messages: ServerMessage[];
  /** Wait until `pred` matches a collected message, or time out. */
  waitFor(pred: (m: ServerMessage) => boolean, timeoutMs?: number): Promise<ServerMessage>;  send(obj: unknown): void;
  sendRaw(text: string): void;
  sendBinary(buf: Uint8Array): void;
  close(): void;
}

function connect(port: number): Promise<Client> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
    const messages: ServerMessage[] = [];
    const waiters: Array<{
      pred: (m: ServerMessage) => boolean;
      resolve: (m: ServerMessage) => void;
      reject: (e: Error) => void;
      timer: NodeJS.Timeout;
    }> = [];

    ws.on("message", (data) => {
      const msg = JSON.parse(data.toString()) as ServerMessage;
      messages.push(msg);
      for (let i = waiters.length - 1; i >= 0; i--) {
        if (waiters[i]!.pred(msg)) {
          const w = waiters.splice(i, 1)[0]!;
          clearTimeout(w.timer);
          w.resolve(msg);
        }
      }
    });
    ws.on("error", reject);
    ws.on("open", () =>
      resolve({
        ws,
        messages,
        waitFor: (pred, timeoutMs = 10_000) =>
          new Promise((res, rej) => {
            const existing = messages.find(pred);
            if (existing) return res(existing);
            const waiter = {
              pred,
              resolve: res,
              reject: rej,
              timer: setTimeout(
                () => rej(new Error(`timed out waiting for message (got ${messages.length})`)),
                timeoutMs,
              ),
            };
            waiters.push(waiter);
          }),
        send: (obj) => ws.send(JSON.stringify(obj)),
        sendRaw: (text) => ws.send(text),
        sendBinary: (buf) => ws.send(buf),
        close: () => ws.close(),
      }),
    );
  });
}

async function advance(srv: RunningServer, clock: SimClock, virtualMs: number, stepMs = 100): Promise<void> {
  for (let t = 0; t < virtualMs; t += stepMs) {
    clock.advance(stepMs);
    srv.tickAll(clock.now());
  }
}

test("health endpoint answers", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  try {
    const res = await fetch(`http://127.0.0.1:${srv.port}/health`);
    assert.equal(res.status, 200);
    const body = (await res.json()) as { ok: boolean; source: string; rooms: number };
    assert.equal(body.ok, true);
    assert.equal(body.source, "mock");
    assert.equal(body.rooms, 0);
  } finally {
    await srv.close();
  }
});

test("mock session over ws: hello -> consent -> start -> full accelerated run", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const client = await connect(srv.port);
  try {
    client.send({ type: "hello", clientVersion: "test-1" });
    const welcome = await client.waitFor((m) => m.type === "welcome");
    assert.equal(welcome.type, "welcome");
    if (welcome.type === "welcome") {
      assert.match(welcome.roomId, /^solo-/);
      assert.equal(welcome.participantId, "p1");
      assert.equal(welcome.signalSource, "mock");
    }

    client.send({ type: "consent_granted" });
    client.send({ type: "start_session", durationMin: 3, mode: "solo" });

    // Until the driver ticks, nothing flows (manual driver). Synchronize on
    // ping/pong so the assertion can't race the server under load.
    client.send({ type: "ping" });
    await client.waitFor((m) => m.type === "pong");
    assert.ok(
      !client.messages.some((m) => m.type === "pacer_update"),
      "no pacer traffic before the server ticks",
    );

    // Run the full 3-minute session in accelerated virtual time.
    await advance(srv, clock, 3 * 60_000);

    const end = await client.waitFor((m) => m.type === "session_end");
    if (end.type === "session_end") {
      assert.ok(end.lines.length >= 4, "summary lines present");
      assert.match(end.lines[0]!, /observations only/);
      assert.ok(end.summary, "natural end carries a summary");
    }

    const stateWalk = client.messages
      .filter((m): m is Extract<ServerMessage, { type: "state_changed" }> => m.type === "state_changed")
      .map((m) => m.to);
    assert.deepEqual([...new Set(stateWalk)], ["calibrate", "match", "lead", "cooldown", "ended"]);

    const samples = client.messages.filter((m) => m.type === "sample");
    assert.ok(samples.length > 50, `samples flowed (${samples.length})`);

    const pacers = client.messages.filter((m) => m.type === "pacer_update");
    assert.ok(pacers.length > 1000, `pacer updates at driver cadence (${pacers.length})`);
  } finally {
    client.close();
    await srv.close();
  }
});

test("frames are refused before consent, accepted after (counted; mock ignores but doesn't crash)", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const client = await connect(srv.port);
  try {
    client.send({ type: "hello" });
    await client.waitFor((m) => m.type === "welcome");

    const frame = new Uint8Array([...encodeFrameHeader(1_000_000), ...new Uint8Array(64)]);
    client.sendBinary(frame);
    const err = await client.waitFor((m) => m.type === "error");
    assert.equal((err as { code: string }).code, "consent_required");

    client.send({ type: "consent_granted" });
    client.sendBinary(frame);
    // No error after consent; the mock source ignores frame content by design.
    // Synchronize on a ping/pong pair (same connection, ordered) so the
    // assertion below can't race the server's processing under load.
    client.send({ type: "ping" });
    await client.waitFor((m) => m.type === "pong");
    const framesReceivedAfter = client.messages.filter(
      (m) => m.type === "error" && (m as { code: string }).code === "consent_required",
    ).length;
    assert.equal(framesReceivedAfter, 1, "only the pre-consent frame was refused");
  } finally {
    client.close();
    await srv.close();
  }
});

test("malformed, unknown and oversize inputs get typed errors and the connection survives", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const client = await connect(srv.port);
  try {
    client.sendRaw("{not json");
    const bad = await client.waitFor((m) => m.type === "error");
    assert.equal((bad as { code: string }).code, "malformed_json");

    client.send({ type: "hello" });
    await client.waitFor((m) => m.type === "welcome");

    client.send({ type: "teleport" });
    const unk = await client.waitFor((m) => m.type === "error" && (m as { code: string }).code === "unknown_type");
    assert.ok(unk);

    client.sendRaw("x".repeat(400_000));
    const big = await client.waitFor(
      (m) => m.type === "error" || m.type === "pong",
      3000,
    );
    assert.ok(big.type === "error", "oversize/timeout handled without hanging");

    client.send({ type: "ping" });
    const pong = await client.waitFor((m) => m.type === "pong");
    assert.ok(pong, "connection still alive after abuse");
  } finally {
    client.close();
    await srv.close();
  }
});

test("start before consent is refused; consent revocation ends the session", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const client = await connect(srv.port);
  try {
    client.send({ type: "hello" });
    await client.waitFor((m) => m.type === "welcome");

    client.send({ type: "start_session", durationMin: 3, mode: "solo" });
    const refused = await client.waitFor((m) => m.type === "error");
    assert.equal((refused as { code: string }).code, "start_refused");
    assert.match((refused as { message: string }).message, /Consent/);

    client.send({ type: "consent_granted" });
    client.send({ type: "start_session", durationMin: 3, mode: "solo" });
    // Wait until the server has actually started the session before ticking.
    await client.waitFor((m) => m.type === "state_changed");
    await advance(srv, clock, 30_000);
    // Pacer messages may still be in flight; wait for at least one to arrive.
    await client.waitFor((m) => m.type === "pacer_update");
    assert.ok(client.messages.some((m) => m.type === "pacer_update"));

    client.send({ type: "consent_revoked" });
    const notice = await client.waitFor(
      (m) => m.type === "notice" && m.kind === "consent_revoked",
    );
    assert.ok(notice);

    // The session is stopped: no more pacer traffic after ticks.
    const before = client.messages.filter((m) => m.type === "pacer_update").length;
    await advance(srv, clock, 20_000);
    const after = client.messages.filter((m) => m.type === "pacer_update").length;
    assert.equal(after, before, "pacer stops when consent is revoked");
  } finally {
    client.close();
    await srv.close();
  }
});

test("pair session over two ws clients: create -> join -> ready -> start -> shared run", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const host = await connect(srv.port);
  const guest = await connect(srv.port);
  try {
    host.send({ type: "hello" });
    await host.waitFor((m) => m.type === "welcome");
    guest.send({ type: "hello" });
    await guest.waitFor((m) => m.type === "welcome");

    host.send({ type: "consent_granted" });
    host.send({ type: "create_room" });
    const created = await host.waitFor((m) => m.type === "room_created");
    const code = (created as { code: string }).code;
    assert.match(code, /^[A-Z0-9]{6}$/);

    guest.send({ type: "consent_granted" });
    guest.send({ type: "join_room", code, label: "Guest" });
    const guestState = await guest.waitFor(
      (m) => m.type === "room_state" && m.participants.length === 2,
    );
    assert.equal(guestState.type, "room_state");
    const hostState = await host.waitFor(
      (m) => m.type === "room_state" && m.participants.length === 2,
    );
    assert.ok(hostState);

    // Host cannot start before everyone agrees and is ready.
    host.send({ type: "host_start", durationMin: 3 });
    const early = await host.waitFor(
      (m) => m.type === "error" && (m as { code: string }).code === "not_all_ready",
    );
    assert.ok(early);

    host.send({ type: "participant_ready", ready: true, label: "Host" });
    guest.send({ type: "participant_ready", ready: true, label: "Guest" });
    await host.waitFor(
      (m) =>
        m.type === "room_state" &&
        m.participants.every((p) => p.consent && p.ready),
    );

    host.send({ type: "host_start", durationMin: 3 });
    await host.waitFor((m) => m.type === "room_state" && m.phase === "session");
    await guest.waitFor((m) => m.type === "room_state" && m.phase === "session");

    // Run the full 3-minute pair session accelerated.
    await advance(srv, clock, 3 * 60_000);

    // Both clients got shared pacer streams. (Wait for a session_end first:
    // TCP ordering means every earlier pacer message has then arrived —
    // counting immediately after a synchronous advance would race I/O.)
    await host.waitFor((m) => m.type === "session_end");
    await guest.waitFor((m) => m.type === "session_end");
    const hostPacers = host.messages.filter((m) => m.type === "pacer_update").length;
    const guestPacers = guest.messages.filter((m) => m.type === "pacer_update").length;
    assert.ok(hostPacers > 1000, `host pacers ${hostPacers}`);
    assert.ok(guestPacers > 1000, `guest pacers ${guestPacers}`);

    // Phase-lock: identical pacer sequences on both screens (same server
    // snapshot stream), within same values.
    const hostSeq = host.messages.filter(
      (m): m is Extract<ServerMessage, { type: "pacer_update" }> => m.type === "pacer_update",
    );
    const guestSeq = guest.messages.filter(
      (m): m is Extract<ServerMessage, { type: "pacer_update" }> => m.type === "pacer_update",
    );
    const n = Math.min(hostSeq.length, guestSeq.length);
    let maxRateDiff = 0;
    for (let i = 0; i < n; i += 50) {
      maxRateDiff = Math.max(maxRateDiff, Math.abs(hostSeq[i]!.rate - guestSeq[i]!.rate));
    }
    assert.equal(maxRateDiff, 0, "shared pacer is identical on both screens");

    // Privacy: no peer_rate relayed (nobody opted in), ever.
    assert.ok(
      !host.messages.some((m) => m.type === "peer_rate") &&
        !guest.messages.some((m) => m.type === "peer_rate"),
      "peer_rate never sent without opt-in",
    );

    // Alignment updates flowed to both.
    assert.ok(host.messages.some((m) => m.type === "alignment_update"));
    assert.ok(guest.messages.some((m) => m.type === "alignment_update"));

    // Pair summary arrived alongside the per-side session ends.
    const summary = await host.waitFor((m) => m.type === "pair_summary");
    assert.ok(
      ["mostly", "partly", "different", "none"].includes(
        (summary as { band: string }).band,
      ),
    );
  } finally {
    host.close();
    guest.close();
    await srv.close();
  }
});

test("peer_rate flows only after the sharer opts in", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const host = await connect(srv.port);
  const guest = await connect(srv.port);
  try {
    host.send({ type: "hello" });
    await host.waitFor((m) => m.type === "welcome");
    guest.send({ type: "hello" });
    await guest.waitFor((m) => m.type === "welcome");
    host.send({ type: "consent_granted" });
    host.send({ type: "create_room" });
    const created = await host.waitFor((m) => m.type === "room_created");
    guest.send({ type: "consent_granted" });
    guest.send({ type: "join_room", code: (created as { code: string }).code, label: "Guest" });
    await guest.waitFor((m) => m.type === "room_state" && m.participants.length === 2);
    host.send({ type: "participant_ready", ready: true, label: "Host" });
    guest.send({ type: "participant_ready", ready: true, label: "Guest" });
    await host.waitFor(
      (m) => m.type === "room_state" && m.participants.every((p) => p.ready),
    );
    host.send({ type: "host_start", durationMin: 3 });
    await host.waitFor((m) => m.type === "room_state" && m.phase === "session");

    await advance(srv, clock, 20_000);
    assert.ok(
      !guest.messages.some((m) => m.type === "peer_rate"),
      "no rates relayed before opt-in",
    );

    // Host opts in -> guest starts receiving host rates. (Synchronize on
    // ping/pong so the server has processed the toggle before advancing —
    // advances run synchronously and would otherwise beat the message.)
    host.send({ type: "share_rate_toggle", share: true });
    host.send({ type: "ping" });
    await host.waitFor((m) => m.type === "pong");
    await advance(srv, clock, 10_000);
    await guest.waitFor(
      (m) => m.type === "peer_rate" && m.label === "Host",
      3000,
    );
    // Revoking stops the relay again.
    host.send({ type: "share_rate_toggle", share: false });
    host.send({ type: "ping" });
    await host.waitFor((m) => m.type === "pong");
    await advance(srv, clock, 10_000);
    const before = guest.messages.filter((m) => m.type === "peer_rate").length;
    await advance(srv, clock, 5_000);
    const after = guest.messages.filter((m) => m.type === "peer_rate").length;
    assert.equal(after, before, "relay stops after revoking");
  } finally {
    host.close();
    guest.close();
    await srv.close();
  }
});

test("solo consent never broadcasts room_state (no lobby teleport)", async () => {
  const clock = new SimClock();
  const srv = await startTestServer(clock);
  const client = await connect(srv.port);
  try {
    client.send({ type: "hello" });
    await client.waitFor((m) => m.type === "welcome");
    client.send({ type: "consent_granted" });
    client.send({ type: "ping" });
    await client.waitFor((m) => m.type === "pong");
    assert.ok(
      !client.messages.some((m) => m.type === "room_state"),
      "solo rooms never get lobby broadcasts",
    );
  } finally {
    client.close();
    await srv.close();
  }
});
