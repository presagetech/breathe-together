/**
 * Local app server (Phase 2): express serves the static client, `ws` handles
 * the browser connection, and a RoomManager owns rooms/sessions on an
 * injected clock.
 *
 * Responsibilities here are deliberately thin: protocol validation lives in
 * protocol.ts, session wiring in sessionPipeline.ts/rooms.ts. This file adds
 * only transport, connection state, rate limiting, source selection from env,
 * logs and graceful shutdown.
 *
 * Privacy-relevant rules enforced HERE (not left to the client):
 * - camera frames are refused unless that connection granted consent;
 * - consent revocation stops the session and the source immediately;
 * - signal content is never logged (counters only).
 */

import express from "express";
import { WebSocketServer, WebSocket, type RawData } from "ws";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Clock, NormalizedSample } from "../signal/SignalSource.js";
import { FROZEN_DEFAULT_SETTINGS, type Settings } from "../config.js";
import {
  decodeFrameHeader,
  parseClientMessage,
  validateFramePayload,
  TokenBucket,
  type ServerMessage,
} from "./protocol.js";
import { RoomManager, type RoomParticipant, type SourceKind } from "./rooms.js";
import { RoomCoach, pipelineFeed } from "../coach/roomCoach.js";
import type { ParticipantPipelineEvent } from "./sessionPipeline.js";

/** Monotonic wall clock for production use. */
export class RealClock implements Clock {
  now(): number {
    return performance.now();
  }
}

interface ConnState {
  socket: WebSocket;
  welcomeSent: boolean;
  room?: string;
  participantId?: string;
  controlBucket: TokenBucket;
  frameBucket: TokenBucket;
  consentGranted: boolean;
  framesReceived: number;
}

export type FrameHandler = (info: {
  roomId: string;
  participantId: string;
  timestampUs: number;
  pixels: Uint8Array;
}) => void;

export interface ServerOptions {
  port?: number;
  clock?: Clock;
  settings?: Settings;
  /** Requested signal source kind (mock|sdk). Default "mock". */
  signalSource?: SourceKind;
  /** Dev-only mock scenario switches. Default false. */
  allowDevControls?: boolean;
  /** "interval" (production driver) or "manual" (tests drive tickAll directly). */
  driver?: "interval" | "manual";
  /** Optional camera-frame consumer (Step 6 attaches the SDK bridge). */
  onFrame?: FrameHandler;
  log?: (entry: Record<string, unknown>) => void;
}

export interface RunningServer {
  port: number;
  manager: RoomManager;
  /** Tests push time through this instead of any real interval. */
  tickAll(nowMs: number): void;
  close(): Promise<void>;
}

export async function startServer(opts: ServerOptions = {}): Promise<RunningServer> {
  const settings = withPort(opts.settings ?? FROZEN_DEFAULT_SETTINGS, opts.port);
  const clock = opts.clock ?? new RealClock();
  const signalSource: SourceKind = opts.signalSource ?? "mock";
  const manager = new RoomManager({
    clock,
    settings,
    allowDevControls: opts.allowDevControls ?? signalSource === "mock",
  });
  const log = opts.log ?? (() => undefined);

  // -- HTTP surface -------------------------------------------------------
  const app = express();
  app.disable("x-powered-by");
  app.get("/health", (_req, res) => {
    res.json({
      ok: true,
      uptimeSec: Math.round(performance.now() / 1000),
      rooms: manager.roomCount,
      source: signalSource,
    });
  });
  const publicDir = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../public",
  );
  app.use(express.static(publicDir));

  const httpServer = http.createServer(app);
  const wss = new WebSocketServer({
    noServer: true,
    // Two-tier size defense: ws itself hard-caps at 2x the app limit (a
    // payload beyond that tears the connection down with 1009 — logged, not
    // crashed), while the app layer rejects anything over the limit with a
    // typed error and keeps serving the connection.
    maxPayload: settings.server.maxMessageBytes * 2,
    perMessageDeflate: false,
  });
  wss.on("error", (err) => log({ event: "ws_error", message: String(err) }));

  httpServer.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname !== "/ws") {
      socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
      socket.destroy();
      return;
    }
    // Cross-site WebSocket hijacking guard: a browser Origin must match the
    // Host it dialed. Missing Origin (non-browser clients, tests) is allowed.
    const originHeader = req.headers.origin;
    if (originHeader) {
      try {
        const originHost = new URL(originHeader).hostname;
        const hostHost = (req.headers.host ?? "").split(":")[0] ?? "";
        if (originHost !== hostHost) {
          socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
          socket.destroy();
          return;
        }
      } catch {
        socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
        socket.destroy();
        return;
      }
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  // -- websocket handling --------------------------------------------------
  const connections = new Set<ConnState>();
  // Feeds per pair room (creation order), so leaveRoom can drop a leaver's
  // proposal from the shared pacer (solo continuation). Populated at
  // host_start; leaveRoom reads it safely (undefined before any start).
  const pairFeeds = new Map<
    string,
    Array<{ pipeline: RoomParticipant["pipeline"]; remove(): void }>
  >();

  const send = (conn: ConnState, msg: ServerMessage): void =>
    wsSend(conn.socket, msg);

  // Participant pipelines created at join time; wire their events here.
  manager.onRoomEvent((e) => {
    if (e.kind !== "participant_added") return;
    const room = manager.getRoom(e.roomId);
    const p = room?.getParticipant(e.participantId);
    if (p) attachPipeline(p);
  });

  // Last abstract signal status sent to a room per participant (transition-only).
  const lastPeerStatus = new Map<string, string>();

  // Rooms that expire unused: tell anyone still waiting in them.
  manager.onRoomEvent((e) => {
    if (e.kind !== "room_expired") return;
    for (const c of connsInRoom(e.roomId)) {
      send(c, {
        type: "room_closed",
        reason: "The room expired before anyone started. Create a new room to try again.",
      });
      c.room = undefined;
      c.participantId = undefined;
    }
  });

  /**
   * Leave a room. Host leaving a pair room closes it for everyone (all
   * pipelines stop); a joiner leaving just ends their side and the host is
   * told calmly. Solo "rooms" are cleaned like a disconnect.
   */
  function leaveRoom(conn: ConnState, participant: RoomParticipant | undefined): void {
    const roomId = conn.room;
    const pid = conn.participantId;
    if (!roomId || !pid) return;
    const room = manager.getRoom(roomId);
    const isHost = room?.hostId === pid;
    const label = participant?.label ?? "Participant";
    conn.room = undefined;
    conn.participantId = undefined;
    conn.consentGranted = false;
    if (!room) {
      manager.removeParticipant(roomId, pid);
      return;
    }
    if (room.isPair) {
      // A side is leaving: their proposal drops out of the shared rate and
      // alignment freezes (their latest sample goes stale naturally).
      const feeds = pairFeeds.get(roomId);
      for (const entry of feeds ?? []) {
        if (entry.pipeline === participant?.pipeline) entry.remove();
      }
      if (isHost) {
        // Host leaving closes the pair room for everyone.
        for (const c of connsInRoom(roomId)) {
          send(c, {
            type: "room_closed",
            reason: "The host left, so the room closed. Cameras are off.",
          });
        }
        for (const id of [...room.participants().keys()]) {
          manager.removeParticipant(roomId, id);
        }
        for (const c of connsInRoom(roomId)) {
          c.room = undefined;
          c.participantId = undefined;
        }
        pairFeeds.delete(roomId);
        for (const key of [...lastPeerStatus.keys()]) {
          if (key.startsWith(`${roomId}/`)) lastPeerStatus.delete(key);
        }
        return;
      }
      // Joiner leaving: remove only that side; the host continues with the
      // shared rate following their proposal alone, and is told calmly.
      broadcastToRoom(roomId, { type: "peer_left", label }, undefined);
      manager.removeParticipant(roomId, pid);
      if (room.participants().size === 0) pairFeeds.delete(roomId);
      broadcastRoomState(roomId);
      return;
    }
    manager.removeParticipant(roomId, pid);
  }

  function attachPipeline(p: RoomParticipant): void {
    p.pipeline.onEvent((ev: ParticipantPipelineEvent) => {
      const conn = findConn(p.roomId, p.id);
      if (!conn) return; // participant with no live socket: drop quietly
      if (ev.type === "sample") {
        sendSample(conn, ev.sample);
        // Privacy relay: the partner sees this participant's rate ONLY when
        // they opted in (shareRate defaults off, revocable anytime).
        if (p.shareRate && ev.sample.breathingRate !== undefined) {
          broadcastToRoom(
            p.roomId,
            {
              type: "peer_rate",
              label: p.label,
              rate: Math.round(ev.sample.breathingRate * 10) / 10,
            },
            p.id,
          );
        }
        // Abstract peer signal health, on transitions only.
        const key = `${p.roomId}/${p.id}`;
        const statusKey = ev.sample.status === "ok" ? "signal-ok" : ev.sample.status === "lost" ? "signal-lost" : "";
        if (statusKey !== "" && lastPeerStatus.get(key) !== statusKey) {
          lastPeerStatus.set(key, statusKey);
          broadcastToRoom(
            p.roomId,
            { type: "peer_status", kind: statusKey as "signal-ok" | "signal-lost", label: p.label },
            p.id,
          );
        }
      } else if (ev.type === "pacer_update") {
        send(conn, {
          type: "pacer_update",
          t: ev.t,
          rate: ev.snap.rate,
          ...(ev.snap.pendingRate !== undefined ? { pendingRate: ev.snap.pendingRate } : {}),
          phase: ev.snap.phase,
          phaseProgress: ev.snap.phaseProgress,
          cycles: ev.snap.cyclesCompleted,
        });
      } else if (ev.type === "state_changed") {
        send(conn, { type: "state_changed", t: ev.t, from: ev.from, to: ev.to });
      } else if (ev.type === "notice") {
        send(conn, {
          type: "notice",
          t: ev.t,
          kind: ev.kind,
          text: ev.text,
          level: ev.kind === "unavailable" || ev.kind === "hold" ? "guidance" : "info",
        });
      } else if (ev.type === "source_degraded") {
        send(conn, {
          type: "notice",
          t: ev.t,
          kind: "source_degraded",
          text: "The camera feed paused, so the guide is running on its own. Your pace is not being observed.",
          level: "guidance",
        });
      } else if (ev.type === "source_capped") {
        send(conn, {
          type: "notice",
          t: ev.t,
          kind: "source_capped",
          text: "The daily measurement time for this app is used up. The guide keeps running on its own.",
          level: "guidance",
        });
      } else if (ev.type === "session_ended") {
        send(conn, {
          type: "session_end",
          t: ev.t,
          ...(ev.summary ? { summary: ev.summary as unknown as Record<string, number> } : {}),
          lines: ev.lines,
        });
        log({
          event: "session_ended",
          roomId: p.roomId,
          participantId: p.id,
          natural: ev.natural,
        });
      }
    });
  }

  function findConn(roomId: string, participantId: string): ConnState | undefined {
    for (const c of connections) {
      if (c.room === roomId && c.participantId === participantId) return c;
    }
    return undefined;
  }

  function connsInRoom(roomId: string, exceptId?: string): ConnState[] {
    return [...connections].filter(
      (c) => c.room === roomId && c.participantId !== exceptId,
    );
  }

  /** Broadcast the lobby/session shape to everyone in a room (pair rooms only). */
  function broadcastRoomState(roomId: string): void {
    const room = manager.getRoom(roomId);
    if (!room) return;
    // Solo rooms have no lobby; their UI is driven by session messages.
    // Broadcasting here would kick solo users into the pair lobby screen.
    if (!room.isPair) return;
    const participants = [...room.participants().values()].map((p) => ({
      id: p.id,
      label: p.label,
      ready: p.ready,
      consent: p.consentGranted,
    }));
    const anyActive = participants.some((p) =>
      room.getParticipant(p.id)?.pipeline.isSessionActive,
    );
    const msg: ServerMessage = {
      type: "room_state",
      ...(room.isPair ? { code: room.code } : {}),
      hostId: room.hostId ?? participants[0]?.id ?? "",
      phase: anyActive ? "session" : "lobby",
      participants,
      ...(room.isPair ? { quotaRemainingMin: manager.quotaRemainingMin() } : {}),
    };
    for (const c of connsInRoom(roomId)) send(c, msg);
  }

  function broadcastToRoom(roomId: string, msg: ServerMessage, exceptId?: string): void {
    for (const c of connsInRoom(roomId, exceptId)) send(c, msg);
  }

  function sendSample(conn: ConnState, s: NormalizedSample): void {
    send(conn, {
      type: "sample",
      sample: {
        t: s.t,
        ...(s.breathingRate !== undefined ? { breathingRate: s.breathingRate } : {}),
        confidence: s.confidence,
        status: s.status,
      },
    });
  }

  wss.on("connection", (socket: WebSocket) => {
    const conn: ConnState = {
      socket,
      welcomeSent: false,
      controlBucket: new TokenBucket(
        settings.server.controlBurst,
        settings.server.controlRatePerSec,
      ),
      frameBucket: new TokenBucket(
        settings.server.frameBurst,
        settings.server.frameRatePerSec,
      ),
      consentGranted: false,
      framesReceived: 0,
    };
    connections.add(conn);

    socket.on("message", (data: RawData, isBinary: boolean) => {
      const now = clock.now();
      if (isBinary) {
        handleBinaryFrame(conn, data, now);
        return;
      }
      handleClientText(conn, data, now);
    });

    socket.on("close", () => {
      if (conn.room && conn.participantId) {
        const roomId = conn.room;
        const participantId = conn.participantId;
        const gone = manager.getRoom(roomId)?.getParticipant(participantId);
        // One path: solo cleanup, joiner peer_left, host-closes-room.
        leaveRoom(conn, gone);
        log({ event: "disconnected", roomId, participantId });
      }
      connections.delete(conn);
    });
  });

  function handleClientText(conn: ConnState, data: RawData, now: number): void {
    const text = data.toString("utf8");
    const oversized = Buffer.byteLength(text, "utf8") > settings.server.maxMessageBytes;
    if (oversized || !conn.controlBucket.trySpend(now)) {
      send(conn, {
        type: "error",
        code: oversized ? "oversize" : "rate_limited",
        message: oversized ? "Message too large." : "Too many messages; dropping.",
      });
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      send(conn, { type: "error", code: "malformed_json", message: "Not valid JSON." });
      return;
    }
    handleClientMessage(conn, parsed, now);
  }

  function handleClientMessage(conn: ConnState, parsed: unknown, now: number): void {
    const result = parseClientMessage(parsed);
    if (!result.ok) {
      send(conn, { type: "error", code: result.code, message: result.message });
      return;
    }
    const msg = result.message;
    const participant = conn.participantId
      ? manager.getRoom(conn.room ?? "")?.getParticipant(conn.participantId)
      : undefined;

    if (!conn.welcomeSent && msg.type !== "hello" && msg.type !== "ping") {
      send(conn, {
        type: "error",
        code: "hello_first",
        message: "Send hello before anything else.",
      });
      return;
    }

    switch (msg.type) {
      case "hello": {
        if (conn.welcomeSent) {
          send(conn, {
            type: "error",
            code: "already_helloed",
            message: "hello already sent.",
          });
          return;
        }
        const joined = manager.join(
          msg.wantedRoomId ? { wantedRoomId: msg.wantedRoomId } : {},
        );
        conn.room = joined.roomId;
        conn.participantId = joined.participantId;
        conn.welcomeSent = true;
        send(conn, {
          type: "welcome",
          roomId: joined.roomId,
          participantId: joined.participantId,
          signalSource,
          devControls: manager.devControlsAllowed,
        });
        log({
          event: "hello_received",
          roomId: joined.roomId,
          participantId: joined.participantId,
          source: signalSource,
        });
        break;
      }

      case "consent_granted": {
        participant?.setConsent(true);
        conn.consentGranted = true;
        if (conn.room) broadcastRoomState(conn.room);
        break;
      }

      case "consent_revoked": {
        conn.consentGranted = false;
        participant?.setConsent(false);
        participant?.stopSession();
        if (conn.room && participant) {
          broadcastToRoom(
            conn.room,
            { type: "peer_status", kind: "signal-lost", label: participant.label },
            participant.id,
          );
        }
        if (conn.room) broadcastRoomState(conn.room);
        send(conn, {
          type: "notice",
          t: now,
          kind: "consent_revoked",
          text: "Consent removed — the session is closed and nothing further is observed.",
          level: "info",
        });
        break;
      }

      case "start_session": {
        if (!participant) return;
        if (msg.guideOnly) {
          const started = participant.startGuideOnlySession(msg.durationMin);
          if (!started.ok) {
            send(conn, {
              type: "error",
              code: "start_refused",
              message: started.error,
            });
          }
          break;
        }
        const started = participant.startSession(msg.durationMin);
        if (!started.ok) {
          send(conn, {
            type: "error",
            code: "start_refused",
            message: started.error,
          });
          break;
        }
        if (started.note) {
          send(conn, {
            type: "notice",
            t: now,
            kind: "source_fallback",
            text: started.note,
            level: "info",
          });
        }
        break;
      }

      case "stop_session": {
        participant?.stopSession();
        break;
      }

      case "preview_start": {
        if (!participant) return;
        const started = participant.startPreview();
        if (!started.ok) {
          send(conn, {
            type: "error",
            code: "preview_refused",
            message: started.error,
          });
          break;
        }
        if (started.note) {
          send(conn, {
            type: "notice",
            t: now,
            kind: "source_fallback",
            text: started.note,
            level: "info",
          });
        }
        break;
      }

      case "preview_stop": {
        participant?.stopPreview();
        break;
      }

      case "create_room": {
        if (!conn.room || !conn.participantId) return;
        const room = manager.createPairRoom();
        const moved = manager.moveParticipant(conn.room, conn.participantId, room);
        if (!moved.ok) {
          send(conn, { type: "error", code: "move_failed", message: moved.error });
          break;
        }
        conn.room = room.id;
        conn.participantId = moved.participantId;
        send(conn, { type: "room_created", code: room.code });
        broadcastRoomState(room.id);
        break;
      }

      case "join_room": {
        if (!conn.room || !conn.participantId) return;
        const target = manager.findPairRoomByCode(msg.code);
        if (!target) {
          send(conn, {
            type: "error",
            code: "room_not_found",
            message: "That room code wasn't found. Check the code and try again.",
          });
          break;
        }
        const clock = manager.internal.clock;
        const expiryMs = settings.pair.codeExpiryMinutes * 60_000;
        if (!target.sessionStarted && clock.now() - target.createdAtMs > expiryMs) {
          send(conn, {
            type: "error",
            code: "room_expired",
            message: "That room has expired. Ask your partner to create a new one.",
          });
          break;
        }
        if (target.participants().size >= target.maxParticipants) {
          send(conn, {
            type: "error",
            code: "room_full",
            message: "That room is full (2 people).",
          });
          break;
        }
        const moved = manager.moveParticipant(
          conn.room,
          conn.participantId,
          target,
          msg.label,
        );
        if (!moved.ok) {
          send(conn, { type: "error", code: "move_failed", message: moved.error });
          break;
        }
        conn.room = target.id;
        conn.participantId = moved.participantId;
        broadcastToRoom(
          target.id,
          { type: "peer_status", kind: "joined", label: msg.label ?? "Participant" },
          conn.participantId,
        );
        broadcastRoomState(target.id);
        break;
      }

      case "participant_ready": {
        if (!participant) return;
        participant.ready = msg.ready;
        if (msg.label) participant.label = msg.label;
        if (conn.room) broadcastRoomState(conn.room);
        break;
      }

      case "share_rate_toggle": {
        if (!participant) return;
        participant.shareRate = msg.share;
        break;
      }

      case "leave_room": {
        if (!conn.room || !conn.participantId) break;
        leaveRoom(conn, participant);
        break;
      }

      case "host_start": {
        if (!participant || !conn.room) return;
        const room = manager.getRoom(conn.room);
        if (!room?.isPair) {
          send(conn, { type: "error", code: "not_pair_room", message: "Pair rooms only." });
          break;
        }
        if (room.hostId !== conn.participantId) {
          send(conn, {
            type: "error",
            code: "not_host",
            message: "Only the host can start the session.",
          });
          break;
        }
        if (room.participants().size < 2) {
          send(conn, {
            type: "error",
            code: "waiting_for_partner",
            message: "Waiting for your partner to join.",
          });
          break;
        }
        if (msg.durationMin > settings.pair.maxDurationMin) {
          send(conn, {
            type: "error",
            code: "duration_too_long",
            message: `Pair sessions run up to ${settings.pair.maxDurationMin} minutes.`,
          });
          break;
        }
        // Quota: pair sessions consume twice the wall clock. Refuse a start
        // that would exceed the daily budget mid-session.
        if (signalSource === "sdk") {
          const remainingMin = manager.quotaRemainingMin();
          if (remainingMin < 2 * msg.durationMin) {
            send(conn, {
              type: "error",
              code: "quota_insufficient",
              message: `Not enough daily measurement time left for a pair session (${remainingMin} minutes remaining). The guide can still run without sensing.`,
            });
            break;
          }
        }
        const everyone = [...room.participants().values()];
        const unready = everyone.filter((p) => !p.consentGranted || !p.ready);
        if (unready.length > 0) {
          send(conn, {
            type: "error",
            code: "not_all_ready",
            message: "Everyone needs to agree and be ready first.",
          });
          break;
        }
        for (const p of everyone) {
          const started = p.startSession(msg.durationMin);
          if (!started.ok) {
            send(conn, {
              type: "error",
              code: "start_refused",
              message: `Could not start for ${p.label}: ${started.error}`,
            });
          }
        }
        // Pair coordinator: one shared pacer over both pipelines' proposals.
        if (everyone.length >= 2) {
          if (room.roomCoach) room.roomCoach.restart();
          else {
            const feeds = everyone.map((p) => pipelineFeed(p.pipeline));
            const coach = new RoomCoach({
              clock: manager.internal.clock,
              settings: settings,
              feeds: [feeds[0]!, feeds[1]!],
            });
            room.roomCoach = coach;
            for (const p of everyone) p.pipeline.setSharedPacer(coach.pacer);
            coach.onEvent((ev) => {
            if (ev.type === "alignment_update") {
              const st = ev.state;
              broadcastToRoom(room.id, {
                type: "alignment_update",
                ...(st.value !== undefined ? { value: st.value } : {}),
                stale: st.stale,
                waiting: st.waiting,
              });
            } else if (ev.type === "pair_ended") {
              broadcastToRoom(room.id, {
                type: "pair_summary",
                band: ev.band,
                freshSeconds: ev.freshSeconds,
                ...(ev.meanAlignment !== undefined
                  ? { meanAlignment: ev.meanAlignment }
                  : {}),
              });
            }
            });
            pairFeeds.set(
              room.id,
              feeds.map((feed, i) => ({
                pipeline: everyone[i]!.pipeline,
                remove: () => coach.removeFeed(feed),
              })),
            );
          }
        }
        room.sessionStarted = true;
        broadcastRoomState(room.id);
        break;
      }

      case "set_scenario": {
        if (!participant) return;
        const err = participant.setDevScenario(
          msg.scenario,
          manager.devControlsAllowed,
        );
        if (err) {
          send(conn, { type: "error", code: "dev_controls_off", message: err });
        }
        break;
      }

      case "ping":
        send(conn, { type: "pong" });
        break;
    }
  }

  function handleBinaryFrame(conn: ConnState, data: RawData, now: number): void {
    if (!conn.frameBucket.trySpend(now)) return; // silently throttled
    if (!conn.consentGranted || !conn.room || !conn.participantId) {
      send(conn, {
        type: "error",
        code: "consent_required",
        message: "Camera data arrives only after consent; frames dropped.",
      });
      return;
    }
    const buf = toArrayBuffer(data);
    const check = validateFramePayload(buf);
    if (!check.ok) {
      send(conn, { type: "error", code: check.code, message: check.message });
      return;
    }
    conn.framesReceived++;
    const { timestampUs, pixels } = decodeFrameHeader(buf);
    (opts.onFrame ?? (signalSource === "sdk" ? frameRouter : undefined))?.({
      roomId: conn.room,
      participantId: conn.participantId,
      timestampUs,
      pixels,
    });
    // In mock mode frames are counted but not consumed: the mock is the
    // source of truth by design. Step 6 attaches the SDK bridge here.
  }

  // -- driver + lifecycle ---------------------------------------------------
  // Camera frames (SDK mode): route each binary frame to the owning
  // participant's pipeline, which forwards it to the SDK source.
  const frameRouter: FrameHandler = (info) => {
    manager
      .getRoom(info.roomId)
      ?.getParticipant(info.participantId)
      ?.pipeline.pushFrame(info.timestampUs, info.pixels);
  };

  let driverInterval: NodeJS.Timeout | undefined;
  if ((opts.driver ?? "interval") === "interval") {
    const stepMs = Math.round(1000 / settings.server.pacerUpdateHz);
    driverInterval = setInterval(() => manager.tickAll(clock.now()), stepMs);
    driverInterval.unref();
  }

  const port = settings.server.port;
  await new Promise<void>((resolve) => httpServer.listen(port, () => resolve()));
  const addr = httpServer.address();
  // Port 0 (ephemeral) is used by tests; report the real bound port.
  const boundPort =
    addr && typeof addr === "object" && "port" in addr ? (addr.port as number) : port;

  async function close(): Promise<void> {
    if (driverInterval) clearInterval(driverInterval);
    manager.shutdownAll();
    for (const c of connections) {
      c.socket.close(1001, "server shutting down");
      c.socket.terminate();
    }
    await new Promise<void>((resolve) => wss.close(() => resolve()));
    httpServer.closeAllConnections?.();
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  }

  return {
    port: boundPort,
    manager,
    tickAll: (nowMs: number) => manager.tickAll(nowMs),
    close,
  };
}

// -- helpers ----------------------------------------------------------------

function withPort(base: Settings, port?: number): Settings {
  const chose = port ?? readPortFromEnv() ?? base.server.port;
  return { ...base, server: { ...base.server, port: chose } };
}

function readPortFromEnv(): number | undefined {
  const raw = process.env.PORT;
  if (!raw) return undefined;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 && n < 65536 ? n : undefined;
}

function wsSend(socket: WebSocket, msg: ServerMessage): void {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(msg));
}

function toArrayBuffer(data: RawData): ArrayBuffer {
  if (data instanceof ArrayBuffer) return data;
  if (Array.isArray(data)) return Buffer.concat(data).buffer as ArrayBuffer;
  const buf = data as Buffer;
  const out = new ArrayBuffer(buf.byteLength);
  new Uint8Array(out).set(buf);
  return out;
}

// -- CLI entrypoint (npm run serve) -----------------------------------------

export function main(): void {
  const sourceKind: SourceKind = process.env.SIGNAL_SOURCE === "sdk" ? "sdk" : "mock";
  const nodeEnv = process.env.NODE_ENV ?? "development";
  const log = (entry: Record<string, unknown>): void => {
    // Structured logs: counters and ids only — never signal content or frames.
    process.stdout.write(`${JSON.stringify({ ...entry, at: new Date().toISOString() })}\n`);
  };
  if (sourceKind === "sdk" && !process.env.SMARTSPECTRA_API_KEY) {
    // Fail fast with a clear message instead of starting a server that could
    // never measure anything (auth is constructor-injected; see SDK_NOTES §3).
    log({
      event: "startup_failed",
      message:
        "SIGNAL_SOURCE=sdk requires SMARTSPECTRA_API_KEY in the environment (copy .env.example to .env).",
    });
    process.exit(1);
  }
  startServer({
    signalSource: sourceKind,
    allowDevControls: nodeEnv !== "production" && sourceKind === "mock",
    log,
  })
    .then((srv) => {
      log({ event: "listening", port: srv.port, source: sourceKind });
      const stop = (): void => {
        log({ event: "shutdown_begin" });
        srv
          .close()
          .then(() => {
            log({ event: "shutdown_complete" });
            process.exit(0);
          })
          .catch(() => process.exit(0));
      };
      process.on("SIGINT", stop);
      process.on("SIGTERM", stop);
    })
    .catch((err: unknown) => {
      log({ event: "startup_failed", message: String(err) });
      process.exit(1);
    });
}

// Only run as CLI when invoked directly (not under the test runner imports).
if (process.argv[1]?.replaceAll("\\", "/").endsWith("src/server/index.ts")) {
  main();
}
