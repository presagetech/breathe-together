/**
 * Breathe Together browser app.
 *
 * Screen flow: consent -> setup -> session -> summary. Nothing here touches
 * the camera until the consent button is clicked, and consent is kept only
 * in page memory — a reload asks again, and Stop revokes it (ending the
 * session and releasing the camera immediately).
 *
 * The server is the source of truth for the coach; this client only renders
 * what it sends (pacer updates, samples, states, notices, summary lines).
 */

import { CameraError, CameraManager } from "./camera.js";
import { WsClient } from "./wsClient.js";
import { BreathingAudio } from "./audio.js";
import { orbStateAt } from "./orb.js";
import { DEFAULT_SETTINGS } from "../config.js";
import type {
  ClientSample,
  ServerMessage,
} from "../server/protocol.js";

const SCREENS = ["consent", "setup", "lobby", "session", "summary"] as const;
type Screen = (typeof SCREENS)[number];

const PAIR_BAND_COPY: Record<string, { line: string; detail: string }> = {
  mostly: {
    line: "Across the session, the two measured breathing rates were mostly similar.",
    detail:
      "The score blends how close the two reported breathing rates were to each other and how closely each matched the shared pacing guide. It says nothing about anything but those two numbers.",
  },
  partly: {
    line: "Across the session, the two measured breathing rates were partly similar.",
    detail:
      "The score blends how close the two reported breathing rates were to each other and how closely each matched the shared pacing guide. It says nothing about anything but those two numbers.",
  },
  different: {
    line: "Across the session, the two measured breathing rates were different.",
    detail:
      "The score blends how close the two reported breathing rates were to each other and how closely each matched the shared pacing guide. It says nothing about anything but those two numbers.",
  },
  none: {
    line: "The session didn't record a shared alignment view.",
    detail:
      "Alignment needs a clear view of both people for most of the session. Nothing was measured, so nothing is claimed.",
  },
};

const STATE_LABELS: Record<string, string> = {
  calibrate: "Settling in",
  match: "Matching your pace",
  lead: "Slowing gently",
  hold: "Staying with you",
  cooldown: "Winding down",
  ended: "Done",
};

const STATUS_GUIDANCE: Record<string, string> = {
  starting: "Getting settled…",
  ok: "Looking good.",
  no_face: "Center your face in the frame.",
  off_center: "Center your face in the frame.",
  poor_light: "Move to better light.",
  lost: "No clear view — try adjusting your position.",
};

function el<T extends HTMLElement>(id: string): T {
  const found = document.getElementById(id);
  if (!found) throw new Error(`missing element #${id}`);
  return found as T;
}

class App {
  private readonly camera = new CameraManager();
  private readonly ws = new WsClient();
  private readonly audio = new BreathingAudio();
  private readonly frameCanvas = document.createElement("canvas");
  private frameTimer: number | undefined;
  private rafId: number | undefined;

  private consentGranted = false;
  private guideOnly = false;
  private sessionActive = false;
  private durationMin = 3 as 3 | 5 | 10;
  private sessionStartedAt = 0;
  private devScenario = "agitated_then_settling";
  private devControls = false;

  // -- pair state ------------------------------------------------------------
  private pairMode = false;
  private pairActive = false;
  private roomCode = "";
  private selfLabel = "";
  private alignment: { value?: number; stale: boolean; waiting: boolean } | undefined;
  private pairSummary: { band: string } | undefined;

  // Latest pacer snapshot for rAF interpolation between 10 Hz updates.
  private lastPacer: {
    rate: number;
    phase: "inhale" | "exhale";
    phaseProgress: number;
    receivedAt: number;
  } | undefined;

  constructor() {
    this.frameCanvas.width = 320;
    this.frameCanvas.height = 240;
    this.wireUi();
    this.ws.onMessage((m) => this.onServerMessage(m));
    this.ws.onState((s) => {
      if (s === "closed" && this.consentGranted) {
        this.handleDisconnect();
      }
    });
    this.ws.connect(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
    this.ws.send({ type: "hello", clientVersion: "web-1" });
    this.showScreen("consent");
  }

  // -- UI wiring ------------------------------------------------------------

  private wireUi(): void {
    el<HTMLButtonElement>("consent-button").addEventListener("click", () =>
      void this.handleConsent(),
    );    el<HTMLButtonElement>("guide-only-button").addEventListener("click", () =>
      this.handleGuideOnly(),
    );
    el<HTMLButtonElement>("reconnect-button").addEventListener("click", () => {
      el("reconnect-button").hidden = true;
      this.ws.connect(
        `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`,
      );
      this.ws.send({ type: "hello", clientVersion: "web-1" });
      this.showScreen("consent");
    });
    el<HTMLButtonElement>("stop-button").addEventListener("click", () =>
      this.handleStop(),
    );
    el<HTMLButtonElement>("create-room-button").addEventListener("click", () => {
      this.selfLabel = el<HTMLInputElement>("pair-label").value.trim();
      this.ws.send({ type: "create_room" });
    });
    el<HTMLButtonElement>("join-room-button").addEventListener("click", () => {
      const code = el<HTMLInputElement>("pair-code-input").value.trim().toUpperCase();
      this.selfLabel = el<HTMLInputElement>("pair-label").value.trim();
      el("pair-room-error").hidden = true;
      if (code.length < 1) {
        el("pair-room-error").textContent = "Enter the room code your partner shared.";
        el("pair-room-error").hidden = false;
        return;
      }
      this.ws.send({
        type: "join_room",
        code,
        ...(this.selfLabel ? { label: this.selfLabel } : {}),
      });
    });
    el<HTMLButtonElement>("ready-button").addEventListener("click", () => {
      this.audio.ensureStarted(); // joiner's user gesture for audio
      this.ws.send({
        type: "participant_ready",
        ready: true,
        ...(this.selfLabel ? { label: this.selfLabel } : {}),
      });
      el<HTMLButtonElement>("ready-button").disabled = true;
    });
    el<HTMLButtonElement>("host-start-button").addEventListener("click", () => {
      this.audio.ensureStarted();
      this.ws.send({ type: "host_start", durationMin: this.durationMin });
    });
    const leaveRoom = (): void => {
      this.ws.send({ type: "leave_room" });
      this.pairActive = false;
      this.camera.stop();
      this.stopFrameLoop();
      this.audio.stop();
      this.consentGranted = false;
      el("camera-indicator").hidden = true;
      el("stop-button").hidden = true;
      this.showScreen("consent");
    };
    el<HTMLButtonElement>("leave-room-button").addEventListener("click", leaveRoom);
    el<HTMLButtonElement>("leave-room-button-2").addEventListener("click", leaveRoom);
    el<HTMLInputElement>("share-toggle").addEventListener("change", (ev) => {
      const on = (ev.target as HTMLInputElement).checked;
      this.ws.send({ type: "share_rate_toggle", share: on });
      el("session-notice").textContent = on
        ? "Sharing your breathing rate with your partner."
        : "Sharing is off. Only the shared visual is visible to your partner.";
    });
    el<HTMLButtonElement>("begin-button").addEventListener("click", () =>
      this.handleBegin(),
    );
    el<HTMLButtonElement>("again-button").addEventListener("click", () => {
      if (this.pairActive) {
        // Pair sessions restart from the lobby (the room persists).
        this.showScreen("lobby");
        return;
      }
      this.showScreen("setup");
      if (!this.guideOnly) this.ws.send({ type: "preview_start" });
    });
    el<HTMLButtonElement>("delete-button").addEventListener("click", () => {
      localStorage.removeItem("bt.history");
      el("delete-confirm").hidden = false;
    });

    const reduced = el<HTMLInputElement>("opt-reduced-motion");
    reduced.checked = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const saveHistory = el<HTMLInputElement>("opt-save-history");
    saveHistory.checked = localStorage.getItem("bt.historyEnabled") === "1";
    saveHistory.addEventListener("change", () => {
      localStorage.setItem("bt.historyEnabled", saveHistory.checked ? "1" : "0");
    });

    const volume = el<HTMLInputElement>("volume");
    this.audio.setVolume(Number(volume.value) / 100);
    volume.addEventListener("input", () => {
      this.audio.setVolume(Number(volume.value) / 100);
    });

    document
      .querySelectorAll<HTMLInputElement>('input[name="duration"]')
      .forEach((radio) =>
        radio.addEventListener("change", () => {
          if (radio.checked) this.durationMin = Number(radio.value) as 3 | 5 | 10;
        }),
      );

    document
      .querySelectorAll<HTMLInputElement>('input[name="mode"]')
      .forEach((radio) =>
        radio.addEventListener("change", () => {
          this.pairMode = radio.value === "pair";
          this.applyModeUi();
        }),
      );
    this.applyModeUi();

    el<HTMLSelectElement>("dev-scenario").addEventListener("change", (ev) => {
      this.devScenario = (ev.target as HTMLSelectElement).value;
      this.ws.send({ type: "set_scenario", scenario: this.devScenario });
    });

    this.camera.setOnUnexpectedEnd(() => {
      this.showError("The camera stopped unexpectedly. Stopping here — nothing was recorded.");
      this.handleStop(/* silent */ true);
    });

    // Hidden tabs: audio keeps its schedule (tones are the point of
    // eyes-closed sessions); the orb resyncs from server timestamps on
    // return because interpolation always measures against the last update.
    document.addEventListener("visibilitychange", () => undefined);
  }

  private applyModeUi(): void {
    el("pair-options").hidden = !this.pairMode;
    el<HTMLButtonElement>("begin-button").hidden = this.pairMode;
    const radios = Array.from(
      document.querySelectorAll<HTMLInputElement>('input[name="duration"]'),
    );
    if (this.pairMode) {
      for (const radio of radios) {
        radio.disabled = radio.value === "10";
        if (radio.value === "10" && radio.checked) {
          (
            document.querySelector('input[name="duration"][value="3"]') as HTMLInputElement | null
          )?.click();
        }
      }
    } else {
      for (const radio of radios) {
        radio.disabled = false;
      }
    }
  }

  private showScreen(name: Screen): void {
    for (const s of SCREENS) {
      el(`screen-${s}`).hidden = s !== name;
    }
  }

  private showError(text: string): void {
    const err = el("camera-error");
    err.textContent = text;
    err.hidden = false;
  }

  // -- consent + camera -------------------------------------------------------

  private async handleConsent(): Promise<void> {
    el("camera-error").hidden = true;
    el("guide-only-button").hidden = true;
    if (!window.isSecureContext) {
      this.showError(
        "This page isn't a secure connection, so the camera can't start. Open the app on the host's own computer, or via HTTPS (for example a secure tunnel) — then try again.",
      );
      el("guide-only-button").hidden = false;
      return;
    }
    const video = el<HTMLVideoElement>("preview-video");
    try {
      await this.camera.start(video);
    } catch (err) {
      if (err instanceof CameraError) {
        this.showError(err.message);
        // Guide-only is always offered when the camera can't be used.
        el("guide-only-button").hidden = false;
      } else {
        this.showError("The camera could not be started. Nothing was opened.");
        el("guide-only-button").hidden = false;
      }
      return;
    }
    this.guideOnly = false;
    this.consentGranted = true;
    this.ws.send({ type: "consent_granted" });
    el("camera-indicator").hidden = false;
    el("stop-button").hidden = false;
    this.showScreen("setup");
    this.ws.send({ type: "preview_start" });
    this.startFrameLoop();
  }

  /** Guide-only: consent to the session terms with the camera off entirely. */
  private handleGuideOnly(): void {
    el("camera-error").hidden = true;
    el("guide-only-button").hidden = true;
    this.guideOnly = true;
    this.consentGranted = true;
    this.ws.send({ type: "consent_granted" });
    // Stop is still shown (ends the session); no camera indicator, no preview.
    el("stop-button").hidden = false;
    el("preview-video").hidden = true;
    el("setup-guidance").textContent =
      "Guide-only mode: a steady pacer with no sensing. Nothing is observed.";
    el("setup-ready").hidden = true;
    this.showScreen("setup");
  }

  private handleStop(silent = false): void {
    if (!silent) {
      this.ws.send({ type: "consent_revoked" });
    }
    this.audio.stop();
    this.camera.stop();
    this.stopFrameLoop();
    this.consentGranted = false;
    this.guideOnly = false;
    el("preview-video").hidden = false;
    this.sessionActive = false;
    el("camera-indicator").hidden = true;
    el("stop-button").hidden = true;
    this.drawOrbIdle();
    this.showScreen("consent");
  }

  private handleDisconnect(): void {
    // The session cannot continue without the server; end safely and release
    // the camera rather than leaving it running. Reconnect is offered as a
    // fresh connection + fresh consent (consent is per page interaction).
    this.audio.stop();
    this.camera.stop();
    this.stopFrameLoop();
    this.consentGranted = false;
    this.guideOnly = false;
    this.sessionActive = false;
    el("preview-video").hidden = false;
    el("camera-indicator").hidden = true;
    el("stop-button").hidden = true;
    this.showError("The connection was lost, so the session ended. The camera is off.");
    el("reconnect-button").hidden = false;
    this.showScreen("consent");
  }

  // -- session ----------------------------------------------------------------

  private handleBegin(): void {
    this.sessionActive = true;
    this.sessionStartedAt = performance.now();
    // AudioContext may only start from a user gesture — Begin is that gesture.
    this.audio.ensureStarted();
    this.ws.send({
      type: "start_session",
      durationMin: this.durationMin,
      mode: "solo",
      ...(this.guideOnly ? { guideOnly: true } : {}),
    });
    this.showScreen("session");
    el("session-notice").textContent = "";
    this.setPairVisuals(false);
    this.startOrbLoop();
  }

  private setPairVisuals(on: boolean): void {
    el("pair-visual").hidden = !on;
    el("solo-visual").hidden = on;
    el("pair-alignment").hidden = !on;
    el("share-row").hidden = !on;
    el("peer-rate").hidden = true;
    el("opt-align-pct").closest("label")!.hidden = !on;
  }

  private renderLobby(msg: Extract<ServerMessage, { type: "room_state" }>): void {
    // Belt and braces: a lobby exists only for pair rooms (with a code).
    // Solo contexts never render it.
    if (!msg.code) return;
    el("room-code").textContent = msg.code ?? "——";
    const list = el("lobby-participants");
    list.replaceChildren();
    for (const p of msg.participants) {
      const li = document.createElement("li");
      const who = document.createElement("span");
      who.textContent = `${p.label}${p.id === msg.hostId ? " (host)" : ""}`;
      const state = document.createElement("span");
      state.textContent = p.consent && p.ready ? "Ready" : "Not ready yet";
      li.append(who, state);
      list.appendChild(li);
    }
    el("lobby-quota").textContent =
      msg.quotaRemainingMin !== undefined
        ? `Measurement time left today: about ${msg.quotaRemainingMin} minutes (counted per person).`
        : "";
    const bothReady =
      msg.participants.length >= 2 &&
      msg.participants.every((p) => p.consent && p.ready);
    el("host-start-button").hidden = !(msg.hostId && bothReady);
    el("lobby-guidance").textContent =
      msg.phase === "session"
        ? "The session is starting…"
        : msg.participants.length < 2
          ? "Waiting for your partner to join with the room code."
          : bothReady
            ? "Everyone is ready. The host can start."
            : "When both people are ready, the host can start.";
    if (msg.phase === "session") {
      this.sessionActive = true;
      this.sessionStartedAt = performance.now();
      this.pairActive = true;
      this.showScreen("session");
      el("session-notice").textContent = "";
      this.setPairVisuals(true);
      this.startOrbLoop();
    } else {
      this.showScreen("lobby");
    }
  }

  private renderAlignment(): void {
    const a = this.alignment;
    const line = el("pair-alignment");
    if (!a || a.waiting) {
      line.textContent = "Waiting for signal…";
      el("shared-glow").style.opacity = "0";
      return;
    }
    const bandCopy = a.value !== undefined && a.value >= 0.8
      ? "Rates look mostly similar."
      : a.value !== undefined && a.value >= 0.45
        ? "Rates are becoming similar."
        : "Rates look different for now.";
    const pct = el<HTMLInputElement>("opt-align-pct").checked && a.value !== undefined
      ? ` (${Math.round(a.value * 100)}%)`
      : "";
    line.textContent = a.stale
      ? "Waiting for a clear view to resume…"
      : bandCopy + pct;
    el("shared-glow").style.opacity = String(0.15 + 0.65 * (a.value ?? 0));
    const pair = el("orb-pair");
    const apart = 4 - 2.5 * (a.value ?? 0);
    pair.style.gap = `${apart}rem`;
    el("orb-peer").style.opacity = a.stale ? "0.4" : "0.85";
  }

  private onServerMessage(msg: ServerMessage): void {
    switch (msg.type) {
      case "welcome": {
        this.devControls = msg.devControls;
        el("dev-panel").hidden = !msg.devControls;
        const note = el("source-note");
        if (msg.signalSourceNote) {
          note.textContent = msg.signalSourceNote;
          note.hidden = false;
        }
        break;
      }
      case "room_created":
        this.roomCode = msg.code;
        el("room-code").textContent = msg.code;
        break;
      case "room_state":
        this.renderLobby(msg);
        break;
      case "alignment_update":
        this.alignment = { value: msg.value, stale: msg.stale, waiting: msg.waiting };
        this.renderAlignment();
        break;
      case "peer_status":
        el("session-notice").textContent =
          msg.kind === "joined"
            ? `${msg.label} joined.`
            : msg.kind === "left"
              ? `${msg.label} left. The guide keeps running for you.`
              : msg.kind === "signal-lost"
                ? `Waiting for ${msg.label}'s signal…`
                : `${msg.label}'s signal is back.`;
        break;
      case "peer_rate": {
        const peerRate = el("peer-rate");
        peerRate.textContent = `${msg.label} is pacing at about ${Math.round(msg.rate)} breaths a minute.`;
        peerRate.hidden = false;
        break;
      }
      case "peer_left":
        el("session-notice").textContent =
          `${msg.label} left. The guide keeps running for you — no pressure to stay.`;
        el("orb-peer").style.opacity = "0.3";
        break;
      case "room_closed":
        this.pairActive = false;
        this.sessionActive = false;
        this.camera.stop();
        this.stopFrameLoop();
        this.audio.stop();
        el("camera-indicator").hidden = true;
        el("stop-button").hidden = true;
        el("session-notice").textContent = msg.reason;
        this.showError(msg.reason);
        this.showScreen("consent");
        break;
      case "pair_summary":
        this.pairSummary = { band: msg.band };
        break;
      case "sample":
        this.onSample(msg.sample);
        break;
      case "pacer_update":
        this.lastPacer = {
          rate: msg.rate,
          phase: msg.phase,
          phaseProgress: msg.phaseProgress,
          receivedAt: performance.now(),
        };
        this.audio.onPacer(
          msg.phase,
          msg.rate,
          DEFAULT_SETTINGS.pacer.inhaleShare,
          this.audio.currentTime(),
        );
        this.renderSessionClock();
        break;
      case "state_changed":
        el("session-state").textContent = STATE_LABELS[msg.to] ?? "…";
        break;
      case "notice":
        el("session-notice").textContent = msg.text;
        break;
      case "session_end":
        this.sessionActive = false;
        this.stopOrbLoop();
        this.audio.stop();
        this.renderSummary(msg.lines);
        this.showScreen("summary");
        break;
      case "error":
        if (msg.code === "consent_required") {
          this.showError("Camera data is sent only after consent.");
        }
        break;
      default:
        break;
    }
  }

  private onSample(sample: ClientSample): void {
    if (!this.sessionActive) {
      // Setup preview guidance.
      const guidance = el("setup-guidance");
      const ready = el("setup-ready");
      guidance.textContent = STATUS_GUIDANCE[sample.status] ?? "";
      ready.hidden = sample.status !== "ok";
      return;
    }
    const rate = el("session-rate");
    rate.textContent =
      sample.breathingRate !== undefined
        ? `Your pace: about ${Math.round(sample.breathingRate)} breaths a minute`
        : "Your pace: —";
    const view = el("session-view");
    view.textContent =
      sample.status === "ok"
        ? "View: clear"
        : sample.status === "poor_light"
          ? "View: dim"
          : "View: unclear";
  }

  private renderSessionClock(): void {
    const elapsedSec = (performance.now() - this.sessionStartedAt) / 1000;
    const remaining = Math.max(0, this.durationMin * 60 - elapsedSec);
    const mm = Math.floor(remaining / 60);
    const ss = Math.floor(remaining % 60);
    el("session-time").textContent = `${mm}:${String(ss).padStart(2, "0")} left`;
  }

  // -- orb ----------------------------------------------------------------------

  private startOrbLoop(): void {
    this.stopOrbLoop();
    this.rafId = requestAnimationFrame(() => this.orbTick());
  }

  private stopOrbLoop(): void {
    if (this.rafId !== undefined) cancelAnimationFrame(this.rafId);
    this.rafId = undefined;
  }

  private drawOrbIdle(): void {
    el("orb").style.transform = "scale(1)";
  }

  private orbTick(): void {
    const reduced = el<HTMLInputElement>("opt-reduced-motion").checked;
    const audioOnly = el<HTMLInputElement>("opt-audio-only").checked;
    const p = this.lastPacer;
    if (p) {
      // Re-anchored on every 10 Hz server snapshot, so drift cannot
      // accumulate (orb.test.ts proves the bound over 10 minutes).
      const state = orbStateAt(
        { rate: p.rate, phase: p.phase, phaseProgress: p.phaseProgress, receivedAtMs: p.receivedAt },
        performance.now(),
        DEFAULT_SETTINGS.pacer.inhaleShare,
        reduced,
      );
      // Pair mode: both orbs follow the SAME shared pacer (phase-locked).
      const orbs = this.pairActive
        ? [el("orb-self"), el("orb-peer")]
        : [el("orb")];
      for (const orb of orbs) {
        orb.style.transform = `scale(${state.scale})`;
        orb.style.visibility = audioOnly ? "hidden" : "visible";
      }
      const phaseText = el("session-phase");
      phaseText.textContent = audioOnly
        ? state.phase === "inhale"
          ? "Breathe in…"
          : "Breathe out…"
        : "";
      // Text alternative for the visual, kept current for assistive tech.
      const label = `Breathing guide: ${state.phase === "inhale" ? "breathe in" : "breathe out"}`;
      for (const orb of orbs) orb.setAttribute("aria-label", label);
    }
    this.rafId = requestAnimationFrame(() => this.orbTick());
  }

  // -- frames ---------------------------------------------------------------------

  private startFrameLoop(): void {
    this.stopFrameLoop();
    const ctx = this.frameCanvas.getContext("2d");
    if (!ctx) return;
    this.frameTimer = window.setInterval(() => {
      const video = el<HTMLVideoElement>("preview-video");
      if (!this.camera.isActive || video.readyState < 2) return;
      ctx.drawImage(video, 0, 0, this.frameCanvas.width, this.frameCanvas.height);
      const img = ctx.getImageData(0, 0, this.frameCanvas.width, this.frameCanvas.height);
      // Monotonic µs for the SDK transport (performance.now is monotonic).
      this.ws.sendFrame(new Uint8Array(img.data.buffer), Math.round(performance.now() * 1000));
    }, Math.round(1000 / 15)); // 15 fps ceiling — the minimum the SDK needs, not 30
  }

  private stopFrameLoop(): void {
    if (this.frameTimer !== undefined) {
      clearInterval(this.frameTimer);
      this.frameTimer = undefined;
    }
  }

  // -- summary ----------------------------------------------------------------------

  private renderSummary(lines: string[]): void {
    const container = el("summary-lines");
    container.replaceChildren();
    for (const line of lines) {
      const p = document.createElement("p");
      p.textContent = line;
      container.appendChild(p);
    }
    // Pair: add the alignment band (observation-only definition included).
    const pairBox = el("pair-summary");
    if (this.pairSummary) {
      pairBox.hidden = false;
      const copy = PAIR_BAND_COPY[this.pairSummary.band] ?? PAIR_BAND_COPY["none"]!;
      el("pair-band-line").textContent = copy.line;
      el("pair-band-detail").textContent = copy.detail;
      this.pairSummary = undefined;
    } else {
      pairBox.hidden = true;
    }
    // Opt-in history (OFF by default): only stored when the user enables it
    // in the future; "Delete my data" clears whatever is here.
    if (localStorage.getItem("bt.historyEnabled") === "1" && lines.length > 0) {
      try {
        const history = JSON.parse(localStorage.getItem("bt.history") ?? "[]") as unknown[];
        history.push({ at: new Date().toISOString(), lines });
        localStorage.setItem("bt.history", JSON.stringify(history));
      } catch {
        // Storage full/blocked: history is best-effort, never fatal.
      }
    }
    el("delete-confirm").hidden = true;
  }
}

new App();
