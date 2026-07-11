/**
 * STANDALONE node/bun boot of the REAL Phase-1 voice-session backend as a
 * long-running HTTP + WS service (127.0.0.1 only; nginx fronts it).
 *
 * PURPOSE: sol-dev (https://sol-dev.shad0w.xyz) serves the integration voice UI,
 * but the voice routes live in this CF-Worker package (`packages/cloud/api`) and
 * the headless sol-real agent server does not mount them. This module boots the
 * SAME real route logic (consent nonce, scoped-JWT mint, `attachVoiceWsHandler`,
 * `VoiceSession`, `AmbientSession`, merged Deepgram/Cartesia adapters, metering,
 * revoke) as a standalone node service so nginx can route
 * `/api/v1/voice/session/**` to it and the in-browser voice test runs E2E.
 *
 * RELATIONSHIP TO harness-real-server.ts: that module is the *test-harness* boot
 * (port 0, in-process `mint()`; no HTTP, no auth gate, in-process pendant store).
 * This module is the *deployment* boot: fixed host:port, real HTTP consent/mint
 * routes, a shared-secret auth gate, an injectable (file-backed) pendant store,
 * and a health/lifecycle surface. The REAL-vs-SHIMMED transport boundary is
 * IDENTICAL to the harness (documented there): WebSocketPair->node `ws`, Workers
 * outbound upgrade->header-preserving `ws` factory (channels stripped), Redis->
 * MOCK_REDIS in-memory (real consent/claim/revoke/dir + durable metering run
 * against it), JWKS->installed ES256 keypair (real sign/verify path).
 *
 * AUTH BOUNDARY (honest — this is a DEV/TEST deployment):
 *   The production CF worker authenticates consent + mint via cloud auth
 *   (`requireUserOrApiKeyWithOrg`) + per-user tenancy (ownership repos). This
 *   standalone service has no cloud auth/DB. It gates consent + mint behind a
 *   SHARED SECRET (`VOICE_STANDALONE_AUTH_TOKEN`, presented as
 *   `Authorization: Bearer <token>` or `X-Voice-Standalone-Auth`). nginx injects
 *   this header on the already-`/__auth`-gated sol-dev vhost, so the public
 *   surface stays auth-gated end to end and there is NO open unauthenticated
 *   mint on any port. The mint is bound to a FIXED dev identity (org/user/agent/
 *   conversation) — this service does not do multi-tenant ownership; that is the
 *   documented seam vs the CF worker. The voice server's OWN security
 *   (token verify/claim/scope/metering/revoke/consent-nonce single-use) is fully
 *   real and runs unmodified.
 */

import type { IncomingMessage, ServerResponse } from "node:http";
import { createServer, type Server } from "node:http";
import { timingSafeEqual } from "node:crypto";
import { WebSocket as NodeWs, WebSocketServer, type WebSocket as NodeWebSocket } from "ws";

import {
  mintVoiceSessionToken,
  recordVoiceSessionJti,
  claimVoiceSessionToken,
  isVoiceSessionTokenRevoked,
  revokeVoiceSessionToken,
} from "@/lib/voice-session/jwt";
import { issueConsentNonce, consumeConsentNonce } from "@/lib/voice-session/consent-nonce";
import {
  getVoiceSessionRegistry,
  __resetVoiceSessionRegistryForTests,
} from "@/lib/voice-session/session-registry";
import {
  resolveElizaModel,
  resolveMaxSessions,
  resolveVoiceUsageLimits,
  type VoiceRealtimeEnv,
} from "@/lib/voice-session/config";
import {
  attachVoiceWsHandler,
  type ServerWebSocketLike,
} from "@/lib/voice-session/ws-handler";
import {
  InMemoryVoiceUsageStore,
  createDurableVoiceUsageStore,
  type VoiceUsageStore,
} from "@/lib/services/voice-usage-meter";
import { buildRedisClient } from "@/lib/cache/redis-factory";
import { installVoiceSessionTestSigningKey } from "@/lib/voice-session/test-signing";
import type {
  AmbientSegmentStore,
  AmbientSessionProvisioner,
} from "@/lib/voice-session/pendant-store-client";
import { VoiceSession } from "./session";
import { AmbientSession } from "./ambient-session";

import type {
  DeepgramFluxWebSocket,
  DeepgramFluxWebSocketFactory,
  DeepgramFluxTransportRequest,
} from "../../stt/providers/deepgram-flux";
import {
  CartesiaSonicTtsAdapter,
  type CartesiaWebSocketFactory,
  type CartesiaWebSocketLike,
  type CartesiaWebSocketFactoryOptions,
} from "@/lib/services/cartesia-sonic-tts";

// =========================================================================
// Node `ws` transport factories (header-preserving, channels-stripped) — the
// SAME transport shim the harness documents. Byte-for-byte equivalents of the
// Workers factories in provider-socket-factory.ts; every other line of the
// pipeline is the real production code.
// =========================================================================

type WsLike = DeepgramFluxWebSocket & CartesiaWebSocketLike;

function wrapNodeWsAsDom(socket: NodeWebSocket): WsLike {
  const listenerMap = new WeakMap<(e: unknown) => void, (...a: unknown[]) => void>();
  const toDom = (type: string, ...args: unknown[]): unknown => {
    switch (type) {
      case "open":
        return { type: "open" };
      case "message": {
        const raw = args[0];
        let data: unknown = raw;
        if (typeof raw !== "string") {
          if (Buffer.isBuffer(raw)) data = raw.toString("utf8");
          else if (raw instanceof ArrayBuffer) data = Buffer.from(raw).toString("utf8");
          else if (ArrayBuffer.isView(raw))
            data = Buffer.from(
              (raw as ArrayBufferView).buffer,
              (raw as ArrayBufferView).byteOffset,
              (raw as ArrayBufferView).byteLength,
            ).toString("utf8");
          else if (Array.isArray(raw)) data = Buffer.concat(raw as Buffer[]).toString("utf8");
        }
        return { type: "message", data };
      }
      case "error": {
        const err = args[0] as Error;
        return { type: "error", message: err?.message, error: err };
      }
      case "close": {
        const code = args[0] as number;
        const reason = args[1];
        return {
          type: "close",
          code,
          reason: Buffer.isBuffer(reason) ? reason.toString("utf8") : String(reason ?? ""),
          wasClean: code === 1000,
        };
      }
      default:
        return { type };
    }
  };
  const wrapped = {
    get readyState() {
      return socket.readyState;
    },
    set binaryType(v: string) {
      (socket as unknown as { binaryType: string }).binaryType =
        v === "arraybuffer" ? "arraybuffer" : "nodebuffer";
    },
    get binaryType() {
      return (socket as unknown as { binaryType: string }).binaryType;
    },
    send(data: string | ArrayBuffer | ArrayBufferView) {
      socket.send(data as never);
    },
    close(code?: number, reason?: string) {
      socket.close(code, reason);
    },
    addEventListener(type: string, listener: (e: unknown) => void) {
      const handler = (...args: unknown[]) => listener(toDom(type, ...args));
      listenerMap.set(listener, handler);
      socket.on(type, handler as never);
    },
    removeEventListener(type: string, listener: (e: unknown) => void) {
      const handler = listenerMap.get(listener);
      if (handler) socket.off(type, handler as never);
    },
  };
  return wrapped as unknown as WsLike;
}

function stripChannelsParam(rawUrl: string): string {
  try {
    const url = new URL(rawUrl);
    url.searchParams.delete("channels");
    return url.toString();
  } catch {
    return rawUrl;
  }
}

function safeHost(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return "<unparseable>";
  }
}

export interface StandaloneHooks {
  log: (level: "info" | "warn" | "error", msg: string, data?: Record<string, unknown>) => void;
}

function makeNodeDeepgramFactory(hooks: StandaloneHooks): DeepgramFluxWebSocketFactory {
  return (request: DeepgramFluxTransportRequest): DeepgramFluxWebSocket => {
    const url = stripChannelsParam(request.url);
    hooks.log("info", "deepgram outbound WS (channels stripped)", { host: safeHost(url) });
    const socket = new NodeWs(url, { headers: request.headers }) as unknown as NodeWebSocket;
    return wrapNodeWsAsDom(socket) as DeepgramFluxWebSocket;
  };
}

function makeNodeCartesiaFactory(hooks: StandaloneHooks): CartesiaWebSocketFactory {
  return (url: string, options: CartesiaWebSocketFactoryOptions): CartesiaWebSocketLike => {
    hooks.log("info", "cartesia outbound WS", { host: safeHost(url) });
    const socket = new NodeWs(url, { headers: options.headers }) as unknown as NodeWebSocket;
    return wrapNodeWsAsDom(socket) as CartesiaWebSocketLike;
  };
}

// Adapt an inbound node `ws` socket to the REAL handler's transport contract.
function adaptInboundSocket(ws: NodeWebSocket): ServerWebSocketLike {
  return {
    send(data: string | ArrayBuffer | Uint8Array) {
      try {
        ws.send(data as never);
      } catch {
        /* closing */
      }
    },
    close(code?: number, reason?: string) {
      try {
        ws.close(code, reason);
      } catch {
        /* already closing */
      }
    },
    addEventListener(
      type: "message" | "close" | "error",
      listener: (event?: { data: unknown }) => void,
    ) {
      if (type === "message") {
        ws.on("message", (data: unknown, isBinary: boolean) => {
          if (isBinary) {
            const buf = data as Buffer;
            const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
            (listener as (e: { data: unknown }) => void)({ data: ab });
          } else {
            const text = Buffer.isBuffer(data) ? data.toString("utf8") : String(data);
            (listener as (e: { data: unknown }) => void)({ data: text });
          }
        });
      } else if (type === "close") {
        ws.on("close", () => (listener as () => void)());
      } else if (type === "error") {
        ws.on("error", () => (listener as () => void)());
      }
    },
  } as ServerWebSocketLike;
}

// =========================================================================
// Standalone service config + lifecycle.
// =========================================================================

export interface StandaloneServerConfig {
  host: string;
  port: number;
  /** Shared secret gating consent + mint (Bearer or X-Voice-Standalone-Auth). */
  authToken: string;
  deepgramApiKey: string;
  cartesiaApiKey: string;
  cartesiaVoiceId: string;
  elizaEndpoint: string;
  elizaAuthorization: string;
  /** Fixed dev identity the mint binds (this service is not multi-tenant). */
  organizationId: string;
  userId: string;
  agentId: string;
  conversationId: string;
  /** File-backed ambient pendant store (durable across restart). */
  ambientStore: AmbientSegmentStore & AmbientSessionProvisioner;
  hooks: StandaloneHooks;
  /** Injectable only for contract tests; production uses the global fetch. */
  deepgramFetch?: typeof fetch;
  /** Injectable only for contract tests; production uses the node ws factory. */
  cartesiaWebSocketFactory?: CartesiaWebSocketFactory;
}

export interface RunningStandaloneServer {
  host: string;
  port: number;
  stop: () => Promise<void>;
}

const CONSENT_PATH = "/api/v1/voice/session/consent";
const MINT_PATH = "/api/v1/voice/session";
const WS_PATH = "/api/v1/voice/session/ws";
const HEALTH_PATH = "/api/v1/voice/session/health";
const ASR_CLOUD_PATH = "/api/asr/cloud";
const TTS_CLOUD_PATH = "/api/tts/cloud";
/** Read-only segment inspection for the standalone service's own store. */
const SEGMENTS_PATH_PREFIX = "/api/v1/voice/session/segments";

const AMBIENT_LEASE_MS = 5 * 60_000;

function bearerFrom(req: IncomingMessage): string | null {
  const auth = req.headers["authorization"];
  if (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ")) {
    return auth.slice(7).trim();
  }
  const alt = req.headers["x-voice-standalone-auth"];
  if (typeof alt === "string" && alt.trim()) return alt.trim();
  return null;
}

function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

function json(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

async function readJsonBody(req: IncomingMessage, limitBytes = 64 * 1024): Promise<unknown> {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (c: Buffer) => {
      total += c.length;
      if (total > limitBytes) {
        reject(new Error("body too large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8").trim();
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(new Error("invalid json"));
      }
    });
    req.on("error", reject);
  });
}

class BodyTooLargeError extends Error {}

/**
 * Parsed shape of a PCM WAV header, plus a normalized body ready to forward.
 *
 * iOS/Safari's MediaRecorder-free capture path (packages/ui local-asr-capture)
 * hand-encodes a WAV in `encodeMonoPcm16Wav`. That encoder writes correct sizes
 * for a clean single-shot buffer, BUT the bytes that actually arrive here can
 * still carry sizes that DON'T match the payload: a proxy/recorder that flushes
 * before the final size patch, a trailing/short read, or a non-canonical chunk
 * order all leave the declared `RIFF`/`data` sizes inconsistent with the real
 * byte length. Deepgram's pre-recorded endpoint answers such a WAV with a 408
 * (it waits for the bytes the header promised and times out) — which is exactly
 * the iPhone symptom (desktop Chrome WAVs are always self-consistent, so they
 * pass). The fix is deterministic: reparse the fmt chunk, locate the real audio
 * data, and REWRITE a canonical 44-byte header whose sizes match the payload we
 * actually hold before forwarding. See IOS-ASR-FIX-REPORT.md.
 */
interface NormalizedWav {
  /** A freshly-built canonical 44-byte-header PCM WAV whose sizes are correct. */
  body: Buffer;
  /** Sample rate read from the fmt chunk (Hz) — forwarded to Deepgram as a hint. */
  sampleRate: number;
  channels: number;
  bitsPerSample: number;
  /** Actual PCM data byte length after normalization. */
  dataBytes: number;
  /** Diagnostics: what the incoming header *declared* vs the real byte length. */
  declaredRiffSize: number;
  declaredDataBytes: number;
  incomingBytes: number;
  /** True when we had to rewrite because declared sizes were wrong. */
  rewritten: boolean;
}

/**
 * Parse a PCM WAV and return a normalized, self-consistent copy. Walks the RIFF
 * chunk list (does NOT assume a canonical 44-byte header) to find `fmt ` and
 * `data`. If the `data` sub-chunk's declared size is a streaming placeholder
 * (0, 0xffffffff, or larger than the bytes actually present), the real data is
 * taken as "everything from the data payload start to end of buffer". A clean
 * 44-byte header is then rebuilt so the forwarded WAV's RIFF/data sizes always
 * match its payload. Returns null when the body can't be parsed as PCM WAV.
 */
function normalizePcmWav(buf: Buffer): NormalizedWav | null {
  if (buf.length < 12) return null;
  if (buf.toString("ascii", 0, 4) !== "RIFF") return null;
  if (buf.toString("ascii", 8, 12) !== "WAVE") return null;
  const declaredRiffSize = buf.readUInt32LE(4);

  let fmtOffset = -1;
  let dataOffset = -1;
  let dataDeclared = 0;
  // Walk sub-chunks starting after the 12-byte RIFF/WAVE header.
  let pos = 12;
  while (pos + 8 <= buf.length) {
    const id = buf.toString("ascii", pos, pos + 4);
    const size = buf.readUInt32LE(pos + 4);
    const bodyStart = pos + 8;
    if (id === "fmt ") {
      fmtOffset = bodyStart;
    } else if (id === "data") {
      dataOffset = bodyStart;
      dataDeclared = size;
      // Do NOT trust `size` for advancing when it's a placeholder; break here —
      // audio data is conventionally the last chunk and any trailing bytes are
      // the real samples.
      break;
    }
    // Advance by the declared size (chunks are word-aligned/padded to even).
    const advance = size + (size % 2);
    if (advance <= 0) break;
    pos = bodyStart + advance;
  }
  if (fmtOffset < 0 || fmtOffset + 16 > buf.length) return null;
  if (dataOffset < 0) return null;

  const audioFormat = buf.readUInt16LE(fmtOffset);
  const channels = buf.readUInt16LE(fmtOffset + 2);
  const sampleRate = buf.readUInt32LE(fmtOffset + 4);
  const bitsPerSample = buf.readUInt16LE(fmtOffset + 14);
  // Only linear PCM (1) is supported by the client encoder + the rewrite below.
  if (audioFormat !== 1) return null;
  if (channels < 1 || sampleRate < 1 || bitsPerSample < 8) return null;

  const bytesAvailable = buf.length - dataOffset;
  // Trust the declared data size only when it fits inside the bytes we actually
  // hold and isn't a streaming placeholder (0 / 0xffffffff). Otherwise use the
  // real remaining bytes.
  const isPlaceholder =
    dataDeclared === 0 ||
    dataDeclared === 0xffffffff ||
    dataDeclared > bytesAvailable;
  let dataBytes = isPlaceholder ? bytesAvailable : dataDeclared;
  const blockAlign = channels * Math.floor(bitsPerSample / 8);
  if (blockAlign > 0) dataBytes -= dataBytes % blockAlign; // whole frames only
  if (dataBytes <= 0) {
    return {
      body: Buffer.alloc(0),
      sampleRate,
      channels,
      bitsPerSample,
      dataBytes: 0,
      declaredRiffSize,
      declaredDataBytes: dataDeclared,
      incomingBytes: buf.length,
      rewritten: true,
    };
  }

  // Detect whether the incoming header was already canonical + self-consistent
  // (44-byte header, correct sizes) so we can skip the rewrite for the desktop
  // path and only touch the pathological iOS case.
  const wasCanonical =
    dataOffset === 44 &&
    dataDeclared === dataBytes &&
    declaredRiffSize === 36 + dataBytes;

  const bytesPerSample = Math.floor(bitsPerSample / 8);
  const header = Buffer.alloc(44);
  header.write("RIFF", 0, "ascii");
  header.writeUInt32LE(36 + dataBytes, 4);
  header.write("WAVE", 8, "ascii");
  header.write("fmt ", 12, "ascii");
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(channels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * channels * bytesPerSample, 28);
  header.writeUInt16LE(channels * bytesPerSample, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write("data", 36, "ascii");
  header.writeUInt32LE(dataBytes, 40);
  const body = Buffer.concat([
    header,
    buf.subarray(dataOffset, dataOffset + dataBytes),
  ]);
  return {
    body,
    sampleRate,
    channels,
    bitsPerSample,
    dataBytes,
    declaredRiffSize,
    declaredDataBytes: dataDeclared,
    incomingBytes: buf.length,
    rewritten: !wasCanonical,
  };
}


function wavFromPcm16Mono(pcm: Buffer, sampleRate: number): Buffer {
  const dataBytes = pcm.length - (pcm.length % 2);
  const header = Buffer.alloc(44);
  header.write("RIFF", 0, "ascii");
  header.writeUInt32LE(36 + dataBytes, 4);
  header.write("WAVE", 8, "ascii");
  header.write("fmt ", 12, "ascii");
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36, "ascii");
  header.writeUInt32LE(dataBytes, 40);
  return Buffer.concat([header, pcm.subarray(0, dataBytes)]);
}

async function synthesizeCartesiaWav(
  config: StandaloneServerConfig,
  hooks: StandaloneHooks,
  text: string,
): Promise<Buffer> {
  const sampleRate = 16_000;
  const frames: Buffer[] = [];
  let providerError: Error | null = null;
  let completeResolve!: () => void;
  const complete = new Promise<void>((resolve) => {
    completeResolve = resolve;
  });
  const adapter = new CartesiaSonicTtsAdapter({
    apiKey: config.cartesiaApiKey,
    voiceId: config.cartesiaVoiceId,
    websocketFactory: config.cartesiaWebSocketFactory ?? makeNodeCartesiaFactory(hooks),
    sampleRate,
    channels: 1,
    encoding: "pcm_s16le",
  });
  const stream = adapter.createStream(
    { traceId: crypto.randomUUID(), maxBufferDelayMs: 50 },
    {
      onAudioFrame: (event) => frames.push(Buffer.from(event.bytes)),
      onComplete: () => completeResolve(),
      onProviderError: (event) => {
        providerError = new Error(event.message || event.title || "Cartesia provider failed");
        hooks.log("warn", "cartesia tts provider error", {
          code: event.code,
          statusCode: event.statusCode,
          title: event.title,
        });
        completeResolve();
      },
      onCancelled: () => completeResolve(),
    },
  );
  try {
    await Promise.race([
      stream.opened,
      new Promise((_, reject) => setTimeout(() => reject(new Error("Cartesia open timeout")), 10_000)),
    ]);
    stream.sendPhrase({ text, continueContext: false, flush: true, maxBufferDelayMs: 50 });
    await Promise.race([
      complete,
      new Promise((_, reject) => setTimeout(() => reject(new Error("Cartesia synthesis timeout")), 30_000)),
    ]);
    if (providerError) throw providerError;
    const pcm = Buffer.concat(frames);
    if (pcm.length === 0) throw new Error("Cartesia returned no audio");
    return wavFromPcm16Mono(pcm, sampleRate);
  } finally {
    await Promise.race([stream.closed, new Promise((resolve) => setTimeout(resolve, 1_000))]).catch(() => {});
  }
}

async function readRawBody(req: IncomingMessage, limitBytes: number): Promise<Buffer> {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    let settled = false;
    req.on("data", (chunk: Buffer) => {
      if (settled) return;
      total += chunk.length;
      if (total > limitBytes) {
        settled = true;
        reject(new BodyTooLargeError("body too large"));
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => { if (!settled) resolve(Buffer.concat(chunks)); });
    req.on("error", (err) => { if (!settled) reject(err); });
  });
}

/**
 * Boot the standalone voice backend. Installs the ES256 signing key + MOCK_REDIS
 * (same real sign/verify + consent/claim/revoke/metering paths as the harness),
 * selects the usage store the route selects, and mounts HTTP + WS on host:port.
 */
export async function startStandaloneVoiceServer(
  config: StandaloneServerConfig,
): Promise<RunningStandaloneServer> {
  const { hooks } = config;

  // Real sign/verify path; only the key material is generated (harness SHIM 4).
  await installVoiceSessionTestSigningKey();
  // In-memory Lua-capable Redis so the REAL consent/claim/revoke/dir + durable
  // metering paths run against a real store interface (harness SHIM 3).
  process.env.MOCK_REDIS = "1";
  process.env.VOICE_REALTIME_WS_ENABLED = "true";
  process.env.VOICE_REALTIME_CARTESIA_VOICE_ID = config.cartesiaVoiceId;
  process.env.VOICE_REALTIME_ELIZA_ENDPOINT = config.elizaEndpoint;
  process.env.VOICE_REALTIME_ELIZA_AUTHORIZATION = config.elizaAuthorization;
  process.env.DEEPGRAM_API_KEY = config.deepgramApiKey;
  process.env.CARTESIA_API_KEY = config.cartesiaApiKey;
  process.env.VOICE_AMBIENT_ENABLED = "true";
  // Placeholder base URL: the standalone service injects a local file-backed
  // store (no network hop), the same seam the harness documents for its store.
  process.env.VOICE_AMBIENT_PENDANT_BASE_URL =
    process.env.VOICE_AMBIENT_PENDANT_BASE_URL ?? "http://standalone.local";
  process.env.VOICE_AMBIENT_PENDANT_AUTHORIZATION =
    process.env.VOICE_AMBIENT_PENDANT_AUTHORIZATION ?? "Bearer standalone-server-held";

  const env = process.env as unknown as VoiceRealtimeEnv;

  // Fresh registry (process-global) so a prior boot's sessions never count.
  __resetVoiceSessionRegistryForTests();

  const usageLimits = resolveVoiceUsageLimits(env);
  const durableStore = createDurableVoiceUsageStore(
    env as unknown as Parameters<typeof createDurableVoiceUsageStore>[0],
  );
  const rawRedis = buildRedisClient(env as unknown as Parameters<typeof buildRedisClient>[0]);
  const evalCapable = typeof (rawRedis as unknown as { eval?: unknown } | null)?.eval === "function";
  const usageStore: VoiceUsageStore =
    durableStore && evalCapable ? durableStore : new InMemoryVoiceUsageStore();
  hooks.log("info", "usage store selected", { durable: Boolean(durableStore && evalCapable) });

  const maxSessions = resolveMaxSessions(env);
  const elizaModel = resolveElizaModel(env);
  const store = config.ambientStore;

  // ---- WS upgrade handler ----
  const wss = new WebSocketServer({ noServer: true });

  function attachRealHandler(ws: NodeWebSocket, sessionId: string): void {
    const serverSocket = adaptInboundSocket(ws);
    attachVoiceWsHandler(serverSocket, {
      requestedSessionId: sessionId,
      claimToken: (jti, expSeconds) => claimVoiceSessionToken(jti, expSeconds),
      admitSession: () => getVoiceSessionRegistry().size() < maxSessions,
      buildAmbientSession: ({
        claims,
        jti,
        tokenExpSeconds,
        pendantSessionId,
        captureLeaseToken,
        downlink,
      }) =>
        new AmbientSession({
          sessionId: claims.sessionId,
          jti,
          organizationId: claims.organizationId,
          userId: claims.userId,
          agentId: claims.agentId,
          pendantSessionId,
          captureLeaseToken,
          tokenExpSeconds,
          deepgramApiKey: config.deepgramApiKey,
          deepgramWebSocketFactory: makeNodeDeepgramFactory(hooks),
          store,
          usageStore,
          usageLimits,
          leaseMs: 20_000,
          isRevoked: (j) => isVoiceSessionTokenRevoked(j),
          onTeardownRevoke: (j, exp) => revokeVoiceSessionToken(j, exp),
          refreshRevocationDirectory: (j, expSeconds) =>
            recordVoiceSessionJti({
              organizationId: claims.organizationId,
              userId: claims.userId,
              sessionId: claims.sessionId,
              jti: j,
              expSeconds,
            }),
          downlink,
        }),
      buildSession: ({ claims, jti, tokenExpSeconds, downlink }) =>
        new VoiceSession({
          sessionId: claims.sessionId,
          jti,
          organizationId: claims.organizationId,
          userId: claims.userId,
          agentId: claims.agentId,
          conversationId: claims.conversationId,
          tokenExpSeconds,
          deepgramApiKey: config.deepgramApiKey,
          deepgramWebSocketFactory: makeNodeDeepgramFactory(hooks),
          cartesiaApiKey: config.cartesiaApiKey,
          cartesiaVoiceId: config.cartesiaVoiceId,
          cartesiaWebSocketFactory: makeNodeCartesiaFactory(hooks),
          elizaEndpoint: config.elizaEndpoint,
          elizaAuthorization: config.elizaAuthorization,
          elizaModel,
          usageStore,
          usageLimits,
          isRevoked: (j) => isVoiceSessionTokenRevoked(j),
          onTeardownRevoke: (j, exp) => revokeVoiceSessionToken(j, exp),
          downlink,
        }),
    });
  }

  // ---- mint helpers (REAL consent + jwt chain) ----
  async function mintConsent(): Promise<{ nonce: string; expiresAt: string }> {
    const issued = await issueConsentNonce(config.userId);
    if (!issued) throw new Error("consent store not configured");
    return issued;
  }

  async function mintConversation(consentNonce: string): Promise<{
    sessionId: string;
    token: string;
    expiresAt: string;
    wsUrl: string;
  }> {
    const consented = await consumeConsentNonce(config.userId, consentNonce);
    if (!consented) throw new ConsentError();
    const sessionId = crypto.randomUUID();
    const minted = await mintVoiceSessionToken({
      sessionId,
      organizationId: config.organizationId,
      userId: config.userId,
      agentId: config.agentId,
      conversationId: config.conversationId,
    });
    await recordVoiceSessionJti({
      organizationId: config.organizationId,
      userId: config.userId,
      sessionId,
      jti: minted.jti,
      expSeconds: minted.expSeconds,
    });
    return {
      sessionId,
      token: minted.token,
      expiresAt: minted.expiresAt,
      wsUrl: `${WS_PATH}?sessionId=${encodeURIComponent(sessionId)}`,
    };
  }

  async function mintAmbient(consentNonce: string): Promise<{
    sessionId: string;
    token: string;
    expiresAt: string;
    wsUrl: string;
    pendantSessionId: string;
    captureLeaseToken: string;
    leaseExpiresAt: string;
  }> {
    const consented = await consumeConsentNonce(config.userId, consentNonce);
    if (!consented) throw new ConsentError();
    const sessionId = crypto.randomUUID();
    const created = await store.createSession("cloud");
    const pendantSessionId = created.pendantSessionId;
    const lease = await store.acquireLease(pendantSessionId, `ambient:${sessionId}`, AMBIENT_LEASE_MS);
    const minted = await mintVoiceSessionToken({
      sessionId,
      organizationId: config.organizationId,
      userId: config.userId,
      agentId: config.agentId,
      conversationId: config.conversationId,
      mode: "ambient",
      pendantSessionId,
    });
    await recordVoiceSessionJti({
      organizationId: config.organizationId,
      userId: config.userId,
      sessionId,
      jti: minted.jti,
      expSeconds: minted.expSeconds,
    });
    return {
      sessionId,
      token: minted.token,
      expiresAt: minted.expiresAt,
      wsUrl: `${WS_PATH}?sessionId=${encodeURIComponent(sessionId)}`,
      pendantSessionId,
      captureLeaseToken: lease.leaseToken,
      leaseExpiresAt: lease.leaseExpiresAt,
    };
  }

  // ---- HTTP server ----
  const httpServer: Server = createServer((req, res) => {
    void handleHttp(req, res).catch((err) => {
      hooks.log("error", "http handler threw", { err: String(err) });
      if (!res.headersSent) json(res, 500, { error: "internal error" });
    });
  });

  async function handleHttp(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", "http://localhost");
    const path = url.pathname;

    // Health is UNAUTHENTICATED (localhost-only; nginx does not expose it
    // publicly unless a location is added). It reveals no secrets.
    if (path === HEALTH_PATH && req.method === "GET") {
      json(res, 200, {
        ok: true,
        service: "voice-standalone",
        liveSessions: getVoiceSessionRegistry().size(),
        maxSessions,
      });
      return;
    }

    // Everything else requires the shared-secret gate.
    const presented = bearerFrom(req);
    if (!presented || !safeEqual(presented, config.authToken)) {
      json(res, 401, { error: "unauthorized" });
      return;
    }

    if (path === CONSENT_PATH && req.method === "POST") {
      const issued = await mintConsent();
      json(res, 200, { consentNonce: issued.nonce, expiresAt: issued.expiresAt });
      return;
    }

    if (path === ASR_CLOUD_PATH && req.method === "POST") {
      let audio: Buffer;
      try {
        audio = await readRawBody(req, 25 * 1024 * 1024);
      } catch (err) {
        json(res, err instanceof BodyTooLargeError ? 413 : 400, {
          error: err instanceof BodyTooLargeError ? "audio exceeds 25MB limit" : "invalid audio body",
        });
        return;
      }
      if (audio.length === 0) {
        json(res, 400, { error: "audio body required" });
        return;
      }
      const contentType = String(req.headers["content-type"] ?? "").split(";", 1)[0].trim();
      if (contentType !== "audio/wav" || audio.length < 12 || audio.toString("ascii", 0, 4) !== "RIFF" || audio.toString("ascii", 8, 12) !== "WAVE") {
        json(res, 400, { error: "valid audio/wav body required" });
        return;
      }
      // #ios-asr-fix: iPhone (installed PWA) posts WAVs whose declared RIFF/data
      // sizes don't match the payload — Deepgram answers those with a 408. Parse
      // + rewrite a canonical, self-consistent header before forwarding, and
      // forward the true sample_rate/encoding as query hints so Deepgram never
      // has to trust a header at all. Desktop WAVs are already canonical and
      // pass through byte-identical (rewritten=false).
      const normalized = normalizePcmWav(audio);
      // TEMP diagnostic: log exactly what the client posted so we can confirm
      // the iPhone header shape from /tmp/voice-standalone.log. Remove once the
      // iOS capture path is verified clean end to end.
      hooks.log("info", "asr cloud request wav", {
        incomingBytes: audio.length,
        declaredRiffSize: normalized?.declaredRiffSize ?? null,
        declaredDataBytes: normalized?.declaredDataBytes ?? null,
        sampleRate: normalized?.sampleRate ?? null,
        channels: normalized?.channels ?? null,
        bitsPerSample: normalized?.bitsPerSample ?? null,
        realDataBytes: normalized?.dataBytes ?? null,
        rewritten: normalized?.rewritten ?? null,
      });
      if (!normalized) {
        json(res, 400, { error: "unparseable WAV body" });
        return;
      }
      if (normalized.dataBytes <= 0) {
        // Genuinely empty/near-silent capture (iOS suspended AudioContext or
        // denied mic) — a clear client-facing error beats a doomed 408 round-trip.
        json(res, 400, { error: "audio contained no samples (check microphone permission)" });
        return;
      }
      const dgUrl = new URL("https://api.deepgram.com/v1/listen");
      dgUrl.searchParams.set("model", "nova-3");
      dgUrl.searchParams.set("smart_format", "true");
      // Forward strategy: for 16-bit PCM (what encodeMonoPcm16Wav always emits),
      // strip the WAV header entirely and send RAW PCM with explicit encoding
      // hints. This side-steps ANY header trust issue — Deepgram decodes from
      // the query params, so a malformed/placeholder iOS header can't cause a
      // 408. For any other bit depth, fall back to forwarding the rewritten
      // (now self-consistent) WAV so the container/format is still valid.
      let forwardBody: Buffer;
      if (normalized.bitsPerSample === 16) {
        forwardBody = normalized.body.subarray(44); // raw PCM, header stripped
        dgUrl.searchParams.set("encoding", "linear16");
        dgUrl.searchParams.set("sample_rate", String(normalized.sampleRate));
        dgUrl.searchParams.set("channels", String(normalized.channels));
      } else {
        forwardBody = normalized.body; // rewritten canonical WAV container
      }
      let provider: Response;
      try {
        provider = await (config.deepgramFetch ?? fetch)(
          dgUrl.toString(),
          {
            method: "POST",
            headers: {
              Authorization: `Token ${config.deepgramApiKey}`,
              // Raw PCM forward uses octet-stream; the WAV-container fallback keeps audio/wav.
              "Content-Type": normalized.bitsPerSample === 16 ? "application/octet-stream" : "audio/wav",
              Accept: "application/json",
            },
            body: forwardBody.buffer.slice(
              forwardBody.byteOffset,
              forwardBody.byteOffset + forwardBody.byteLength,
            ) as ArrayBuffer,
          },
        );
      } catch (err) {
        hooks.log("warn", "batch ASR provider transport failed", { err: String(err) });
        json(res, 502, { error: "ASR provider unavailable" });
        return;
      }
      if (!provider.ok) {
        hooks.log("warn", "batch ASR provider rejected request", { status: provider.status });
        json(res, 502, { error: "ASR provider failed" });
        return;
      }
      const payload = (await provider.json().catch(() => null)) as { results?: { channels?: Array<{ alternatives?: Array<{ transcript?: unknown }> }> } } | null;
      const transcript = payload?.results?.channels?.[0]?.alternatives?.[0]?.transcript;
      if (typeof transcript !== "string") {
        json(res, 502, { error: "ASR provider returned an invalid response" });
        return;
      }
      json(res, 200, { text: transcript.trim() });
      return;
    }


    if (path === TTS_CLOUD_PATH && req.method === "POST") {
      let body: { text?: unknown };
      try {
        body = (await readJsonBody(req, 128 * 1024)) as typeof body;
      } catch {
        json(res, 400, { error: "invalid tts request body" });
        return;
      }
      const text = typeof body.text === "string" ? body.text.trim() : "";
      if (!text) {
        json(res, 400, { error: "text required" });
        return;
      }
      try {
        const audio = await synthesizeCartesiaWav(config, hooks, text);
        res.writeHead(200, {
          "content-type": "audio/wav",
          "content-length": audio.length,
          "cache-control": "no-store",
        });
        res.end(audio);
      } catch (err) {
        hooks.log("warn", "cartesia cloud tts failed", {
          err: err instanceof Error ? err.message : String(err),
        });
        json(res, 502, { error: "TTS provider failed" });
      }
      return;
    }

    if (path === MINT_PATH && req.method === "POST") {
      let body: { consentNonce?: unknown; mode?: unknown };
      try {
        body = (await readJsonBody(req)) as typeof body;
      } catch {
        json(res, 400, { error: "invalid mint request body" });
        return;
      }
      const consentNonce = typeof body.consentNonce === "string" ? body.consentNonce : "";
      if (!consentNonce) {
        json(res, 400, { error: "consentNonce required" });
        return;
      }
      const mode = body.mode === "ambient" ? "ambient" : "conversation";
      try {
        if (mode === "ambient") {
          const minted = await mintAmbient(consentNonce);
          json(res, 200, {
            sessionId: minted.sessionId,
            wsUrl: minted.wsUrl,
            token: minted.token,
            expiresAt: minted.expiresAt,
            mode: "ambient",
            pendantSessionId: minted.pendantSessionId,
            captureLeaseToken: minted.captureLeaseToken,
            leaseExpiresAt: minted.leaseExpiresAt,
            uplink: { codecs: ["pcm16"] },
            downlink: { codecs: [] },
            processingLocation: "cloud",
            iceServers: null,
          });
        } else {
          const minted = await mintConversation(consentNonce);
          json(res, 200, {
            sessionId: minted.sessionId,
            wsUrl: minted.wsUrl,
            token: minted.token,
            expiresAt: minted.expiresAt,
            uplink: { codecs: ["pcm16"] },
            downlink: { codecs: ["pcm16"] },
            iceServers: null,
          });
        }
      } catch (err) {
        if (err instanceof ConsentError) {
          json(res, 403, { error: "consent required", code: "consent_required" });
          return;
        }
        hooks.log("error", "mint failed", { err: String(err) });
        json(res, 500, { error: "failed to mint voice session" });
      }
      return;
    }

    // Read-only segment inspection for the standalone service's own file store.
    if (path.startsWith(SEGMENTS_PATH_PREFIX) && req.method === "GET") {
      const pendantSessionId = url.searchParams.get("pendantSessionId");
      const inspectable = store as unknown as {
        readSegments?: (id: string) => unknown[];
        listSessions?: () => unknown[];
      };
      if (pendantSessionId && typeof inspectable.readSegments === "function") {
        json(res, 200, { pendantSessionId, segments: inspectable.readSegments(pendantSessionId) });
      } else if (typeof inspectable.listSessions === "function") {
        json(res, 200, { sessions: inspectable.listSessions() });
      } else {
        json(res, 404, { error: "no inspectable store" });
      }
      return;
    }

    json(res, 404, { error: "not found" });
  }

  // ---- WS upgrade wiring ----
  httpServer.on("upgrade", (req: IncomingMessage, socket, head) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname !== WS_PATH) {
      socket.destroy();
      return;
    }
    const sessionId = url.searchParams.get("sessionId");
    if (!sessionId) {
      socket.destroy();
      return;
    }
    // Capacity pre-check against the LIVE registry (mirrors ws/route.ts). The
    // WS itself is headerless (token rides in the first hello frame, verified by
    // attachVoiceWsHandler) — exactly the production contract.
    if (getVoiceSessionRegistry().size() >= maxSessions) {
      socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      attachRealHandler(ws, sessionId);
    });
  });

  await new Promise<void>((resolve) => httpServer.listen(config.port, config.host, resolve));
  hooks.log("info", "standalone voice server listening", { host: config.host, port: config.port });

  async function stop(): Promise<void> {
    // Graceful shutdown severs live sessions (DoD lifecycle test).
    for (const client of wss.clients) {
      try {
        client.terminate();
      } catch {
        /* gone */
      }
    }
    await withTimeout(new Promise<void>((resolve) => wss.close(() => resolve())), 2000);
    await withTimeout(new Promise<void>((resolve) => httpServer.close(() => resolve())), 2000);
    try {
      httpServer.closeAllConnections?.();
    } catch {
      /* older node */
    }
    __resetVoiceSessionRegistryForTests();
  }

  return { host: config.host, port: config.port, stop };
}

class ConsentError extends Error {
  constructor() {
    super("consent nonce consume failed");
    this.name = "ConsentError";
  }
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T | void> {
  return Promise.race([p, new Promise<void>((resolve) => setTimeout(resolve, ms))]);
}
