/**
 * Cloud TTS helpers — proxy to Eliza Cloud (`elizacloud.ai`).
 *
 * Pure / config-driven helpers (TTS API key resolution, base URL resolution,
 * voice / model id normalization, compat header mirroring) live in
 * `@elizaos/shared/elizacloud/server-cloud-tts` so host-layer packages can
 * resolve cloud TTS configuration without reverse-importing this plugin.
 *
 * The HTTP request handler (`handleCloudTtsPreviewRoute`) stays here because
 * it wires together the runtime route system.
 */
import type http from "node:http";
import { logger, sanitizeSpeechText } from "@elizaos/core";
import {
  _internalResolveCloudApiKey,
  ELIZA_CLOUD_TTS_MAX_TEXT_CHARS,
  resolveCloudProxyTtsModel,
  resolveCloudSttCandidateUrls,
  resolveCloudTtsCandidateUrls,
  resolveElizaCloudTtsVoiceId,
  shouldRetryCloudTtsUpstream,
  ttsDebug,
  ttsDebugTextPreview,
} from "@elizaos/shared";

export {
  __resetCloudBaseUrlCache,
  ELIZA_CLOUD_TTS_MAX_TEXT_CHARS,
  ensureCloudTtsApiKeyAlias,
  mirrorCompatHeaders,
  normalizeElizaCloudTtsModelId,
  resolveCloudProxyTtsModel,
  resolveCloudTtsBaseUrl,
  resolveCloudTtsCandidateUrls,
  resolveElevenLabsApiKeyForCloudMode,
  resolveElizaCloudTtsVoiceId,
  shouldRetryCloudTtsUpstream,
} from "@elizaos/shared";

/** Browser → API correlation (never forwarded to Eliza Cloud). */
export function readTtsDebugClientHeaders(
  req: Pick<http.IncomingMessage, "headers">,
): {
  messageId?: string;
  clipSegment?: string;
  hearingFull?: string;
} {
  const pick = (name: string): string | undefined => {
    const raw = req.headers[name];
    if (raw == null) return undefined;
    const v = Array.isArray(raw) ? raw[0] : raw;
    return typeof v === "string" && v.trim() ? v.trim() : undefined;
  };
  const decode = (enc: string | undefined): string | undefined => {
    if (!enc) return undefined;
    try {
      return decodeURIComponent(enc);
    } catch {
      return enc;
    }
  };
  return {
    messageId: decode(pick("x-elizaos-tts-message-id")),
    clipSegment: decode(pick("x-elizaos-tts-clip-segment")),
    hearingFull: decode(pick("x-elizaos-tts-full-preview")),
  };
}

function ttsClientDbgFields(
  hdr: ReturnType<typeof readTtsDebugClientHeaders>,
): Record<string, string> {
  const o: Record<string, string> = {};
  if (hdr.messageId) o.messageId = hdr.messageId;
  if (hdr.clipSegment) o.clipSegment = hdr.clipSegment;
  if (hdr.hearingFull) o.hearingFull = hdr.hearingFull;
  return o;
}

function pickBodyString(
  body: Record<string, unknown>,
  camel: string,
  snake: string,
): unknown {
  const a = body[camel];
  if (typeof a === "string" && a.trim()) return a;
  const b = body[snake];
  if (typeof b === "string" && b.trim()) return b;
  return undefined;
}

async function readRawRequestBody(req: http.IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks);
}

function sendJsonResponse(
  res: http.ServerResponse,
  status: number,
  body: unknown,
): void {
  if (res.headersSent) return;
  res.statusCode = status;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

function sendJsonErrorResponse(
  res: http.ServerResponse,
  status: number,
  message: string,
): void {
  sendJsonResponse(res, status, { error: message });
}

function forwardCloudTtsUpstreamError(
  res: http.ServerResponse,
  status: number,
  bodyText: string,
): void {
  if (res.headersSent) return;
  const trimmed = bodyText.trim();
  if (
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
    (trimmed.startsWith("[") && trimmed.endsWith("]"))
  ) {
    try {
      const parsed: unknown = JSON.parse(trimmed);
      sendJsonResponse(res, status, parsed);
      return;
    } catch {
      /* fall through */
    }
  }
  res.statusCode = status;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.end(
    JSON.stringify({ error: trimmed || "Eliza Cloud TTS request failed" }),
  );
}

export async function handleCloudTtsPreviewRoute(
  req: http.IncomingMessage,
  res: http.ServerResponse,
): Promise<boolean> {
  const clientTtsDbg = readTtsDebugClientHeaders(req);
  const dbgExtra = ttsClientDbgFields(clientTtsDbg);

  const cloudApiKey = _internalResolveCloudApiKey();
  if (!cloudApiKey) {
    ttsDebug("server:cloud-tts:reject", {
      reason: "no_api_key",
      ...dbgExtra,
    });
    sendJsonErrorResponse(
      res,
      401,
      "Eliza Cloud is not connected. Connect your Eliza Cloud account first.",
    );
    return true;
  }

  const rawBody = await readRawRequestBody(req);
  let body: Record<string, unknown>;
  try {
    body = JSON.parse(rawBody.toString("utf8")) as Record<string, unknown>;
  } catch {
    sendJsonErrorResponse(res, 400, "Invalid JSON request body");
    return true;
  }

  const text = sanitizeSpeechText(
    typeof body.text === "string" ? body.text : "",
  );
  if (!text) {
    sendJsonErrorResponse(res, 400, "Missing text");
    return true;
  }

  if (text.length > ELIZA_CLOUD_TTS_MAX_TEXT_CHARS) {
    sendJsonErrorResponse(
      res,
      400,
      `Text too long. Maximum length is ${ELIZA_CLOUD_TTS_MAX_TEXT_CHARS} characters`,
    );
    return true;
  }

  const cloudModel = resolveCloudProxyTtsModel(
    pickBodyString(body, "modelId", "model_id"),
  );
  const cloudVoice = resolveElizaCloudTtsVoiceId(
    pickBodyString(body, "voiceId", "voice_id"),
  );
  // Optional WAV passthrough: clients whose audio stack can't decode MP3 (the
  // Light Phone III / LightOS WebView) send `format:"wav"`; forward it to the
  // upstream so it returns PCM16 WAV. Default (unset) keeps the MP3 response.
  const cloudFormat = body.format === "wav" ? "wav" : undefined;
  const cloudUrls = resolveCloudTtsCandidateUrls();

  const ttsPreview = ttsDebugTextPreview(text);
  ttsDebug("server:cloud-tts:proxy", {
    textChars: text.length,
    preview: ttsPreview,
    modelId: cloudModel,
    voiceId: cloudVoice,
    urlCandidates: cloudUrls.length,
    ...dbgExtra,
  });

  try {
    let lastStatus = 0;
    let lastDetails = "unknown error";
    let cloudResponse: Response | null = null;
    for (let i = 0; i < cloudUrls.length; i++) {
      const cloudUrl = cloudUrls[i];
      if (cloudUrl === undefined) {
        continue;
      }
      const attempt = await fetch(cloudUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${cloudApiKey}`,
          "x-api-key": cloudApiKey,
          "Content-Type": "application/json",
          Accept: cloudFormat === "wav" ? "audio/wav" : "audio/mpeg",
        },
        body: JSON.stringify({
          text,
          voiceId: cloudVoice,
          modelId: cloudModel,
          ...(cloudFormat ? { format: cloudFormat } : {}),
        }),
      });

      if (attempt.ok) {
        cloudResponse = attempt;
        ttsDebug("server:cloud-tts:upstream-ok", {
          urlIndex: i,
          status: attempt.status,
          preview: ttsPreview,
          ...dbgExtra,
        });
        break;
      }

      lastStatus = attempt.status;
      lastDetails = await attempt.text().catch(() => "unknown error");
      ttsDebug("server:cloud-tts:upstream-retry", {
        urlIndex: i,
        status: attempt.status,
        preview: ttsPreview,
        ...dbgExtra,
      });

      const hasMoreCandidates = i < cloudUrls.length - 1;
      if (!hasMoreCandidates || !shouldRetryCloudTtsUpstream(attempt.status)) {
        break;
      }
    }
    if (!cloudResponse) {
      ttsDebug("server:cloud-tts:reject", {
        reason: "upstream_failed",
        lastStatus,
        preview: ttsPreview,
        ...dbgExtra,
      });
      if (
        lastStatus === 400 ||
        lastStatus === 401 ||
        lastStatus === 402 ||
        lastStatus === 403 ||
        lastStatus === 429
      ) {
        forwardCloudTtsUpstreamError(res, lastStatus, lastDetails);
        return true;
      }
      sendJsonErrorResponse(
        res,
        502,
        `Eliza Cloud TTS failed (${lastStatus || 502}): ${lastDetails}`,
      );
      return true;
    }

    const audioBuffer = Buffer.from(await cloudResponse.arrayBuffer());
    ttsDebug("server:cloud-tts:success", {
      bytes: audioBuffer.length,
      preview: ttsPreview,
      ...dbgExtra,
    });
    res.statusCode = 200;
    // Relay the upstream container so a WAV request returns audio/wav (the MP3
    // default is preserved when no format was requested).
    res.setHeader(
      "Content-Type",
      cloudResponse.headers.get("content-type")?.startsWith("audio/")
        ? (cloudResponse.headers.get("content-type") as string)
        : cloudFormat === "wav"
          ? "audio/wav"
          : "audio/mpeg",
    );
    res.setHeader("Cache-Control", "no-store");
    res.end(audioBuffer);
    return true;
  } catch (err) {
    sendJsonErrorResponse(
      res,
      502,
      `Eliza Cloud TTS request failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    return true;
  }
}

/** WAV bodies larger than this are rejected before proxying (cloud caps at 25MB). */
const ELIZA_CLOUD_STT_MAX_BYTES = 25 * 1024 * 1024;

/**
 * Read the raw request body up to a byte cap, aborting past the limit. The STT
 * body is opaque audio bytes (a WAV), so it is streamed straight through rather
 * than JSON-parsed — the cap prevents an oversized upload from buffering
 * unbounded before the cloud's own size check rejects it.
 */
async function readCappedRequestBody(
  req: http.IncomingMessage,
  maxBytes: number,
): Promise<Buffer | null> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    const buf = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
    total += buf.length;
    if (total > maxBytes) return null;
    chunks.push(buf);
  }
  return Buffer.concat(chunks);
}

/**
 * Agent-side proxy for interactive web STT (`POST /api/asr/cloud`). Interactive
 * capture records a mono PCM16 WAV client-side and POSTs the raw bytes here;
 * this forwards them as a multipart `audio` field to the same upstream cloud STT
 * route (`<cloud-base>/voice/stt`) the server-side transcription model uses, and
 * returns `{ text }`. It mirrors {@link handleCloudTtsPreviewRoute} exactly:
 * same base-URL / api-key resolution, same www/apex candidate fan-out, same
 * fail-loud contract — a missing key is 401 and an unreachable upstream is 502
 * so the capture surface renders a distinguishable error state rather than
 * silently downgrading to the browser recognizer.
 */
export async function handleCloudSttRoute(
  req: http.IncomingMessage,
  res: http.ServerResponse,
): Promise<boolean> {
  const cloudApiKey = _internalResolveCloudApiKey();
  if (!cloudApiKey) {
    sendJsonErrorResponse(
      res,
      401,
      "Eliza Cloud is not connected. Connect your Eliza Cloud account first.",
    );
    return true;
  }

  const rawBody = await readCappedRequestBody(req, ELIZA_CLOUD_STT_MAX_BYTES);
  if (rawBody === null) {
    sendJsonErrorResponse(res, 413, "Audio too large");
    return true;
  }
  if (rawBody.length === 0) {
    sendJsonErrorResponse(res, 400, "Missing audio body");
    return true;
  }

  const contentType = req.headers["content-type"];
  // The Android local-agent IPC forwards STRING request bodies only, so the
  // on-device WebView cannot POST raw audio bytes — it sends
  // `{ audioBase64, mimeType }` JSON instead (mirrors the local-inference ASR
  // route). Decode that to the same (bytes, mime) the raw-binary path uses so
  // cloud STT works over the native bridge, not just over real HTTP.
  let audioBytes: Buffer = rawBody;
  let jsonMime: string | undefined;
  if (
    typeof contentType === "string" &&
    contentType.includes("application/json")
  ) {
    try {
      const parsed = JSON.parse(rawBody.toString("utf8")) as {
        audioBase64?: unknown;
        mimeType?: unknown;
        mime?: unknown;
      };
      if (typeof parsed.audioBase64 === "string" && parsed.audioBase64) {
        audioBytes = Buffer.from(parsed.audioBase64, "base64");
        jsonMime =
          typeof parsed.mimeType === "string"
            ? parsed.mimeType
            : typeof parsed.mime === "string"
              ? parsed.mime
              : undefined;
      }
    } catch {
      // error-policy:J3 not the base64 envelope — fall through to the raw body.
    }
  }
  if (audioBytes.length === 0) {
    sendJsonErrorResponse(res, 400, "Missing audio body");
    return true;
  }

  const mime =
    jsonMime && jsonMime.trim()
      ? jsonMime.split(";", 1)[0]?.trim() || "audio/wav"
      : typeof contentType === "string" &&
          contentType.trim() &&
          !contentType.includes("application/json")
        ? contentType.split(";", 1)[0]?.trim() || "audio/wav"
        : "audio/wav";
  const filename = mime.includes("wav") ? "recording.wav" : "recording.bin";

  const cloudUrls = resolveCloudSttCandidateUrls();
  logger.debug(
    `[Cloud STT] proxying ${rawBody.length}B ${mime} to ${cloudUrls.length} candidate(s)`,
  );

  try {
    let lastStatus = 0;
    let lastDetails = "unknown error";
    let cloudResponse: Response | null = null;
    for (let i = 0; i < cloudUrls.length; i++) {
      const cloudUrl = cloudUrls[i];
      if (cloudUrl === undefined) continue;
      // Build the multipart body by hand rather than via FormData: Bun's fetch
      // on Android musl does not reliably emit the `multipart/form-data;
      // boundary=…` Content-Type from a FormData body when an explicit `headers`
      // object is present, so the upstream 400s ("Expected multipart form data
      // with audio field"). Hand-assembling the body guarantees the boundary and
      // part headers are correct on every runtime (web/desktop unaffected).
      const boundary = "elizaformboundary7MA4YWxkTrZu0gW1r8x";
      const partHead = Buffer.from(
        `--${boundary}\r\n` +
          `Content-Disposition: form-data; name="audio"; filename="${filename}"\r\n` +
          `Content-Type: ${mime}\r\n\r\n`,
        "utf8",
      );
      const partTail = Buffer.from(`\r\n--${boundary}--\r\n`, "utf8");
      const multipartBody = Buffer.concat([partHead, audioBytes, partTail]);
      const attempt = await fetch(cloudUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${cloudApiKey}`,
          "x-api-key": cloudApiKey,
          "Content-Type": `multipart/form-data; boundary=${boundary}`,
        },
        body: multipartBody,
      });
      if (attempt.ok) {
        cloudResponse = attempt;
        break;
      }
      lastStatus = attempt.status;
      try {
        lastDetails = await attempt.text();
      } catch {
        // error-policy:J6 upstream error bodies are diagnostic-only.
        lastDetails = "unknown error";
      }
      const hasMoreCandidates = i < cloudUrls.length - 1;
      if (!hasMoreCandidates || !shouldRetryCloudTtsUpstream(attempt.status)) {
        break;
      }
    }

    if (!cloudResponse) {
      logger.warn(`[Cloud STT] upstream failed (${lastStatus || 502})`);
      if (
        lastStatus === 400 ||
        lastStatus === 401 ||
        lastStatus === 402 ||
        lastStatus === 403 ||
        lastStatus === 429
      ) {
        forwardCloudTtsUpstreamError(res, lastStatus, lastDetails);
        return true;
      }
      sendJsonErrorResponse(
        res,
        502,
        `Eliza Cloud STT failed (${lastStatus || 502}): ${lastDetails}`,
      );
      return true;
    }

    let data: { text?: unknown; transcript?: unknown } | null;
    try {
      data = (await cloudResponse.json()) as {
        text?: unknown;
        transcript?: unknown;
      } | null;
    } catch {
      // error-policy:J3 malformed upstream JSON becomes an empty transcript.
      data = null;
    }
    const text =
      typeof data?.text === "string"
        ? data.text.trim()
        : typeof data?.transcript === "string"
          ? data.transcript.trim()
          : "";
    logger.debug(`[Cloud STT] transcript ${text.length} chars`);
    sendJsonResponse(res, 200, { text });
    return true;
  } catch (err) {
    // error-policy:J1 route boundary translates upstream/proxy failures.
    sendJsonErrorResponse(
      res,
      502,
      `Eliza Cloud STT request failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    return true;
  }
}
