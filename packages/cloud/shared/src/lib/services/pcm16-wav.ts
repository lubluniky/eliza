/**
 * Validates streamed mono PCM16 audio and wraps it in a canonical WAV container.
 * The bounded drain protects Worker memory while preserving the complete byte
 * count required by the RIFF header.
 */

import { ElizaError } from "@elizaos/core";

const WAV_HEADER_BYTES = 44;
const MAX_RIFF_DATA_BYTES = 0xffff_ffff - 36;
const MAX_PCM16_SAMPLE_RATE = Math.floor(0xffff_ffff / 2);

function invalidPcm(
  message: string,
  context: Record<string, unknown>,
  cause?: unknown,
): ElizaError {
  return new ElizaError(message, {
    code: "TTS_PCM_INVALID",
    context,
    severity: "ephemeral",
    ...(cause !== undefined ? { cause } : {}),
  });
}

function validateSampleRate(sampleRate: number): void {
  if (!Number.isSafeInteger(sampleRate) || sampleRate <= 0 || sampleRate > MAX_PCM16_SAMPLE_RATE) {
    throw invalidPcm("PCM16 sample rate must fit the RIFF uint32 byte rate", {
      sampleRate,
      maxSampleRate: MAX_PCM16_SAMPLE_RATE,
    });
  }
}

function writeWavHeader(
  output: Uint8Array<ArrayBuffer>,
  pcmBytes: number,
  sampleRate: number,
): void {
  const view = new DataView(output.buffer);
  const writeAscii = (offset: number, value: string) => {
    for (let index = 0; index < value.length; index += 1) {
      view.setUint8(offset + index, value.charCodeAt(index));
    }
  };

  writeAscii(0, "RIFF");
  view.setUint32(4, 36 + pcmBytes, true);
  writeAscii(8, "WAVE");
  writeAscii(12, "fmt ");
  view.setUint32(16, 16, true);
  view.setUint16(20, 1, true);
  view.setUint16(22, 1, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true);
  view.setUint16(34, 16, true);
  writeAscii(36, "data");
  view.setUint32(40, pcmBytes, true);
}

/**
 * Drains bounded PCM16 into a WAV container without first allocating a merged
 * PCM copy. Peak audio retention is the upstream chunks plus one final output.
 */
export async function drainPcm16ToWav(
  stream: ReadableStream<Uint8Array>,
  maxBytes: number,
  sampleRate: number,
): Promise<Uint8Array<ArrayBuffer>> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0 || maxBytes > MAX_RIFF_DATA_BYTES) {
    throw invalidPcm("PCM16 byte limit is outside the WAV container range", {
      maxBytes,
    });
  }
  validateSampleRate(sampleRate);

  const reader = stream.getReader();
  const chunks: Uint8Array<ArrayBuffer>[] = [];
  let total = 0;
  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) break;
      total += result.value.byteLength;
      if (total > maxBytes) {
        const context = {
          maxBytes,
          receivedBytes: total,
        };
        try {
          await reader.cancel("PCM16 response exceeded the configured byte limit");
        } catch (cause) {
          // error-policy:J2 preserve the quota failure while exposing failed upstream cancellation
          throw invalidPcm(
            "PCM16 response exceeded the configured byte limit",
            { ...context, cancellationFailed: true },
            cause,
          );
        }
        throw invalidPcm("PCM16 response exceeded the configured byte limit", context);
      }
      // A narrow view can otherwise retain an arbitrarily large upstream
      // ArrayBuffer and defeat the byte limit while the stream is drained.
      chunks.push(result.value.slice());
    }
  } catch (cause) {
    // error-policy:J2 stream failures need a stable code without losing the upstream cause
    if (cause instanceof ElizaError) throw cause;
    throw new ElizaError("PCM16 stream read failed", {
      code: "TTS_PCM_READ_FAILED",
      context: { receivedBytes: total },
      cause,
      severity: "ephemeral",
    });
  } finally {
    reader.releaseLock();
  }

  if (total === 0 || total % 2 !== 0) {
    throw invalidPcm("PCM16 response must contain complete 16-bit samples", {
      receivedBytes: total,
    });
  }

  const output = new Uint8Array(WAV_HEADER_BYTES + total);
  writeWavHeader(output, total, sampleRate);
  let offset = WAV_HEADER_BYTES;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

/** Wraps little-endian mono PCM16 samples in a 44-byte RIFF/WAV header. */
export function pcm16ToWav(pcm: Uint8Array, sampleRate: number): Uint8Array<ArrayBuffer> {
  if (pcm.byteLength === 0 || pcm.byteLength % 2 !== 0) {
    throw invalidPcm("PCM16 input must contain complete 16-bit samples", {
      receivedBytes: pcm.byteLength,
    });
  }
  validateSampleRate(sampleRate);
  if (pcm.byteLength > MAX_RIFF_DATA_BYTES) {
    throw invalidPcm("PCM16 input exceeds the WAV container range", {
      receivedBytes: pcm.byteLength,
    });
  }

  const output = new Uint8Array(WAV_HEADER_BYTES + pcm.byteLength);
  writeWavHeader(output, pcm.byteLength, sampleRate);
  output.set(pcm, WAV_HEADER_BYTES);
  return output;
}
