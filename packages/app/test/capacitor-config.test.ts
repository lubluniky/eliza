import { afterEach, describe, expect, it } from "vitest";
import {
  resolveServerUrl,
  TEST_APK_PUBLIC_SERVER_HOST,
} from "../capacitor.config";

const ORIGINAL_ENV = { ...process.env };

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
});

describe("Capacitor server URL override", () => {
  it("keeps private and loopback server overrides available by default", () => {
    expect(resolveServerUrl("http://127.0.0.1:5173/")).toBe(
      "http://127.0.0.1:5173",
    );
    expect(resolveServerUrl("https://agent.internal")).toBe(
      "https://agent.internal",
    );
  });

  it("rejects public server overrides unless the test APK opt-in is enabled", () => {
    expect(
      resolveServerUrl(`https://${TEST_APK_PUBLIC_SERVER_HOST}`),
    ).toBeUndefined();
  });

  it("allows only the sol-dev HTTPS host when the test APK opt-in is enabled", () => {
    expect(
      resolveServerUrl(`https://${TEST_APK_PUBLIC_SERVER_HOST}/`, true),
    ).toBe(`https://${TEST_APK_PUBLIC_SERVER_HOST}`);
    expect(
      resolveServerUrl(`http://${TEST_APK_PUBLIC_SERVER_HOST}`, true),
    ).toBeUndefined();
    expect(resolveServerUrl("https://example.com", true)).toBeUndefined();
    expect(
      resolveServerUrl(`https://user:pass@${TEST_APK_PUBLIC_SERVER_HOST}`, true),
    ).toBeUndefined();
  });

  it("keeps store builds from using any remote server override", () => {
    process.env.ELIZA_CAPACITOR_BUILD_TARGET = "ios";
    process.env.ELIZA_BUILD_VARIANT = "store";
    expect(
      resolveServerUrl(`https://${TEST_APK_PUBLIC_SERVER_HOST}`, true),
    ).toBeUndefined();
  });
});
