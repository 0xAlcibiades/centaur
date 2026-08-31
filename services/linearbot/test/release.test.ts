import { describe, expect, it } from "bun:test";

import { applyRelease } from "../src/index";
import type { LinearbotOptions, LinearbotTrace } from "../src/types";

const THREAD = "linear:issue-1";
const trace: LinearbotTrace = {
  includeContext: false,
  messageId: "release-issue-1",
  mode: "execute",
  openStream: false,
  startedAtMs: 0,
  threadId: THREAD,
};

function releaseOptions(calls: { method: string; url: string }[]): LinearbotOptions {
  return {
    apiUrl: "http://localhost",
    fetch: async (url: string, init?: { method?: string }) => {
      calls.push({ method: init?.method ?? "GET", url: String(url) });
      return new Response(JSON.stringify({ interrupted: true }), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    },
  } as unknown as LinearbotOptions;
}

describe("applyRelease", () => {
  it("interrupts even when the local pending entry says the turn has not started", async () => {
    // A newer assignment overwrote the map, so the entry this release reads
    // says `started: false` -- yet a turn from the evicted, earlier handoff
    // may still be streaming on the now-taken-back issue. Gating the interrupt
    // on that flag is the race that lets work continue, so it must be attempted
    // regardless.
    const calls: { method: string; url: string }[] = [];
    const pending = { released: false, started: false };
    await applyRelease(releaseOptions(calls), THREAD, pending, trace, "issue-1");

    expect(calls).toEqual([
      {
        method: "POST",
        url: "http://localhost/api/session/linear%3Aissue-1/interrupt",
      },
    ]);
    // A still-queued turn is separately marked so it will not start.
    expect(pending.released).toBe(true);
    expect(pending.started).toBe(false);
  });

  it("interrupts with no local pending entry at all", async () => {
    // A turn started before a restart leaves no local entry; the interrupt is
    // the only thing that can still reach it.
    const calls: { method: string; url: string }[] = [];
    await applyRelease(releaseOptions(calls), THREAD, undefined, trace, "issue-1");
    expect(calls).toEqual([
      {
        method: "POST",
        url: "http://localhost/api/session/linear%3Aissue-1/interrupt",
      },
    ]);
  });
});
