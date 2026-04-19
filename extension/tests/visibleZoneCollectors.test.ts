import { describe, expect, it } from "vitest";

import { collectZoneTextSnapshotFromLines } from "../src/content/visibleZoneCollectors";
import { deadlineSampleLines } from "./fixtures/visibleMarketSamples";

describe("collectZoneTextSnapshotFromLines", () => {
  it("hydrates all zones from line fixtures for deterministic tests", () => {
    const snapshot = collectZoneTextSnapshotFromLines(deadlineSampleLines);

    expect(snapshot.header.length).toBeGreaterThan(0);
    expect(snapshot.legend.length).toBe(snapshot.header.length);
    expect(snapshot.contractRow).toEqual(snapshot.header);
  });
});
