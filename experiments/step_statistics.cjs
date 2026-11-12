"use strict";

// Exact rank over conservative 0.1 ms bins, without storing every send delay.
class LagHistogram {
  constructor() { this.bins = new Map(); this.count = 0; }
  add(milliseconds) {
    const bin = Math.ceil(Math.max(0, milliseconds) * 10);
    this.bins.set(bin, (this.bins.get(bin) || 0) + 1); this.count++;
  }
  percentile(q) {
    if (!this.count) return null;
    const rank = Math.ceil(q * this.count); let cumulative = 0;
    for (const bin of [...this.bins.keys()].sort((a, b) => a - b)) {
      cumulative += this.bins.get(bin);
      if (cumulative >= rank) return bin / 10;
    }
    throw Error("Histogram count mismatch");
  }
}
function sortedQuantile(sorted, q) { return sorted.length ? sorted[Math.ceil(sorted.length * q) - 1] : null; }
module.exports = { LagHistogram, sortedQuantile };
