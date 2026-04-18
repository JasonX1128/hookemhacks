import type { VisibleStatItem } from "../shared/contracts";

export function isMeaningfulStat(stat: VisibleStatItem): boolean {
  if (!stat.label || !stat.value) {
    return false;
  }

  if (stat.label === "Displayed %" || stat.label === "Displayed price") {
    return false;
  }

  if (stat.label.toLowerCase().includes("displayed")) {
    return false;
  }

  return true;
}

export function filterMeaningfulStats(stats: VisibleStatItem[]): VisibleStatItem[] {
  const deduped: VisibleStatItem[] = [];
  const seen = new Set<string>();

  for (const stat of stats) {
    if (!isMeaningfulStat(stat)) {
      continue;
    }

    const key = `${stat.label}|${stat.value}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    deduped.push(stat);
  }

  return deduped.sort((left, right) => right.priority - left.priority);
}
