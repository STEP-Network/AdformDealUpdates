import { Deal, AdUnit, MatchedPlacement, DealWithPlacements } from "./types";

/**
 * Match deals to ad units based on overlapping format IDs.
 *
 * For each deal, find ad units that share at least one format.
 * For each matched ad unit, filter its creative settings to only those
 * whose format matches the deal's formats.
 *
 * This replicates the Make.com modules 21-28 logic.
 */
export function matchDealsToAdUnits(deals: Deal[], adUnits: AdUnit[]): DealWithPlacements[] {
  return deals.map((deal) => {
    const dealFormatSet = new Set(deal.formatIds);

    const matchedPlacements: MatchedPlacement[] = [];

    for (const adUnit of adUnits) {
      // Skip ad units with no Adform placement ID
      if (!adUnit.adformPlacementId) continue;

      // Skip INACTIVE / ARCHIVED ad units — they should not be pushed to Adform deals
      const status = (adUnit.statusLabel || "").toUpperCase();
      if (status === "INACTIVE" || status === "ARCHIVED") continue;

      // Check if ad unit has at least one format matching the deal
      const hasFormatOverlap = adUnit.formatIds.some((fid) => dealFormatSet.has(fid));
      if (!hasFormatOverlap) continue;

      // Filter creative settings: only those whose format matches the deal
      const matchedCsIds = adUnit.creativeSettings
        .filter((cs) => cs.formatIds.some((fid) => dealFormatSet.has(fid)))
        .map((cs) => cs.adformCsId)
        .filter((id) => id !== ""); // skip CS with no Adform ID

      matchedPlacements.push({
        adUnitName: adUnit.name,
        adformPlacementId: adUnit.adformPlacementId,
        mondayCsIds: matchedCsIds,
      });
    }

    return {
      mondayId: deal.mondayId,
      name: deal.name,
      adformDealId: deal.adformDealId,
      matchedPlacements,
    };
  });
}

/**
 * Intersect Monday.com CS IDs with Adform's actual placement CS IDs.
 *
 * This replicates the Make.com modules 58-62 logic:
 * - Fetch each placement from Adform to get its real CS IDs
 * - Only keep Monday CS IDs that exist in the Adform placement
 *
 * Returns the filtered list (may be empty → placement should be skipped).
 */
export function intersectCreativeSettings(
  mondayCsIds: string[],
  adformCsIds: number[]
): number[] {
  const adformSet = new Set(adformCsIds);

  return mondayCsIds
    .map((id) => parseInt(id, 10))
    .filter((id) => !isNaN(id) && adformSet.has(id));
}
