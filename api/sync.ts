import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getPublisherLinks,
  getDeals,
  getAdUnits,
  getCreativeSettings,
  attachCreativeSettings,
  updatePublisherStatus,
} from "../lib/monday";
import { authenticate, getDeal, getPlacement, updateDeal } from "../lib/adform";
import { matchDealsToAdUnits, intersectCreativeSettings } from "../lib/matcher";
import type { DealSyncResult, SyncResult, AdformDeal, DealWithPlacements, PlacementDetail, CsInfo } from "../lib/types";

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    // ── Monday webhook challenge (required for webhook registration) ──
    if (req.body?.challenge) {
      return res.status(200).json({ challenge: req.body.challenge });
    }

    // ── Determine publisher ID and options ──
    let publisherId: string | undefined;
    let dryRun = false;
    let maxDeals: number | null = null;
    let verbose = false;

    if (req.method === "GET") {
      // Test mode: GET /api/sync?publisherId=123&maxDeals=1&dryRun=true&verbose=true
      publisherId = req.query.publisherId as string;
      dryRun = req.query.dryRun === "true";
      verbose = req.query.verbose === "true";
      maxDeals = req.query.maxDeals ? parseInt(req.query.maxDeals as string, 10) : null;
    } else if (req.method === "POST") {
      // Monday webhook: POST with event payload
      publisherId = req.body?.event?.pulseId?.toString();

      // Also support direct POST with JSON body for flexibility
      if (!publisherId) {
        publisherId = req.body?.publisherId?.toString();
      }

      // Allow overrides via query params even on POST
      if (req.query.dryRun === "true") dryRun = true;
      if (req.query.verbose === "true") verbose = true;
      if (req.query.maxDeals) maxDeals = parseInt(req.query.maxDeals as string, 10);
    }

    if (!publisherId) {
      return res.status(400).json({
        error: "Missing publisherId. Use GET ?publisherId=123 or POST with Monday webhook payload.",
      });
    }

    console.log(`[Sync] Starting for publisher ${publisherId} | dryRun=${dryRun} | maxDeals=${maxDeals}`);

    // ══════════════════════════════════════════════
    // PHASE 1: Gather data from Monday.com (~4 API calls)
    // ══════════════════════════════════════════════

    // Step 1: Get publisher's linked deal IDs + ad unit IDs (1 call)
    const { dealIds, adUnitIds, publisherName } = await getPublisherLinks(publisherId);
    console.log(`[Sync] Publisher "${publisherName}" has ${dealIds.length} deals, ${adUnitIds.length} ad units`);

    if (dealIds.length === 0) {
      const msg = `No deals linked to publisher "${publisherName}"`;
      await updatePublisherStatus(publisherId, `⚠️ ${new Date().toISOString().replace("T", " ").slice(0, 19)} — ${msg}`);
      return res.status(200).json({ message: msg, publisherName });
    }

    // Step 2: Batch fetch all deals + formats (1 call)
    let deals = await getDeals(dealIds);
    console.log(`[Sync] Fetched ${deals.length} deals`);

    // Apply maxDeals limit
    if (maxDeals !== null && maxDeals > 0) {
      deals = deals.slice(0, maxDeals);
      console.log(`[Sync] Limited to ${deals.length} deals (maxDeals=${maxDeals})`);
    }

    // Step 3: Batch fetch all ad units + their format/CS links (1 call)
    const { adUnits, csIds, adUnitCsLinks } = await getAdUnits(adUnitIds);
    console.log(`[Sync] Fetched ${adUnits.length} ad units, found ${csIds.length} unique CS references`);

    // Step 4: Batch fetch all creative settings (1 call)
    const csMap = await getCreativeSettings(csIds);
    console.log(`[Sync] Fetched ${csMap.size} creative settings`);

    // Attach CS details to ad units
    attachCreativeSettings(adUnits, csMap, adUnitCsLinks);

    // Build Adform CS ID → name lookup (for verbose output)
    const csNameLookup = new Map<number, string>();
    for (const cs of csMap.values()) {
      if (cs.adformCsId && cs.name) {
        csNameLookup.set(parseInt(cs.adformCsId, 10), cs.name);
      }
    }

    // ══════════════════════════════════════════════
    // PHASE 2: Match deals ↔ ad units by format
    // ══════════════════════════════════════════════

    const dealsWithPlacements = matchDealsToAdUnits(deals, adUnits);

    const totalMatched = dealsWithPlacements.reduce((sum, d) => sum + d.matchedPlacements.length, 0);
    console.log(`[Sync] Matched ${totalMatched} placements across ${dealsWithPlacements.length} deals`);

    // ══════════════════════════════════════════════
    // PHASE 3: Adform API — auth + process each deal
    // ══════════════════════════════════════════════

    const token = await authenticate();
    console.log(`[Sync] Adform authenticated`);

    const dealResults: DealSyncResult[] = [];

    // Process deals in batches of 3 to avoid Adform rate limits
    const DEAL_BATCH_SIZE = 3;
    const results: PromiseSettledResult<DealSyncResult>[] = [];

    for (let i = 0; i < dealsWithPlacements.length; i += DEAL_BATCH_SIZE) {
      const batch = dealsWithPlacements.slice(i, i + DEAL_BATCH_SIZE);
      const batchResults = await Promise.allSettled(
        batch.map((dealMatch) =>
          processDeal(token, dealMatch, dryRun, verbose, csNameLookup)
        )
      );
      results.push(...batchResults);
    }

    for (let i = 0; i < results.length; i++) {
      const result = results[i];
      if (result.status === "fulfilled") {
        dealResults.push(result.value);
      } else {
        dealResults.push({
          dealName: dealsWithPlacements[i].name,
          adformDealId: dealsWithPlacements[i].adformDealId,
          placementsAdded: 0,
          placementsKept: 0,
          placementsSkipped: 0,
          status: "error",
          error: result.reason?.message || "Unknown error",
        });
      }
    }

    // ══════════════════════════════════════════════
    // PHASE 4: Update Monday status column
    // ══════════════════════════════════════════════

    const successCount = dealResults.filter((d) => d.status !== "error").length;
    const errorCount = dealResults.filter((d) => d.status === "error").length;
    const totalPlacementsAdded = dealResults.reduce((sum, d) => sum + d.placementsAdded, 0);
    const now = new Date().toISOString().replace("T", " ").slice(0, 19);

    let statusText: string;
    if (dryRun) {
      statusText = `🧪 ${now} — Dry run: ${deals.length} deals, ${totalPlacementsAdded} placements would sync`;
    } else if (errorCount === 0) {
      statusText = `✅ ${now} — ${successCount} deals updated, ${totalPlacementsAdded} placements synced`;
    } else if (successCount > 0) {
      statusText = `⚠️ ${now} — ${successCount} OK, ${errorCount} failed`;
    } else {
      statusText = `❌ ${now} — All ${errorCount} deals failed`;
    }

    await updatePublisherStatus(publisherId, statusText);

    // ── Return result ──
    const syncResult: SyncResult = {
      publisherId,
      publisherName,
      dryRun,
      maxDeals,
      deals: dealResults,
      timestamp: new Date().toISOString(),
    };

    // Add debug data in verbose mode
    if (verbose) {
      // Check which CS IDs were requested vs returned
      const csIdsRequested = csIds;
      const csIdsReturned = Array.from(csMap.keys());
      const csMissing = csIdsRequested.filter((id) => !csMap.has(id));

      // Check adUnitCsLinks for billboard_1
      const billboard1Links = Array.from(adUnitCsLinks.entries()).find(
        ([_, __]) => true // dump all
      );

      (syncResult as any).debug = {
        totalCsRequested: csIdsRequested.length,
        totalCsReturned: csIdsReturned.length,
        csMissing: csMissing.length > 0 ? csMissing.slice(0, 20) : "none",
        adUnitCsLinks: Object.fromEntries(
          Array.from(adUnitCsLinks.entries()).filter(([key]) => {
            const au = adUnits.find((a) => a.mondayId === key);
            return au?.name === "billboard_1";
          })
        ),
        dealsData: deals.map((d) => ({
          name: d.name,
          adformDealId: d.adformDealId,
          formatIds: d.formatIds,
        })),
        adUnitsData: adUnits
          .filter((au) => au.name === "billboard_1")
          .map((au) => ({
            name: au.name,
            mondayId: au.mondayId,
            adformPlacementId: au.adformPlacementId,
            formatIds: au.formatIds,
            creativeSettings: au.creativeSettings.map((cs) => ({
              mondayId: cs.mondayId,
              name: cs.name,
              adformCsId: cs.adformCsId,
              formatIds: cs.formatIds,
            })),
          })),
      };
    }

    console.log(`[Sync] Complete: ${JSON.stringify(syncResult)}`);
    return res.status(200).json(syncResult);

  } catch (err: any) {
    console.error("[Sync] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}

// ── Helper: resolve CS IDs to {id, name} using Monday lookup ──
function resolveCsIds(ids: number[], lookup: Map<number, string>): CsInfo[] {
  return ids.map((id) => ({
    id,
    name: lookup.get(id) || undefined,
  }));
}

// ── Process a single deal: GET → merge → PUT ──
async function processDeal(
  token: string,
  dealMatch: DealWithPlacements,
  dryRun: boolean,
  verbose: boolean,
  csNameLookup: Map<number, string>
): Promise<DealSyncResult> {
  const { adformDealId, matchedPlacements } = dealMatch;

  if (!adformDealId) {
    return {
      dealName: dealMatch.name,
      adformDealId: "",
      placementsAdded: 0,
      placementsKept: 0,
      placementsSkipped: 0,
      status: "error",
      error: "No Adform Deal ID on Monday item",
    };
  }

  // Step 1: GET existing deal from Adform
  const existingDeal = await getDeal(token, adformDealId);
  const existingPlacements = existingDeal.placements || [];

  // Step 2: For each matched placement, fetch its CS from Adform (parallel)
  const newPlacementIds = new Set(
    matchedPlacements.map((mp) => parseInt(mp.adformPlacementId, 10))
  );

  const placementDetailPromises = matchedPlacements.map((mp) =>
    getPlacement(token, mp.adformPlacementId).then((detail) => ({
      mp,
      detail,
    }))
  );
  const placementDetails = await Promise.all(placementDetailPromises);

  // Step 3: Intersect CS — only keep Monday CS IDs that exist in Adform placement
  const newPlacements: { id: number; creativeSettings: number[] }[] = [];
  const placementBreakdown: PlacementDetail[] = [];
  let skippedCount = 0;

  for (const { mp, detail } of placementDetails) {
    const adformCsIds = (detail.creativeSettings || []).map((cs: any) =>
      typeof cs === "number" ? cs : cs.id
    );

    const intersected = intersectCreativeSettings(mp.mondayCsIds, adformCsIds);

    if (intersected.length === 0) {
      console.log(`[Sync] Skipping placement ${mp.adformPlacementId} (${mp.adUnitName}) — no CS after intersection`);
      skippedCount++;
      if (verbose) {
        placementBreakdown.push({
          placementId: parseInt(mp.adformPlacementId, 10),
          adUnitName: mp.adUnitName,
          creativeSettings: [],
          action: "skipped",
          skipReason: `Monday CS [${mp.mondayCsIds.map(id => { const n = csNameLookup.get(parseInt(id,10)); return n ? `${id} (${n})` : id; }).join(", ")}] had no overlap with Adform CS [${adformCsIds.map(id => { const n = csNameLookup.get(id); return n ? `${id} (${n})` : String(id); }).join(", ")}]`,
        });
      }
      continue;
    }

    newPlacements.push({
      id: parseInt(mp.adformPlacementId, 10),
      creativeSettings: intersected,
    });
  }

  // Step 4: Merge placements
  // - Existing placements NOT matched by Monday → keep as-is
  // - Existing placements matched by Monday → keep existing CS + add new CS from Monday (never remove)
  // - New placements (not on Adform yet) → add with intersected CS

  const existingPlacementMap = new Map<number, any>();
  for (const ep of existingPlacements) {
    existingPlacementMap.set(ep.id, ep);
  }

  const mergedPlacements: { id: number; creativeSettings: number[] }[] = [];
  const addedPlacementIds = new Set<number>();

  // First: handle all Monday-matched placements
  for (const np of newPlacements) {
    const existing = existingPlacementMap.get(np.id);
    if (existing) {
      // Existing placement — merge CS: keep all existing + add new from Monday
      const existingCsSet = new Set<number>(existing.creativeSettings);
      const addedCs: number[] = [];
      for (const csId of np.creativeSettings) {
        if (!existingCsSet.has(csId)) {
          addedCs.push(csId);
        }
      }
      const mergedCs = [...existing.creativeSettings, ...addedCs];
      mergedPlacements.push({ id: np.id, creativeSettings: mergedCs });
      addedPlacementIds.add(np.id);

      if (verbose) {
        placementBreakdown.push({
          placementId: np.id,
          adUnitName: matchedPlacements.find(mp => parseInt(mp.adformPlacementId, 10) === np.id)?.adUnitName || "(matched)",
          creativeSettings: resolveCsIds(addedCs, csNameLookup),
          action: addedCs.length > 0 ? "merged" : "kept",
          ...(addedCs.length > 0 ? { mergeDetail: `Kept ${existing.creativeSettings.length} existing CS, added ${addedCs.length} new` } : {}),
        });
      }
    } else {
      // New placement — add with intersected CS
      mergedPlacements.push(np);
      addedPlacementIds.add(np.id);

      if (verbose) {
        placementBreakdown.push({
          placementId: np.id,
          adUnitName: matchedPlacements.find(mp => parseInt(mp.adformPlacementId, 10) === np.id)?.adUnitName || "(new)",
          creativeSettings: resolveCsIds(np.creativeSettings, csNameLookup),
          action: "added",
        });
      }
    }
  }

  // Then: keep all existing placements that weren't touched by Monday
  const keptPlacements = existingPlacements.filter(
    (ep: any) => !addedPlacementIds.has(ep.id)
  );

  for (const kp of keptPlacements) {
    mergedPlacements.push({ id: kp.id, creativeSettings: kp.creativeSettings });
    if (verbose) {
      placementBreakdown.push({
        placementId: kp.id,
        adUnitName: "(existing — not from Monday)",
        creativeSettings: resolveCsIds(kp.creativeSettings, csNameLookup),
        action: "kept",
      });
    }
  }

  // Step 5: Build the full deal body (preserve all original fields, replace placements)
  const updatedDeal: AdformDeal = {
    ...existingDeal,
    placements: mergedPlacements,
  };

  // Step 6: PUT (unless dry run)
  if (!dryRun) {
    await updateDeal(token, adformDealId, updatedDeal);
    console.log(`[Sync] ✅ Deal "${dealMatch.name}" (${adformDealId}) updated: ${newPlacements.length} new, ${keptPlacements.length} kept, ${skippedCount} skipped`);
  } else {
    console.log(`[Sync] 🧪 DRY RUN — Deal "${dealMatch.name}" (${adformDealId}) would update: ${newPlacements.length} new, ${keptPlacements.length} kept, ${skippedCount} skipped`);
  }

  // Count: truly new placements (not previously on Adform)
  const trulyNewCount = newPlacements.filter(np => !existingPlacementMap.has(np.id)).length;
  // Count: existing placements that gained new CS
  const mergedCount = newPlacements.filter(np => {
    const existing = existingPlacementMap.get(np.id);
    if (!existing) return false;
    const existingSet = new Set(existing.creativeSettings);
    return np.creativeSettings.some(cs => !existingSet.has(cs));
  }).length;

  const result: DealSyncResult = {
    dealName: dealMatch.name,
    adformDealId,
    placementsAdded: trulyNewCount,
    placementsMerged: mergedCount,
    placementsKept: keptPlacements.length + (newPlacements.length - trulyNewCount - mergedCount),
    placementsSkipped: skippedCount,
    status: dryRun ? "dryRun" : "updated",
  };

  if (verbose) {
    result.verbose = {
      existingDeal,    // Full Adform GET response (BEFORE)
      updatedDeal,     // Full PUT body (AFTER)
      placementBreakdown,
    };
  }

  return result;
}
