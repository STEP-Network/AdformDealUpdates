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

// ── Time budget: stop processing before Vercel kills us ──
const MAX_DURATION_MS = 300_000; // matches vercel.json maxDuration (Pro plan)
const SAFETY_MARGIN_MS = 10_000; // stop 10s early for status write + response
const TIME_BUDGET_MS = MAX_DURATION_MS - SAFETY_MARGIN_MS;

export default async function handler(req: VercelRequest, res: VercelResponse) {
  // Declare outside try so catch block can access them
  let publisherId: string | undefined;
  let publisherName: string | undefined;
  const START_TIME = Date.now();

  function timeRemaining(): number {
    return TIME_BUDGET_MS - (Date.now() - START_TIME);
  }
  function isTimeUp(): boolean {
    return timeRemaining() <= 0;
  }
  function elapsedSec(): number {
    return Math.round((Date.now() - START_TIME) / 1000);
  }

  try {
    // ── Monday webhook challenge (required for webhook registration) ──
    if (req.body?.challenge) {
      return res.status(200).json({ challenge: req.body.challenge });
    }

    // ── Determine publisher ID and options ──
    let dryRun = false;
    let maxDeals: number | null = null;
    let skipDeals = 0;
    let verbose = false;

    if (req.method === "GET") {
      publisherId = req.query.publisherId as string;
      dryRun = req.query.dryRun === "true";
      verbose = req.query.verbose === "true";
      maxDeals = req.query.maxDeals ? parseInt(req.query.maxDeals as string, 10) : null;
      skipDeals = req.query.skipDeals ? parseInt(req.query.skipDeals as string, 10) : 0;
    } else if (req.method === "POST") {
      publisherId = req.body?.event?.pulseId?.toString();
      if (!publisherId) publisherId = req.body?.publisherId?.toString();
      if (req.query.dryRun === "true") dryRun = true;
      if (req.query.verbose === "true") verbose = true;
      if (req.query.maxDeals) maxDeals = parseInt(req.query.maxDeals as string, 10);
      if (req.query.skipDeals) skipDeals = parseInt(req.query.skipDeals as string, 10);
    }

    if (!publisherId) {
      return res.status(400).json({
        error: "Missing publisherId. Use GET ?publisherId=123 or POST with Monday webhook payload.",
      });
    }

    console.log(`[Sync] Starting for publisher ${publisherId} | dryRun=${dryRun} | maxDeals=${maxDeals} | skipDeals=${skipDeals}`);

    // ══════════════════════════════════════════════
    // PHASE 1: Gather data from Monday.com (~4 API calls)
    // ══════════════════════════════════════════════

    const links = await getPublisherLinks(publisherId);
    publisherName = links.publisherName;
    const { dealIds, adUnitIds } = links;
    console.log(`[Sync] Publisher "${publisherName}" has ${dealIds.length} deals, ${adUnitIds.length} ad units`);

    if (dealIds.length === 0) {
      const msg = `No deals linked to publisher "${publisherName}"`;
      await updatePublisherStatus(publisherId, `⚠️ ${new Date().toISOString().replace("T", " ").slice(0, 19)} — ${msg}`);
      return res.status(200).json({ message: msg, publisherName });
    }

    // Step 2: Batch fetch all deals + formats
    let deals = await getDeals(dealIds);
    const totalDealsOnMonday = deals.length;
    console.log(`[Sync] Fetched ${deals.length} deals`);

    // Filter out deals with excluded statuses (e.g. "Needs format/placments")
    const EXCLUDED_STATUSES = ["Needs format/placments"];
    const beforeFilter = deals.length;
    deals = deals.filter((d) => !d.statusLabel || !EXCLUDED_STATUSES.includes(d.statusLabel));
    if (beforeFilter !== deals.length) {
      console.log(`[Sync] Excluded ${beforeFilter - deals.length} deals with status in [${EXCLUDED_STATUSES.join(", ")}], ${deals.length} remaining`);
    }

    // Apply skipDeals + maxDeals limit
    if (skipDeals > 0) {
      deals = deals.slice(skipDeals);
      console.log(`[Sync] Skipped first ${skipDeals} deals, ${deals.length} remaining`);
    }
    if (maxDeals !== null && maxDeals > 0) {
      deals = deals.slice(0, maxDeals);
      console.log(`[Sync] Limited to ${deals.length} deals (maxDeals=${maxDeals})`);
    }

    // Step 3: Batch fetch all ad units + their format/CS links
    const { adUnits, csIds, adUnitCsLinks } = await getAdUnits(adUnitIds);
    console.log(`[Sync] Fetched ${adUnits.length} ad units, found ${csIds.length} unique CS references`);

    // Step 4: Batch fetch all creative settings
    const csMap = await getCreativeSettings(csIds);
    console.log(`[Sync] Fetched ${csMap.size} creative settings`);

    // Attach CS details to ad units
    attachCreativeSettings(adUnits, csMap, adUnitCsLinks);

    // Build Adform CS ID → name lookup (for verbose output)
    const csNameLookup = new Map<number, string>();
    csMap.forEach((cs) => {
      if (cs.adformCsId && cs.name) {
        csNameLookup.set(parseInt(cs.adformCsId, 10), cs.name);
      }
    });

    console.log(`[Sync] Phase 1 complete in ${elapsedSec()}s`);

    // ══════════════════════════════════════════════
    // PHASE 2: Match deals ↔ ad units by format
    // ══════════════════════════════════════════════

    const dealsWithPlacements = matchDealsToAdUnits(deals, adUnits);
    const totalMatched = dealsWithPlacements.reduce((sum, d) => sum + d.matchedPlacements.length, 0);
    console.log(`[Sync] Matched ${totalMatched} placements across ${dealsWithPlacements.length} deals`);

    // ══════════════════════════════════════════════
    // PHASE 3: Adform API — auth + process each deal (TIME-GUARDED)
    // ══════════════════════════════════════════════

    const token = await authenticate();
    console.log(`[Sync] Adform authenticated. ${elapsedSec()}s elapsed, ${Math.round(timeRemaining() / 1000)}s budget remaining`);

    // Log start to Monday
    await updatePublisherStatus(
      publisherId,
      `⏳ Starting: ${dealsWithPlacements.length} deals to process...`
    ).catch(() => {});

    const DEAL_BATCH_SIZE = 3;
    const PROGRESS_LOG_INTERVAL = 10;
    const dealResults: DealSyncResult[] = [];
    let processedCount = 0;
    let timedOut = false;

    for (let i = 0; i < dealsWithPlacements.length; i += DEAL_BATCH_SIZE) {
      // ── Time check BEFORE starting a new batch ──
      if (isTimeUp()) {
        timedOut = true;
        console.log(`[Sync] ⏱️ Time budget exhausted after ${processedCount}/${dealsWithPlacements.length} deals (${elapsedSec()}s). Stopping gracefully.`);
        break;
      }

      const batch = dealsWithPlacements.slice(i, i + DEAL_BATCH_SIZE);
      const batchResults = await Promise.allSettled(
        batch.map((dealMatch) =>
          processDeal(token, dealMatch, dryRun, verbose, csNameLookup)
        )
      );

      // Collect results
      for (let j = 0; j < batchResults.length; j++) {
        const result = batchResults[j];
        if (result.status === "fulfilled") {
          dealResults.push(result.value);
        } else {
          const dealMatch = batch[j];
          console.error(`[Sync] Deal "${dealMatch.name}" (${dealMatch.adformDealId}) failed:`, result.reason?.message);
          dealResults.push({
            dealName: dealMatch.name,
            adformDealId: dealMatch.adformDealId,
            placementsAdded: 0,
            placementsKept: 0,
            placementsSkipped: 0,
            status: "error",
            error: result.reason?.message || "Unknown error",
          });
        }
      }
      processedCount += batch.length;

      // ── Progressive logging every N deals ──
      if (processedCount % PROGRESS_LOG_INTERVAL < DEAL_BATCH_SIZE && processedCount > 0) {
        const errors = dealResults.filter((d) => d.status === "error").length;
        const errorNote = errors > 0 ? `, ${errors} errors` : "";
        const progressText = `⏳ ${processedCount}/${dealsWithPlacements.length} deals processed (${elapsedSec()}s${errorNote})`;
        console.log(`[Sync] ${progressText}`);
        // Fire and forget — don't block processing
        updatePublisherStatus(publisherId, progressText).catch(() => {});
      }
    }

    // ══════════════════════════════════════════════
    // PHASE 4: Final status + response
    // ══════════════════════════════════════════════

    const successCount = dealResults.filter((d) => d.status !== "error").length;
    const errorCount = dealResults.filter((d) => d.status === "error").length;
    const totalPlacementsAdded = dealResults.reduce((sum, d) => sum + d.placementsAdded, 0);
    const totalPlacementsMerged = dealResults.reduce((sum, d) => sum + (d.placementsMerged || 0), 0);
    const now = new Date().toISOString().replace("T", " ").slice(0, 19);
    const elapsed = elapsedSec();

    // Build detailed error list for logging
    const errorDeals = dealResults
      .filter((d) => d.status === "error")
      .map((d) => `${d.dealName}: ${d.error}`)
      .slice(0, 5); // max 5 in status

    let statusText: string;
    if (dryRun) {
      statusText = `🧪 ${now} — Dry run: ${deals.length} deals, ${totalPlacementsAdded} ad units would be added (${elapsed}s)`;
    } else if (timedOut) {
      const remaining = dealsWithPlacements.length - processedCount;
      const nextSkip = skipDeals + processedCount;
      statusText = `⏱️ ${now} — ${successCount}/${dealsWithPlacements.length} deals in ${elapsed}s. +${totalPlacementsAdded} ad units added, ${totalPlacementsMerged} merged.`;
      if (errorCount > 0) statusText += ` ${errorCount} errors.`;
      statusText += ` ${remaining} remain → skipDeals=${nextSkip}`;
    } else if (errorCount === 0) {
      statusText = `✅ ${now} — ${successCount} deals, +${totalPlacementsAdded} ad units added, ${totalPlacementsMerged} merged (${elapsed}s)`;
    } else if (successCount > 0) {
      statusText = `⚠️ ${now} — ${successCount} OK, ${errorCount} failed, +${totalPlacementsAdded} ad units added (${elapsed}s). Errors: ${errorDeals.join("; ")}`;
    } else {
      statusText = `❌ ${now} — All ${errorCount} deals failed (${elapsed}s). ${errorDeals[0] || ""}`;
    }

    // Await final status write — this one matters
    await updatePublisherStatus(publisherId, statusText);

    // ── Build response ──
    const syncResult: SyncResult = {
      publisherId,
      publisherName,
      dryRun,
      maxDeals,
      deals: dealResults,
      timestamp: new Date().toISOString(),
      timedOut,
      processedCount,
      totalCount: dealsWithPlacements.length,
      elapsedMs: Date.now() - START_TIME,
      ...(timedOut ? { nextSkipDeals: skipDeals + processedCount } : {}),
    };

    // Add debug data in verbose mode
    if (verbose) {
      const csMissing = csIds.filter((id) => !csMap.has(id));
      (syncResult as any).debug = {
        totalDealsOnMonday,
        skipDeals,
        totalCsRequested: csIds.length,
        totalCsReturned: csMap.size,
        csMissing: csMissing.length > 0 ? csMissing.slice(0, 20) : "none",
      };
    }

    console.log(`[Sync] Complete: ${successCount} OK, ${errorCount} errors, ${timedOut ? "TIMED OUT" : "all done"} in ${elapsed}s`);
    return res.status(200).json(syncResult);

  } catch (err: any) {
    console.error("[Sync] Fatal error:", err);

    // Log failure to Monday if we have a publisherId
    if (publisherId) {
      const now = new Date().toISOString().replace("T", " ").slice(0, 19);
      const elapsed = elapsedSec();
      await updatePublisherStatus(
        publisherId,
        `❌ ${now} — Fatal error after ${elapsed}s: ${err.message || "Unknown error"}`
      ).catch(() => {});
    }

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

  // Step 4: Merge placements (MERGE-ONLY — never remove existing CS)
  const existingPlacementMap = new Map<number, any>();
  for (const ep of existingPlacements) {
    existingPlacementMap.set(ep.id, ep);
  }

  const mergedPlacements: { id: number; creativeSettings: number[] }[] = [];
  const addedPlacementIds = new Set<number>();

  // Handle all Monday-matched placements
  for (const np of newPlacements) {
    const existing = existingPlacementMap.get(np.id);
    if (existing) {
      // Existing placement — merge: keep all existing CS + add new from Monday
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

  // Keep all existing placements not touched by Monday
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

  // Step 5: Build updated deal (preserve all original fields, replace placements)
  const updatedDeal: AdformDeal = {
    ...existingDeal,
    placements: mergedPlacements,
  };

  // Step 6: PUT (unless dry run)
  if (!dryRun) {
    await updateDeal(token, adformDealId, updatedDeal);
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
      existingDeal,
      updatedDeal,
      placementBreakdown,
    };
  }

  return result;
}
