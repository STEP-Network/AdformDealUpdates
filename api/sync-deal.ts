import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getDealWithPublishers,
  getPublisherAdUnitIds,
  getAdUnits,
  getCreativeSettings,
  attachCreativeSettings,
  updateDealSyncLog,
} from "../lib/monday";
import { authenticate, getDeal, getPlacement, updateDeal } from "../lib/adform";
import { matchDealsToAdUnits, intersectCreativeSettings } from "../lib/matcher";
import type { DealSyncResult, AdformDeal, PlacementDetail, CsInfo } from "../lib/types";

// ── Time budget ──
const MAX_DURATION_MS = 300_000;
const SAFETY_MARGIN_MS = 10_000;
const TIME_BUDGET_MS = MAX_DURATION_MS - SAFETY_MARGIN_MS;

// Excluded statuses
const EXCLUDED_STATUSES = ["Needs format/placments"];

/**
 * POST /api/sync-deal  — Single-deal sync (deal → all its publishers)
 *
 * Triggered by Monday button webhook on deals board.
 * Also supports GET ?dealId=123&dryRun=true for manual testing.
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  let dealId: string | undefined;
  let dealName: string | undefined;
  const START_TIME = Date.now();

  function elapsedSec(): number {
    return Math.round((Date.now() - START_TIME) / 1000);
  }
  function isTimeUp(): boolean {
    return (Date.now() - START_TIME) >= TIME_BUDGET_MS;
  }

  try {
    // ── Monday webhook challenge ──
    if (req.body?.challenge) {
      return res.status(200).json({ challenge: req.body.challenge });
    }

    // ── Determine deal ID and options ──
    let dryRun = false;
    let verbose = false;

    if (req.method === "GET") {
      dealId = req.query.dealId as string;
      dryRun = req.query.dryRun === "true";
      verbose = req.query.verbose === "true";
    } else if (req.method === "POST") {
      // Monday button webhook sends pulseId
      dealId = req.body?.event?.pulseId?.toString();
      if (!dealId) dealId = req.body?.dealId?.toString();
      if (req.query.dryRun === "true") dryRun = true;
      if (req.query.verbose === "true") verbose = true;
    }

    if (!dealId) {
      return res.status(400).json({
        error: "Missing dealId. Use GET ?dealId=123 or POST with Monday webhook payload.",
      });
    }

    console.log(`[SyncDeal] Starting for deal ${dealId} | dryRun=${dryRun}`);

    // ══════════════════════════════════════════════
    // PHASE 1: Fetch deal + publishers from Monday
    // ══════════════════════════════════════════════

    const { deal, publisherIds, dealName: name } = await getDealWithPublishers(dealId);
    dealName = name;

    console.log(`[SyncDeal] Deal "${dealName}" (Adform: ${deal.adformDealId}) | ${publisherIds.length} publishers`);

    // Check status exclusion
    if (deal.statusLabel && EXCLUDED_STATUSES.includes(deal.statusLabel)) {
      const msg = `⚠️ Skipped — status "${deal.statusLabel}" is excluded from sync`;
      await updateDealSyncLog(dealId, msg);
      return res.status(200).json({ message: msg, dealName, status: deal.statusLabel });
    }

    // Check Adform ID
    if (!deal.adformDealId) {
      const msg = `❌ No Adform Deal ID on this item`;
      await updateDealSyncLog(dealId, msg);
      return res.status(200).json({ message: msg, dealName });
    }

    // Check publishers
    if (publisherIds.length === 0) {
      const msg = `⚠️ No publishers linked (Site specifik column is empty)`;
      await updateDealSyncLog(dealId, msg);
      return res.status(200).json({ message: msg, dealName });
    }

    // Log start
    await updateDealSyncLog(dealId, `⏳ Starting: ${publisherIds.length} publishers...`).catch(() => {});

    // ══════════════════════════════════════════════
    // PHASE 2: Gather ad units from all publishers
    // ══════════════════════════════════════════════

    const { allAdUnitIds, publisherNames } = await getPublisherAdUnitIds(publisherIds);
    console.log(`[SyncDeal] Publishers: [${publisherNames.join(", ")}] → ${allAdUnitIds.length} total ad units`);

    if (allAdUnitIds.length === 0) {
      const msg = `⚠️ No ad units found on ${publisherNames.length} publishers`;
      await updateDealSyncLog(dealId, msg);
      return res.status(200).json({ message: msg, dealName, publishers: publisherNames });
    }

    // Fetch ad units with formats + CS links
    const { adUnits, csIds, adUnitCsLinks } = await getAdUnits(allAdUnitIds);
    console.log(`[SyncDeal] Fetched ${adUnits.length} ad units, ${csIds.length} unique CS refs`);

    // Fetch creative settings
    const csMap = await getCreativeSettings(csIds);
    console.log(`[SyncDeal] Fetched ${csMap.size} creative settings`);

    // Attach CS to ad units
    attachCreativeSettings(adUnits, csMap, adUnitCsLinks);

    // Build CS name lookup
    const csNameLookup = new Map<number, string>();
    csMap.forEach((cs) => {
      if (cs.adformCsId && cs.name) {
        csNameLookup.set(parseInt(cs.adformCsId, 10), cs.name);
      }
    });

    console.log(`[SyncDeal] Phase 1+2 complete in ${elapsedSec()}s`);

    // ══════════════════════════════════════════════
    // PHASE 3: Match this deal against all ad units
    // ══════════════════════════════════════════════

    // matchDealsToAdUnits expects an array of deals — we pass just one
    const dealsWithPlacements = matchDealsToAdUnits([deal], adUnits);
    const dealMatch = dealsWithPlacements[0];

    if (!dealMatch || dealMatch.matchedPlacements.length === 0) {
      const msg = `⚠️ No format matches found across ${publisherNames.length} publishers (${adUnits.length} ad units)`;
      await updateDealSyncLog(dealId, msg);
      return res.status(200).json({ message: msg, dealName, publishers: publisherNames, adUnitsChecked: adUnits.length });
    }

    console.log(`[SyncDeal] Matched ${dealMatch.matchedPlacements.length} placements`);

    // ══════════════════════════════════════════════
    // PHASE 4: Adform — auth + process this deal
    // ══════════════════════════════════════════════

    if (isTimeUp()) {
      const msg = `⏱️ Time budget exhausted before Adform sync (${elapsedSec()}s)`;
      await updateDealSyncLog(dealId, msg);
      return res.status(200).json({ message: msg, timedOut: true });
    }

    const token = await authenticate();
    console.log(`[SyncDeal] Adform authenticated. Processing deal...`);

    const result = await processSingleDeal(token, dealMatch, dryRun, verbose, csNameLookup);

    // ══════════════════════════════════════════════
    // PHASE 5: Log result
    // ══════════════════════════════════════════════

    const elapsed = elapsedSec();
    const now = new Date().toISOString().replace("T", " ").slice(0, 19);

    let statusText: string;
    if (result.status === "error") {
      statusText = `❌ ${now} — Error: ${result.error} (${elapsed}s)`;
    } else if (dryRun) {
      statusText = `🧪 ${now} — Dry run: +${result.placementsAdded} placements, ${result.placementsMerged || 0} merged from ${publisherNames.length} publishers (${elapsed}s)`;
    } else {
      statusText = `✅ ${now} — +${result.placementsAdded} placements, ${result.placementsMerged || 0} merged from ${publisherNames.length} publishers [${publisherNames.join(", ")}] (${elapsed}s)`;
    }

    await updateDealSyncLog(dealId, statusText);

    console.log(`[SyncDeal] Complete: ${result.status} in ${elapsed}s`);

    return res.status(200).json({
      dealId,
      dealName,
      adformDealId: deal.adformDealId,
      publishers: publisherNames,
      dryRun,
      result,
      elapsedMs: Date.now() - START_TIME,
    });

  } catch (err: any) {
    console.error("[SyncDeal] Fatal error:", err);

    if (dealId) {
      const now = new Date().toISOString().replace("T", " ").slice(0, 19);
      await updateDealSyncLog(
        dealId,
        `❌ ${now} — Fatal: ${err.message || "Unknown error"} (${elapsedSec()}s)`
      ).catch(() => {});
    }

    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}

// ── Helper: resolve CS IDs to {id, name} ──
function resolveCsIds(ids: number[], lookup: Map<number, string>): CsInfo[] {
  return ids.map((id) => ({
    id,
    name: lookup.get(id) || undefined,
  }));
}

// ── Process a single deal: GET → merge → PUT ──
async function processSingleDeal(
  token: string,
  dealMatch: { adformDealId: string; name: string; matchedPlacements: { adUnitName: string; adformPlacementId: string; mondayCsIds: string[] }[] },
  dryRun: boolean,
  verbose: boolean,
  csNameLookup: Map<number, string>
): Promise<DealSyncResult> {
  const { adformDealId, matchedPlacements } = dealMatch;

  // GET existing deal from Adform
  const existingDeal = await getDeal(token, adformDealId);
  const existingPlacements = existingDeal.placements || [];

  // For each matched placement, fetch its CS from Adform
  const placementDetailPromises = matchedPlacements.map((mp) =>
    getPlacement(token, mp.adformPlacementId).then((detail) => ({
      mp,
      detail,
    }))
  );
  const placementDetails = await Promise.all(placementDetailPromises);

  // Intersect CS
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
          skipReason: `No CS overlap`,
        });
      }
      continue;
    }

    newPlacements.push({
      id: parseInt(mp.adformPlacementId, 10),
      creativeSettings: intersected,
    });
  }

  // Merge placements (MERGE-ONLY)
  const existingPlacementMap = new Map<number, any>();
  for (const ep of existingPlacements) {
    existingPlacementMap.set(ep.id, ep);
  }

  const mergedPlacements: { id: number; creativeSettings: number[] }[] = [];
  const addedPlacementIds = new Set<number>();

  for (const np of newPlacements) {
    const existing = existingPlacementMap.get(np.id);
    if (existing) {
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
          ...(addedCs.length > 0 ? { mergeDetail: `Kept ${existing.creativeSettings.length} existing, added ${addedCs.length} new` } : {}),
        });
      }
    } else {
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

  // Keep existing placements not touched by Monday
  const keptPlacements = existingPlacements.filter(
    (ep: any) => !addedPlacementIds.has(ep.id)
  );
  for (const kp of keptPlacements) {
    mergedPlacements.push({ id: kp.id, creativeSettings: kp.creativeSettings });
  }

  // Build updated deal
  const updatedDeal: AdformDeal = {
    ...existingDeal,
    placements: mergedPlacements,
  };

  // PUT (unless dry run)
  if (!dryRun) {
    await updateDeal(token, adformDealId, updatedDeal);
  }

  // Counts
  const trulyNewCount = newPlacements.filter(np => !existingPlacementMap.has(np.id)).length;
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
