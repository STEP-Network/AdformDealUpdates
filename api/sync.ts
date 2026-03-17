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
import type { DealSyncResult, SyncResult, AdformDeal, DealWithPlacements } from "../lib/types";

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

    if (req.method === "GET") {
      // Test mode: GET /api/sync?publisherId=123&maxDeals=1&dryRun=true
      publisherId = req.query.publisherId as string;
      dryRun = req.query.dryRun === "true";
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

    // Process deals in parallel
    const dealPromises = dealsWithPlacements.map((dealMatch) =>
      processDeal(token, dealMatch, dryRun)
    );
    const results = await Promise.allSettled(dealPromises);

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

    console.log(`[Sync] Complete: ${JSON.stringify(syncResult)}`);
    return res.status(200).json(syncResult);

  } catch (err: any) {
    console.error("[Sync] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}

// ── Process a single deal: GET → merge → PUT ──
async function processDeal(
  token: string,
  dealMatch: DealWithPlacements,
  dryRun: boolean
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
  let skippedCount = 0;

  for (const { mp, detail } of placementDetails) {
    const adformCsIds = (detail.creativeSettings || []).map((cs: any) =>
      typeof cs === "number" ? cs : cs.id
    );

    const intersected = intersectCreativeSettings(mp.mondayCsIds, adformCsIds);

    if (intersected.length === 0) {
      // Skip placements with no valid CS after intersection (matches Make.com module 55 filter)
      console.log(`[Sync] Skipping placement ${mp.adformPlacementId} (${mp.adUnitName}) — no CS after intersection`);
      skippedCount++;
      continue;
    }

    newPlacements.push({
      id: parseInt(mp.adformPlacementId, 10),
      creativeSettings: intersected,
    });
  }

  // Step 4: Merge — keep existing placements that we're NOT replacing + add new ones
  const keptPlacements = existingPlacements.filter(
    (ep: any) => !newPlacementIds.has(ep.id)
  );

  const mergedPlacements = [
    ...keptPlacements.map((kp: any) => ({
      id: kp.id,
      creativeSettings: kp.creativeSettings,
    })),
    ...newPlacements,
  ];

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

  return {
    dealName: dealMatch.name,
    adformDealId,
    placementsAdded: newPlacements.length,
    placementsKept: keptPlacements.length,
    placementsSkipped: skippedCount,
    status: dryRun ? "dryRun" : "updated",
  };
}
