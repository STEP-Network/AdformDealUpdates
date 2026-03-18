import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getAllPublishersWithAdUnits,
  getAdUnits,
  updatePublisherAdformId,
} from "../lib/monday";
import { authenticate } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";

/**
 * GET /api/backfill-publisher-ids
 *
 * For each Monday publisher that has ad units but no Adform publisher ID:
 * 1. Take the first ad unit's Adform placement ID
 * 2. GET /v1/seller/placements/{id} → extract publisherId
 * 3. Write that ID to numeric_mm1hsqn1 on the publisher board
 *
 * The placement detail endpoint returns: { id, publisherId, name, type, status, creativeSettings }
 *
 * Query params:
 *   dryRun=true   — show what would be filled, don't write
 *   limit=10      — process only N publishers (default: all)
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const dryRun = req.query.dryRun === "true";
    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : null;

    console.log(`[Backfill] Starting | dryRun=${dryRun} | limit=${limit}`);

    // Step 1: Get all publishers from Monday
    const allPublishers = await getAllPublishersWithAdUnits();
    console.log(`[Backfill] Found ${allPublishers.length} total publishers`);

    // Filter to those that have ad units but no Adform ID yet
    let toProcess = allPublishers.filter(
      (p) => p.adUnitIds.length > 0 && !p.adformPubId
    );
    console.log(`[Backfill] ${toProcess.length} publishers need Adform ID`);

    if (limit) {
      toProcess = toProcess.slice(0, limit);
    }

    // Step 2: Authenticate with Adform
    const token = await authenticate();

    // Step 3: For each publisher, get first ad unit → placement → publisherId
    const results: {
      publisherName: string;
      publisherId: string;
      adformPubId: string | null;
      placementUsed: string;
      status: string;
    }[] = [];

    // Process in batches of 5
    const BATCH_SIZE = 5;
    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      const batch = toProcess.slice(i, i + BATCH_SIZE);

      const batchResults = await Promise.allSettled(
        batch.map(async (pub) => {
          // Get first ad unit's Adform placement ID from Monday
          const { adUnits } = await getAdUnits(pub.adUnitIds.slice(0, 1));

          if (adUnits.length === 0 || !adUnits[0].adformPlacementId) {
            return {
              publisherName: pub.name,
              publisherId: pub.id,
              adformPubId: null,
              placementUsed: "none",
              status: "skipped — no ad unit with Adform placement ID",
            };
          }

          const placementId = adUnits[0].adformPlacementId;

          // GET placement detail from Adform (includes publisherId)
          const resp = await fetch(`${API_BASE}/placements/${placementId}`, {
            headers: { Authorization: `Bearer ${token}` },
          });

          if (!resp.ok) {
            return {
              publisherName: pub.name,
              publisherId: pub.id,
              adformPubId: null,
              placementUsed: placementId,
              status: `skipped — Adform GET placement failed ${resp.status}`,
            };
          }

          const placement: any = await resp.json();
          const adformPubId = placement.publisherId;

          if (!adformPubId) {
            return {
              publisherName: pub.name,
              publisherId: pub.id,
              adformPubId: null,
              placementUsed: placementId,
              status: "skipped — placement has no publisherId",
            };
          }

          const adformPubIdStr = String(adformPubId);

          if (!dryRun) {
            await updatePublisherAdformId(pub.id, adformPubIdStr);
          }

          return {
            publisherName: pub.name,
            publisherId: pub.id,
            adformPubId: adformPubIdStr,
            placementUsed: placementId,
            status: dryRun ? "dryRun" : "updated",
          };
        })
      );

      for (let j = 0; j < batchResults.length; j++) {
        const r = batchResults[j];
        if (r.status === "fulfilled") {
          results.push(r.value);
        } else {
          results.push({
            publisherName: batch[j].name,
            publisherId: batch[j].id,
            adformPubId: null,
            placementUsed: "",
            status: `error: ${r.reason?.message || "unknown"}`,
          });
        }
      }
    }

    const updated = results.filter((r) => r.status === "updated" || r.status === "dryRun").length;
    const skipped = results.filter((r) => r.status.startsWith("skipped")).length;
    const errors = results.filter((r) => r.status.startsWith("error")).length;

    console.log(`[Backfill] Done: ${updated} matched, ${skipped} skipped, ${errors} errors`);

    return res.status(200).json({
      dryRun,
      mondayPublishers: allPublishers.length,
      needsBackfill: allPublishers.filter((p) => p.adUnitIds.length > 0 && !p.adformPubId).length,
      processed: results.length,
      matched: updated,
      skipped,
      errors,
      results,
    });
  } catch (err: any) {
    console.error("[Backfill] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
