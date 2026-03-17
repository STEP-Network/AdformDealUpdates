import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getAllPublishersWithAdUnits,
  updatePublisherAdformId,
} from "../lib/monday";
import { authenticate, getPlacement } from "../lib/adform";

/**
 * GET /api/backfill-publisher-ids
 *
 * For each publisher on Monday that has ad units but no Adform publisher ID:
 * 1. Take the first ad unit's Adform placement ID
 * 2. GET that placement from Adform → extract inventorySourceId
 * 3. Write that ID to the publisher's numeric_mm1hsqn1 column
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

    // Step 1: Get all publishers with their ad unit links
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

    // Step 3: For each publisher, look up their first ad unit's placement
    // to find the inventorySourceId
    const results: {
      publisherName: string;
      publisherId: string;
      adformPubId: string | null;
      adUnitUsed: string;
      status: string;
    }[] = [];

    // Process in batches of 5 to avoid rate limits
    const BATCH_SIZE = 5;
    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      const batch = toProcess.slice(i, i + BATCH_SIZE);

      const batchResults = await Promise.allSettled(
        batch.map(async (pub) => {
          // Get first ad unit's details from Monday
          const { adUnits } = await import("../lib/monday").then((m) =>
            m.getAdUnits(pub.adUnitIds.slice(0, 1))
          );

          if (adUnits.length === 0 || !adUnits[0].adformPlacementId) {
            return {
              publisherName: pub.name,
              publisherId: pub.id,
              adformPubId: null,
              adUnitUsed: "none",
              status: "skipped — no ad unit with Adform placement ID",
            };
          }

          const adUnit = adUnits[0];

          // GET placement from Adform to find inventorySourceId
          const placement = await getPlacement(token, adUnit.adformPlacementId);
          const invSourceId = (placement as any).inventorySourceId;

          if (!invSourceId) {
            return {
              publisherName: pub.name,
              publisherId: pub.id,
              adformPubId: null,
              adUnitUsed: adUnit.adformPlacementId,
              status: "skipped — placement has no inventorySourceId",
            };
          }

          const adformPubId = String(invSourceId);

          if (!dryRun) {
            await updatePublisherAdformId(pub.id, adformPubId);
          }

          return {
            publisherName: pub.name,
            publisherId: pub.id,
            adformPubId,
            adUnitUsed: adUnit.adformPlacementId,
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
            adUnitUsed: "",
            status: `error: ${r.reason?.message || "unknown"}`,
          });
        }
      }
    }

    const updated = results.filter((r) => r.status === "updated" || r.status === "dryRun").length;
    const skipped = results.filter((r) => r.status.startsWith("skipped")).length;
    const errors = results.filter((r) => r.status.startsWith("error")).length;

    console.log(`[Backfill] Done: ${updated} updated, ${skipped} skipped, ${errors} errors`);

    return res.status(200).json({
      dryRun,
      total: allPublishers.length,
      processed: results.length,
      updated,
      skipped,
      errors,
      results,
    });
  } catch (err: any) {
    console.error("[Backfill] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
