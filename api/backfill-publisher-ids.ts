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
 * 1. Take up to 3 ad units' Adform placement IDs
 * 2. GET /v1/seller/placements/{id} for each → extract publisherId
 * 3. If ALL placements agree on the same publisherId → write it
 * 4. If they DISAGREE → flag as conflict (don't write)
 *
 * Query params:
 *   dryRun=true   — show what would be filled, don't write
 *   limit=10      — process only N publishers
 *   write=true    — actually write to Monday (requires dryRun=false)
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const dryRun = req.query.dryRun !== "false"; // default to dry run for safety
    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : null;
    const CHECK_COUNT = 3; // check up to 3 ad units per publisher

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

    // Step 3: For each publisher, check multiple ad units
    const results: {
      publisherName: string;
      mondayId: string;
      adformPubId: string | null;
      adformPubIds: string[]; // all unique IDs found across ad units
      placementsChecked: { placementId: string; adformPubId: string; placementName: string }[];
      adUnitCount: number;
      status: string;
    }[] = [];

    // Track which Adform publisher IDs are claimed by which Monday publishers
    const adformIdToMonday = new Map<string, string[]>();

    // Process in batches of 3 (fewer since each publisher checks multiple placements)
    const BATCH_SIZE = 3;
    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      const batch = toProcess.slice(i, i + BATCH_SIZE);

      const batchResults = await Promise.allSettled(
        batch.map(async (pub) => {
          // Get up to CHECK_COUNT ad units
          const adUnitIds = pub.adUnitIds.slice(0, CHECK_COUNT);
          const { adUnits } = await getAdUnits(adUnitIds);

          const withPlacementId = adUnits.filter((au) => au.adformPlacementId);
          if (withPlacementId.length === 0) {
            return {
              publisherName: pub.name,
              mondayId: pub.id,
              adformPubId: null,
              adformPubIds: [] as string[],
              placementsChecked: [] as any[],
              adUnitCount: pub.adUnitIds.length,
              status: "skipped — no ad units with Adform placement ID",
            };
          }

          // Fetch each placement from Adform
          const placementChecks = await Promise.all(
            withPlacementId.map(async (au) => {
              try {
                const resp = await fetch(`${API_BASE}/placements/${au.adformPlacementId}`, {
                  headers: { Authorization: `Bearer ${token}` },
                });
                if (!resp.ok) {
                  return { placementId: au.adformPlacementId, adformPubId: "error", placementName: au.name };
                }
                const data: any = await resp.json();
                return {
                  placementId: au.adformPlacementId,
                  adformPubId: String(data.publisherId || "unknown"),
                  placementName: data.name || au.name,
                };
              } catch {
                return { placementId: au.adformPlacementId, adformPubId: "error", placementName: au.name };
              }
            })
          );

          // Find unique publisher IDs (excluding errors)
          const validIds = placementChecks
            .map((p) => p.adformPubId)
            .filter((id) => id !== "error" && id !== "unknown");
          const uniqueIds = [...new Set(validIds)];

          let status: string;
          let adformPubId: string | null = null;

          if (uniqueIds.length === 0) {
            status = "skipped — could not determine publisher ID";
          } else if (uniqueIds.length === 1) {
            adformPubId = uniqueIds[0];
            status = dryRun ? "dryRun" : "updated";
          } else {
            status = `⚠️ CONFLICT — found multiple Adform publisher IDs: ${uniqueIds.join(", ")}`;
          }

          return {
            publisherName: pub.name,
            mondayId: pub.id,
            adformPubId,
            adformPubIds: uniqueIds,
            placementsChecked: placementChecks,
            adUnitCount: pub.adUnitIds.length,
            status,
          };
        })
      );

      for (let j = 0; j < batchResults.length; j++) {
        const r = batchResults[j];
        if (r.status === "fulfilled") {
          results.push(r.value);
          // Track Adform ID → Monday publisher mapping
          if (r.value.adformPubId) {
            const existing = adformIdToMonday.get(r.value.adformPubId) || [];
            existing.push(r.value.publisherName);
            adformIdToMonday.set(r.value.adformPubId, existing);
          }
        } else {
          results.push({
            publisherName: batch[j].name,
            mondayId: batch[j].id,
            adformPubId: null,
            adformPubIds: [],
            placementsChecked: [],
            adUnitCount: batch[j].adUnitIds.length,
            status: `error: ${r.reason?.message || "unknown"}`,
          });
        }
      }
    }

    // Check for duplicates: multiple Monday publishers claiming the same Adform ID
    const duplicates: { adformPubId: string; publishers: string[] }[] = [];
    for (const [adId, publishers] of adformIdToMonday) {
      if (publishers.length > 1) {
        duplicates.push({ adformPubId: adId, publishers });
        // Mark these as conflicts
        for (const r of results) {
          if (r.adformPubId === adId) {
            r.status = `⚠️ DUPLICATE — Adform ID ${adId} also claimed by: ${publishers.filter(p => p !== r.publisherName).join(", ")}`;
          }
        }
      }
    }

    // Step 4: Write to Monday (only clean matches, no conflicts/duplicates)
    let written = 0;
    if (!dryRun) {
      for (const r of results) {
        if (r.adformPubId && r.status === "updated") {
          await updatePublisherAdformId(r.mondayId, r.adformPubId);
          written++;
        }
      }
    }

    const matched = results.filter((r) => r.status === "dryRun" || r.status === "updated").length;
    const conflicts = results.filter((r) => r.status.includes("CONFLICT") || r.status.includes("DUPLICATE")).length;
    const skipped = results.filter((r) => r.status.startsWith("skipped")).length;
    const errors = results.filter((r) => r.status.startsWith("error")).length;

    return res.status(200).json({
      dryRun,
      mondayPublishers: allPublishers.length,
      needsBackfill: allPublishers.filter((p) => p.adUnitIds.length > 0 && !p.adformPubId).length,
      processed: results.length,
      matched,
      conflicts,
      skipped,
      errors,
      written,
      duplicates: duplicates.length > 0 ? duplicates : undefined,
      results,
    });
  } catch (err: any) {
    console.error("[Backfill] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
