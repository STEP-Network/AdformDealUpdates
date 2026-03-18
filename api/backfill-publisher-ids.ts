import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getAllPublishersWithAdUnits,
  updatePublisherAdformId,
} from "../lib/monday";
import { authenticate, getInventorySources } from "../lib/adform";

/**
 * GET /api/backfill-publisher-ids
 *
 * 1. Fetches ALL inventory sources (publishers) from Adform
 * 2. Gets ALL publishers from Monday
 * 3. Matches by name (normalized: lowercase, trimmed)
 * 4. Writes the Adform inventory source ID to numeric_mm1hsqn1 on Monday
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

    // Step 1: Get all inventory sources from Adform
    const token = await authenticate();
    const inventorySources = await getInventorySources(token);
    console.log(`[Backfill] Adform has ${inventorySources.length} inventory sources`);

    // Build a lookup: normalized name → { id, name }
    // Also build a lookup by domain (e.g. "anna-mad.dk")
    const adformByName = new Map<string, { id: number; name: string }>();
    const adformByDomain = new Map<string, { id: number; name: string }>();

    for (const src of inventorySources) {
      const name = (src.name || "").trim().toLowerCase();
      if (name) {
        adformByName.set(name, { id: src.id, name: src.name });
      }
      // Also try extracting domain-like patterns
      const domainMatch = name.match(/([a-z0-9-]+\.[a-z]{2,})/);
      if (domainMatch) {
        adformByDomain.set(domainMatch[1], { id: src.id, name: src.name });
      }
    }

    // Step 2: Get all publishers from Monday
    const allPublishers = await getAllPublishersWithAdUnits();
    console.log(`[Backfill] Monday has ${allPublishers.length} publishers`);

    // Filter to those without Adform ID
    let toProcess = allPublishers.filter((p) => !p.adformPubId);
    console.log(`[Backfill] ${toProcess.length} publishers need Adform ID`);

    if (limit) {
      toProcess = toProcess.slice(0, limit);
    }

    // Step 3: Match by name
    const results: {
      publisherName: string;
      publisherId: string;
      adformPubId: string | null;
      adformName: string;
      matchType: string;
      status: string;
    }[] = [];

    for (const pub of toProcess) {
      const normalizedName = pub.name.trim().toLowerCase();

      // Try exact name match first
      let match = adformByName.get(normalizedName);
      let matchType = "exact_name";

      // Try domain match
      if (!match) {
        const domainMatch = normalizedName.match(/([a-z0-9-]+\.[a-z]{2,})/);
        if (domainMatch) {
          match = adformByDomain.get(domainMatch[1]);
          matchType = "domain";
        }
      }

      // Try partial match: Monday name contained in Adform name or vice versa
      if (!match) {
        for (const [adName, adSrc] of adformByName) {
          if (adName.includes(normalizedName) || normalizedName.includes(adName)) {
            match = adSrc;
            matchType = "partial";
            break;
          }
        }
      }

      if (!match) {
        results.push({
          publisherName: pub.name,
          publisherId: pub.id,
          adformPubId: null,
          adformName: "",
          matchType: "none",
          status: "skipped — no Adform match found",
        });
        continue;
      }

      const adformPubId = String(match.id);

      if (!dryRun) {
        await updatePublisherAdformId(pub.id, adformPubId);
      }

      results.push({
        publisherName: pub.name,
        publisherId: pub.id,
        adformPubId,
        adformName: match.name,
        matchType,
        status: dryRun ? "dryRun" : "updated",
      });
    }

    const updated = results.filter((r) => r.status === "updated" || r.status === "dryRun").length;
    const skipped = results.filter((r) => r.status.startsWith("skipped")).length;

    console.log(`[Backfill] Done: ${updated} matched, ${skipped} no match`);

    return res.status(200).json({
      dryRun,
      adformInventorySources: inventorySources.length,
      mondayPublishers: allPublishers.length,
      processed: results.length,
      matched: updated,
      noMatch: skipped,
      results,
    });
  } catch (err: any) {
    console.error("[Backfill] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
