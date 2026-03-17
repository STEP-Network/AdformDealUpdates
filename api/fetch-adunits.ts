import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getPublisherLinks,
  getAllCreativeSettings,
  getAllAdUnitsForLookup,
  createItem,
  updateColumnJson,
  updatePublisherStatus,
  BOARDS,
  COLUMNS,
} from "../lib/monday";
import { authenticate, getInventorySourcePlacements, getPlacement } from "../lib/adform";

// Column ID for Adform publisher ID on publisher board
const COL_PUBLISHER_ADFORM_ID = "numeric_mm1hsqn1";

/**
 * GET /api/fetch-adunits?publisherId=XXX
 * POST /api/fetch-adunits (Monday webhook)
 *
 * Fetches all placements from Adform for the publisher's inventorySourceId,
 * then:
 * 1. Creates ad unit items on Monday if they don't exist (matched by Adform placement ID)
 * 2. Creates CS items on Monday if they don't exist (matched by Adform CS ID)
 * 3. Links CS to ad units, ad units to publisher
 *
 * Query params:
 *   dryRun=true   — show what would be created, don't write
 *   maxPlacements=5 — limit placements processed
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    // ── Monday webhook challenge ──
    if (req.body?.challenge) {
      return res.status(200).json({ challenge: req.body.challenge });
    }

    // ── Determine publisher ID and options ──
    let publisherId: string | undefined;
    let dryRun = false;
    let maxPlacements: number | null = null;

    if (req.method === "GET") {
      publisherId = req.query.publisherId as string;
      dryRun = req.query.dryRun === "true";
      maxPlacements = req.query.maxPlacements
        ? parseInt(req.query.maxPlacements as string, 10)
        : null;
    } else if (req.method === "POST") {
      publisherId = req.body?.event?.pulseId?.toString();
      if (!publisherId) publisherId = req.body?.publisherId?.toString();
      if (req.query.dryRun === "true") dryRun = true;
      if (req.query.maxPlacements)
        maxPlacements = parseInt(req.query.maxPlacements as string, 10);
    }

    if (!publisherId) {
      return res.status(400).json({
        error: "Missing publisherId",
      });
    }

    console.log(
      `[FetchAdUnits] Starting for publisher ${publisherId} | dryRun=${dryRun} | maxPlacements=${maxPlacements}`
    );

    // ── Step 1: Get publisher info + Adform publisher ID from Monday ──
    const { publisherName } = await getPublisherLinks(publisherId);

    // Get the Adform publisher ID from the numeric column
    const pubData = await import("../lib/monday").then((m) =>
      m.default ? undefined : undefined
    );

    // Fetch the publisher item directly to get Adform ID
    const mondayMod = await import("../lib/monday");
    const pubItems = await (mondayMod as any).batchFetchItems
      ? null
      : null;

    // Use a direct query to get the Adform publisher ID
    const MONDAY_API_URL = "https://api.monday.com/v2";
    const token = process.env.MONDAY_API_TOKEN;
    if (!token) throw new Error("MONDAY_API_TOKEN not set");

    const pubResp = await fetch(MONDAY_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: token },
      body: JSON.stringify({
        query: `query ($id: [ID!]!) {
          items(ids: $id) {
            column_values(ids: ["${COL_PUBLISHER_ADFORM_ID}"]) {
              id
              text
            }
          }
        }`,
        variables: { id: [publisherId] },
      }),
    });
    const pubJson: any = await pubResp.json();
    const adformPubIdText =
      pubJson.data?.items?.[0]?.column_values?.[0]?.text || "";

    if (!adformPubIdText) {
      const msg = `Publisher "${publisherName}" has no Adform publisher ID. Run backfill first.`;
      return res.status(400).json({ error: msg });
    }

    const adformPublisherId = adformPubIdText.replace(/\.0$/, ""); // numeric columns may have .0
    console.log(
      `[FetchAdUnits] Publisher "${publisherName}" → Adform ID: ${adformPublisherId}`
    );

    // ── Step 2: Authenticate with Adform and fetch all placements ──
    const adformToken = await authenticate();
    const placements = await getInventorySourcePlacements(
      adformToken,
      adformPublisherId
    );
    console.log(
      `[FetchAdUnits] Adform returned ${placements.length} placements`
    );

    let toProcess = placements;
    if (maxPlacements) {
      toProcess = placements.slice(0, maxPlacements);
    }

    // ── Step 3: Fetch CS details for each placement (parallel, batched) ──
    const BATCH_SIZE = 5;
    const placementDetails: { placement: any; csDetails: any[] }[] = [];

    for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
      const batch = toProcess.slice(i, i + BATCH_SIZE);
      const batchResults = await Promise.all(
        batch.map(async (p: any) => {
          const detail = await getPlacement(adformToken, String(p.id));
          return {
            placement: p,
            csDetails: (detail.creativeSettings || []).map((cs: any) =>
              typeof cs === "number" ? { id: cs } : cs
            ),
          };
        })
      );
      placementDetails.push(...batchResults);
    }

    // ── Step 4: Load existing Monday data for dedup ──
    console.log(`[FetchAdUnits] Loading existing Monday data for dedup...`);
    const [existingAdUnits, existingCs] = await Promise.all([
      getAllAdUnitsForLookup(),
      getAllCreativeSettings(),
    ]);
    console.log(
      `[FetchAdUnits] Existing: ${existingAdUnits.size} ad units, ${existingCs.size} CS`
    );

    // ── Step 5: Process each placement ──
    const results: {
      placementName: string;
      placementId: number;
      adUnitAction: string;
      adUnitMondayId?: string;
      csActions: { csId: number; name: string; action: string; mondayId?: string }[];
    }[] = [];

    for (const { placement, csDetails } of placementDetails) {
      const placementId = placement.id;
      const placementName = placement.name || `placement_${placementId}`;
      const existing = existingAdUnits.get(String(placementId));

      let adUnitMondayId: string;
      let adUnitAction: string;

      if (existing) {
        // Already exists on Monday
        adUnitMondayId = existing.mondayId;
        adUnitAction = "exists";
      } else if (dryRun) {
        adUnitMondayId = "dry-run";
        adUnitAction = "would_create";
      } else {
        // Create the ad unit on Monday
        const columnValues: Record<string, unknown> = {
          [COLUMNS.ADUNIT_ADFORM_PLACEMENT_ID]: String(placementId),
          [COLUMNS.ADUNIT_SOURCE]: { label: "Adform" },
          [COLUMNS.ADUNIT_TYPE]: { label: "Ad Unit" },
        };

        // Extract sizes from CS details for the Sizes dropdown
        const sizes = new Set<string>();
        for (const cs of csDetails) {
          const csName = cs.name || "";
          // Try to extract size from CS name (e.g. "Standard 300x250" → "300x250")
          const sizeMatch = csName.match(/(\d+x\d+)/);
          if (sizeMatch) sizes.add(sizeMatch[1]);
        }

        if (sizes.size > 0) {
          columnValues[COLUMNS.ADUNIT_SIZES] = {
            labels: Array.from(sizes),
          };
        }

        adUnitMondayId = await createItem(
          BOARDS.AD_UNITS,
          "group_mkvgv1kg", // "adform ad units" group
          placementName,
          columnValues
        );

        // Link ad unit to publisher
        await updateColumnJson(
          BOARDS.AD_UNITS,
          adUnitMondayId,
          COLUMNS.ADUNIT_PUBLISHER_LINK,
          { item_ids: [parseInt(publisherId, 10)] }
        );

        // Link publisher to ad unit (reverse relation)
        // Get current ad unit links and add new one
        await updateColumnJson(
          BOARDS.PUBLISHER,
          publisherId,
          COLUMNS.PUBLISHER_AD_UNITS,
          { item_ids: [parseInt(adUnitMondayId, 10)] }
        );

        adUnitAction = "created";
        // Add to lookup so subsequent placements can find it
        existingAdUnits.set(String(placementId), {
          mondayId: adUnitMondayId,
          name: placementName,
          adformPlacementId: String(placementId),
        });
      }

      // Process CS for this placement
      const csActions: {
        csId: number;
        name: string;
        action: string;
        mondayId?: string;
      }[] = [];
      const csMonadyIdsForLink: number[] = [];

      for (const cs of csDetails) {
        const csId = cs.id || cs;
        const csIdStr = String(csId);
        const existingCsItem = existingCs.get(csIdStr);
        const csName = cs.name || `CS_${csId}`;

        if (existingCsItem) {
          csActions.push({
            csId,
            name: existingCsItem.name,
            action: "exists",
            mondayId: existingCsItem.mondayId,
          });
          csMonadyIdsForLink.push(parseInt(existingCsItem.mondayId, 10));
        } else if (dryRun) {
          csActions.push({
            csId,
            name: csName,
            action: "would_create",
          });
        } else {
          // Extract size from name
          const sizeMatch = csName.match(/(\d+x\d+)/);
          const size = sizeMatch ? sizeMatch[1] : "";

          // Determine type (RM vs Standard)
          const isRM = /\bRM\b/i.test(csName);

          const csColumnValues: Record<string, unknown> = {
            [COLUMNS.CS_ADFORM_ID]: csIdStr,
            [COLUMNS.CS_SIZE]: size,
            [COLUMNS.CS_TYPE]: { label: isRM ? "RM" : "Standard" },
          };

          const newCsMondayId = await createItem(
            BOARDS.CS,
            "topics", // default group
            csName,
            csColumnValues
          );

          csActions.push({
            csId,
            name: csName,
            action: "created",
            mondayId: newCsMondayId,
          });
          csMonadyIdsForLink.push(parseInt(newCsMondayId, 10));

          // Add to lookup for dedup
          existingCs.set(csIdStr, {
            mondayId: newCsMondayId,
            name: csName,
            adformCsId: csIdStr,
            size,
          });
        }
      }

      // Link CS to ad unit (if not dry run and we have CS to link)
      if (!dryRun && csMonadyIdsForLink.length > 0 && adUnitMondayId !== "dry-run") {
        await updateColumnJson(
          BOARDS.AD_UNITS,
          adUnitMondayId,
          COLUMNS.ADUNIT_CREATIVE_SETTINGS,
          { item_ids: csMonadyIdsForLink }
        );
      }

      results.push({
        placementName,
        placementId,
        adUnitAction,
        adUnitMondayId,
        csActions,
      });
    }

    // ── Step 6: Update status ──
    const created = results.filter((r) => r.adUnitAction === "created").length;
    const existed = results.filter((r) => r.adUnitAction === "exists").length;
    const csCreated = results.reduce(
      (sum, r) => sum + r.csActions.filter((c) => c.action === "created").length,
      0
    );

    const now = new Date().toISOString().replace("T", " ").slice(0, 19);
    const statusText = dryRun
      ? `🧪 ${now} — Dry run: ${toProcess.length} placements, ${created} would create`
      : `📥 ${now} — ${created} ad units created, ${existed} existed, ${csCreated} CS created`;

    if (!dryRun) {
      await updatePublisherStatus(publisherId, statusText);
    }

    return res.status(200).json({
      publisherId,
      publisherName,
      adformPublisherId,
      dryRun,
      totalAdformPlacements: placements.length,
      processed: results.length,
      adUnitsCreated: created,
      adUnitsExisted: existed,
      csCreated,
      results,
      timestamp: new Date().toISOString(),
    });
  } catch (err: any) {
    console.error("[FetchAdUnits] Fatal error:", err);
    return res
      .status(500)
      .json({ error: err.message || "Internal server error" });
  }
}
