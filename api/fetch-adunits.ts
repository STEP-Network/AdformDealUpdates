import type { VercelRequest, VercelResponse } from "@vercel/node";
import {
  getPublisherLinks,
  getAllCreativeSettings,
  getAllAdUnitsForLookup,
  getFormatNames,
  createItem,
  updateColumnJson,
  updatePublisherStatus,
  BOARDS,
  COLUMNS,
} from "../lib/monday";
import { authenticate, getPublisherPlacements } from "../lib/adform";

// ── Format filtering: checks BOTH device type AND format type against ad unit ──

/**
 * Determines if a format is allowed on a given ad unit based on:
 * 1. Type-specific rules (anchor only on anchor, sticky only on sticky, etc.)
 * 2. Device matching (desktop formats on desktop ad units, etc.)
 *
 * Format → Ad unit rules:
 *   "Anchor desktop/mobile"      → only on ad units named "anchor"
 *   "Sticky desktop"             → only on sticky_* ad units
 *   "Rectangle desktop"          → only on rectangle_* ad units
 *   "Billboard desktop"          → only on billboard_* ad units
 *   "Standard mobile"            → only on mobile_* ad units
 *   "Topscroll desktop/mobile"   → only on topscroll_* ad units
 *   "Preroll Click-to-play"      → only on *_CTP* / preroll_snvs_ctp ad units
 *   "Preroll Autoplay"           → only on *_AP / preroll_snvs_ap ad units
 *   Other formats (Midscroll, Skin/Wallpaper, Native, Outstream, etc.)
 *     → allowed based on device matching only
 */
function isFormatAllowedForAdUnit(formatName: string, adUnitName: string): boolean {
  const f = formatName.toLowerCase();
  const a = adUnitName.toLowerCase();

  // ── Type-specific rules (checked first) ──

  // Anchor formats → only on "anchor" ad units
  if (f.includes("anchor")) {
    return a === "anchor" || a.startsWith("anchor_") || a.startsWith("anchor ");
  }

  // Sticky format → only on sticky_* ad units
  if (f.includes("sticky")) {
    return a.startsWith("sticky");
  }

  // Rectangle format → only on rectangle_* ad units
  if (f.includes("rectangle")) {
    return a.startsWith("rectangle");
  }

  // Billboard format → only on billboard_* ad units
  if (f.includes("billboard")) {
    return a.startsWith("billboard");
  }

  // Standard mobile → only on mobile_* ad units
  if (f.includes("standard") && f.includes("mobile")) {
    return a.startsWith("mobile");
  }

  // Topscroll format → only on topscroll_* ad units
  if (f.includes("topscroll")) {
    return a.includes("topscroll");
  }

  // Preroll Click-to-play → only on CTP ad units
  if (f.includes("click-to-play") || f.includes("click to play")) {
    return a.includes("ctp");
  }

  // Preroll Autoplay → only on AP ad units (but not CTP-OTT etc.)
  if (f.includes("autoplay")) {
    // Match "_ap" at end, or "sec_ap" pattern, but NOT "ctp"
    return (a.endsWith("_ap") || a.includes("sec_ap")) && !a.includes("ctp");
  }

  // ── Device matching fallback for other formats (Midscroll, Skin, Native, Outstream, etc.) ──
  const isDesktopFormat = f.includes("desktop");
  const isMobileFormat = f.includes("mobile");

  // Format doesn't specify device → allow it
  if (!isDesktopFormat && !isMobileFormat) return true;

  // Determine ad unit device type
  if (a.startsWith("mobile") || a.includes("interscroller")) {
    return isMobileFormat;
  }
  if (a.startsWith("billboard") || a.startsWith("sticky") || a.startsWith("rectangle")) {
    return isDesktopFormat;
  }
  if (a.includes("desktop") && !a.includes("mobile")) return isDesktopFormat;
  if (a.includes("mobile") && !a.includes("desktop")) return isMobileFormat;

  // Default: allow
  return true;
}

/** Short device-type label for response output */
function getDeviceLabel(adUnitName: string): string {
  const a = adUnitName.toLowerCase();
  if (a.startsWith("mobile") || a.includes("interscroller")) return "mobile";
  if (a.startsWith("billboard") || a.startsWith("sticky") || a.startsWith("rectangle")) return "desktop";
  if (a === "anchor" || a.startsWith("anchor")) return "both";
  if (a.includes("desktop") && !a.includes("mobile")) return "desktop";
  if (a.includes("mobile") && !a.includes("desktop")) return "mobile";
  if (a.includes("ctp") || a.includes("_ap") || a.includes("preroll") || a.includes("snvs")) return "video";
  return "both";
}

// Column ID for Adform publisher ID on publisher board
const COL_PUBLISHER_ADFORM_ID = "text_mm1jgnpe";

/**
 * GET /api/fetch-adunits?publisherId=XXX
 * POST /api/fetch-adunits (Monday webhook)
 *
 * Fetches all placements from Adform via GET /v1/seller/placements?publisherIds={id}
 * The response includes CS with names — no per-placement calls needed.
 *
 * Then:
 * 1. Creates ad unit items on Monday if they don't exist (matched by Adform placement ID)
 * 2. Creates CS items on Monday if they don't exist (matched by Adform CS ID)
 * 3. Links CS to ad units, ad units to publisher
 *
 * Query params:
 *   dryRun=true       — show what would be created, don't write
 *   maxPlacements=5   — limit placements processed
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
      return res.status(400).json({ error: "Missing publisherId" });
    }

    console.log(`[FetchAdUnits] Starting for publisher ${publisherId} | dryRun=${dryRun} | maxPlacements=${maxPlacements}`);

    // ── Step 1: Get publisher info + Adform publisher ID from Monday ──
    const { publisherName } = await getPublisherLinks(publisherId);

    // Fetch Adform publisher ID from the text column
    const MONDAY_API_URL = "https://api.monday.com/v2";
    const mondayToken = process.env.MONDAY_API_TOKEN;
    if (!mondayToken) throw new Error("MONDAY_API_TOKEN not set");

    const pubResp = await fetch(MONDAY_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: mondayToken },
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
    const adformPublisherId = (pubJson.data?.items?.[0]?.column_values?.[0]?.text || "").trim();

    if (!adformPublisherId) {
      return res.status(400).json({
        error: `Publisher "${publisherName}" has no Adform publisher ID. Run backfill first.`,
      });
    }

    console.log(`[FetchAdUnits] Publisher "${publisherName}" → Adform ID: ${adformPublisherId}`);

    // ── Step 2: Fetch all placements from Adform (includes CS with names) ──
    const adformToken = await authenticate();
    const allPlacements = await getPublisherPlacements(adformToken, adformPublisherId);
    console.log(`[FetchAdUnits] Adform returned ${allPlacements.length} placements`);

    let toProcess = allPlacements;
    if (maxPlacements) {
      toProcess = allPlacements.slice(0, maxPlacements);
    }

    // ── Step 3: Load existing Monday data for dedup ──
    console.log(`[FetchAdUnits] Loading existing Monday data for dedup...`);
    const [existingAdUnits, existingCs] = await Promise.all([
      getAllAdUnitsForLookup(),
      getAllCreativeSettings(),
    ]);
    console.log(`[FetchAdUnits] Existing: ${existingAdUnits.size} ad units, ${existingCs.size} CS`);

    // ── Step 3b: Pre-collect all format IDs from CS items for batch name lookup ──
    const allFormatIdsFromCs = new Set<string>();
    existingCs.forEach((csItem) => {
      if (csItem.formatIds) {
        for (const fid of csItem.formatIds) {
          allFormatIdsFromCs.add(fid);
        }
      }
    });
    console.log(`[FetchAdUnits] Loading format names for ${allFormatIdsFromCs.size} format IDs...`);
    const formatNameMap = await getFormatNames(Array.from(allFormatIdsFromCs));
    console.log(`[FetchAdUnits] Loaded ${formatNameMap.size} format names`);

    // ── Step 4: Process each placement ──
    const results: {
      placementName: string;
      placementId: number;
      status: string;
      adUnitAction: string;
      adUnitMondayId?: string;
      deviceType?: string;
      formatsLinked?: number[];
      formatsSkipped?: string[];
      csActions: { csId: number; name: string; action: string; mondayId?: string }[];
    }[] = [];

    let skippedInactive = 0;
    for (const placement of toProcess) {
      const placementId = placement.id;
      const placementName = placement.name || `placement_${placementId}`;
      const placementStatus = placement.status || "unknown";

      // Skip inactive placements — only create active ad units
      if (placementStatus !== "active") {
        skippedInactive++;
        console.log(`[FetchAdUnits] Skipping inactive placement: ${placementName} (${placementId}) — status: ${placementStatus}`);
        results.push({
          placementName,
          placementId,
          status: placementStatus,
          adUnitAction: "skipped_inactive",
          deviceType: getDeviceLabel(placementName),
          csActions: [],
        });
        continue;
      }

      const csDetails: { id: number; name: string }[] = (placement.creativeSettings || []).map(
        (cs: any) => (typeof cs === "number" ? { id: cs, name: `CS_${cs}` } : cs)
      );

      const existing = existingAdUnits.get(String(placementId));
      let adUnitMondayId: string;
      let adUnitAction: string;

      if (existing) {
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
          [COLUMNS.ADUNIT_STATUS]: { label: placementStatus === "active" ? "ACTIVE" : "INACTIVE" },
        };

        adUnitMondayId = await createItem(
          BOARDS.AD_UNITS,
          "group_mkvgv1kg", // "adform ad units" group
          placementName,
          columnValues
        );

        // Link ad unit → publisher (two-way relation auto-populates the reverse)
        await updateColumnJson(
          BOARDS.AD_UNITS,
          adUnitMondayId,
          COLUMNS.ADUNIT_PUBLISHER_LINK,
          { item_ids: [parseInt(publisherId, 10)] }
        );

        adUnitAction = "created";
        existingAdUnits.set(String(placementId), {
          mondayId: adUnitMondayId,
          name: placementName,
          adformPlacementId: String(placementId),
        });
      }

      // Process CS for this placement
      const csActions: { csId: number; name: string; action: string; mondayId?: string }[] = [];
      const csMondayIdsForLink: number[] = [];

      for (const cs of csDetails) {
        const csIdStr = String(cs.id);
        const existingCsItem = existingCs.get(csIdStr);

        if (existingCsItem) {
          csActions.push({
            csId: cs.id,
            name: existingCsItem.name,
            action: "exists",
            mondayId: existingCsItem.mondayId,
          });
          csMondayIdsForLink.push(parseInt(existingCsItem.mondayId, 10));
        } else if (dryRun) {
          csActions.push({
            csId: cs.id,
            name: cs.name,
            action: "would_create",
          });
        } else {
          // Extract size from name (e.g. "Standard 300x250" → "300x250")
          const sizeMatch = cs.name.match(/(\d+x\d+)/);
          const size = sizeMatch ? sizeMatch[1] : "";
          const isRM = /\bRM\b/i.test(cs.name);

          const csColumnValues: Record<string, unknown> = {
            [COLUMNS.CS_ADFORM_ID]: csIdStr,
            [COLUMNS.CS_SIZE]: size,
          };

          const newCsMondayId = await createItem(
            BOARDS.CS,
            "topics",
            cs.name,
            csColumnValues
          );

          csActions.push({
            csId: cs.id,
            name: cs.name,
            action: "created",
            mondayId: newCsMondayId,
          });
          csMondayIdsForLink.push(parseInt(newCsMondayId, 10));

          // Add to lookup for dedup across placements
          existingCs.set(csIdStr, {
            mondayId: newCsMondayId,
            name: cs.name,
            adformCsId: csIdStr,
            size,
            formatIds: [], // newly created CS won't have formats yet
          });
        }
      }

      // Collect format IDs from CS items, then filter by ad unit type + device
      const deviceType = getDeviceLabel(placementName);
      const allFormatIds = new Set<number>();
      const filteredFormatIds = new Set<number>();
      const skippedFormats: string[] = [];

      for (const cs of csDetails) {
        const csIdStr = String(cs.id);
        const existingCsItem = existingCs.get(csIdStr);
        if (existingCsItem && existingCsItem.formatIds) {
          for (const fid of existingCsItem.formatIds) {
            const fidNum = parseInt(fid, 10);
            allFormatIds.add(fidNum);

            const formatName = formatNameMap.get(fid) || "";
            if (isFormatAllowedForAdUnit(formatName, placementName)) {
              filteredFormatIds.add(fidNum);
            } else {
              skippedFormats.push(`${formatName} (${fid})`);
            }
          }
        }
      }

      if (skippedFormats.length > 0) {
        console.log(`[FetchAdUnits] ${placementName} (${deviceType}): filtered out ${skippedFormats.length} formats: ${skippedFormats.join(", ")}`);
      }

      // Link CS to ad unit + filtered formats to ad unit
      if (!dryRun && adUnitMondayId !== "dry-run") {
        if (csMondayIdsForLink.length > 0) {
          await updateColumnJson(
            BOARDS.AD_UNITS,
            adUnitMondayId,
            COLUMNS.ADUNIT_CREATIVE_SETTINGS,
            { item_ids: csMondayIdsForLink }
          );
        }
        if (filteredFormatIds.size > 0) {
          await updateColumnJson(
            BOARDS.AD_UNITS,
            adUnitMondayId,
            COLUMNS.ADUNIT_FORMATS,
            { item_ids: Array.from(filteredFormatIds) }
          );
        }
      }

      results.push({
        placementName,
        placementId,
        status: placementStatus,
        adUnitAction,
        adUnitMondayId,
        deviceType,
        formatsLinked: Array.from(filteredFormatIds),
        formatsSkipped: skippedFormats,
        csActions,
      });
    }

    // ── Step 5: Summary + status update ──
    const adUnitsCreated = results.filter((r) => r.adUnitAction === "created" || r.adUnitAction === "would_create").length;
    const adUnitsExisted = results.filter((r) => r.adUnitAction === "exists").length;
    const csCreated = results.reduce(
      (sum, r) => sum + r.csActions.filter((c) => c.action === "created" || c.action === "would_create").length,
      0
    );
    const csExisted = results.reduce(
      (sum, r) => sum + r.csActions.filter((c) => c.action === "exists").length,
      0
    );

    const now = new Date().toISOString().replace("T", " ").slice(0, 19);
    const statusText = dryRun
      ? `🧪 ${now} — Dry run: ${toProcess.length} placements (${skippedInactive} inactive skipped)`
      : `📥 ${now} — ${adUnitsCreated} ad units created, ${adUnitsExisted} existed, ${skippedInactive} inactive skipped, ${csCreated} CS created`;

    if (!dryRun) {
      await updatePublisherStatus(publisherId, statusText);
    }

    return res.status(200).json({
      publisherId,
      publisherName,
      adformPublisherId,
      dryRun,
      totalAdformPlacements: allPlacements.length,
      processed: results.length,
      adUnitsCreated,
      adUnitsExisted,
      skippedInactive,
      csCreated,
      csExisted,
      results,
      timestamp: new Date().toISOString(),
    });
  } catch (err: any) {
    console.error("[FetchAdUnits] Fatal error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
