import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";
const BATCH_SIZE = 100; // Adform max limit per request
const MONDAY_BOARD_ID = 1623368485;
const MONDAY_GROUP_ID = "new_group_mkmf3wkn"; // "Deals" group

// Column IDs
const COL_DEAL_ID = "text__1";
const COL_DEAL_NAMES = "deal_names__1";
const COL_CPM = "cpm_mkmjsxc1";
const COL_PRICING_TYPE = "pricing_type_mkmj9a6x";
const COL_STATUS = "color_mkqby95j";       // Status → "Deal godkendt" = index 3
const COL_SSP = "color_mkyj312q";          // SSP → "Adform" = index 107
const COL_DSP = "color_mkyjvjs6";          // DSP → "Adform" = index 107
const COL_SEATS = "text_mkmjx8pa";
const COL_ADDED_FROM_ADFORM = "text_mm1jhjcj";

/**
 * GET /api/import-adform-deals
 *
 * Finds Adform deals missing from Monday and creates them.
 * Query params:
 *   dryRun=true    — default true, set to false to actually create items
 *   months=6       — how many months back to look (default: 6)
 *   limit=50       — max deals to import per run (default: 50)
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const dryRun = req.query.dryRun !== "false"; // default true
    const months = req.query.months ? parseInt(req.query.months as string, 10) : 6;
    const importLimit = req.query.limit ? parseInt(req.query.limit as string, 10) : 50;

    const token = await authenticate();

    // ── 1. Fetch ALL deals from Adform ──
    const allDeals: any[] = [];
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const url = `${API_BASE}/deals?offset=${offset}&limit=${BATCH_SIZE}`;
      console.log(`[ImportDeals] Fetching offset=${offset}`);

      const resp = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!resp.ok) {
        if (offset === 0) {
          const text = await resp.text();
          return res.status(resp.status).json({ error: "Adform API failed", body: text });
        }
        hasMore = false;
        break;
      }

      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      if (deals.length === 0) { hasMore = false; break; }

      allDeals.push(...deals);
      offset += deals.length;

      if (deals.length < BATCH_SIZE) hasMore = false;
      if (offset >= 20000) hasMore = false; // safety
    }

    console.log(`[ImportDeals] Total Adform deals: ${allDeals.length}`);

    // ── 2. Filter: accepted + recent ──
    const cutoff = new Date();
    cutoff.setMonth(cutoff.getMonth() - months);
    const cutoffStr = cutoff.toISOString().slice(0, 10);

    const candidates = allDeals.filter((d: any) => {
      if (d.status !== "accepted") return false;
      const dateStr = d.validPeriod?.from || d.createdAt?.slice(0, 10) || "";
      if (!dateStr) return true;
      return dateStr >= cutoffStr;
    });

    console.log(`[ImportDeals] Accepted + recent (>= ${cutoffStr}): ${candidates.length}`);

    // ── 3. Get existing deal IDs from Monday ──
    const mondayToken = process.env.MONDAY_API_TOKEN;
    if (!mondayToken) throw new Error("MONDAY_API_TOKEN not set");

    const mondayDealIds = new Set<string>();
    let cursor: string | null = null;

    do {
      let query: string;
      let variables: any = {};
      if (!cursor) {
        query = `query {
          boards(ids: [${MONDAY_BOARD_ID}]) {
            items_page(limit: 500) {
              cursor
              items {
                id
                column_values(ids: ["${COL_DEAL_ID}"]) {
                  text
                }
              }
            }
          }
        }`;
      } else {
        query = `query ($cursor: String!) {
          next_items_page(cursor: $cursor, limit: 500) {
            cursor
            items {
              id
              column_values(ids: ["${COL_DEAL_ID}"]) {
                text
              }
            }
          }
        }`;
        variables = { cursor };
      }

      const mondayResp = await fetch("https://api.monday.com/v2", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: mondayToken },
        body: JSON.stringify({ query, variables }),
      });
      const mondayJson: any = await mondayResp.json();

      const pg = cursor
        ? mondayJson.data?.next_items_page
        : mondayJson.data?.boards?.[0]?.items_page;

      cursor = pg?.cursor || null;
      for (const item of pg?.items || []) {
        const adformId = item.column_values?.[0]?.text?.trim();
        if (adformId) mondayDealIds.add(adformId);
      }
    } while (cursor);

    console.log(`[ImportDeals] Monday has ${mondayDealIds.size} deals with Adform IDs`);

    // ── 4. Find missing deals ──
    const missingDeals = candidates.filter((d: any) => {
      const dealId = d.dealId || d.id?.toString();
      return dealId && !mondayDealIds.has(dealId) && !mondayDealIds.has(String(d.id));
    });

    // Sort newest first
    missingDeals.sort((a: any, b: any) => {
      const dateA = a.validPeriod?.from || a.createdAt?.slice(0, 10) || "";
      const dateB = b.validPeriod?.from || b.createdAt?.slice(0, 10) || "";
      return dateB.localeCompare(dateA);
    });

    const toImport = missingDeals.slice(0, importLimit);
    console.log(`[ImportDeals] Missing: ${missingDeals.length}, importing: ${toImport.length}`);

    // ── 5. Create items on Monday ──
    const now = new Date().toISOString().slice(0, 16).replace("T", " ");
    const created: any[] = [];
    const errors: any[] = [];

    for (const deal of toImport) {
      const dealId = deal.dealId || String(deal.id);
      const fullName = deal.name || dealId;
      const cpm = deal.price?.value != null ? String(deal.price.value) : "";
      const pricingType = deal.price?.type || "";
      const agencyIds = (deal.buyers?.agencyIds || []).join(", ");

      // Build column values
      const columnValues: Record<string, any> = {
        [COL_DEAL_ID]: dealId,
        [COL_DEAL_NAMES]: fullName,
        [COL_CPM]: cpm,
        [COL_PRICING_TYPE]: pricingType,
        [COL_STATUS]: { index: 3 },           // "Deal godkendt"
        [COL_SSP]: { index: 107 },            // "Adform"
        [COL_DSP]: { index: 107 },            // "Adform"
        [COL_SEATS]: agencyIds ? `Agency ID: ${agencyIds}` : "",
        [COL_ADDED_FROM_ADFORM]: `${now} via API`,
      };

      if (dryRun) {
        created.push({
          dealId,
          name: fullName,
          cpm,
          pricingType,
          agencyIds,
          status: "dryRun",
        });
        continue;
      }

      // Create item on Monday
      try {
        const mutation = `mutation ($boardId: ID!, $groupId: String!, $itemName: String!, $columnValues: JSON!) {
          create_item(board_id: $boardId, group_id: $groupId, item_name: $itemName, column_values: $columnValues) {
            id
            name
          }
        }`;

        const resp = await fetch("https://api.monday.com/v2", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: mondayToken },
          body: JSON.stringify({
            query: mutation,
            variables: {
              boardId: String(MONDAY_BOARD_ID),
              groupId: MONDAY_GROUP_ID,
              itemName: fullName,
              columnValues: JSON.stringify(columnValues),
            },
          }),
        });

        const result: any = await resp.json();

        if (result.errors) {
          console.error(`[ImportDeals] Error creating ${dealId}:`, result.errors);
          errors.push({ dealId, name: fullName, error: result.errors[0]?.message });
        } else {
          const newId = result.data?.create_item?.id;
          console.log(`[ImportDeals] Created ${dealId} → Monday item ${newId}`);
          created.push({
            dealId,
            name: fullName,
            mondayItemId: newId,
            status: "created",
          });
        }

        // Small delay to avoid Monday rate limits
        await new Promise((r) => setTimeout(r, 200));
      } catch (err: any) {
        console.error(`[ImportDeals] Failed ${dealId}:`, err.message);
        errors.push({ dealId, name: fullName, error: err.message });
      }
    }

    return res.status(200).json({
      dryRun,
      adformTotalDeals: allDeals.length,
      acceptedRecent: candidates.length,
      mondayExisting: mondayDealIds.size,
      missingFromMonday: missingDeals.length,
      imported: created.length,
      errors: errors.length > 0 ? errors : undefined,
      dateCutoff: cutoffStr,
      deals: created,
    });
  } catch (err: any) {
    console.error("[ImportDeals] Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
