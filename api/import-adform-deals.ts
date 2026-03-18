import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";
const BATCH_SIZE = 100; // Adform max limit per request
const MONDAY_BOARD_ID = 1623368485;
const MONDAY_GROUP_ID = "new_group_mkmf3wkn"; // "Deals" group
const CS_BOARD_ID = 2133994341; // creativeSettings board

// Column IDs — Deals board
const COL_DEAL_ID = "text__1";
const COL_DEAL_NAMES = "deal_names__1";
const COL_CPM = "cpm_mkmjsxc1";
const COL_PRICING_TYPE = "pricing_type_mkmj9a6x";
const COL_STATUS = "color_mkqby95j";       // Status → "Deal godkendt" = index 3
const COL_SSP = "color_mkyj312q";          // SSP → "Adform" = index 107
const COL_SEATS = "text_mkmjx8pa";
const COL_FORMAT = "board_relation_mkyj3jbe"; // *Format relation
const COL_ADDED_FROM_ADFORM = "text_mm1jhjcj";

// Column IDs — CS board
const CS_COL_ID = "text_mkvgpdzj";                    // Adform CS ID
const CS_COL_FORMAT = "board_relation_mkvghsh2";       // link to *Formater

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

    const mondayToken = process.env.MONDAY_API_TOKEN;
    if (!mondayToken) throw new Error("MONDAY_API_TOKEN not set");

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
      if (offset >= 20000) hasMore = false;
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

    // ── 3. Build CS ID → Format IDs lookup from Monday CS board ──
    // Fetch all 207 CS items with their Adform ID and format relations
    const csToFormats = new Map<string, string[]>(); // Adform CS ID → format item IDs
    let csCursor: string | null = null;

    do {
      let query: string;
      let variables: any = {};
      if (!csCursor) {
        query = `query {
          boards(ids: [${CS_BOARD_ID}]) {
            items_page(limit: 500) {
              cursor
              items {
                id
                column_values(ids: ["${CS_COL_ID}", "${CS_COL_FORMAT}"]) {
                  id
                  text
                  ... on BoardRelationValue {
                    linked_item_ids
                  }
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
              column_values(ids: ["${CS_COL_ID}", "${CS_COL_FORMAT}"]) {
                id
                text
                ... on BoardRelationValue {
                  linked_item_ids
                }
              }
            }
          }
        }`;
        variables = { cursor: csCursor };
      }

      const resp = await fetch("https://api.monday.com/v2", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: mondayToken },
        body: JSON.stringify({ query, variables }),
      });
      const json: any = await resp.json();

      const pg = csCursor
        ? json.data?.next_items_page
        : json.data?.boards?.[0]?.items_page;

      csCursor = pg?.cursor || null;

      for (const item of pg?.items || []) {
        let csId = "";
        let formatIds: string[] = [];

        for (const col of item.column_values || []) {
          if (col.id === CS_COL_ID) {
            csId = col.text?.trim() || "";
          }
          if (col.id === CS_COL_FORMAT && col.linked_item_ids) {
            formatIds = col.linked_item_ids;
          }
        }

        if (csId && formatIds.length > 0) {
          csToFormats.set(csId, formatIds);
        }
      }
    } while (csCursor);

    console.log(`[ImportDeals] CS → Format lookup: ${csToFormats.size} CS items with formats`);

    // ── 4. Get existing deal IDs from Monday ──
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

    // ── 5. Find missing deals ──
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

    // ── 6. Create items on Monday ──
    const now = new Date().toISOString().slice(0, 16).replace("T", " ");
    const created: any[] = [];
    const errors: any[] = [];

    for (const deal of toImport) {
      const dealId = deal.dealId || String(deal.id);
      const fullName = deal.name || dealId;
      const cpm = deal.price?.value != null ? String(deal.price.value) : "";
      const pricingType = deal.price?.type || "";
      const agencyIds = (deal.buyers?.agencyIds || []).join(", ");

      // Resolve formats from deal's placements → CS IDs → format IDs
      const formatIdSet = new Set<string>();
      for (const placement of deal.placements || []) {
        for (const csId of placement.creativeSettings || []) {
          const formats = csToFormats.get(String(csId));
          if (formats) {
            formats.forEach((fId: string) => formatIdSet.add(fId));
          }
        }
      }
      const formatIds: string[] = [];
      formatIdSet.forEach((fId) => formatIds.push(fId));

      // Build column values
      const columnValues: Record<string, any> = {
        [COL_DEAL_ID]: dealId,
        [COL_DEAL_NAMES]: fullName,
        [COL_CPM]: cpm,
        [COL_PRICING_TYPE]: pricingType,
        [COL_STATUS]: { index: 3 },           // "Deal godkendt"
        [COL_SSP]: { index: 107 },            // "Adform"
        [COL_SEATS]: agencyIds ? `Agency ID: ${agencyIds}` : "",
        [COL_ADDED_FROM_ADFORM]: `${now} via API`,
      };

      // Add format relation if we found any
      if (formatIds.length > 0) {
        columnValues[COL_FORMAT] = { item_ids: formatIds.map(Number) };
      }

      if (dryRun) {
        created.push({
          dealId,
          name: fullName,
          cpm,
          pricingType,
          agencyIds,
          formats: formatIds.length,
          formatIds: formatIds.length > 0 ? formatIds : undefined,
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
          console.log(`[ImportDeals] Created ${dealId} → Monday item ${newId} (${formatIds.length} formats)`);
          created.push({
            dealId,
            name: fullName,
            mondayItemId: newId,
            formats: formatIds.length,
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
      csWithFormats: csToFormats.size,
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
