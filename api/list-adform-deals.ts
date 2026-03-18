import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";
const BATCH_SIZE = 100; // Adform max limit per request

/**
 * GET /api/list-adform-deals
 *
 * Lists all deals from Adform and compares with Monday board.
 * Query params:
 *   status=accepted    — client-side filter by deal status (default: accepted)
 *   limit=50           — max missing deals to return (default: 100)
 *   raw=true           — include raw Adform data for last 3 deals
 *   months=6           — how many months back to look (default: 6)
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const statusFilter = (req.query.status as string) || "accepted";
    const outputLimit = req.query.limit ? parseInt(req.query.limit as string, 10) : 100;
    const raw = req.query.raw === "true";
    const months = req.query.months ? parseInt(req.query.months as string, 10) : 6;

    const token = await authenticate();

    // ── Fetch ALL deals from Adform using offset+limit pagination ──
    // API supports: ?offset=N&limit=N (max limit=100)
    const allDeals: any[] = [];
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const url = `${API_BASE}/deals?offset=${offset}&limit=${BATCH_SIZE}`;
      console.log(`[ListDeals] Fetching offset=${offset}`);

      const resp = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!resp.ok) {
        const text = await resp.text();
        if (offset === 0) {
          return res.status(resp.status).json({
            error: `Adform API failed`,
            status: resp.status,
            body: text,
          });
        }
        // Pagination may return error when offset exceeds total
        console.log(`[ListDeals] offset=${offset} failed (${resp.status}), stopping`);
        hasMore = false;
        break;
      }

      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];

      if (deals.length === 0) {
        hasMore = false;
        break;
      }

      allDeals.push(...deals);
      offset += deals.length;

      console.log(`[ListDeals] Got ${deals.length} deals, total so far: ${allDeals.length}`);

      // If fewer than requested, that's the last page
      if (deals.length < BATCH_SIZE) {
        hasMore = false;
      }

      // Safety: max 20k deals
      if (offset >= 20000) {
        console.log(`[ListDeals] Hit 20k safety limit, stopping`);
        hasMore = false;
      }
    }

    console.log(`[ListDeals] Total deals from Adform: ${allDeals.length}`);

    // ── Client-side filtering ──

    // Filter by status
    const statusFiltered = statusFilter
      ? allDeals.filter((d: any) => d.status === statusFilter)
      : allDeals;

    console.log(`[ListDeals] After status='${statusFilter}' filter: ${statusFiltered.length}`);

    // Filter by date: only deals from last N months
    const cutoff = new Date();
    cutoff.setMonth(cutoff.getMonth() - months);
    const cutoffStr = cutoff.toISOString().slice(0, 10);

    const recentDeals = statusFiltered.filter((d: any) => {
      const dateStr = d.validPeriod?.from || d.createdAt?.slice(0, 10) || "";
      if (!dateStr) return true;
      return dateStr >= cutoffStr;
    });

    console.log(`[ListDeals] After date filter (>= ${cutoffStr}): ${recentDeals.length}`);

    // ── Get all deal IDs from Monday for comparison ──
    const mondayToken = process.env.MONDAY_API_TOKEN;
    if (!mondayToken) throw new Error("MONDAY_API_TOKEN not set");

    const mondayDealIds = new Set<string>();
    let cursor: string | null = null;

    do {
      let query: string;
      let variables: any = {};
      if (!cursor) {
        query = `query {
          boards(ids: [1623368485]) {
            items_page(limit: 500) {
              cursor
              items {
                id
                name
                column_values(ids: ["text__1"]) {
                  id
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
              name
              column_values(ids: ["text__1"]) {
                id
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
        if (adformId) {
          mondayDealIds.add(adformId);
        }
      }
    } while (cursor);

    console.log(`[ListDeals] Monday has ${mondayDealIds.size} deals with Adform IDs`);

    // ── Find missing deals ──
    const missingDeals = recentDeals.filter((d: any) => {
      const dealId = d.dealId || d.id?.toString();
      return dealId && !mondayDealIds.has(dealId) && !mondayDealIds.has(String(d.id));
    });

    // Sort by date (newest first)
    missingDeals.sort((a: any, b: any) => {
      const dateA = a.validPeriod?.from || a.createdAt?.slice(0, 10) || "";
      const dateB = b.validPeriod?.from || b.createdAt?.slice(0, 10) || "";
      return dateB.localeCompare(dateA);
    });

    const output = missingDeals.slice(0, outputLimit).map((d: any) => ({
      id: d.id,
      dealId: d.dealId,
      name: d.name,
      status: d.status,
      validFrom: d.validPeriod?.from,
      validTo: d.validPeriod?.to,
      createdAt: d.createdAt,
      price: d.price,
      inventorySourceId: d.inventorySourceId,
    }));

    // Status breakdown
    const statusBreakdown: Record<string, number> = {};
    for (const d of allDeals) {
      statusBreakdown[d.status] = (statusBreakdown[d.status] || 0) + 1;
    }

    return res.status(200).json({
      adformTotalDeals: allDeals.length,
      statusBreakdown,
      afterStatusFilter: statusFiltered.length,
      afterDateFilter: recentDeals.length,
      mondayDealsWithAdformId: mondayDealIds.size,
      missingFromMonday: missingDeals.length,
      dateCutoff: cutoffStr,
      statusFilter,
      sampleRaw: raw ? allDeals.slice(-3) : undefined,
      missingDeals: output,
    });
  } catch (err: any) {
    console.error("[ListDeals] Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
