import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";

/**
 * GET /api/list-adform-deals
 *
 * Lists all deals from Adform and compares with Monday board.
 * Query params:
 *   status=accepted    — filter by deal status (default: accepted)
 *   limit=50           — max deals to return in response
 *   raw=true           — return raw Adform response for first 5 deals
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const status = (req.query.status as string) || "";
    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : 100;
    const raw = req.query.raw === "true";

    const token = await authenticate();

    // Try listing deals — explore pagination
    const allDeals: any[] = [];
    let page = 1;
    let hasMore = true;

    while (hasMore) {
      const params = new URLSearchParams();
      if (status) params.set("status", status);
      params.set("page", String(page));
      params.set("pageSize", "100");

      const url = `${API_BASE}/deals?${params.toString()}`;
      console.log(`[ListDeals] Fetching: ${url}`);

      const resp = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!resp.ok) {
        const text = await resp.text();
        // If pagination params don't work, try without
        if (page === 1) {
          // Try simple fetch without pagination
          const simpleResp = await fetch(`${API_BASE}/deals`, {
            headers: { Authorization: `Bearer ${token}` },
          });
          if (!simpleResp.ok) {
            const simpleText = await simpleResp.text();
            return res.status(simpleResp.status).json({
              error: `Adform API failed`,
              status: simpleResp.status,
              body: simpleText,
              triedUrl: url,
            });
          }
          const simpleData = await simpleResp.json();
          const deals = Array.isArray(simpleData) ? simpleData : simpleData.deals || simpleData.data || [simpleData];
          allDeals.push(...deals);
          hasMore = false;
          break;
        }
        return res.status(resp.status).json({ error: text, page });
      }

      const data = await resp.json();
      const deals = Array.isArray(data) ? data : data.deals || data.data || [];
      allDeals.push(...deals);

      // Check if there are more pages
      const totalHeader = resp.headers.get("x-total-count");
      if (deals.length < 100) {
        hasMore = false;
      } else if (totalHeader && allDeals.length >= parseInt(totalHeader, 10)) {
        hasMore = false;
      } else {
        page++;
        // Safety: don't fetch more than 50 pages
        if (page > 50) hasMore = false;
      }
    }

    console.log(`[ListDeals] Total deals from Adform: ${allDeals.length}`);

    // Filter by date: only deals from last 6 months
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
    const sixMonthsAgoStr = sixMonthsAgo.toISOString().slice(0, 10);

    const recentDeals = allDeals.filter((d: any) => {
      // Check validPeriod.from or createdAt or similar date field
      const from = d.validPeriod?.from || d.startDate || d.createdAt || "";
      if (!from) return true; // include if no date info
      return from >= sixMonthsAgoStr;
    });

    // Get all deal IDs from Monday for comparison
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
            items_page(limit: 100) {
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
          next_items_page(cursor: $cursor, limit: 100) {
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

      const page = cursor
        ? mondayJson.data?.next_items_page
        : mondayJson.data?.boards?.[0]?.items_page;

      cursor = page?.cursor || null;
      for (const item of page?.items || []) {
        const adformId = item.column_values?.[0]?.text?.trim();
        if (adformId) {
          mondayDealIds.add(adformId);
        }
      }
    } while (cursor);

    console.log(`[ListDeals] Monday has ${mondayDealIds.size} deals with Adform IDs`);

    // Find missing deals
    const missingDeals = recentDeals.filter((d: any) => {
      const dealId = d.dealId || d.id?.toString();
      return dealId && !mondayDealIds.has(dealId) && !mondayDealIds.has(String(d.id));
    });

    // Sort by date (newest first)
    missingDeals.sort((a: any, b: any) => {
      const dateA = a.validPeriod?.from || a.startDate || "";
      const dateB = b.validPeriod?.from || b.startDate || "";
      return dateB.localeCompare(dateA);
    });

    const output = missingDeals.slice(0, limit).map((d: any) => ({
      id: d.id,
      dealId: d.dealId,
      name: d.name,
      status: d.status,
      validFrom: d.validPeriod?.from,
      validTo: d.validPeriod?.to,
      price: d.price,
      inventorySourceId: d.inventorySourceId,
    }));

    return res.status(200).json({
      adformTotalDeals: allDeals.length,
      adformRecentDeals: recentDeals.length,
      mondayDealsWithAdformId: mondayDealIds.size,
      missingFromMonday: missingDeals.length,
      sixMonthsCutoff: sixMonthsAgoStr,
      statusFilter: status || "none",
      sampleRaw: raw ? allDeals.slice(0, 3) : undefined,
      missingDeals: output,
    });
  } catch (err: any) {
    console.error("[ListDeals] Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
