import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";

/**
 * GET /api/probe-deals-api
 *
 * Debug endpoint to test Adform deals API pagination and filtering.
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const token = await authenticate();
    const results: any[] = [];

    // Helper to test a URL
    async function probe(label: string, url: string) {
      const resp = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const headers: Record<string, string> = {};
      resp.headers.forEach((v, k) => { headers[k] = v; });

      let count = 0;
      let firstId: any = null;
      let lastId: any = null;
      let invSrcIds: any[] = [];
      let error: string | undefined;

      if (resp.ok) {
        const data = await resp.json();
        const deals = Array.isArray(data) ? data : [];
        count = deals.length;
        firstId = deals[0]?.id;
        lastId = deals[deals.length - 1]?.id;
        invSrcIds = [...new Set(deals.map((d: any) => d.inventorySourceId))];
      } else {
        error = await resp.text();
      }

      results.push({
        test: label,
        status: resp.status,
        count,
        firstId,
        lastId,
        inventorySourceIds: invSrcIds.length <= 10 ? invSrcIds : `${invSrcIds.length} unique`,
        headers: Object.keys(headers).length > 0 ? headers : undefined,
        error: error?.slice(0, 200),
      });
    }

    // Test different param combos
    await probe("no params", `${API_BASE}/deals`);
    await probe("page=2", `${API_BASE}/deals?page=2`);
    await probe("page=0", `${API_BASE}/deals?page=0`);
    await probe("offset=100", `${API_BASE}/deals?offset=100`);
    await probe("offset=100&limit=50", `${API_BASE}/deals?offset=100&limit=50`);
    await probe("limit=5", `${API_BASE}/deals?limit=5`);
    await probe("limit=200", `${API_BASE}/deals?limit=200`);
    await probe("limit=1000", `${API_BASE}/deals?limit=1000`);
    await probe("pageSize=200", `${API_BASE}/deals?pageSize=200`);
    await probe("status=accepted", `${API_BASE}/deals?status=accepted`);

    // Also check inventory sources endpoint briefly
    const invResp = await fetch(`${API_BASE}/inventory-sources`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    let invSources: any[] = [];
    if (invResp.ok) {
      invSources = await invResp.json();
    }

    // Test with specific inventory source IDs
    if (invSources.length > 0) {
      await probe(`inventorySourceIds=${invSources[0].id}`, `${API_BASE}/deals?inventorySourceIds=${invSources[0].id}`);
    }
    if (invSources.length > 1) {
      await probe(`inventorySourceIds=${invSources[1].id}`, `${API_BASE}/deals?inventorySourceIds=${invSources[1].id}`);
    }

    return res.status(200).json({
      inventorySources: invSources.slice(0, 20).map((s: any) => ({ id: s.id, name: s.name })),
      totalInventorySources: invSources.length,
      tests: results,
    });
  } catch (err: any) {
    console.error("[ProbeDeals] Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
