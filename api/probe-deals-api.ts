import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate, getInventorySources } from "../lib/adform";

const API_BASE = "https://api.adform.com/v1/seller";

/**
 * GET /api/probe-deals-api
 *
 * Debug endpoint to test Adform deals API pagination and filtering.
 * Tests multiple parameter combinations and reports results.
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const token = await authenticate();

    // First, get all inventory sources to know which ones to query
    const inventorySources = await getInventorySources(token);

    const results: any[] = [];

    // Test 1: Plain GET (no params)
    {
      const resp = await fetch(`${API_BASE}/deals`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      const headers: Record<string, string> = {};
      resp.headers.forEach((v, k) => { headers[k] = v; });
      const invSrcIds = [...new Set(deals.map((d: any) => d.inventorySourceId))];
      results.push({
        test: "GET /deals (no params)",
        status: resp.status,
        count: deals.length,
        inventorySourceIds: invSrcIds,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
        headers,
      });
    }

    // Test 2: With inventorySourceIds param (first source)
    if (inventorySources.length > 0) {
      const srcId = inventorySources[0].id;
      const resp = await fetch(`${API_BASE}/deals?inventorySourceIds=${srcId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: `GET /deals?inventorySourceIds=${srcId}`,
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    // Test 3: With inventorySourceId (singular) param
    if (inventorySources.length > 0) {
      const srcId = inventorySources[0].id;
      const resp = await fetch(`${API_BASE}/deals?inventorySourceId=${srcId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: `GET /deals?inventorySourceId=${srcId}`,
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    // Test 4: page=2 (check if different from page 1)
    {
      const resp = await fetch(`${API_BASE}/deals?page=2`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: "GET /deals?page=2",
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    // Test 5: offset=100
    {
      const resp = await fetch(`${API_BASE}/deals?offset=100`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: "GET /deals?offset=100",
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    // Test 6: limit=5
    {
      const resp = await fetch(`${API_BASE}/deals?limit=5`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: "GET /deals?limit=5",
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    // Test 7: Check if second inventory source has different deals
    if (inventorySources.length > 1) {
      const srcId = inventorySources[1].id;
      const resp = await fetch(`${API_BASE}/deals?inventorySourceIds=${srcId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: `GET /deals?inventorySourceIds=${srcId} (2nd source)`,
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    // Test 8: Multiple inventory sources comma-separated
    if (inventorySources.length >= 2) {
      const ids = inventorySources.slice(0, 3).map((s: any) => s.id).join(",");
      const resp = await fetch(`${API_BASE}/deals?inventorySourceIds=${ids}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await resp.json();
      const deals = Array.isArray(data) ? data : [];
      results.push({
        test: `GET /deals?inventorySourceIds=${ids} (multiple)`,
        status: resp.status,
        count: deals.length,
        firstId: deals[0]?.id,
        lastId: deals[deals.length - 1]?.id,
      });
    }

    return res.status(200).json({
      inventorySources: inventorySources.map((s: any) => ({
        id: s.id,
        name: s.name,
      })),
      totalInventorySources: inventorySources.length,
      tests: results,
    });
  } catch (err: any) {
    console.error("[ProbeDeals] Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
