import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate } from "../lib/adform";

/**
 * GET /api/debug-adform?path=/v1/seller/inventory-sources
 *
 * Debug endpoint to test arbitrary Adform API paths.
 * Returns raw response for investigation.
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const path = (req.query.path as string) || "/v1/seller/inventory-sources";
    const token = await authenticate();

    const url = `https://api.adform.com${path}`;
    console.log(`[Debug] Fetching: ${url}`);

    const resp = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });

    const contentType = resp.headers.get("content-type") || "";
    const rawText = await resp.text();
    let body: any;
    try {
      body = JSON.parse(rawText);
    } catch {
      body = rawText || "(empty response)";
    }

    return res.status(200).json({
      url,
      status: resp.status,
      contentType,
      body: typeof body === "string" ? body.slice(0, 2000) : body,
      // If it's an array, show count + first 3 items
      ...(Array.isArray(body) ? { count: body.length, sample: body.slice(0, 3) } : {}),
    });
  } catch (err: any) {
    return res.status(500).json({ error: err.message });
  }
}
