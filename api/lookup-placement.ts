import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate, getPlacement } from "../lib/adform";

/**
 * GET /api/lookup-placement?placementId=1240377
 *
 * Returns the full Adform placement record so we can debug
 * which publisher / ad unit a stray placement ID belongs to.
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const placementId = (req.query.placementId as string) || "";
    if (!placementId) {
      return res.status(400).json({ error: "Missing placementId query param" });
    }

    const token = await authenticate();
    const placement = await getPlacement(token, placementId);

    return res.status(200).json({ placementId, placement });
  } catch (err: any) {
    console.error("[LookupPlacement] Error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
