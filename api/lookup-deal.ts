import type { VercelRequest, VercelResponse } from "@vercel/node";
import { authenticate, getDeal } from "../lib/adform";

/**
 * GET /api/lookup-deal?dealId=DID-122-XXX
 *
 * Returns the full Adform deal record so we can debug placements/CS.
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const dealId = (req.query.dealId as string) || "";
    if (!dealId) {
      return res.status(400).json({ error: "Missing dealId query param" });
    }

    const token = await authenticate();
    const deal = await getDeal(token, dealId);

    return res.status(200).json({ dealId, deal });
  } catch (err: any) {
    console.error("[LookupDeal] Error:", err);
    return res.status(500).json({ error: err.message || "Internal server error" });
  }
}
