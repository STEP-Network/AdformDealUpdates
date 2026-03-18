import { AdformDeal, AdformPlacementDetail } from "./types";

const AUTH_URL = "https://id.adform.com/sts/connect/token";
const API_BASE = "https://api.adform.com/v1/seller";
const SCOPES = [
  "https://api.adform.com/scope/eapi",
  "https://api.adform.com/scope/seller.stats",
  "https://api.adform.com/scope/seller.stats.metadata",
  "https://api.adform.com/scope/seller.deals",
  "https://api.adform.com/scope/api.placements.read",
  "https://api.adform.com/scope/seller.buyers",
].join(" ");

let cachedToken: { token: string; expiresAt: number } | null = null;

// ── Concurrency limiter (max N simultaneous Adform API calls) ──
const MAX_CONCURRENT = 5;
let activeRequests = 0;
const queue: (() => void)[] = [];

async function withConcurrencyLimit<T>(fn: () => Promise<T>): Promise<T> {
  if (activeRequests >= MAX_CONCURRENT) {
    await new Promise<void>((resolve) => queue.push(resolve));
  }
  activeRequests++;
  try {
    return await fn();
  } finally {
    activeRequests--;
    if (queue.length > 0) {
      const next = queue.shift()!;
      next();
    }
  }
}

// ── Retry wrapper with exponential backoff ──
async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await withConcurrencyLimit(fn);
    } catch (err: any) {
      const status = err?.status || err?.statusCode;
      const isRetryable = status === 429 || status >= 500;

      if (!isRetryable || attempt === maxRetries) {
        throw err;
      }

      // For 429, use longer delay
      const delay = status === 429
        ? Math.pow(2, attempt) * 1500  // 3s, 6s, 12s for rate limits
        : Math.pow(2, attempt) * 500;  // 1s, 2s, 4s for 5xx
      console.log(`[Adform] ${label} attempt ${attempt} failed (${status}), retrying in ${delay}ms...`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw new Error("Unreachable");
}

// ── Authenticate ──
export async function authenticate(): Promise<string> {
  // Return cached token if still valid (with 30s buffer)
  if (cachedToken && Date.now() < cachedToken.expiresAt - 30000) {
    return cachedToken.token;
  }

  const clientId = process.env.ADFORM_CLIENT_ID;
  const clientSecret = process.env.ADFORM_CLIENT_SECRET;
  if (!clientId || !clientSecret) throw new Error("ADFORM_CLIENT_ID or ADFORM_CLIENT_SECRET not set");

  const resp = await fetch(AUTH_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: "client_credentials",
      scope: SCOPES,
    }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Adform auth failed ${resp.status}: ${text}`);
  }

  const data: any = await resp.json();
  cachedToken = {
    token: data.access_token,
    expiresAt: Date.now() + data.expires_in * 1000,
  };

  return cachedToken.token;
}

// ── GET a deal by Adform ID ──
export async function getDeal(token: string, adformDealId: string): Promise<AdformDeal> {
  return withRetry(async () => {
    const resp = await fetch(`${API_BASE}/deals/${adformDealId}`, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!resp.ok) {
      const text = await resp.text();
      const err: any = new Error(`Adform GET deal ${adformDealId} failed ${resp.status}: ${text}`);
      err.status = resp.status;
      throw err;
    }

    return resp.json() as Promise<AdformDeal>;
  }, `GET deal ${adformDealId}`);
}

// ── GET a placement's creative settings ──
export async function getPlacement(token: string, placementId: string): Promise<AdformPlacementDetail> {
  return withRetry(async () => {
    const resp = await fetch(`${API_BASE}/placements/${placementId}`, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!resp.ok) {
      const text = await resp.text();
      const err: any = new Error(`Adform GET placement ${placementId} failed ${resp.status}: ${text}`);
      err.status = resp.status;
      throw err;
    }

    return resp.json() as Promise<AdformPlacementDetail>;
  }, `GET placement ${placementId}`);
}

// ── GET all inventory sources (publishers) from Adform ──
export async function getInventorySources(token: string): Promise<any[]> {
  return withRetry(async () => {
    const resp = await fetch(`${API_BASE}/inventory-sources`, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!resp.ok) {
      const text = await resp.text();
      const err: any = new Error(
        `Adform GET inventory-sources failed ${resp.status}: ${text}`
      );
      err.status = resp.status;
      throw err;
    }

    return resp.json() as Promise<any[]>;
  }, "GET inventory-sources");
}

// ── GET all placements for an inventory source (publisher) ──
export async function getInventorySourcePlacements(
  token: string,
  inventorySourceId: string
): Promise<any[]> {
  return withRetry(async () => {
    const resp = await fetch(
      `${API_BASE}/inventory-sources/${inventorySourceId}/placements`,
      { headers: { Authorization: `Bearer ${token}` } }
    );

    if (!resp.ok) {
      const text = await resp.text();
      const err: any = new Error(
        `Adform GET inventory-source ${inventorySourceId} placements failed ${resp.status}: ${text}`
      );
      err.status = resp.status;
      throw err;
    }

    return resp.json() as Promise<any[]>;
  }, `GET inventory-source ${inventorySourceId} placements`);
}

// ── PUT an updated deal ──
export async function updateDeal(token: string, adformDealId: string, dealBody: AdformDeal): Promise<void> {
  return withRetry(async () => {
    const resp = await fetch(`${API_BASE}/deals/${adformDealId}`, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(dealBody),
    });

    if (!resp.ok) {
      const text = await resp.text();
      const err: any = new Error(`Adform PUT deal ${adformDealId} failed ${resp.status}: ${text}`);
      err.status = resp.status;
      throw err;
    }
  }, `PUT deal ${adformDealId}`);
}
