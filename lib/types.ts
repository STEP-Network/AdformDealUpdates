// ── Monday.com types ──

export interface MondayItem {
  id: string;
  name: string;
  column_values: MondayColumnValue[];
}

export interface MondayColumnValue {
  id: string;
  type: string;
  text: string | null;
  value: string | null;
  linked_item_ids?: string[];
  // For board_relation columns, the linked IDs come via linked_items or linked_item_ids
}

// ── Domain types ──

export interface Deal {
  mondayId: string;
  name: string;
  adformDealId: string;
  formatIds: string[]; // Monday pulse IDs from Formats board
}

export interface AdUnit {
  mondayId: string;
  name: string;
  adformPlacementId: string;
  formatIds: string[]; // Monday pulse IDs from Formats board
  creativeSettings: CreativeSetting[];
}

export interface CreativeSetting {
  mondayId: string;
  name: string;
  adformCsId: string;
  formatIds: string[]; // Monday pulse IDs from Formats board
}

// ── Matching result types ──

export interface MatchedPlacement {
  adUnitName: string;
  adformPlacementId: string;
  mondayCsIds: string[]; // Adform CS IDs from Monday (filtered by format match)
}

export interface DealWithPlacements {
  mondayId: string;
  name: string;
  adformDealId: string;
  matchedPlacements: MatchedPlacement[];
}

// ── Adform types ──

export interface AdformDeal {
  id: number;
  name: string;
  placements: AdformPlacement[];
  [key: string]: unknown; // preserve all other fields for PUT
}

export interface AdformPlacement {
  id: number;
  creativeSettings: number[];
  [key: string]: unknown;
}

export interface AdformPlacementDetail {
  id: number;
  creativeSettings: { id: number }[];
  [key: string]: unknown;
}

// ── Sync result types ──

export interface CsInfo {
  id: number;
  name?: string; // Monday CS name (e.g. "320x480 - Interscroller")
}

export interface PlacementDetail {
  placementId: number;
  adUnitName: string;
  creativeSettings: CsInfo[];
  action: "added" | "kept" | "skipped";
  skipReason?: string;
}

export interface DealSyncResult {
  dealName: string;
  adformDealId: string;
  placementsAdded: number;
  placementsKept: number;
  placementsSkipped: number; // empty CS after intersection
  status: "updated" | "dryRun" | "error";
  error?: string;
  // Verbose fields (only when verbose=true)
  verbose?: {
    existingDeal: any;            // Full GET response from Adform
    updatedDeal: any;             // Full PUT body we would send
    placementBreakdown: PlacementDetail[];
  };
}

export interface SyncResult {
  publisherId: string;
  publisherName?: string;
  dryRun: boolean;
  maxDeals: number | null;
  deals: DealSyncResult[];
  timestamp: string;
}
