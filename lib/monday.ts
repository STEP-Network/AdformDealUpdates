import { Deal, AdUnit, CreativeSetting } from "./types";

const MONDAY_API_URL = "https://api.monday.com/v2";

// ── Board & Column IDs (from Make.com blueprint) ──
const PUBLISHER_BOARD = "1222800432";
const DEALS_BOARD = "1623368485";
const AD_UNITS_BOARD = "1558569789";
const CS_BOARD = "2133994341";

// Publisher board relation columns
const COL_PUBLISHER_DEALS = "board_relation_mm149gqq";
const COL_PUBLISHER_AD_UNITS = "board_relation_mkvg7sz5";

// Deal board columns
const COL_DEAL_ADFORM_ID = "text__1";
const COL_DEAL_FORMATS = "board_relation_mkyj3jbe";

// Ad Unit board columns
const COL_ADUNIT_ADFORM_PLACEMENT_ID = "text__1";
const COL_ADUNIT_FORMATS = "board_relation_mm1aq629";
const COL_ADUNIT_CREATIVE_SETTINGS = "board_relation_mkzrqg57";

// Creative Setting board columns
const COL_CS_ADFORM_ID = "text_mkvgpdzj";
const COL_CS_FORMAT = "board_relation_mkvghsh2";

// Status column on publisher board
const COL_LAST_DEAL_UPDATE = "text_mm1h7h1h";

function getToken(): string {
  const token = process.env.MONDAY_API_TOKEN;
  if (!token) throw new Error("MONDAY_API_TOKEN not set");
  return token;
}

async function mondayQuery(query: string, variables?: Record<string, unknown>): Promise<any> {
  const resp = await fetch(MONDAY_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: getToken(),
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Monday API ${resp.status}: ${text}`);
  }

  const json: any = await resp.json();

  if (json.errors && json.errors.length > 0) {
    throw new Error(`Monday GraphQL error: ${JSON.stringify(json.errors)}`);
  }

  return json.data;
}

// ── Helper: extract linked item IDs from a column value ──
function getLinkedIds(item: any, columnId: string): string[] {
  const col = item.column_values?.find((c: any) => c.id === columnId);
  if (!col) return [];

  // linked_items_ids is the field name in Monday's GraphQL API
  if (col.linked_item_ids && Array.isArray(col.linked_item_ids)) {
    return col.linked_item_ids.map(String);
  }

  // Fallback: parse value JSON
  if (col.value) {
    try {
      const parsed = JSON.parse(col.value);
      if (parsed?.linkedPulseIds) {
        return parsed.linkedPulseIds.map((lp: any) => String(lp.linkedPulseId));
      }
    } catch {}
  }

  return [];
}

function getTextValue(item: any, columnId: string): string {
  const col = item.column_values?.find((c: any) => c.id === columnId);
  return col?.text || col?.value?.replace(/"/g, "") || "";
}

// ── Step 1: Get publisher's linked deal IDs and ad unit IDs in ONE query ──
export async function getPublisherLinks(publisherId: string): Promise<{
  dealIds: string[];
  adUnitIds: string[];
  publisherName: string;
}> {
  const data = await mondayQuery(`
    query ($id: [ID!]!) {
      items(ids: $id) {
        id
        name
        column_values(ids: ["${COL_PUBLISHER_DEALS}", "${COL_PUBLISHER_AD_UNITS}"]) {
          id
          text
          value
          ... on BoardRelationValue {
            linked_item_ids
          }
        }
      }
    }
  `, { id: [publisherId] });

  const publisher = data.items?.[0];
  if (!publisher) throw new Error(`Publisher ${publisherId} not found`);

  return {
    publisherName: publisher.name,
    dealIds: getLinkedIds(publisher, COL_PUBLISHER_DEALS),
    adUnitIds: getLinkedIds(publisher, COL_PUBLISHER_AD_UNITS),
  };
}

// ── Step 2: Batch fetch all deals with their details + formats ──
export async function getDeals(dealIds: string[]): Promise<Deal[]> {
  if (dealIds.length === 0) return [];

  const data = await mondayQuery(`
    query ($ids: [ID!]!) {
      items(ids: $ids) {
        id
        name
        column_values(ids: ["${COL_DEAL_ADFORM_ID}", "${COL_DEAL_FORMATS}"]) {
          id
          text
          value
          ... on BoardRelationValue {
            linked_item_ids
          }
        }
      }
    }
  `, { ids: dealIds });

  return (data.items || []).map((item: any) => ({
    mondayId: String(item.id),
    name: item.name,
    adformDealId: getTextValue(item, COL_DEAL_ADFORM_ID),
    formatIds: getLinkedIds(item, COL_DEAL_FORMATS),
  }));
}

// ── Step 3: Batch fetch all ad units with details + formats + CS links ──
export async function getAdUnits(adUnitIds: string[]): Promise<{
  adUnits: AdUnit[];
  csIds: string[];
  adUnitCsLinks: Map<string, string[]>;
}> {
  if (adUnitIds.length === 0) return { adUnits: [], csIds: [], adUnitCsLinks: new Map() };

  const data = await mondayQuery(`
    query ($ids: [ID!]!) {
      items(ids: $ids) {
        id
        name
        column_values(ids: ["${COL_ADUNIT_ADFORM_PLACEMENT_ID}", "${COL_ADUNIT_FORMATS}", "${COL_ADUNIT_CREATIVE_SETTINGS}"]) {
          id
          text
          value
          ... on BoardRelationValue {
            linked_item_ids
          }
        }
      }
    }
  `, { ids: adUnitIds });

  const allCsIds = new Set<string>();
  const adUnitCsLinks = new Map<string, string[]>();

  const adUnits: AdUnit[] = (data.items || []).map((item: any) => {
    const csIds = getLinkedIds(item, COL_ADUNIT_CREATIVE_SETTINGS);
    csIds.forEach((id) => allCsIds.add(id));
    adUnitCsLinks.set(String(item.id), csIds);

    return {
      mondayId: String(item.id),
      name: item.name,
      adformPlacementId: getTextValue(item, COL_ADUNIT_ADFORM_PLACEMENT_ID),
      formatIds: getLinkedIds(item, COL_ADUNIT_FORMATS),
      creativeSettings: [], // filled in after CS batch fetch
    };
  });

  return { adUnits, csIds: Array.from(allCsIds), adUnitCsLinks };
}

// ── Step 4: Batch fetch all creative settings ──
export async function getCreativeSettings(csIds: string[]): Promise<Map<string, CreativeSetting>> {
  if (csIds.length === 0) return new Map();

  const data = await mondayQuery(`
    query ($ids: [ID!]!) {
      items(ids: $ids) {
        id
        name
        column_values(ids: ["${COL_CS_ADFORM_ID}", "${COL_CS_FORMAT}"]) {
          id
          text
          value
          ... on BoardRelationValue {
            linked_item_ids
          }
        }
      }
    }
  `, { ids: csIds });

  const map = new Map<string, CreativeSetting>();
  for (const item of data.items || []) {
    map.set(String(item.id), {
      mondayId: String(item.id),
      name: item.name,
      adformCsId: getTextValue(item, COL_CS_ADFORM_ID),
      formatIds: getLinkedIds(item, COL_CS_FORMAT),
    });
  }
  return map;
}

// ── Attach creative settings to ad units ──
export function attachCreativeSettings(
  adUnits: AdUnit[],
  csMap: Map<string, CreativeSetting>,
  adUnitCsLinks: Map<string, string[]>
): void {
  for (const adUnit of adUnits) {
    const csIds = adUnitCsLinks.get(adUnit.mondayId) || [];
    adUnit.creativeSettings = csIds
      .map((id) => csMap.get(id))
      .filter((cs): cs is CreativeSetting => cs !== undefined);
  }
}

// ── Write sync status to publisher row ──
export async function updatePublisherStatus(publisherId: string, status: string): Promise<void> {
  try {
    await mondayQuery(`
      mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: String!) {
        change_simple_column_value(
          board_id: $boardId
          item_id: $itemId
          column_id: $columnId
          value: $value
        ) {
          id
        }
      }
    `, {
      boardId: PUBLISHER_BOARD,
      itemId: publisherId,
      columnId: COL_LAST_DEAL_UPDATE,
      value: status,
    });
  } catch (err) {
    console.error("Failed to update publisher status:", err);
  }
}
