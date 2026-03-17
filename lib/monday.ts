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

// ── Helper: chunk array into batches of N (Monday limit = 100) ──
function chunk<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

const MONDAY_BATCH_SIZE = 100;

// ── Helper: fetch items in batches, return all items flat ──
async function batchFetchItems(
  ids: string[],
  columnIds: string[]
): Promise<any[]> {
  if (ids.length === 0) return [];

  const batches = chunk(ids, MONDAY_BATCH_SIZE);
  const colFilter = columnIds.map((c) => `"${c}"`).join(", ");

  const results = await Promise.all(
    batches.map((batchIds) =>
      mondayQuery(`
        query ($ids: [ID!]!) {
          items(ids: $ids, limit: 100) {
            id
            name
            column_values(ids: [${colFilter}]) {
              id
              text
              value
              ... on BoardRelationValue {
                linked_item_ids
              }
            }
          }
        }
      `, { ids: batchIds })
    )
  );

  return results.flatMap((data) => data.items || []);
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
  const items = await batchFetchItems(dealIds, [COL_DEAL_ADFORM_ID, COL_DEAL_FORMATS]);

  return items.map((item: any) => ({
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

  const items = await batchFetchItems(adUnitIds, [
    COL_ADUNIT_ADFORM_PLACEMENT_ID,
    COL_ADUNIT_FORMATS,
    COL_ADUNIT_CREATIVE_SETTINGS,
  ]);

  const allCsIds = new Set<string>();
  const adUnitCsLinks = new Map<string, string[]>();

  const adUnits: AdUnit[] = items.map((item: any) => {
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

  const items = await batchFetchItems(csIds, [COL_CS_ADFORM_ID, COL_CS_FORMAT]);

  const map = new Map<string, CreativeSetting>();
  for (const item of items) {
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

// ── Column IDs for the new Adform publisher ID column ──
const COL_PUBLISHER_ADFORM_ID = "numeric_mm1hsqn1";

// ── Helper: create item on a board ──
export async function createItem(
  boardId: string,
  groupId: string,
  itemName: string,
  columnValues: Record<string, unknown>
): Promise<string> {
  const data = await mondayQuery(`
    mutation ($boardId: ID!, $groupId: String!, $name: String!, $cols: JSON!) {
      create_item(
        board_id: $boardId
        group_id: $groupId
        item_name: $name
        column_values: $cols
      ) {
        id
      }
    }
  `, {
    boardId,
    groupId,
    name: itemName,
    cols: JSON.stringify(columnValues),
  });
  return String(data.create_item.id);
}

// ── Helper: update a column value ──
export async function updateColumnValue(
  boardId: string,
  itemId: string,
  columnId: string,
  value: string
): Promise<void> {
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
  `, { boardId, itemId, columnId, value });
}

// ── Helper: update column with JSON value (for relations, dropdowns, etc.) ──
export async function updateColumnJson(
  boardId: string,
  itemId: string,
  columnId: string,
  value: Record<string, unknown>
): Promise<void> {
  await mondayQuery(`
    mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        board_id: $boardId
        item_id: $itemId
        column_id: $columnId
        value: $value
      ) {
        id
      }
    }
  `, { boardId, itemId, columnId, value: JSON.stringify(value) });
}

// ── Helper: search board items by a text column value ──
export async function findItemByColumnValue(
  boardId: string,
  columnId: string,
  value: string
): Promise<{ id: string; name: string } | null> {
  const data = await mondayQuery(`
    query ($boardId: ID!, $columnId: String!, $value: CompareValue!) {
      boards(ids: [$boardId]) {
        items_page(limit: 1, query_params: {
          rules: [{ column_id: $columnId, compare_value: [$value], operator: any_of }]
        }) {
          items {
            id
            name
          }
        }
      }
    }
  `, { boardId, columnId, value });

  const items = data.boards?.[0]?.items_page?.items || [];
  return items.length > 0 ? { id: String(items[0].id), name: items[0].name } : null;
}

// ── Get all publishers with their ad units (for backfill) ──
export async function getAllPublishersWithAdUnits(): Promise<
  { id: string; name: string; adUnitIds: string[]; adformPubId: string }[]
> {
  const publishers: { id: string; name: string; adUnitIds: string[]; adformPubId: string }[] = [];
  let cursor: string | null = null;

  do {
    let data: any;
    if (!cursor) {
      data = await mondayQuery(`
        query {
          boards(ids: [${PUBLISHER_BOARD}]) {
            items_page(limit: 100) {
              cursor
              items {
                id
                name
                column_values(ids: ["${COL_PUBLISHER_AD_UNITS}", "${COL_PUBLISHER_ADFORM_ID}"]) {
                  id
                  text
                  value
                  ... on BoardRelationValue {
                    linked_item_ids
                  }
                }
              }
            }
          }
        }
      `);
      const page = data.boards[0].items_page;
      cursor = page.cursor;
      for (const item of page.items) {
        publishers.push({
          id: String(item.id),
          name: item.name,
          adUnitIds: getLinkedIds(item, COL_PUBLISHER_AD_UNITS),
          adformPubId: getTextValue(item, COL_PUBLISHER_ADFORM_ID),
        });
      }
    } else {
      data = await mondayQuery(`
        query ($cursor: String!) {
          next_items_page(cursor: $cursor, limit: 100) {
            cursor
            items {
              id
              name
              column_values(ids: ["${COL_PUBLISHER_AD_UNITS}", "${COL_PUBLISHER_ADFORM_ID}"]) {
                id
                text
                value
                ... on BoardRelationValue {
                  linked_item_ids
                }
              }
            }
          }
        }
      `, { cursor });
      const page = data.next_items_page;
      cursor = page.cursor;
      for (const item of page.items) {
        publishers.push({
          id: String(item.id),
          name: item.name,
          adUnitIds: getLinkedIds(item, COL_PUBLISHER_AD_UNITS),
          adformPubId: getTextValue(item, COL_PUBLISHER_ADFORM_ID),
        });
      }
    }
  } while (cursor);

  return publishers;
}

// ── Update publisher's Adform ID ──
export async function updatePublisherAdformId(
  publisherId: string,
  adformId: string
): Promise<void> {
  await updateColumnValue(PUBLISHER_BOARD, publisherId, COL_PUBLISHER_ADFORM_ID, adformId);
}

// ── Get all CS items from the CS board (for lookup by Adform ID) ──
export async function getAllCreativeSettings(): Promise<
  Map<string, { mondayId: string; name: string; adformCsId: string; size: string }>
> {
  const csMap = new Map<string, { mondayId: string; name: string; adformCsId: string; size: string }>();
  let cursor: string | null = null;

  do {
    let data: any;
    if (!cursor) {
      data = await mondayQuery(`
        query {
          boards(ids: [${CS_BOARD}]) {
            items_page(limit: 100) {
              cursor
              items {
                id
                name
                column_values(ids: ["${COL_CS_ADFORM_ID}", "text_mkvgb9np"]) {
                  id
                  text
                  value
                }
              }
            }
          }
        }
      `);
      const page = data.boards[0].items_page;
      cursor = page.cursor;
      for (const item of page.items) {
        const adformId = getTextValue(item, COL_CS_ADFORM_ID);
        if (adformId) {
          csMap.set(adformId, {
            mondayId: String(item.id),
            name: item.name,
            adformCsId: adformId,
            size: getTextValue(item, "text_mkvgb9np"),
          });
        }
      }
    } else {
      data = await mondayQuery(`
        query ($cursor: String!) {
          next_items_page(cursor: $cursor, limit: 100) {
            cursor
            items {
              id
              name
              column_values(ids: ["${COL_CS_ADFORM_ID}", "text_mkvgb9np"]) {
                id
                text
                value
              }
            }
          }
        }
      `, { cursor });
      const page = data.next_items_page;
      cursor = page.cursor;
      for (const item of page.items) {
        const adformId = getTextValue(item, COL_CS_ADFORM_ID);
        if (adformId) {
          csMap.set(adformId, {
            mondayId: String(item.id),
            name: item.name,
            adformCsId: adformId,
            size: getTextValue(item, "text_mkvgb9np"),
          });
        }
      }
    }
  } while (cursor);

  return csMap;
}

// ── Get all ad unit items from the ad units board (for lookup by Adform placement ID) ──
export async function getAllAdUnitsForLookup(): Promise<
  Map<string, { mondayId: string; name: string; adformPlacementId: string }>
> {
  const adUnitMap = new Map<string, { mondayId: string; name: string; adformPlacementId: string }>();
  let cursor: string | null = null;

  do {
    let data: any;
    if (!cursor) {
      data = await mondayQuery(`
        query {
          boards(ids: [${AD_UNITS_BOARD}]) {
            items_page(limit: 100) {
              cursor
              items {
                id
                name
                column_values(ids: ["${COL_ADUNIT_ADFORM_PLACEMENT_ID}"]) {
                  id
                  text
                  value
                }
              }
            }
          }
        }
      `);
      const page = data.boards[0].items_page;
      cursor = page.cursor;
      for (const item of page.items) {
        const placementId = getTextValue(item, COL_ADUNIT_ADFORM_PLACEMENT_ID);
        if (placementId) {
          adUnitMap.set(placementId, {
            mondayId: String(item.id),
            name: item.name,
            adformPlacementId: placementId,
          });
        }
      }
    } else {
      data = await mondayQuery(`
        query ($cursor: String!) {
          next_items_page(cursor: $cursor, limit: 100) {
            cursor
            items {
              id
              name
              column_values(ids: ["${COL_ADUNIT_ADFORM_PLACEMENT_ID}"]) {
                id
                text
                value
              }
            }
          }
        }
      `, { cursor });
      const page = data.next_items_page;
      cursor = page.cursor;
      for (const item of page.items) {
        const placementId = getTextValue(item, COL_ADUNIT_ADFORM_PLACEMENT_ID);
        if (placementId) {
          adUnitMap.set(placementId, {
            mondayId: String(item.id),
            name: item.name,
            adformPlacementId: placementId,
          });
        }
      }
    }
  } while (cursor);

  return adUnitMap;
}

// Board/column IDs exported for use in endpoints
export const BOARDS = {
  PUBLISHER: PUBLISHER_BOARD,
  DEALS: DEALS_BOARD,
  AD_UNITS: AD_UNITS_BOARD,
  CS: CS_BOARD,
} as const;

export const COLUMNS = {
  ADUNIT_ADFORM_PLACEMENT_ID: COL_ADUNIT_ADFORM_PLACEMENT_ID,
  ADUNIT_FORMATS: COL_ADUNIT_FORMATS,
  ADUNIT_CREATIVE_SETTINGS: COL_ADUNIT_CREATIVE_SETTINGS,
  ADUNIT_PUBLISHER_LINK: "board_relation_mkvgpb85",
  ADUNIT_SOURCE: "color_mkqpmnmr",
  ADUNIT_TYPE: "color_mkqp16yy",
  ADUNIT_SIZES: "dropdown_mkqxzvgj",
  CS_ADFORM_ID: COL_CS_ADFORM_ID,
  CS_SIZE: "text_mkvgb9np",
  CS_TYPE: "color_mkvgxw8x",
  CS_ADUNIT_LINK: "board_relation_mkznq1wq",
  PUBLISHER_AD_UNITS: COL_PUBLISHER_AD_UNITS,
  PUBLISHER_ADFORM_ID: COL_PUBLISHER_ADFORM_ID,
} as const;

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
