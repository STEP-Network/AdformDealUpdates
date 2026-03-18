import type { VercelRequest, VercelResponse } from "@vercel/node";

/**
 * GET /api/debug-monday?csId=1757
 * Quick debug: fetch a CS item and show raw column data for format relation
 */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  const csAdformId = (req.query.csId as string) || "1757";
  const token = process.env.MONDAY_API_TOKEN;
  if (!token) return res.status(500).json({ error: "No token" });

  // First: use items_page query (same as getAllCreativeSettings uses)
  const resp = await fetch("https://api.monday.com/v2", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: token },
    body: JSON.stringify({
      query: `query {
        boards(ids: [2133994341]) {
          items_page(limit: 5, query_params: {
            rules: [{ column_id: "text_mkvgpdzj", compare_value: ["${csAdformId}"], operator: any_of }]
          }) {
            items {
              id
              name
              column_values(ids: ["text_mkvgpdzj", "text_mkvgb9np", "board_relation_mkvghsh2"]) {
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
      }`,
    }),
  });

  const json = await resp.json();

  // Also test with items(ids:) query (like batchFetchItems uses)
  const item = (json as any).data?.boards?.[0]?.items_page?.items?.[0];
  let batchResult: any = null;
  if (item) {
    const resp2 = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: token },
      body: JSON.stringify({
        query: `query ($ids: [ID!]!) {
          items(ids: $ids, limit: 1) {
            id
            name
            column_values(ids: ["text_mkvgpdzj", "text_mkvgb9np", "board_relation_mkvghsh2"]) {
              id
              text
              value
              ... on BoardRelationValue {
                linked_item_ids
              }
            }
          }
        }`,
        variables: { ids: [item.id] },
      }),
    });
    batchResult = await resp2.json();
  }

  return res.status(200).json({
    searchQuery: csAdformId,
    itemsPageResult: item,
    batchFetchResult: (batchResult as any)?.data?.items?.[0],
  });
}
