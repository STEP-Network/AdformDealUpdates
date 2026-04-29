# Legacy Cleanup Backlog

Things we'll clean up once a one-time migration is fully complete.

Each entry: **what to remove**, **trigger condition**, **why we keep it for now**.

---

## 1. Per-seconds preroll naming (`6sec` / `20sec` / `30sec` in ad unit names)

**Status:** In progress (started 2026-04-29). Adform consolidating to `preroll_snvs_ap` / `preroll_snvs_ctp`.

**What can be cleaned up when ALL publishers are migrated:**

- **`api/fetch-adunits.ts` — Rename detection block** (lines around the `if (existing.name !== placementName)` check).
  Once all publishers have run through fetch and renamed their 30sec ad units, rename detection becomes dead code.
  → Keep for now: needed to handle each publisher the first time it's synced after the Adform rename.

- **`api/fetch-adunits.ts` — `isLegacy6or20Sec` guard inside the rename block** (skips renaming 6sec/20sec ad units).
  Only relevant during the consolidation transition; the 6/20sec ad units are being deactivated, not renamed.
  → Remove together with the rename block once all 30sec ad units are renamed.

- **`api/fetch-adunits.ts` — "placement removed from Adform" deactivation case**.
  Once the 6sec/20sec ad units are all deactivated (or the user deletes them on Monday), this branch will rarely fire. It's still useful in general for any future placement removals though, so consider this **permanent, not legacy**.
  → Keep permanently.

- **`isFormatAllowedForAdUnit` — the per-seconds checks are not actually present**, only `_ap` / `_ctp` / `ctp` / `autoplay` rules. Nothing to remove here. The function is already future-proof.

- **CS items "Preroll 6 sek - SNVS" and "Preroll 20 sek - SNVS"**.
  Still present on the Monday CS board with both Click-to-play AND Autoplay format relations.
  → Keep for now: until you confirm Adform has fully removed those CS IDs from all renamed placements. After that, we can mark them deactivated on Monday too.

**Trigger to revisit this section:** when a fetch-adunits run on every active publisher reports `0 renamed` and `0 deactivated (placement removed from Adform)` for two consecutive runs across the board.

---

## 2. Group `"adform ad units"` (group_mkvgv1kg) on Ad Units board

**Status:** Active. Used by fetch-adunits when creating new ad units.

**What can be cleaned up:** Nothing. This is the live destination group. Listed here only as a reminder — don't accidentally remove this group ID constant.

---

## 3. `placementsKept` and `placementsSkipped` in DealSyncResult

**Status:** Currently tracked but not surfaced in any log column.

**What can be cleaned up:** these fields can be removed from `lib/types.ts` and `api/sync.ts` if we never surface them. They're internal-only metrics today.

→ Keep: cheap to track, useful when verbose=true responses are inspected.

---

## 4. Make.com scenario #8844946 ("Update deals with publishers based on placements set")

**Status:** Active. Triggers when a deal's `*Placements` column changes → fills `Site specifik` on the deal.

**What can be cleaned up:** could be migrated into our Vercel codebase as a webhook endpoint. We already do the reverse (publisher → deals) in fetch-adunits. Not urgent — the Make scenario works.

→ Keep: works fine, no benefit to migrating yet.

---

## 5. Old per-seconds CS items on creativeSettings board

**Status:** Items exist (e.g. "Preroll 6 sek - SNVS" `text_mkvgpdzj=146357`).

**Cleanup trigger:** once Adform no longer returns these CS IDs in any placement's `creativeSettings` array.

→ Keep: not directly harmful. Will become orphaned naturally.

---

## How to use this file

When a "trigger condition" above is met, search this file for the section, do the cleanup, and either delete the section or move it to a "Done" subsection.

Don't proactively clean items here without confirming the trigger condition — the legacy code is intentional safety during migrations.
