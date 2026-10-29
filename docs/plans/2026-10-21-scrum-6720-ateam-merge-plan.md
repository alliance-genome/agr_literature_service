<!-- Plan for SCRUM-6720 (step 2 of the ABC + A-team shared database, spike SCRUM-6650). Written 2026-10-21 from the code on main (275fd9b6) and the A-team code; replaces the uncommitted 2026-10-15 plan doc that SCRUM-6719/6720 cite. -->

# SCRUM-6720 — Merge the ABC database with the A-team persistent store (step 2 of 2)

## Context
Step 1 (SCRUM-6719, PR #1342, merged 2026-10-20, live on stage) brought ABC to A-team conventions:
primary keys named `id`, `users` → `agent`, and LinkML audit columns. Docker Postgres is on 18.4,
the version A-team runs. Step 2 is the work that needs the A-team tables in the same database:
- ABC's ontology-curie strings become foreign keys to A-team `ontologyterm`;
- `topic_entity_tag.entity` becomes a foreign key to `biologicalentity`;
- A-team's foreign keys are repointed at ABC's `reference` / `person` (**the A-team's own work**,
  see Phase 5);
- `ateam_db_helpers` becomes direct SQL;
- the search pipeline keeps working.

**Status (2026-10-23):** Phases 0–4 are built on the `ateam_merge` branch (draft PR #1350), with
every revision dry-run on the merged test DB. The whole chain, including main's bigint revision
(#1352, fixed for the merged DB in #1353), was rehearsed there on 2026-10-22 (Phase 7). Phase 4 was
reworked on 2026-10-23 to stream `ontologyterm` and join it in ksql; Valerio's #1356 (merged into
the branch) made the joins INNER, dropped the ES labels and added bulk-load settings. Phase 5 is
the A-team's. Left for ABC: Phase 6 and the Phase 7 rollout. On 2026-10-28 the merged test DB was
rebuilt from fresh dumps and the ateam_merge chain rehearsed again: about 73 min end to end (Phase 7).
A second run that day, from a prod dump, rehearsed the whole chain from prod's revision (SCRUM-6719,
bigint, ateam_merge): 4 h 40 end to end, 2 h 49 of it SCRUM-6719's audit revision.

Per the ticket, its alembic revisions go on the `ateam_merge` branch and are applied only to the
merged database. That is the long-lived git branch `ateam_merge` (see Phase 0); `alembic/env.py`
on `main` already supports targeting another database with `-x db_url`. The plan doc the tickets cite (`docs/plans/2026-10-15-ateam-merge-curie-to-fk-plan.md`)
is not in the repo. This plan is rebuilt from the code on `main` and the A-team code, and should
become that doc.

Facts from the spike (SCRUM-6650):
- the merged test DB `literature_ateam_merge_test` on the dev-box Postgres is 290 tables (96 ABC + 194 A-team);
- A-team `person` / `reference` were renamed `ateam_person` / `ateam_reference`;
- 131 A-team foreign keys point at people, 12 at references;
- 517,344 of 517,346 A-team references match ABC by curie;
- a dump is at `s3://agr-literature/build/database_dump/ondemand/literature_ateam_merge_test_2026-10-16_19-33-42.dump`.
  It predates SCRUM-6719, so it must be rebuilt.

## Guiding decisions
1. **The API keeps speaking curies.** Follow the SCRUM-6719 pattern used for `created_by`:
   - the DB column becomes `<col>_id` (FK to `ontologyterm.id`);
   - the model keeps the attribute `<col>` as a `column_property` that selects `ontologyterm.curie`;
   - a write-side hook resolves curie → id.
   Schemas, routers and UI are unchanged.
2. **Elasticsearch documents keep curie strings under the same field names,** so the ES mappings
   and `search_crud` filters and facets don't change. ksqlDB joins a streamed `ontologyterm` table
   (Phase 4) and emits only the curies; the API names them with one SQL lookup (labels in ES were
   tried on 2026-10-23 and removed in #1356).
3. **`biologicalentity` is not streamed** (too large a state store). `topic_entity_tag` gets
   `entity_id` (FK) and keeps `entity` (string) filled for every row: the curie for A-team
   entities, and the leftover WB/SGD-validated value otherwise. ksql keeps reading the string.

## Phases

### Phase 0 — Test bed and scaffolding (done 2026-10-21, except recording the guard)
- **Where the work lives:** a long-lived **git** branch `ateam_merge`, not an alembic branch label.
  Deploys run a plain `alembic upgrade head` (`apply_alembic_migration.sh:129`), and a second head
  would make it refuse. The Phase 1–4 code only works on the merged DB anyway, so it can't reach
  `main` before cutover. The revisions chain after main's head `80595e1e0d4d` and are applied only to
  the merged DB with `alembic -x db_url=… upgrade head`. `main` is merged in as it moves; the branch
  goes to `main` at cutover.
- **Guard revision** `c4a1e7b93d20`: changes nothing, but stops the chain on a database without the
  A-team tables (`ontologyterm`, `ontologytermclosure`, `ontologyterm_subsets`, `biologicalentity`,
  `ateam_reference`, `ateam_person`).
- **Merged test DB rebuilt** with `ateam_merge_load.sh` (on the branch; a parameterised
  `~shuai/merge_load.sh`). Inputs: the A-team dump `2026-10-13_21-00-27` and a fresh stage dump
  (2026-10-21 19:00 UTC, PG 18.4, alembic `80595e1e0d4d`).
  - **Result:** 41 min, no restore errors, 290 tables; 131 A-team FKs to `ateam_person`, 12 to
    `ateam_reference`; 491,970 ontology terms; 1,300,446 ABC references.
  - **A-team's person key** is `person_pkey1`, so it doesn't collide with ABC's `person_pkey`; only
    the reference renames are needed.
- **Test suite:** `tests/ateam_fixture_schema.py` creates the subset of `ontologyterm`,
  `ontologytermclosure`, `ontologyterm_subsets` and `biologicalentity` that ABC reads. It fills them
  from the same hierarchy as the in-memory mock (`fixtures.atp_test_hierarchy()`).
  - **Checked against the merged DB:** every fixture column exists in the real tables.
  - **Not needed yet:** `species`.
  - **`set_globals` seeding stays** for the mocked cache. Phase 3 added tests that build the cache
    from the fixture tables with SQL and compare it with that mock.
- **Left:** record the guard revision on the merged DB by running `alembic upgrade head` with the
  container's Postgres credentials.

### Phase 1 — Ontology curie columns → FK `ontologyterm` (done 2026-10-22)
These are the 29 columns plus `_version` twins, all ATP except where noted:
- `topic_entity_tag`: topic, entity_type, display_tag, data_novelty, data_context, species (NCBITaxon);
- `workflow_tag`: workflow_tag_id, curation_tag; `workflow_tag_topic`: workflow_tag, topic;
- `workflow_transition`: transition_from, transition_to;
- `curation_status`: topic, curation_status, curation_tag;
  `curation_status_source_association`: curation_status, curation_tag;
- `tag_source.source_evidence_assertion` (ECO **or** ATP, mixed);
- `ml_model`: topic, species, data_novelty, data_context; `dataset.data_type`;
- `indexing_priority`: predicted/curator; `manual_indexing_tag.curation_tag`;
- `reference.retraction_status`.

Per table, one revision:
- add `<col>_id bigint` with an FK to `ontologyterm(id)`;
- backfill it with a join on `curie`, checking `ontologytermtype`;
- **stop if any value is unresolved** and list it. Known exceptions: 2 data fixes and the
  `START_STATE` sentinel in `workflow_transition.transition_from`, which become NULL plus a flag or
  a real ATP term, to be decided;
- drop the string; the `_version` twins get the same treatment, keeping `_mod` flags;
- replace `LIKE 'ATP:%'` CHECKs (workflow_tag, indexing_priority, manual_indexing_tag) with
  FK + type checks, by trigger or CRUD.

Then:
- **Models:** `column_property` curie readers plus a resolver for writes. Example models:
  `topic_entity_tag_model.py`, `workflow_tag_model.py`, `curation_status_model.py`.
- **Raw SQL:** about 150 references, mostly `workflow_tag_crud.py`, `topic_entity_tag_crud.py`,
  `db_write_utils.py` and `check_wft_in_progress.py`. Each joins `ontologyterm` or compares ids.
  Extend `tests/test_legacy_schema_sql.py` with a rule that flags these column names in raw SQL
  when there's no `ontologyterm` join.

**What was built:** 11 revisions, one per table: `b8d2f4e6a031` manual_indexing_tag, `d3f5a7c9e142`
indexing_priority, `e6a8c0b2d453` ml_model, `f2b4d6e8a170` workflow_tag_topic, `a9c1e3f5b072` dataset,
`c7e9a1b3d584` workflow_transition, `d8f0b2c4e695` workflow_tag, `e1a3c5d7f906` curation_status (and
its source associations), `f4b6d8e0a217` tag_source, `a5c7e9f1b328` reference, `b7d9f1a3c540`
topic_entity_tag.
- **The pattern** (`api/models/ontology_term_model.py`): `ontology_curie()` column properties with a
  comparator, so filters on the curie compare ids; `register_curie_columns()` resolves an assigned
  curie, checking the term type, and an unknown curie is a 422. Persistent objects resolve on
  assignment, so a curie-only change still gets a history row. `CurieLabelPlugin` shows history
  changesets as curies.
- **Two id columns got new names** because the obvious one was taken: `workflow_tag.workflow_tag_id`
  is stored as `workflow_tag_term_id`, and `curation_status.curation_status` as
  `curation_status_term_id`. The model attributes keep the old names.
- **Unresolved values on the merged DB:** only `tag_source` row 227's `ECO_0006156`, corrected to
  `ECO:0006156` in the migration (and in `load_pdb_associations`). There is no `START_STATE` in the data.
  The 2026-10-28 rehearsal from a fresh restore also found 55,389 `workflow_tag_version` rows with
  `ATP:0000182proceed_on_value` (2024 inserts of since-deleted tags; earlier dry-run downgrades had
  hidden them). `d8f0b2c4e695` maps them to ATP:0000182 (2ed2bc8b).
  The `LIKE 'ATP:%'` CHECKs are gone; the FK plus the resolver's type check replace them.
- **Large tables:** `topic_entity_tag` (4.2M live, 8.6M history rows) drops its old indexes first and
  rewrites each table once: about 10.5 min. `reference_version` (15 GB) is not rewritten; only its
  178,005 rows with a value are updated.
- **Also fixed:** the weekly SGD/ZFIN entity-tag loaders in `data_ingest/for_migration/` still run from
  cron, so their raw SQL was converted too.

### Phase 2 — Entities, species, MOD taxa (done 2026-10-22)
- **TET** (`c2e4a6b8d051`): `entity_id`, an FK to `biologicalentity` matched on `primaryexternalid`,
  `ON DELETE SET NULL`. It is reset from the `entity` string, which stays the source of truth, on
  every write (`api/models/biological_entity_model.py`). An entity A-team doesn't hold leaves it NULL.
  - **Merged DB:** 2,487,426 of 2,644,494 rows linked, 0 mismatched.
  - **Unlinked:** species tags (133,559, whose entity is an NCBITaxon *term*: left as a string only),
    SGD protein complexes and pathways (about 20,700), WB transgenes (2,061), about 285 deleted genes
    and alleles, and 29 malformed values.
  - **So `entity_id` can't be required** for `entity_id_validation = 'alliance'` (most SGD complexes
    are marked so); `valid_entity_type_dependencies` only moved to `entity_type_id`.
  - **History** gets `entity_id` / `entity_id_mod` without a backfill (no 15-minute rewrite); old
    history rows keep their entity string.
- **`dataset_entry.entity`** is NULL in all 100,211 rows, so it was left alone.
- **`mod.taxon_ids`** (`d5f7b9c1e262`) → the ordered table `mod_taxon (mod_id, ordinal,
  ontologyterm_id)`. `ModModel.taxon_ids` stays a list of curies (an association proxy), so the API
  is unchanged, except that GO and AGR now return `[]`, not `null`. Not versioned.

### Phase 3 — `ateam_db_helpers` → direct SQL (done 2026-10-22)
- Keep every public signature in `api/crud/ateam_db_helpers.py`. Implement them on `ontologyterm`
  and `ontologytermclosure`:
  - descendant = `closuresubject_id`, ancestor = `closureobject_id`;
  - parent / child = `distance = 1`; filter on `closuretypes` (is_a);
  - MOD subsets come from `ontologyterm_subsets`.
- **What the real closure table looks like** (merged DB, 2026-10-22):
  - **Every relationship is stored twice,** once with `closuretypes = ["is_a"]` and once with
    `["is_a","part_of"]` (5.9M duplicate subject/object/distance triples in 12.6M rows). Queries need
    `closuretypes ? 'is_a'` plus `DISTINCT`, or every parent and descendant comes back twice.
  - **ATP terms have several `is_a` parents.** "file needed" (ATP:0000141) sits under "file upload"
    (0000140), "not started" (0000155) and "automated workflow" (0000158). The current cache keeps
    one parent per term (`atp_to_parent`), which comes from walking down from the four roots.
    `atp_get_parent`, `atp_get_all_ancestors` and `get_workflow_process_from_tag` must keep that
    meaning: take the parent that lies inside the same root subtree (e.g. under ATP:0000177 for
    workflow tags), not just any distance-1 parent. Compare against the current cache output for
    every ATP term before switching.
  - **No `distance = 0` self rows for ontology terms.** The only subject = object rows are 698
    ROTerm rows with a NULL distance.
  - **Term counts:** 389 ATP, 1,943 NCBITaxon, 2,179 ECO; ATP:0000177 has 360 descendants.
- Replace the BFS cache build (~305 API calls, roots ATP:0000002/0000323/0000177/0000335) with one
  closure query. Keep the in-process cache, now cheap to rebuild.
- `map_curies_to_names` and `atp_return_invalid_ids` become SQL. `get_atp_id_by_name` and
  `get_workflow_tags_from_process` (in `workflow_tag_crud.py`) follow automatically.
- `search_crud.add_curie_to_name_values` and the TET grid name maps become one join.
- **Stays HTTP:** `get_or_create_species` (it writes to A-team), the WB/SGD entity APIs, and
  `resource_descriptor_cache` unless A-team's descriptor table is adopted.
- Drop the `PERSISTENT_STORE_DB_*` dependency from runtime paths.

**What was built:** every lookup that went through `agr_curation_api`'s DB methods is the same
query on ABC's own engine; a failure still raises `AGRAPIError`, so callers still return a 502.
- **The ATP cache** reads every parent→child edge in one query, then runs the unchanged BFS in
  memory. That keeps `atp_to_parent`'s meaning without choosing parents by subtree: the client's
  `DISTINCT` already sorted children by curie, and so does the new query. On the merged DB, 30 of the
  314 cached terms have several parents, and only 2 (ATP:0000044, ATP:0000085) depend on child order.
  Both keep ATP:0000150 as before.
- **A bug fixed:** for non-ATP terms the client's "ancestors" query returned descendants.
- **Stays on the A-team API:** `get_or_create_species`, resource descriptors and the missing-species
  loader. `PERSISTENT_STORE_DB_*` is gone from `docker-compose.yaml`.
- **Not done (optional):** collapsing the search and TET-grid name maps into one join. They already
  go through the SQL `map_curies_to_names`.

### Phase 4 — Search pipeline (Debezium / ksqlDB / ES) (done 2026-10-23, revised in #1356)
- **How it was decided:**
  - **2026-10-22, interim:** trigger-maintained `*_curie` mirror columns next to each streamed id
    column (revision `e8a0c2d4f673`). They worked, but left ATP strings in ABC's tables and cost about
    22 min of backfill in the rehearsal.
  - **2026-10-23:** replaced, on Valerio's advice (SCRUM-6720 comment 98866), by streaming
    `ontologyterm` and joining it in ksql, at first with labels in ES next to the curies.
  - **Re-emit risk, measured:** A-team's loads touch only the terms that change (1–20 ATP terms per
    load date; ATP:0000005 was last updated 2024-04). So the 1.9M-tag re-emit is rare. The mirror
    revision and trigger module were removed, and the rehearsal DB was downgraded off them.
  - **#1356 (Valerio, merged into `ateam_merge`):** INNER joins, curies only in ES, and bulk-load
    settings on the build slot. Details below; the label design it replaced is not in the code.
- **Connector** (`postgres-source-unified.json`, `heal_publication.sh`):
  - `public.ontologyterm` is streamed as a *simple* table (`ExtractField` key `id`), like `mod`. Its
    key must be the bare id for the foreign-key joins to match; a joined table's `{"id": …}` key never
    would.
  - `column.exclude.list` leaves only `id, curie, name, ontologytermtype, obsolete`.
  - No row filter, so no `REPLICA IDENTITY FULL` on A-team's table.
- **ksql** (`ksql_queries.ksql`):
  - An `ontologyterm` table and 14 foreign-key joins, placed before `tag_group_key` and the curie
    CASEs. The six TET terms are chained (`topic_entity_tag_term_1..6`) before the `tag_source` join,
    since ksqlDB 0.26 does one foreign-key join per query.
  - Each curie keeps its field name; no label fields. `retraction_status_name` stays (the index
    already carried it) and now comes from the term name, matching the old hard-coded CASE values.
- **NULL foreign keys** (found in a local ksqlDB run): ksqlDB 0.26 (Kafka Streams 3.2) drops a row
  whose foreign key is NULL from any foreign-key join, `LEFT` included, instead of emitting it with
  NULLs.
  - **Effect without a fix:** every reference without a retraction status, and every tag or priority
    with an unset term, would have vanished from the index.
  - **Fix:** NULL ids become `'__EMPTY__'` before each join, and ksql inserts an `'__EMPTY__'`
    `ontologyterm` row with a NULL curie and name, following the existing `resource` pattern.
    `data_novelty_id` and `data_context_id` are NOT NULL, so they have no sentinel CASE.
  - **INNER joins (#1356):** with the sentinel and the DB foreign keys every lookup has a matching
    row, so the 14 `ontologyterm` joins and the `resource` / `copyright_license` joins are INNER. A row
    waits for its term instead of being emitted first with NULL curies and again once the term
    arrives, which had doubled the `topic_entity_tag` chain during a reindex.
  - **References:** they use two narrow tables (`reference_retraction_ids`, `reference_retraction`)
    joined on the primary key, so the full reference row isn't copied again.
- **Elasticsearch:** no mapping change; the curie fields are as before. The `*_name` label fields
  were removed in #1356: they added 21% to the average document (41% on references with tags), and
  Valerio measured the dev reindex 43–56 min longer, spent in the ES sink. (On stage, a like-for-like
  comparison showed the labelled build at 4 h 41 against 4 h 40 for `main`'s ksql.)
- **Search:** `search_crud` is back to its pre-label version. Facet buckets and the large-scale tag
  summaries are named by `add_curie_to_name_values`, which now goes through the SQL
  `map_curies_to_names` (one batched `ontologyterm` lookup), not an A-team API call. Entity names
  (gene symbols, …) still come from the backend, since `biologicalentity` isn't streamed.
- **Bulk-load settings (#1356):** the rebuild is bound by Elasticsearch CPU, and the build slot ran
  with live-search defaults (1 s refresh, an fsync per bulk request). Both slots are now created with
  a 30 s refresh, an async translog and a 2 GB flush threshold; `status_manager.sh` resets them to the
  defaults just before the force-merge and alias flip (best effort, like the optimize).
- **`status_manager.sh`'s promotion gate** counted tags on the dropped columns and would have failed
  closed on every rebuild. It now groups by the term ids (one-to-one with curies) and joins
  `ontologyterm` for the source-evidence group.
- **Verified** end to end in a local Kafka plus ksqlDB (`cp-ksqldb-server:7.2.15`, arm64) with
  Debezium-shaped records: references with and without a retraction status, and tags with and
  without NULL term ids, came out in both the private and public topics with curies and NULLs.
- **Test build (Valerio, 2026-10-23, labelled ksql, 8 GB heap):** 4 h 41 min; peaks Kafka 58.7 GB,
  ksql state 53.0 GB (12 GB more than `main`), ksql memory 16 GB; indexes 4.43 / 3.69 GB with all
  1,300,446 references and 3,458,332 TETs. The curies-only build after #1356 has not been timed on
  stage yet.
- **Operations:**
  - Persistent queries go from 44 to 60, and ksqlDB's record cache is allocated per query. On prod
    (75 MiB × 60) that's about 4.4 GB of the 8 GB heap, up from about 3.3 GB. Check the headroom
    before the cutover rebuild (sizing note updated in `docker-compose.yaml`).
  - Tell the A-team that a replication publication now covers `ontologyterm`.
- **Still to do at rollout:** a full reindex, then compare a document sample against the pre-merge
  index, as in the 2026-10-15 PG 18.4 test.

### Phase 5 — A-team foreign keys → ABC `reference` / `person` (the A-team's own work)
**Decided 2026-10-22: the A-team does these FK changes separately.** What follows is kept as our
reading of their schema, for reference.
- **References:** repoint the 12 FKs (`*_reference.references_id`, `*.singlereference_id`,
  `reference_crossreference`) from `ateam_reference.id` to ABC `reference.id`, using the curie
  map (517,344/517,346; the 2 misses are resolved by hand).
- **Their decision:** about 20 evidence FKs (disease, phenotype and expression annotations,
  `*_informationcontententity.evidence_id`) point at A-team **`informationcontententity`**, the
  supertype of `reference`, not at `reference`. Options to agree with A-team:
  (a) keep `informationcontententity` and give each ABC reference a matching row with the same id
  (sync on insert);
  (b) repoint the evidence FKs at `reference` and keep `informationcontententity` only for
  external database references.
- **People:** 131 A-team `createdby_id` / `updatedby_id` FKs point at A-team `person`. ABC audits
  through `agent` (SCRUM-6719); mapping their people to ABC `person` / `agent` is theirs to decide.
- Then drop `ateam_reference` / `ateam_person`. Stop A-team's ES-based reference sync
  (`ReferenceSynchronisationHelper`); the table is shared now.

### Phase 6 — Merged references (decided in SCRUM-6742)
- ABC-side changes can ship on `main` first. Option 1 (Blue Team preference) adds the losing curie
  as a cross reference, replaces `obsolete_reference_curie`, and repoints A-team FKs during
  `merge_references` (`reference_crud.py:1090`). Whether ABC may touch the A-team's FKs is one of the
  open questions (Phase 5 is theirs).
- **Fix while there:** today's merge copies only TETs. `workflow_tag`, `curation_status`,
  `indexing_priority` and `manual_indexing_tag` on the losing reference are lost by cascade.
  Decide what moves.
- Recover the 72 previously deleted references (30 with no record), per SCRUM-6742.

### Phase 7 — Rollout
Rehearse on the merged test DB, then stage, then prod. In order: A-team freeze; dump/load and
rename (`ateam_merge_load.sh`); `alembic upgrade head` from the `ateam_merge` branch; validation reports; Debezium rebuild and full reindex;
API and automated scripts back on; A-team app pointed at the shared DB. Measure downtime on the
rehearsal, as SCRUM-6719 did. Keep a rollback (the pre-merge dumps).
- **Rehearsal, 2026-10-22,** on `literature_ateam_merge_test`: the whole chain in one transaction,
  after a `VACUUM ANALYZE` of the tables the dry runs had churned.
  - **No password needed:** alembic ran in a throwaway API-image container sharing the Postgres
    container's network, which trusts 127.0.0.1.
  - **Run 1 stopped in #1352's revision:** A-team's `flyway_schema_history.installed_rank` failed its
    integer-id check. #1353 skips it.
  - **Run 2:** 79 min (4,765 s): the bigint conversion about 16.5 min, Phase 1 about 30.5 min
    (`topic_entity_tag` 17, `workflow_tag` 10), Phase 2 about 10 min, and the since-removed mirrors
    about 22 min.
  - **Checks:** 0 mirror mismatches, 2,487,426 entity links, `reference.id` bigint, DB 108 → 115 GB.
  - **Without the mirrors** the chain is about 57 min, or about 40 if #1352 reaches prod before the
    cutover. Add the 41-min load for the A-team freeze.
  - **The rehearsal DB is now at `d5f7b9c1e262`,** downgraded off the mirror revision.
- **Rehearsal, 2026-10-28,** from fresh dumps: `literature_ateam_merge_test` rebuilt with
  `ateam_merge_load.sh` from the A-team dump `2026-10-20_21-00-36` and the stage dump of 2026-10-23
  (already bigint, alembic `039dbc7de1ef`), then `ANALYZE` and `alembic upgrade head` in one
  transaction. Only the ateam_merge revisions ran; no mirrors.

  | Step | Time |
  |---|---|
  | Drop/recreate DB | 5 s |
  | Restore A-team dump (incl. 19 s copy) | 17 min 24 s |
  | Rename `person`/`reference` | < 1 s |
  | Restore ABC stage dump (incl. 53 s copy) | 20 min 48 s |
  | **Load total** | **38 min 18 s** |
  | `ANALYZE` (a fresh restore has no planner statistics) | 3 min 41 s |
  | **`alembic upgrade`** (`039dbc7de1ef` → `d5f7b9c1e262`) | **31 min 5 s** |
  | **End to end** | **about 73 min** |

  - **Run 1 stopped after 4 min** in `d8f0b2c4e695` on the `ATP:0000182proceed_on_value` history rows
    (Phase 1) and rolled back cleanly; fixed in 2ed2bc8b. A scan of every Phase 1 curie column, live
    and history, then found nothing else unresolved. **Rehearse only on a fresh restore:** dry-run
    downgrades rewrite history strings from ids and hide bad values.
  - **Alembic took 31 min,** under the ~40-min estimate above. No per-revision breakdown this time
    (alembic logs no revision lines in the rehearsal container; sample `pg_stat_activity` for one).
  - **Checks:** 291 tables, 0 invalid indexes, 28 ABC FKs to `ontologyterm`; A-team tables untouched
    (131 FKs to `ateam_person`, 12 to `ateam_reference`); `workflow_tag` 2,156,586 live and 5,191,462
    history rows; `topic_entity_tag` 4,158,547 rows, 2,487,426 of the 2,644,494 with an entity string
    linked by `entity_id`; DB 124 GB.
  - **Downtime estimate for the A-team freeze:** about 73 min for load plus migration, before the
    Debezium rebuild (about 4 h 40 on stage).
- **Rehearsal, 2026-10-28, full chain from a prod dump** (SCRUM-6720 comment 98994): rebuilt from the
  A-team dump `2026-10-20_21-00-36` and Shuai's ABC prod dump of 2026-10-27 23:28 (`pg_dump` 17.11,
  alembic `b5e2d8a41c73`, i.e. without SCRUM-6719 or #1352). Then `ANALYZE` and `alembic upgrade head`
  through SCRUM-6719, the bigint conversion and the ateam_merge revisions. No restore errors, alembic
  exit 0. Per-step times are from `~/ateam_merge_rehearsal_sampler.sh` (`pg_stat_activity` every 15 s).

  | Step | Time |
  |---|---|
  | Restore A-team dump (incl. copy) | 18 min 39 s |
  | Rename + restore prod dump (incl. copy) | 19 min 25 s |
  | **Load total** | **38 min 4 s** |
  | `ANALYZE` | 3 min 8 s |
  | SCRUM-6719 `47d74bfbbe87`: audit backfill | ~2 h 27 min |
  | ↳ unresolved-user checks, then `VACUUM FULL` | ~3 min + ~19 min |
  | SCRUM-6719 `1e8243e92015` (`users` → `agent`) and `80595e1e0d4d` (keys → `id`) | < 1 min |
  | bigint `039dbc7de1ef`, incl. FK re-adds and `ANALYZE` | ~30 min |
  | ateam_merge revisions (`c4a1e7b93d20` → `d5f7b9c1e262`) | ~39 min |
  | **`alembic upgrade` total** | **3 h 58 min** |
  | **End to end** | **4 h 40 min** |

  - **SCRUM-6719's backfill is 70% of the run.** Largest tables: `reference_version` 32 min, `author`
    27, `cross_reference` 15, `author_version` 13, `referencefile` 12, `resource` 12. Its docstring's
    dev-copy estimate was 3 h 09.
  - **ateam_merge took 39 min,** against 31 on the stage-data run: `topic_entity_tag` FKs about 15 min
    (live and history), `workflow_tag` FKs about 13, the `entity_id` fill about 8.
  - **bigint took about 30 min** (16.5 on 10-22); `reference_version` alone about 14.5.
  - **SCRUM-6719 is safe on the merged DB:** its table lists come from column names
    (`created_by`, `created_by_id`) that no A-team table has, and it skips `ateam_*`. Prod had no
    `START_STATE` left.
  - **Checks:** 291 tables, 0 invalid indexes, `reference.id` bigint, 1,617 agents, 1,301,188
    references; A-team tables untouched (131 / 12 FKs); `workflow_tag` 2,165,122 live and 5,263,895
    history rows; `topic_entity_tag` 4,173,771 rows, 2,498,957 of the 2,656,760 with an entity string
    linked; DB 124 GB.
  - **Cutover downtime:** with SCRUM-6719 and #1352 already on prod, about 38 min load + 39 min
    ateam_merge ≈ **1 h 20**; all in one go ≈ **4 h 40**. Both are before the Debezium rebuild.
    **Recommendation:** deploy SCRUM-6719 and #1352 to prod in an earlier, separate release (its
    backfill commits in batches and needs the API and automated scripts stopped).
- **Still possible:** fold Phase 2's `entity_id` fill into Phase 1's `topic_entity_tag` rewrite, so the
  4.2M rows are rewritten once (about 10 min less).
- **Before `alembic upgrade`:** run `oneoff_scripts/SCRUM-6754_replace_start_state.py`. Revision
  `c7e9a1b3d584` maps history `START_STATE` to ATP:0000177 but stops on a live one.

## Open questions for the A-team
The current list is the questions comment on SCRUM-6720 (updated 2026-10-22). The questions on their
own FKs (evidence and audit) are dropped, since Phase 5 is theirs, and `START_STATE` is moot. Eight
remain (the publication on `ontologyterm`, dropped while we used mirrors, is back under 3):
1. Who owns the ontology loads and the closure refresh; are term ids stable across reloads.
2. SCRUM-6742: may ABC's merge move their FKs to the surviving reference.
3. How reloads treat obsolete terms, and whether a term's type or curie can change for its id.
   Also: ABC's Debezium now has a replication publication on their `ontologyterm` (Phase 4).
4. The closure table's duplicate `is_a` / `is_a,part_of` rows and multiple parents.
5. Flyway and alembic in one database.
6. Switching off their ES-based reference sync.
7. Running the shared database (roles, backups, connection limits).
8. Cutover freeze and testing against the merged test DB.

## Critical files
- `alembic/versions/` (new revisions on the `ateam_merge` branch), `alembic/env.py`
- `agr_literature_service/api/models/{topic_entity_tag,workflow_tag,workflow_tag_topic,workflow_transition,curation_status,curation_status_source_association,tag_source,ml_model,dataset,indexing_priority,manual_indexing_tag,reference,mod}_model.py`
- `agr_literature_service/api/crud/ateam_db_helpers.py`, `workflow_tag_crud.py`,
  `topic_entity_tag_crud.py`, `topic_entity_tag_utils.py`, `search_crud.py`, `mod_crud.py`,
  `reference_crud.py`
- `agr_literature_service/lit_processing/data_ingest/utils/db_write_utils.py`,
  `lit_processing/data_check/*`
- `debezium/ksql_queries.ksql`, `debezium/postgres-source-unified.json`,
  `debezium/elasticsearch-settings.json`, `debezium/status_manager.sh`, `debezium/heal_publication.sh`
- `tests/test_legacy_schema_sql.py`, plus the test DB fixtures for the A-team tables

## Verification
- **Per revision, on the merged test DB:** upgrade; unresolved-curie report = 0 (apart from the
  listed exceptions); row counts unchanged; downgrade back to an identical schema (as SCRUM-6719
  did).
- **Test suite:** full suite on a test DB with the seeded A-team fixture tables. Add tests for each
  `column_property` read and write path, the closure-based ancestors/descendants (against the
  current cache output for all ATP roots), `mod.taxons()`, and the TET entity resolution.
- **API parity:** run the dev4006 API against the merged DB and diff JSON for a sample of
  references, TETs, workflow tags and search results, before and after.
- **Search:** full reindex; compare 1,000 sampled ES documents and facet counts with the
  pre-merge index; check live update latency (< 15 s, as on 2026-10-15).
- `flake8`, `mypy`, and the extended `test_legacy_schema_sql.py`.
