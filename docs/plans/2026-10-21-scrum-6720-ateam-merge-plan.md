<!-- Plan for SCRUM-6720 (step 2 of the ABC + A-team shared database, spike SCRUM-6650). Written 2026-10-21 from the code on main (275fd9b6) and the A-team code; replaces the uncommitted 2026-10-15 plan doc that SCRUM-6719/6720 cite. -->

# SCRUM-6720 — Merge the ABC database with the A-team persistent store (step 2 of 2)

## Context
Step 1 (SCRUM-6719, PR #1342, merged 2026-10-20, live on stage) brought ABC to A-team conventions:
primary keys named `id`, `users` → `agent`, and LinkML audit columns. Docker Postgres is on 18.4,
the version A-team runs. Step 2 is the work that needs the A-team tables in the same database:
- ABC's ontology-curie strings become foreign keys to A-team `ontologyterm`;
- `topic_entity_tag.entity` becomes a foreign key to `biologicalentity`;
- A-team's foreign keys are repointed at ABC's `reference` / `person`;
- `ateam_db_helpers` becomes direct SQL;
- the search pipeline keeps working.

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

## Guiding decisions (recommended)
1. **The API keeps speaking curies.** Follow the SCRUM-6719 pattern used for `created_by`:
   - the DB column becomes `<col>_id` (FK to `ontologyterm.id`);
   - the model keeps the attribute `<col>` as a `column_property` that selects `ontologyterm.curie`;
   - a write-side hook resolves curie → id.
   Schemas, routers and UI are unchanged.
2. **Elasticsearch documents keep curie strings under the same field names,** so the ES mappings
   and `search_crud` filters and facets don't change. ksqlDB gets an `ontologyterm` lookup table
   (only the term types we use) and joins id → curie. The joins come before `tag_group_key`
   (`ksql_queries.ksql:285`) and the curie `CASE`s (`:280`, `:452`, `:988`).
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
  - **Still to replace:** the `set_globals` seeding, when Phase 3 moves the helpers to SQL.
- **Left:** record the guard revision on the merged DB by running `alembic upgrade head` with the
  container's Postgres credentials.

### Phase 1 — Ontology curie columns → FK `ontologyterm`
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

### Phase 2 — Entities, species, MOD taxa
- **TET:** add `entity_id` (FK `biologicalentity`), resolved through `primaryexternalid` (or `curie`
  for AGRKB:), for `entity_id_validation='alliance'` rows. Keep the `entity` string as in
  decision 3. Species entities: `entity_type` = species term → move them to `species`.
- **CHECK `valid_entity_type_dependencies`:** recommend a relaxed CHECK plus CRUD enforcement:
  `entity_type`, `entity` and `entity_id_validation` are all set or all NULL, and `entity_id` is
  required when validation is `alliance`. Use a trigger only if the A-team writes TETs directly.
- `dataset_entry.entity` gets the same treatment.
- **`mod.taxon_ids`** → join table `mod_taxon (mod_id, ontologyterm_id, ordinal)`. The only reader
  is `mod_crud.taxons()` (`mod_crud.py:104`); `mod_schemas` keep the list.

### Phase 3 — `ateam_db_helpers` → direct SQL
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

### Phase 4 — Search pipeline (Debezium / ksqlDB / ES)
- Add `ontologyterm` to `postgres-source-unified.json` (`table.include.list`, the simple-table key
  extractors) and to the publication in `setup.sh`.
- ksql: an `ontologyterm` table keyed by id, then lookup joins for TET, tag_source,
  curation_status, workflow_tag, indexing_priority, manual_indexing_tag and reference. These emit
  the same curie field names, keeping the "narrow spine" join ordering (`ksql_queries.ksql:694-931`).
- No ES mapping change. Full reindex, then compare a document sample against the pre-merge index,
  as in the 2026-10-15 PG 18.4 test.

### Phase 5 — A-team foreign keys → ABC `reference` / `person`
- **References:** repoint the 12 FKs (`*_reference.references_id`, `*.singlereference_id`,
  `reference_crossreference`) from `ateam_reference.id` to ABC `reference.id`, using the curie
  map (517,344/517,346; the 2 misses are resolved by hand).
- **Blocker:** about 20 evidence FKs (disease, phenotype and expression annotations,
  `*_informationcontententity.evidence_id`) point at A-team **`informationcontententity`**, the
  supertype of `reference`, not at `reference`. Options to agree with A-team:
  (a) keep `informationcontententity` and give each ABC reference a matching row with the same id
  (sync on insert);
  (b) repoint the evidence FKs at `reference` and keep `informationcontententity` only for
  external database references.
- **People:** 131 A-team `createdby_id` / `updatedby_id` FKs point at A-team `person`. ABC audits
  through `agent` (SCRUM-6719). Recommend mapping A-team people to ABC `person` (and agent),
  keyed on orcid / authemail / uniqueid, then repointing at `agent.id`. This needs A-team Java
  changes (their `AuditedObject` uses `Person`).
- Then drop `ateam_reference` / `ateam_person`. Stop A-team's ES-based reference sync
  (`ReferenceSynchronisationHelper`); the table is shared now.

### Phase 6 — Merged references (decided in SCRUM-6742)
- ABC-side changes can ship on `main` first. Option 1 (Blue Team preference) adds the losing curie
  as a cross reference, replaces `obsolete_reference_curie`, and repoints A-team FKs during
  `merge_references` (`reference_crud.py:1090`).
- **Fix while there:** today's merge copies only TETs. `workflow_tag`, `curation_status`,
  `indexing_priority` and `manual_indexing_tag` on the losing reference are lost by cascade.
  Decide what moves.
- Recover the 72 previously deleted references (30 with no record), per SCRUM-6742.

### Phase 7 — Rollout
Rehearse on the merged test DB, then stage, then prod. In order: A-team freeze; dump/load and
rename (`ateam_merge_load.sh`); `alembic upgrade head` from the `ateam_merge` branch; validation reports; Debezium rebuild and full reindex;
API and automated scripts back on; A-team app pointed at the shared DB. Measure downtime on the
rehearsal, as SCRUM-6719 did. Keep a rollback (the pre-merge dumps).

## Open questions for the A-team (to settle before Phases 5–6)
1. Option (a) or (b) for `informationcontententity` evidence FKs.
2. Audit FKs: A-team → ABC `agent`, or keep a `person` mapping?
3. Who owns `ontologyterm` writes (ontology loads) and the closure refresh, and when? ksql and the
   ABC caches must see the changes.
4. SCRUM-6742 option 1: may ABC's merge update A-team FKs? Plus the maintenance-banner API for
   A-team deploys.
5. `START_STATE` sentinel: is there an ATP term for it, or does it stay NULL?

## Critical files
- `alembic/versions/` (new revisions on the `ateam_merge` branch), `alembic/env.py`
- `agr_literature_service/api/models/{topic_entity_tag,workflow_tag,workflow_tag_topic,workflow_transition,curation_status,curation_status_source_association,tag_source,ml_model,dataset,indexing_priority,manual_indexing_tag,reference,mod}_model.py`
- `agr_literature_service/api/crud/ateam_db_helpers.py`, `workflow_tag_crud.py`,
  `topic_entity_tag_crud.py`, `topic_entity_tag_utils.py`, `search_crud.py`, `mod_crud.py`,
  `reference_crud.py`
- `agr_literature_service/lit_processing/data_ingest/utils/db_write_utils.py`,
  `lit_processing/data_check/*`
- `debezium/ksql_queries.ksql`, `debezium/postgres-source-unified.json`, `debezium/setup.sh`
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
