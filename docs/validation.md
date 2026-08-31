# Topic Entity Tag Validation

Reference documentation for the TET validation subsystem: what it is, how it behaves, and
where the sharp edges are.

Scope note: several unrelated things in this repo are called "validation" — Pydantic
request validation, archive/file structure validation in `bulk_upload_utils.py`, and the
Caltech `curation_status` comparison scripts. **This document is only about Topic Entity
Tag validation**: the curation concept of tags corroborating or contradicting each other,
plus the curator Yes/No grid vote.

---

## Two systems, one name

The most important thing to know before reading any validation code: **there are two
different systems both called "validation", and they live in the same file.** Nearly every
surprising behaviour traces back to this.

| | **System A — inference engine** | **System B — curator grid vote** |
|---|---|---|
| Question it answers | "Do other tags on this paper agree with this tag?" | "Did a curator tick Yes/No on this topic?" |
| Origin | SCRUM-6183, SCRUM-6188 | SCRUM-6242 |
| Storage | `topic_entity_tag_validation` join table + two cached string columns | Ordinary topic-level TETs, counted at read time — nothing persisted |
| Computed | On create / patch / delete / merge, and by the bulk resweep | Fresh on every batch read |
| `validation_type` it keys on | `author`, `professional_biocurator` | `professional_curator` **and** `professional_biocurator` |
| Surfaced as | `validation_by_author`, `validation_by_professional_biocurator` | `validation` + `filter_flags` blocks in the batch response |
| Indexed in Elasticsearch | `validation_by_professional_biocurator` only (SCRUM-6228) | not at all |

They share no code and no rules. The `/validate` write path is effectively invisible to
System A — see Risk 1.

---

## System A — the inference engine

### Data shape

All in `agr_literature_service/api/models/topic_entity_tag_model.py`.

- **`topic_entity_tag_validation`** (`:19-38`) — self-referential join table.
  `validated_topic_entity_tag_id` is the tag *being* validated;
  `validating_topic_entity_tag_id` is the tag *doing* the validating. No payload columns.
  Both FKs are `ondelete='CASCADE'` and indexed. There is a composite PK **and** a
  redundant `UniqueConstraint` named `validation_unique` — redundant as a constraint, but
  the CRUD depends on its *name* as an `ON CONFLICT` target.
- **`validated_by`** (`:151-158`) — the relationship. There is **no inverse** (`validates`
  does not exist), which is why edge insertion drops to raw SQL.
- **`validation_by_author`** (`:160`) and **`validation_by_professional_biocurator`**
  (`:166`) — nullable `String`, no enum, no check constraint.
- **`__versioned__ = {'exclude': ['validated_by']}`** (`:43-45`) — the edge graph has no
  audit history. The two string columns *are* versioned, so every revalidation writes a
  `topic_entity_tag_version` row.
- **`TopicEntityTagSourceModel.validation_type`** (`:246`) — nullable free-text string, and
  the pivot of the whole system. A tag can validate others only if its source has a
  non-null value here. ML/automated sources are `None`: they can *be* validated but never
  validate.

### Scope boundary

`validate_tags` (`topic_entity_tag_crud.py:743-758`) restricts candidates to
**`reference_id` + `secondary_data_provider_id`**. Validation never crosses a paper
boundary and never crosses a MOD boundary. Tags with `negated IS NULL` are excluded
entirely, both as validators and as validatees.

### The five states

`calculate_validation_value_for_tag` (`:396-422`) walks the **transitive closure** of
`validated_by` (BFS with a visited set, unbounded depth), following only edges whose
validating tag's source matches the requested `validation_type`.

| Condition | Result |
|---|---|
| At least one same-type validator, all `negated` values agree with the tag | `validated_right` |
| At least one same-type validator, all agree with each other but differ from the tag | `validated_wrong` |
| Same-type validators disagree amongst themselves | `validation_conflict` |
| No same-type validator, but the tag's own source has that `validation_type` | `validated_right_self` |
| Otherwise | `not_validated` |

`set_validation_values_to_tag` (`:810-814`) writes both columns, one per actor.

```python
ATP_ID_SOURCE_AUTHOR = "author"                    # :69
ATP_ID_SOURCE_CURATOR = "professional_biocurator"  # :70
```

Despite the names, these are **not** ATP ids — they are `validation_type` strings.

### The rules: generic vs specific, in four dimensions

- A new **positive** tag validates existing tags that are **more generic** (`:596`).
- A new **negative** tag validates existing tags that are **more specific** (`:628`).
- Inbound direction, existing tags validating the new one (`:660`); the matrix is
  documented in-line at `:662-665`.
- Special case: a mixed topic+entity tag also validates the pure **entity-only** tag for
  the same entity (`:617`, `:648`, `:694`, `:703`).

"More generic / more specific" is evaluated **simultaneously across three ATP
hierarchies** — `topic`, `entity_type` (via `atp_hierarchy_with_self`, `:580`, SCRUM-6188,
which includes self so exact equality still matches), and `data_novelty`. All three must
agree on direction. Cross-branch novelty — "existing data" `ATP:0000334` versus "novel
data" `ATP:0000321` — blocks validation outright.

**`data_context` is deliberately NOT a dimension.** SCRUM-5697 added a fifth ATP field to
every tag — one of four *disjoint* terms: experimentally studied data `ATP:0000325`,
background information `ATP:0000360`, expression marker `ATP:0000328`, genetic marker
`ATP:0000327`. It is stored, indexed, exported to Elasticsearch and editable, but
`validate_tags` ignores it entirely. That is not an oversight: the three ATP dimensions
above work by walking ancestors/descendants, and disjoint terms have no
generic/specific relationship to walk, so "all dimensions agree on direction" has no
meaning for it. Deciding what validation *should* do when two tags agree on topic, entity
and novelty but disagree on data context (does a background-information tag validate an
experimentally-studied one? neither? conflict?) is SCRUM-5746's job. Until that lands,
tags validate each other across data contexts freely.

**Species is the fourth dimension**, and it follows the same generic/specific logic
without an ontology behind it (`:612`, `:644`, `:684`, `:691`). Read a **null species as
"more generic"** — the assertion applies regardless of organism — and a **specific species
as "more specific"**. The apparent asymmetry between the branches is deliberate and
semantically correct, not a bug:

| New tag | Check | Effect |
|---|---|---|
| Positive | `tag_in_db.species is None or tag_in_db.species == new.species` (`:612`) | A species-specific positive validates a null-species tag: finding *C. elegans* data confirms the broader claim |
| Negative | `new_tag_obj.species is None or tag_in_db.species == new.species` (`:644`) | A species-specific negative does **not** validate a null-species tag: "no *C. elegans* disease data" does not contradict "this paper has disease data for some organism" |

That asymmetry is correct only when a classifier really is species-agnostic. When a model
was *trained* species-specifically but still emits `species = null`, curator negatives
scoped to a species silently fail to invalidate it while their positives validate it — so
the model accrues positive validations and never negative ones. See the Confluence page
linked at the end of this document, which treats this at length and proposes recording
each model's training definition on `ml_model.species`.

Edge insertion is `INSERT ... ON CONFLICT DO NOTHING` (`add_validation_to_db`, `:714-726`)
because the overlapping rules legitimately re-assert the same pair within a single pass.

### When it recomputes

| Trigger | Path | Notes |
|---|---|---|
| `POST /topic_entity_tag/` | `create_tag` → `validate_tags` (`:196`) | Full recompute |
| SCRUM-6183 companion entity tag | `create_entity_tag_for_mixed_tag:276` | Failures swallowed (`:278-287`) |
| `PATCH /topic_entity_tag/{id}` | `revalidate_all_tags(curie_or_reference_id=...)` (`:543`) | Synchronous, whole reference |
| `DELETE /topic_entity_tag/{id}` | Same (`:577`) | Synchronous, whole reference |
| `POST /topic_entity_tag/validate` | `create_tag(validate_on_insert=False)` then resweep (`:2067`) | |
| Reference create / update / merge | `reference_crud.py:454-459`, `:1103-1108` | One resweep after the loop |
| SuperAdmin bulk sweep | `topic_entity_tag_router.py:344-397` | Forked `Process`, emails on completion |
| **MOD bulk loaders (ZFIN, SGD, PDB)** | `validate_on_insert=False` | **Never resweep — tags stay unvalidated** |
| Cleanup one-offs | Per-reference resweep inside a loop | |

There is **no scheduled or cron revalidation**. The only background path is the manual
SuperAdmin endpoint, whose single-flight guard is a per-process `multiprocessing.Value`
(`topic_entity_tag_router.py:29`) — with N gunicorn workers, N concurrent full sweeps are
possible.

---

## System B — the curator grid vote

`POST /topic_entity_tag/validate` (`topic_entity_tag_router.py:62-81`) →
`validate_topic` (`topic_entity_tag_crud.py:1999-2072`).

The endpoint writes a **topic-level (no entity)** tag from the per-MOD ABC curator source:

```python
CURATOR_VALIDATION_SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"        # :85
CURATOR_VALIDATION_SOURCE_METHOD = "abc_literature_system"          # :86
CURATOR_VALIDATION_TYPE = "professional_curator"                    # :87
CURATOR_VALIDATION_DATA_NOVELTY = "ATP:0000335"                     # :88
```

**Replace-prior semantics:** a curator holds at most one validation per (reference, topic).
The prior one is deleted and *flushed, not committed* (`:2044-2052`), so a Yes→No flip never
trips the `opposite_negation` 409, and a failed insert rolls the delete back atomically.
Then one `revalidate_all_tags` for the reference, `db.expire_all()`, and the single
recomputed cell is returned in the same shape as the batch response, so the UI can splice
one cell without refetching.

### Read side

Both blocks are assembled in `show_all_reference_tags_for_references` (`:1767-1921`).

`_build_validation_details` (`:1594`) produces
`reference → TOPIC_CURIE (uppercased) → {state, positives, negatives, by_curator[]}`, where
`state` is `positive`, `negative`, or `conflict`. Only **entity-less** tags from a curator
source count — `_is_curator_source_tag` (`:1466-1468`) accepts *both* curator strings.
Topics with no curator validation are omitted entirely; the client reads absent as
"unvalidated".

`_build_filter_flags` (`:1670`) produces
`{has_any, has_y, has_n, has_note, my_validation_present}` over **all** tags in the cell,
curator or not. `my_validation_present` must be computed from the raw ORM rows, where
`created_by` is still the internal user id, compared against `get_default_user_value()` —
the serialized tags have already had `created_by` replaced with a display name.

The batch read caps at `MAX_BATCH_REFERENCES = 100` (`topic_entity_tag_router.py:207`).

---

## Test coverage

`tests/api/test_topic_entity_tag.py` is the de facto specification, in four clusters:

- **Core states** — `:823`, `:1101`, `:1153`, `:1307`, `:1404`
- **Hierarchy matrix** — nine tests from `:1245` to `:2302`, covering entity_type hierarchy
  (SCRUM-6188), data_novelty branch separation, cross-branch incompatibility, and the
  requirement that topic and novelty move in the *same* generic→specific direction
- **Revalidation on delete** — `:2367`, and the conflict case at `:2509`
- **Grid `/validate`** — `:338`, `:417`, `:472`, `:540` (the biocurator-source flip
  regression), `:602` (atomicity)

One invariant worth protecting:
`test_validation_does_not_change_updated_by_and_date_updated` (`:2957`) — being validated
must not touch the validated tag's audit fields.

The ATP hierarchy is mocked by `load_name_to_atp_and_relationships_mock()`
(`tests/fixtures.py:164`), so tests make no A-team ontology calls.

**Known gaps:** `tests/api/test_topic_entity_tag_source.py` asserts nothing about
`validation_type` — not its allowed values, not that changing it re-derives anything.
`test_data_novelty_branch_separation` (`:1693`) is an assertion-free stub.

---

## Risk register

Ranked by likelihood of causing a surprise.

1. **`professional_curator` vs `professional_biocurator`.** (SCRUM-6476) Grid votes use the former;
   `calculate_validation_value_for_tag` only matches the latter. Those tags *do* create
   edges in the join table (the gate is merely "`validation_type` is not null"), but the
   edges are filtered out of both buckets — so the tag reads `not_validated` and
   contributes nothing to anyone else's rollup, and is invisible to the SCRUM-6228 search
   facet. Because the source unique key **excludes** `validation_type`
   (`topic_entity_tag_model.py:259-263`), which string a given MOD ends up with depends on
   which row happened to be created first.

   **Verified in production 2026-08-28: all seven MOD sources currently carry
   `professional_biocurator`, so this is latent, not live.** Grid votes do count today.
   But `CURATOR_VALIDATION_TYPE` is what the code writes on *creation*, so the first
   genuinely new source row — a new MOD onboarding, or any change to
   `source_evidence_assertion`, `source_method` or `data_provider` — would silently get the
   string that is not counted, with no error and no visible symptom. Since the seven rows
   are already uniform, changing `CURATOR_VALIDATION_TYPE` to `professional_biocurator`
   closes it with no migration and no data change.
2. **`validation_type` is unconstrained free text**
   (`topic_entity_tag_schemas.py:32,52`). Observed values: `author`,
   `professional_biocurator`, `professional_curator`, `manual_validation`
   (`tests/populate_test_db.py:355` — matches nothing, silently inert), and `None`.
   Separately, `ATP:0000035` / `ATP:0000036` (author / professional biocurator assertion)
   live in `source_evidence_assertion` and drive *deletion* filtering
   (`topic_entity_tag_utils.py:339-341`) — completely disconnected from `validation_type`.
3. **Bulk loaders skip validation and never resweep.** Every MOD loader passes
   `validate_on_insert=False`, and nothing triggers a sweep afterwards. The implicit
   contract is a manual sweep that nobody schedules.
4. **`PATCH /topic_entity_tag/source/{id}` can change `validation_type`** — altering every
   rollup that depends on that source — without triggering any revalidation.
5. **The rollup columns are writable through the API**
   (`topic_entity_tag_schemas.py:81-82, 152-153`) despite being server-computed. A PATCH is
   silently overwritten by the ensuing resweep.
6. **No audit trail on validation edges.** "Who validated this, and when" is not answerable
   historically.
7. **The three rule functions are near-duplicates** (`:596`, `:628`, `:660`), each
   re-implementing the same four-dimension matching, plus three near-identical entity-only
   loops. Any rule change must be made consistently in three to six places. Note the
   differing null-tolerance between branches (`:612` versus `:644`) is *intentional* — see
   the species rule above — so this is a maintainability risk, not a correctness one.
8. **`validation_by_author` is a second-class citizen** — computed and stored, but no ES
   index, no facet, no UI surface.
9. **MOD-specific logic in a generic path.** The `opposite_negation` 409 fires only for
   `source_method == "abc_literature_system" AND validation_type == "professional_biocurator"`
   (`:1009`).
10. **Model/DB divergence on `negated`.** The model says `nullable=True`, migration
    `20230811_fa6db70cdfe7` created it `NOT NULL`, and the CRUD relies on three-state logic.

---

## Known defects

All tracked in Jira: SCRUM-6470 through SCRUM-6475.

- **The audit-skip flag is never cleared.** (SCRUM-6470) `set_validation_values_to_tag` (`:811-812`)
  sets `_skip_audit_auto_update` permanently on the instance. The paired
  `enable_set_updated_by_onupdate` / `enable_date_updated_onupdate`
  (`audited_model.py:53,71`) are never called anywhere, so any later mutation of that same
  ORM instance within the same session silently skips `updated_by` / `date_updated`
  stamping.
- **`reference_id` is assigned a `Query` object, not a scalar** (SCRUM-6471) (`:836`), then compared
  against at `:838`, relying on SQLAlchemy's deprecated implicit Query→scalar-subquery
  coercion.
- **`destroy_tag`'s `delete_all_first=False` is dead.** (SCRUM-6472) Line `:833` unconditionally forces
  it to `True` whenever `curie_or_reference_id` is set, making the cheaper per-tag delete
  branch (`:857-859`) unreachable.
- **Companion entity tags can persist permanently unvalidated** (SCRUM-6473) — `:282-285` logs and
  returns on validation failure, leaving `validation_by_*` NULL until someone resweeps.

## Known performance issues

- **Commit-per-edge.** (SCRUM-6474) `add_validation_to_db` (`:730-735`) does an INSERT, a **COMMIT** and
  a SELECT per edge, then two rollup computations that each lazy-load `validated_by` and
  every element's source. One POST on a busy reference can issue hundreds of round trips
  and commits.
- **N+1** (SCRUM-6474) in `calculate_validation_value_for_tag` (`:404-405`) — the object re-fetched at
  `:733` carries no eager options. Only the conflict fan-out (`:797-800`) and the bulk
  sweep (`:825-827`) use `joinedload`.
- **Conflict fan-out** (`:791-806`) recomputes every tag on the reference+MOD whenever a
  new tag lands in `validation_conflict`.
- **A new engine per `revalidate_all_tags` call** (SCRUM-6475) (`:819`), never disposed — and this runs
  synchronously inside `patch_tag`, `destroy_tag`, `validate_topic`, and the per-reference
  loops of both cleanup scripts.
- **Full-reference rebuild for a single-tag edit.** Patch, delete and validate all delete
  and re-derive every edge on the reference, then run a second full pass recomputing values.
- **Sweep cache thrash** (SCRUM-6475) (`:828-830`, `:852-855`) — ordering by
  `reference_id, topic_entity_tag_source_id, secondary_data_provider_id` means tags of the
  same MOD are not guaranteed contiguous. Swapping the last two keys would fix it.
- **No locking anywhere.** (SCRUM-6475) No `SELECT ... FOR UPDATE`, no advisory locks. Two concurrent
  writes to the same reference can interleave one's `DELETE FROM
  topic_entity_tag_validation` with the other's rebuild, leaving edges missing until the
  next sweep.

---

## File index

| File | Why it matters |
|---|---|
| `agr_literature_service/api/crud/topic_entity_tag_crud.py` | ~2100 lines; both systems live here |
| `agr_literature_service/api/models/topic_entity_tag_model.py` | Join table, rollups, `validation_type` |
| `agr_literature_service/api/routers/topic_entity_tag_router.py` | 18 endpoints, including `/validate` and `/revalidate_all_tags/` |
| `agr_literature_service/api/schemas/topic_entity_tag_schemas.py` | What is writable versus computed |
| `agr_literature_service/api/crud/search_crud.py` | The one Elasticsearch facet (`:1369-1376`) |
| `agr_literature_service/api/models/audited_model.py` | The `_skip_audit_auto_update` flag |
| `tests/api/test_topic_entity_tag.py` | The de facto specification |

### Key symbols

| Symbol | Location |
|---|---|
| `calculate_validation_value_for_tag` | `topic_entity_tag_crud.py:396` |
| `atp_hierarchy_with_self` | `topic_entity_tag_crud.py:580` |
| `validate_tags_already_in_db_with_positive_tag` | `topic_entity_tag_crud.py:596` |
| `validate_tags_already_in_db_with_negative_tag` | `topic_entity_tag_crud.py:628` |
| `validate_new_tag_with_existing_tags` | `topic_entity_tag_crud.py:660` |
| `add_validation_to_db` | `topic_entity_tag_crud.py:714` |
| `validate_tags` | `topic_entity_tag_crud.py:739` |
| `set_validation_values_to_tag` | `topic_entity_tag_crud.py:810` |
| `revalidate_all_tags` | `topic_entity_tag_crud.py:817` |
| `_is_curator_source_tag` | `topic_entity_tag_crud.py:1466` |
| `_build_validation_details` | `topic_entity_tag_crud.py:1594` |
| `_build_filter_flags` | `topic_entity_tag_crud.py:1670` |
| `get_or_create_curator_validation_source` | `topic_entity_tag_crud.py:1923` |
| `_recompute_validation_cell` | `topic_entity_tag_crud.py:1979` |
| `validate_topic` | `topic_entity_tag_crud.py:1999` |

---

## See also

**Confluence — [Topic classification, automatic cross-validation, and curator validation:
what curators need to know](https://agr-jira.atlassian.net/wiki/spaces/BLUE/pages/1360494593/Topic+classification+automatic+cross-validation+and+curator+validation+what+curators+need+to+know)**

That page is the curator-facing companion to this one and should be read alongside it. It
covers ground this document deliberately does not:

- the ATP vocabulary's six top-level branches, and what a TET's columns mean to a curator
- `confidence_score` driving `negated` at the 0.5 boundary, and the
  `NEG` / `LOW` / `MEDIUM` / `HIGH` binning of `confidence_level`
- the DB-to-UI remapping of the five rollup values (the author column shows
  "agree"/"disagree"; `validated_right_self` renders as a blank cell)
- worked examples of the topic and species rules, including one negative parent tag
  invalidating several positive children in a single click
- **the species / *C. elegans* problem** — classifiers emit `species = null` regardless of
  their training definition, and the proposal to record it on `ml_model.species`
- that the detailed per-paper TET table's tick/cross is a **third** write path, distinct
  from the topic grid's: it mirrors the exact scope of the row being validated
- that author tags are kept out of the biocurator column deliberately, as "indications"
  rather than validations

One caveat when reading it: its section 5.2 classifier table still has blank cells
awaiting curator input.

Its section 3 claim — that a grid click recomputes the `validated_right` /
`validated_wrong` summary — was checked against production on 2026-08-28 and **holds**.
All seven MOD sources carry `validation_type = 'professional_biocurator'`, so
`get_or_create_curator_validation_source` finds an existing row every time and inherits
that value. Grid votes do reach `validation_by_professional_biocurator` today, and the
species proposal in its sections 4 and 5 stands unamended. The latent mismatch is tracked
as SCRUM-6476 — see risk 1.
