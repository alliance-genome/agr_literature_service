# journal_license_ingest loaders

Two loaders populate journal image-display permissions
(`image_permission` + `resource_image_permission`); a third
(`load_doaj_licenses.py`, monthly cron) maintains OA copyright licenses and is
not covered here.

Both permission loaders share the same safety model:

- **Dry-run by default.** Without `--apply` nothing is committed; the run
  prints exactly what `--apply` would create/update, so review the dry-run
  output first, then rerun with `--apply`.
- **Non-destructive.** Rows present in the input are inserted or updated;
  database rows absent from the input are never deleted.
- **Idempotent.** Rerunning after `--apply` reports everything as unchanged.
- **Cross-loader conflict guard.** Each loader refuses to touch a
  `(resource, year-range)` slot whose grant was created by the other loader,
  and reports it instead (SCRUM-6416). Those reports need a human decision
  about which grant owns the slot.

Run them from this directory (paths default to `data/` relative to the cwd),
inside the container/host that has DB env vars set. `ENV_STATE` must match the
database you intend to touch.

---

## load_alliance_copyright_permissions.py (SCRUM-6416)

Loads the image working group's **publisher copyright sheet** ("Copyright
licence statements"): per-journal grants with attribution text, permission
type, and optional publication-year ranges.

```bash
cd agr_literature_service/lit_processing/data_ingest/journal_license_ingest

# 1. dry run: prints every create/update plus a summary line and problems
python load_alliance_copyright_permissions.py

# 2. review, then commit
python load_alliance_copyright_permissions.py --apply
```

| Option | Default | Meaning |
|---|---|---|
| `--input-file` | `data/alliance_copyright_permissions.tsv` | curated seed TSV (tracked in the repo) |
| `--apply` | off | commit changes; otherwise dry run |

Input: the tracked seed file, one row per (journal, grant). Columns include
`permission_name` (unique grant identity), `attribution_text`
(becomes `image_permission.permission_text`), `can_display_images` (yes/no),
`start_year`/`end_year` (blank = open-ended), `link_notes`, and
`skip`/`skip_reason`.

Semantics to know:

- **skip=yes rows** are reported and never loaded: currently Cold Spring
  Harbor Laboratory Press and The Company of Biologists (statements not yet
  agreed by the working group), eNeuro (no statement in the sheet), and Folia
  Biologica (journal identity unresolved: the ABC resource also carries an old
  Charles Univ Prague grant, i.e. Folia Biologica (Praha), a different
  journal). When resolved, blank the `skip` cell and rerun.
- **Resources are matched by journal title** against resource
  title/abbreviation (exact, then normalized; unique match or unique match
  among resources with references). Unmatched/ambiguous journals are reported
  as problems and not loaded.
- **Publisher synonyms are a sanity check only**: a matched resource whose
  `publisher` is not in the row's synonym list is loaded with a warning (the
  grant is per journal).
- **One journal may carry several grants**: distinguished by year range and,
  for the SfN 2026- pair, by permission (OA CC-BY vs non-OA exclusive), so
  links are keyed on (resource, permission, range).
- Summary line: `rows / skipped / unmatched / publisher_mismatches /
  link_conflicts / permissions +created/~updated/=unchanged / links ...`.
  Dry-run and `--apply` counts match by design.

---

## load_journal_image_permissions.py (SCRUM-6420)

Loads the older **per-journal curator sheet export** (`journal_permission.tsv`,
NLM-abbreviation keyed, with per-MOD permission columns). Derives each
journal's permission name/text and `can_display_images` from the sheet's
License type, MOD columns and comments.

```bash
cd agr_literature_service/lit_processing/data_ingest/journal_license_ingest

# 1. dry run
python load_journal_image_permissions.py

# 2. review data/journal_permission_load_report.tsv for failed rows, then
python load_journal_image_permissions.py --apply
```

| Option | Default | Meaning |
|---|---|---|
| `--input-file` | `data/journal_permission.tsv` | curator sheet export (NOT tracked; place it in `data/`) |
| `--env-file` | none | optional env file to load before connecting |
| `--resource-map` | none | TSV with `Journal (NLM abbrev)` + `resource_curie` columns for manual matches the title lookup cannot resolve |
| `--report-file` | `data/journal_permission_load_report.tsv` | failed/unloadable rows (unmatched, ambiguous, link conflicts, errors) |
| `--apply` | off | commit changes; otherwise dry run |
| `--subset-can-display` | off | treat "granted for a subset" style values as display-allowed |

Semantics to know:

- **Hybrid journals**: "Hybrid Creative Commons" alone does NOT grant display;
  an explicit blanket/contract/granted signal is required (the Chromosome
  Research bug, SCRUM-6420).
- **Links are matched on (resource, year-range) only**, and the loader follows
  its own permission renames (e.g. a curator edits License type) by
  recognizing every name shape it has ever minted. A slot carrying any other
  grant, e.g. an alliance copyright permission, is reported as a `link
  conflict` in the report file and left untouched.
- Unmatched journals land in the report file; resolve them with
  `--resource-map` entries and rerun.

---

## Resolving conflicts

A `link conflict` means a `(resource, year-range)` slot already carries a
grant the running loader does not own. For the known set of grants the
working-group sheet supersedes (Genetics, G3, Biochem J and the J Neurosci
ranges from the old sheet, in two waves: exact-slot collisions and
overlapping-range grants), the resolution is scripted:

```bash
python ../../oneoff_scripts/delete_superseded_image_permission_links.py          # dry run
python ../../oneoff_scripts/delete_superseded_image_permission_links.py --apply
```

Run it BEFORE `load_alliance_copyright_permissions.py --apply` on each
database, or the alliance loader will report those journals as conflicts and
skip them. The script is rerun-safe (already-removed pairs are reported and
skipped) and refuses pairs that match more links than it expects. Any conflict
NOT covered by the script is a new ownership question: take it to the
curators; do not stack a second grant on the slot.

## Rollout order

Run against dev (`literature-4006`) first, then stage, then prod, dry-run
before `--apply` at each step, with the superseded-links cleanup (above)
applied before the alliance loader on each database. If both loaders need
running, apply one, review the other's dry-run for link conflicts, and resolve
ownership before its `--apply`.
