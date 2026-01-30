"""
Script to identify duplicate resources and:
1) write diagnostic outputs
2) optionally delete "empty" duplicates (no refs, no non-obsolete xref curies)
3) optionally move references from duplicates with refs but no non-obsolete xref curies to the canonical resource

Duplicate definition (recommended):
- ISSN-based:
    - same normalized print_issn (NULLIF(print_issn,''))
    OR
    - same normalized online_issn (NULLIF(online_issn,''))
- Title-based (only when BOTH ISSNs missing):
    - same title AND same (iso_abbreviation, medline_abbreviation, publisher)
      using IS NOT DISTINCT FROM to treat NULLs as equal

Canonical pick (per duplicate connected component):
1) highest reference_count
2) highest non_obsolete_xref_curie_count
3) smallest resource_id

Actions for non-canonical resources:
- If reference_count == 0 AND non_obsolete_xref_curie_count == 0:
    Delete it (safety: only if non_obsolete_xref_row_count == 0)
- Else if reference_count > 0 AND non_obsolete_xref_curie_count == 0:
    Move its references to canonical (UPDATE reference)
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


# ----------------------------
# Union-Find to unify duplicate groups across rules (print/online/title)
# ----------------------------
class UnionFind:
    def __init__(self):
        self.parent: Dict[int, int] = {}
        self.rank: Dict[int, int] = {}

    def find(self, x: int) -> int:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


@dataclass(frozen=True)
class ResourceAgg:
    resource_id: int
    curie: str | None
    title: str | None
    print_issn: str | None
    online_issn: str | None
    iso_abbreviation: str | None
    medline_abbreviation: str | None
    publisher: str | None
    reference_count: int
    non_obsolete_xref_row_count: int
    non_obsolete_xref_curie_count: int
    non_obsolete_xref_ids: List[int]
    non_obsolete_xref_curies: List[str]


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)


def main() -> None:  # noqa: C901
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Do not modify DB; only report actions")
    parser.add_argument("--out-prefix", default="duplicates", help="Prefix for output files")
    args = parser.parse_args()

    db = create_postgres_session(False)
    try:
        # 1) Build duplicate edges:
        #    - all ids sharing same print_issn
        #    - all ids sharing same online_issn
        #    - all ids sharing title-key when both ISSNs missing
        #
        # We fetch grouped lists and union them in Python (so overlaps unify into a single component).
        dup_key_sql = text("""
            WITH resource_n AS (
                SELECT
                    resource_id,
                    NULLIF(print_issn,'')  AS print_issn_n,
                    NULLIF(online_issn,'') AS online_issn_n,
                    title,
                    iso_abbreviation,
                    medline_abbreviation,
                    publisher
                FROM resource
            ),

            dup_print AS (
                SELECT print_issn_n AS key, array_agg(resource_id ORDER BY resource_id) AS ids
                FROM resource_n
                WHERE print_issn_n IS NOT NULL
                GROUP BY print_issn_n
                HAVING COUNT(*) > 1
            ),

            dup_online AS (
                SELECT online_issn_n AS key, array_agg(resource_id ORDER BY resource_id) AS ids
                FROM resource_n
                WHERE online_issn_n IS NOT NULL
                GROUP BY online_issn_n
                HAVING COUNT(*) > 1
            ),

            dup_title AS (
                SELECT
                    title,
                    iso_abbreviation,
                    medline_abbreviation,
                    publisher,
                    array_agg(resource_id ORDER BY resource_id) AS ids
                FROM resource_n
                WHERE print_issn_n IS NULL
                  AND online_issn_n IS NULL
                  AND title IS NOT NULL AND title <> ''
                GROUP BY title, iso_abbreviation, medline_abbreviation, publisher
                HAVING COUNT(*) > 1
            )

            SELECT 'PRINT' AS dup_type, key::text AS k, ids
            FROM dup_print
            UNION ALL
            SELECT 'ONLINE' AS dup_type, key::text AS k, ids
            FROM dup_online
            UNION ALL
            SELECT 'TITLE' AS dup_type,
                   (title || '||' ||
                    COALESCE(iso_abbreviation,'') || '||' ||
                    COALESCE(medline_abbreviation,'') || '||' ||
                    COALESCE(publisher,'')) AS k,
                   ids
            FROM dup_title
        """)

        dup_groups = db.execute(dup_key_sql).fetchall()

        uf = UnionFind()
        # Keep notes on which rule created group keys (for reporting)
        group_key_meta: List[Tuple[str, str, List[int]]] = []

        for dup_type, k, ids in dup_groups:
            ids_list = list(ids)  # psycopg2 returns list for array
            group_key_meta.append((dup_type, k, ids_list))
            if len(ids_list) < 2:
                continue
            base = ids_list[0]
            for other in ids_list[1:]:
                uf.union(base, other)

        # If no duplicates at all, exit early with empty reports
        all_ids = set()
        for _, _, ids_list in group_key_meta:
            all_ids.update(ids_list)

        if not all_ids:
            _write_file(f"{args.out_prefix}_summary.txt", "No duplicates found.\n")
            print("No duplicates found.")
            return

        # Build components
        comps: Dict[int, List[int]] = defaultdict(list)
        for rid in all_ids:
            comps[uf.find(rid)].append(rid)

        # 2) Fetch aggregated info for all involved resource_ids
        #    - reference_count
        #    - non-obsolete xref row count
        #    - non-obsolete xref curie count
        #    - list of non-obsolete xref ids/curies (for reporting)
        agg_sql = text("""
            WITH resource_n AS (
                SELECT
                    r.resource_id,
                    r.curie,
                    r.title,
                    NULLIF(r.print_issn,'')  AS print_issn_n,
                    NULLIF(r.online_issn,'') AS online_issn_n,
                    r.iso_abbreviation,
                    r.medline_abbreviation,
                    r.publisher
                FROM resource r
                WHERE r.resource_id = ANY(:ids)
            )
            SELECT
                rn.resource_id,
                rn.curie,
                rn.title,
                rn.print_issn_n AS print_issn,
                rn.online_issn_n AS online_issn,
                rn.iso_abbreviation,
                rn.medline_abbreviation,
                rn.publisher,

                COUNT(DISTINCT ref.reference_id) AS reference_count,

                COUNT(DISTINCT xr.cross_reference_id)
                    FILTER (WHERE xr.cross_reference_id IS NOT NULL AND xr.is_obsolete = false)
                    AS non_obsolete_xref_row_count,

                COUNT(DISTINCT xr.cross_reference_id)
                    FILTER (WHERE xr.is_obsolete = false AND xr.curie IS NOT NULL AND xr.curie <> '')
                    AS non_obsolete_xref_curie_count,

                ARRAY_AGG(DISTINCT xr.cross_reference_id)
                    FILTER (WHERE xr.is_obsolete = false AND xr.cross_reference_id IS NOT NULL)
                    AS non_obsolete_xref_ids,

                ARRAY_AGG(DISTINCT xr.curie)
                    FILTER (WHERE xr.is_obsolete = false AND xr.curie IS NOT NULL AND xr.curie <> '')
                    AS non_obsolete_xref_curies

            FROM resource_n rn
            LEFT JOIN reference ref ON ref.resource_id = rn.resource_id
            LEFT JOIN cross_reference xr ON xr.resource_id = rn.resource_id
            GROUP BY
                rn.resource_id, rn.curie, rn.title, rn.print_issn_n, rn.online_issn_n,
                rn.iso_abbreviation, rn.medline_abbreviation, rn.publisher
        """)

        rows = db.execute(agg_sql, {"ids": list(all_ids)}).fetchall()

        by_id: Dict[int, ResourceAgg] = {}
        for r in rows:
            (
                resource_id, curie, title, print_issn, online_issn,
                iso_abbr, medline_abbr, publisher,
                ref_count, xref_row_count, xref_curie_count,
                xref_ids, xref_curies
            ) = r

            by_id[int(resource_id)] = ResourceAgg(
                resource_id=int(resource_id),
                curie=curie,
                title=title,
                print_issn=print_issn,
                online_issn=online_issn,
                iso_abbreviation=iso_abbr,
                medline_abbreviation=medline_abbr,
                publisher=publisher,
                reference_count=int(ref_count or 0),
                non_obsolete_xref_row_count=int(xref_row_count or 0),
                non_obsolete_xref_curie_count=int(xref_curie_count or 0),
                non_obsolete_xref_ids=list(xref_ids) if xref_ids else [],
                non_obsolete_xref_curies=list(xref_curies) if xref_curies else [],
            )

        # 3) Decide canonical per component + compute actions
        def canonical_id(ids_list: List[int]) -> int:
            # Sort by:
            #   1) reference_count desc
            #   2) non_obsolete_xref_curie_count desc
            #   3) resource_id asc
            def key_fn(rid: int) -> Tuple[int, int, int]:
                a = by_id[rid]
                return (-a.reference_count, -a.non_obsolete_xref_curie_count, a.resource_id)

            return sorted(ids_list, key=key_fn)[0]

        actions: List[str] = []
        delete_ids: List[int] = []
        move_ref_map: List[Tuple[int, int]] = []  # (from_id, to_id)

        # report breakdowns
        without_refs: List[int] = []
        with_refs: List[int] = []

        for _root, ids_list in sorted(comps.items(), key=lambda x: min(x[1])):
            ids_list = sorted(ids_list)
            can_id = canonical_id(ids_list)
            # can = by_id[can_id]

            for rid in ids_list:
                a = by_id[rid]
                if a.reference_count == 0:
                    without_refs.append(rid)
                else:
                    with_refs.append(rid)

                if rid == can_id:
                    continue

                # delete rule
                if a.reference_count == 0 and a.non_obsolete_xref_curie_count == 0:
                    # extra safety: only delete if there are no non-obsolete xref rows at all
                    if a.non_obsolete_xref_row_count == 0:
                        delete_ids.append(rid)
                        actions.append(
                            f"DELETE resource_id={rid} -> canonical={can_id} (no refs, no non-obsolete xrefs)"
                        )
                    else:
                        actions.append(
                            f"SKIP DELETE resource_id={rid} (no xref curies, but has non-obsolete xref rows)"
                        )
                    continue

                # move references rule
                if a.reference_count > 0 and a.non_obsolete_xref_curie_count == 0:
                    move_ref_map.append((rid, can_id))
                    actions.append(
                        f"MOVE REFS {rid} -> {can_id} (refs>0, no non-obsolete xref curies)"
                    )
                    continue

                actions.append(
                    f"NO ACTION resource_id={rid} (refs={a.reference_count}, xref_curies={a.non_obsolete_xref_curie_count})"
                )

        # 4) Write diagnostics
        summary_lines = []
        summary_lines.append("=" * 100)
        summary_lines.append("DUPLICATE RESOURCES SUMMARY (Improved ISSN + Title logic)")
        summary_lines.append("=" * 100)
        summary_lines.append(f"Dry run: {args.dry_run}")
        summary_lines.append(f"Total duplicate resources involved: {len(all_ids)}")
        summary_lines.append(f"Number of duplicate groups (connected components): {len(comps)}")
        summary_lines.append(f"Resources with 0 refs: {sum(1 for rid in all_ids if by_id[rid].reference_count == 0)}")
        summary_lines.append(f"Resources with >=1 refs: {sum(1 for rid in all_ids if by_id[rid].reference_count > 0)}")
        summary_lines.append("")
        summary_lines.append(f"Planned deletes: {len(delete_ids)}")
        summary_lines.append(f"Planned reference moves: {len(move_ref_map)}")
        summary_lines.append("")
        summary_lines.append("Actions:")
        summary_lines.extend(f"  - {a}" for a in actions)
        summary_lines.append("")

        _write_file(f"{args.out_prefix}_summary.txt", "\n".join(summary_lines))

        # also write a per-resource report (similar to your originals)
        def fmt_resource(a: ResourceAgg) -> str:
            return (
                f"Resource ID: {a.resource_id}\n"
                f"  CURIE: {a.curie}\n"
                f"  Title: {a.title}\n"
                f"  Print ISSN: {a.print_issn}\n"
                f"  Online ISSN: {a.online_issn}\n"
                f"  ISO Abbreviation: {a.iso_abbreviation}\n"
                f"  Medline Abbreviation: {a.medline_abbreviation}\n"
                f"  Publisher: {a.publisher}\n"
                f"  Reference Count: {a.reference_count}\n"
                f"  Non-obsolete Xref Row Count: {a.non_obsolete_xref_row_count}\n"
                f"  Non-obsolete Xref Curie Count: {a.non_obsolete_xref_curie_count}\n"
                f"  Non-obsolete Xref IDs: {a.non_obsolete_xref_ids}\n"
                f"  Non-obsolete Xref CURIEs: {a.non_obsolete_xref_curies}\n"
            )

        # Group details file
        detail_lines = []
        detail_lines.append("=" * 100)
        detail_lines.append("DUPLICATE GROUP DETAILS")
        detail_lines.append("=" * 100)
        detail_lines.append("")
        for _root, ids_list in sorted(comps.items(), key=lambda x: min(x[1])):
            ids_list = sorted(ids_list)
            can_id = canonical_id(ids_list)
            detail_lines.append("-" * 100)
            detail_lines.append(f"Group canonical resource_id = {can_id}")
            detail_lines.append(f"Members: {ids_list}")
            detail_lines.append("")
            for rid in ids_list:
                detail_lines.append(fmt_resource(by_id[rid]))
            detail_lines.append("")

        _write_file(f"{args.out_prefix}_groups.txt", "\n".join(detail_lines))

        # 5) Apply DB changes (unless dry-run)
        if not args.dry_run:
            # Move references
            move_sql = text("""
                UPDATE reference
                SET resource_id = :to_id
                WHERE resource_id = :from_id
            """)
            for from_id, to_id in move_ref_map:
                db.execute(move_sql, {"from_id": from_id, "to_id": to_id})

            # Delete resources (safe deletes only)
            del_sql = text("DELETE FROM resource WHERE resource_id = :rid")
            for rid in delete_ids:
                db.execute(del_sql, {"rid": rid})

            db.commit()

        print(f"Wrote: {args.out_prefix}_summary.txt")
        print(f"Wrote: {args.out_prefix}_groups.txt")
        if args.dry_run:
            print("Dry run complete (no DB changes).")
        else:
            print(f"Committed: {len(move_ref_map)} reference moves, {len(delete_ids)} resource deletes.")

    except Exception as e:
        # If SQLAlchemy session supports rollback
        print(f"Error: {e}")
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
