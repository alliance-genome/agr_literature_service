from __future__ import annotations

import csv
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


OUT_COLS = [
    "resource_id",
    "resource_curie",
    "title",
    "print_issn",
    "online_issn",
    "iso_abbreviation",
    "medline_abbreviation",
    "publisher",
    "reference_count",
    "cross_reference_curies",
    "duplicate_type",
]


def clean_cell(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = s.replace("\t", " ")
    return s.strip()


def write_grouped_tsv(path: str, rows_with_group: List[Tuple[int, Dict[str, Any]]]) -> None:
    """
    rows_with_group is a list of (dup_group_id, row_dict) ordered by dup_group_id, resource_id.
    Writes a blank row between groups for easy visual grouping in spreadsheet tools.
    """
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, delimiter="\t", extrasaction="ignore")
        w.writeheader()

        prev_group: Optional[int] = None
        blank = {k: "" for k in OUT_COLS}

        for dup_group_id, row in rows_with_group:
            if prev_group is not None and dup_group_id != prev_group:
                w.writerow(blank)  # separator row

            out: Dict[str, Any] = {}
            for k in OUT_COLS:
                v = row.get(k)
                if k in ("resource_id", "reference_count"):
                    out[k] = v if v is not None else ""
                else:
                    out[k] = clean_cell(v)
            w.writerow(out)
            prev_group = dup_group_id


def main() -> None:
    db = create_postgres_session(False)
    try:
        q = text("""
            WITH RECURSIVE
            resource_n AS (
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
            ),

            edges AS (
                -- print_issn edges
                SELECT unnest(array_remove(ids, min_id)) AS a, min_id AS b
                FROM (
                    SELECT
                        print_issn_n,
                        array_agg(resource_id ORDER BY resource_id) AS ids,
                        MIN(resource_id) AS min_id
                    FROM resource_n
                    WHERE print_issn_n IS NOT NULL
                    GROUP BY print_issn_n
                    HAVING COUNT(*) > 1
                ) s

                UNION ALL

                -- online_issn edges
                SELECT unnest(array_remove(ids, min_id)) AS a, min_id AS b
                FROM (
                    SELECT
                        online_issn_n,
                        array_agg(resource_id ORDER BY resource_id) AS ids,
                        MIN(resource_id) AS min_id
                    FROM resource_n
                    WHERE online_issn_n IS NOT NULL
                    GROUP BY online_issn_n
                    HAVING COUNT(*) > 1
                ) s

                UNION ALL

                -- title-key edges (only when both ISSNs missing)
                SELECT unnest(array_remove(ids, min_id)) AS a, min_id AS b
                FROM (
                    SELECT
                        title,
                        iso_abbreviation,
                        medline_abbreviation,
                        publisher,
                        array_agg(resource_id ORDER BY resource_id) AS ids,
                        MIN(resource_id) AS min_id
                    FROM resource_n
                    WHERE print_issn_n IS NULL
                      AND online_issn_n IS NULL
                      AND title IS NOT NULL AND title <> ''
                    GROUP BY title, iso_abbreviation, medline_abbreviation, publisher
                    HAVING COUNT(*) > 1
                ) s
            ),

            undirected_edges AS (
                SELECT a AS src, b AS dst FROM edges
                UNION ALL
                SELECT b AS src, a AS dst FROM edges
            ),

            nodes AS (
                SELECT src AS node FROM undirected_edges
                UNION
                SELECT dst AS node FROM undirected_edges
            ),

            reach AS (
                SELECT node AS start, node AS node
                FROM nodes
                UNION
                SELECT r.start, e.dst
                FROM reach r
                JOIN undirected_edges e ON e.src = r.node
            ),

            component_min AS (
                SELECT start, MIN(node) AS dup_group_id
                FROM reach
                GROUP BY start
            ),

            dup_map AS (
                SELECT start AS resource_id, dup_group_id
                FROM component_min
            ),

            per_resource_flag AS (
                SELECT
                    dm.dup_group_id,
                    dm.resource_id,
                    BOOL_OR(xr.is_obsolete = false AND xr.curie LIKE 'ZFIN:%') AS has_zfin,
                    BOOL_OR(xr.is_obsolete = false AND xr.curie LIKE 'FB:%')   AS has_fb
                FROM dup_map dm
                LEFT JOIN cross_reference xr ON xr.resource_id = dm.resource_id
                GROUP BY dm.dup_group_id, dm.resource_id
            ),

            group_require_all AS (
                SELECT
                    dup_group_id,
                    COUNT(*) AS group_size,
                    SUM(CASE WHEN has_zfin THEN 1 ELSE 0 END) AS zfin_members,
                    SUM(CASE WHEN has_fb THEN 1 ELSE 0 END) AS fb_members
                FROM per_resource_flag
                GROUP BY dup_group_id
            ),

            zfin_groups AS (
                SELECT dup_group_id
                FROM group_require_all
                WHERE zfin_members = group_size
            ),

            fb_groups AS (
                SELECT dup_group_id
                FROM group_require_all
                WHERE fb_members = group_size
            ),

            base_rows AS (
                SELECT
                    dm.dup_group_id,
                    rn.resource_id,
                    rn.curie AS resource_curie,
                    rn.title,
                    rn.print_issn_n AS print_issn,
                    rn.online_issn_n AS online_issn,
                    rn.iso_abbreviation,
                    rn.medline_abbreviation,
                    rn.publisher,

                    COUNT(DISTINCT ref.reference_id) AS reference_count,

                    ARRAY_AGG(DISTINCT xr.curie)
                        FILTER (WHERE xr.is_obsolete = false AND xr.curie IS NOT NULL)
                        AS cross_reference_curies,

                    CASE
                        WHEN rn.print_issn_n IS NOT NULL OR rn.online_issn_n IS NOT NULL THEN 'ISSN'
                        ELSE 'TITLE'
                    END AS duplicate_type

                FROM dup_map dm
                JOIN resource_n rn ON rn.resource_id = dm.resource_id
                LEFT JOIN reference ref ON ref.resource_id = rn.resource_id
                LEFT JOIN cross_reference xr ON xr.resource_id = rn.resource_id

                GROUP BY
                    dm.dup_group_id,
                    rn.resource_id, rn.curie, rn.title, rn.print_issn_n, rn.online_issn_n,
                    rn.iso_abbreviation, rn.medline_abbreviation, rn.publisher
            )

            SELECT 'ZFIN' AS which, b.*
            FROM base_rows b
            JOIN zfin_groups zg ON zg.dup_group_id = b.dup_group_id

            UNION ALL

            SELECT 'FB' AS which, b.*
            FROM base_rows b
            JOIN fb_groups fg ON fg.dup_group_id = b.dup_group_id

            ORDER BY which, dup_group_id, resource_id
        """)

        rows = db.execute(q).fetchall()

        zfin_rows: List[Tuple[int, Dict[str, Any]]] = []
        fb_rows: List[Tuple[int, Dict[str, Any]]] = []

        for r in rows:
            (
                which,
                dup_group_id,
                resource_id,
                resource_curie,
                title,
                print_issn,
                online_issn,
                iso_abbreviation,
                medline_abbreviation,
                publisher,
                reference_count,
                cross_reference_curies,
                duplicate_type,
            ) = r

            curies_list = list(cross_reference_curies) if cross_reference_curies else []

            rec = {
                "resource_id": int(resource_id),
                "resource_curie": resource_curie,
                "title": title,
                "print_issn": print_issn,
                "online_issn": online_issn,
                "iso_abbreviation": iso_abbreviation,
                "medline_abbreviation": medline_abbreviation,
                "publisher": publisher,
                "reference_count": int(reference_count or 0),
                "cross_reference_curies": "|".join(clean_cell(x) for x in curies_list),
                "duplicate_type": duplicate_type,
            }

            if which == "ZFIN":
                zfin_rows.append((int(dup_group_id), rec))
            else:
                fb_rows.append((int(dup_group_id), rec))

        write_grouped_tsv("duplicate_groups_ALL_ZFIN.tsv", zfin_rows)
        write_grouped_tsv("duplicate_groups_ALL_FB.tsv", fb_rows)

        print(f"Wrote {len(zfin_rows)} rows to duplicate_groups_ALL_ZFIN.tsv")
        print(f"Wrote {len(fb_rows)} rows to duplicate_groups_ALL_FB.tsv")

    finally:
        db.close()


if __name__ == "__main__":
    main()
