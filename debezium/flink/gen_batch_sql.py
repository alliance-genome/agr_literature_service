#!/usr/bin/env python3
"""Generate a BATCH reindex SQL from the streaming references_index.sql.

Each `debezium-json` changelog source becomes an INSERT-only plain-json source
(reading the raw Debezium envelope) plus a primary-key dedup VIEW that projects
`after.*` keeping the latest change per key and dropping deletes. This makes the
source INSERT-only (so Flink BATCH mode accepts it) while staying correct if any
update/delete lands inside the bounded read window. The aggregate views, the
multi-way join and the ES sink are copied through unchanged, so the output stays
byte-identical to the streaming/ksqlDB result.
"""
import re
import sys

src_path, out_path = sys.argv[1], sys.argv[2]
sql = open(src_path).read()

# --- split: everything up to the first aggregate view is the source-table section ---
marker = "-- ============================ AGGREGATE VIEWS"
idx = sql.index(marker)
head, tail = sql[:idx], sql[idx:]   # tail = views + sink + insert, copied verbatim

# --- find each source CREATE TABLE block ---
# CREATE TABLE <name> ( <body> ) WITH ( <props> );
block_re = re.compile(
    r"CREATE TABLE\s+(`?\w+`?)\s*\((.*?)\)\s*WITH\s*\((.*?)\);",
    re.DOTALL,
)

def split_top_level_commas(s):
    """Split on commas that are not inside <...> (for ARRAY<...> etc.)."""
    parts, depth, cur = [], 0, ""
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return parts

out = []
out.append("-- SCRUM-6336: BATCH reindex variant of references_index.sql (AUTO-GENERATED).")
out.append("-- INSERT-only plain-json sources + PK dedup so Flink BATCH mode accepts them;")
out.append("-- each document is written exactly once (no retraction churn -> no stall).")
out.append("-- Aggregate views / multi-way join / ES sink are identical to the streaming file.")
out.append("SET 'execution.runtime-mode' = 'batch';")
out.append("SET 'pipeline.name' = 'references_index_batch';")
out.append("SET 'table.exec.resource.default-parallelism' = '6';")
out.append("")

for m in block_re.finditer(head):
    name, body, props = m.group(1), m.group(2), m.group(3)
    # extract PK
    pk_m = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", body)
    pk = pk_m.group(1).strip() if pk_m else None
    # column defs = body minus the PRIMARY KEY clause
    cols_src = body[: pk_m.start()] if pk_m else body
    col_defs = [c.strip() for c in split_top_level_commas(cols_src) if c.strip()]
    row_fields = ", ".join(col_defs)
    # topic from props
    topic = re.search(r"'topic'\s*=\s*'([^']+)'", props).group(1)
    raw = f"{name.strip('`')}_raw"
    out.append(f"CREATE TABLE {raw} (")
    out.append(f"  `after` ROW<{row_fields}>, `op` STRING, ts_ms BIGINT")
    out.append(") WITH ('connector'='kafka','topic'='%s','properties.bootstrap.servers'='dbz_kafka:9092',"
               % topic)
    out.append(f"  'properties.group.id'='flink-refidx-batch','scan.startup.mode'='earliest-offset',")
    out.append(f"  'scan.bounded.mode'='latest-offset','format'='json','json.ignore-parse-errors'='true');")
    out.append("")
    # dedup view: latest change per PK, drop deletes, expand after.*
    out.append(f"CREATE VIEW {name} AS")
    out.append(f"  SELECT `after`.* FROM (")
    out.append(f"    SELECT `after`, ROW_NUMBER() OVER (PARTITION BY `after`.{pk} ORDER BY ts_ms DESC) AS rn")
    out.append(f"    FROM {raw} WHERE `op` <> 'd' AND `after` IS NOT NULL")
    out.append(f"  ) WHERE rn = 1;")
    out.append("")

out.append(tail)
open(out_path, "w").write("\n".join(out))
n = len(list(block_re.finditer(head)))
print(f"generated {out_path} from {n} source tables")
