# Debezium 3.x + Flink + Elasticsearch 7.10 migration — Design (SCRUM-6336)

**Status:** draft for review · **Branch:** `SCRUM-6336` · **Date:** 2026-07-26
**Supersedes the ksqlDB streaming layer.** Builds on the PG17 + Debezium 2.7 work (SCRUM-6323).

> **Revision note (2026-07-26):** originally scoped as a move to OpenSearch. Corrected after verifying that both search domains run `Elasticsearch_7.10` (AWS control-plane) and that ES-7.x must be preserved for other components. **The search server stays Elasticsearch 7.10**; this milestone changes only the streaming engine (ksqlDB → Flink) and Debezium/Kafka. The ES 7.10 → OpenSearch move is deferred to a separate future effort.

## 1. Goals & non-goals

**Goals**
1. **Fix the ksqlDB memory problem once and for all** — replace 55 individually-unbounded RocksDB state stores with a single engine under a hard memory bound.
2. **Human-readable, maintainable joins with no temporary objects reaching the index** — collapse the current convoluted ~32-stage ksqlDB pairwise-join chain into clean Flink **multi-way joins** + minimal views (a ticket requirement, not a stretch goal); the intermediate join churn stays *inside Flink* (retract semantics), so only the net finished doc per reference is written to Elasticsearch.
3. **Speed up the reindex** — parallel recompute + elimination of the re-emission "drain tail" that today pushes reindex past its 3h Gate-2 cap.
4. **Correct deletions** — row/child deletes propagate to the index natively (the SCRUM-6337 requirement), without the tombstone-config tightrope.
5. **Escape EOL streaming components** — ksqlDB (EOL), ZooKeeper (removed in Kafka 4.x), and the 2019 Jest-based ES sink.

**Non-goals (explicitly out of scope this milestone)**
- **Not** changing the join *output* — the two index documents must stay **byte/field-identical** to today's ksqlDB output (HARD requirement — validated in §9 against golden docs captured pre-teardown). We **do** simplify the join *structure* (Goal 2): the ~32-CTAS pairwise chain becomes clean multi-way joins, as long as the emitted documents are identical.
- **Not** changing the operational model — reindex stays a from-scratch Postgres snapshot; no compacted-topic replay path.
- **Not** replacing the Debezium+Kafka source with Flink CDC, and **not** adopting RisingWave (§12).
- **Not** migrating the search server off **Elasticsearch 7.10** — other components depend on the ES-7.x API; the OpenSearch move is a separate future effort.
- **Not** touching the API or external services (curation site, public-website indexer) — they keep their existing `elasticsearch-py 7.x` / ES-7.x clients unchanged, because the server stays ES 7.10.

## 2. Current architecture (what we replace)

```
PG (RDS) → Debezium 1.9/2.7 (Kafka Connect 3.7) → Kafka + ZooKeeper
  → ksqlDB 0.26.0  (ksql_queries.ksql: 55 CREATE TABLE, 31 JOIN, 15 GROUP BY;
      each intermediate table = its own RocksDB store; every GROUP BY re-emits on
      any upstream change → churn propagates all the way to the sink)
  → kafka-connect-elasticsearch 5.3.2 (Jest, 2019) → Elasticsearch 7.10 (AWS managed, r6gd.large ×1)
      · topic reference_joined → references_index (private, all refs)
      · topic public_reference_joined → public_references_index (public, INNER JOIN mods_in_corpus)
  → blue/green: setup.sh builds the inactive slot, status_manager.sh flips the alias
```

**Pain:** unbounded RocksDB memory (needed a custom config-setter to tame); the re-emission churn is what we watched never settle at the Gate-2 cap; ksqlDB is EOL; deletions depend on a fragile tombstone config. The search **server is already Elasticsearch 7.10** — that stays.

## 3. Target architecture

```
PG17 (RDS)
 └─ Debezium 3.5.2.Final source  (Kafka Connect 4.1, KRaft, Java 17, pgoutput;
      FULL Debezium envelope — unwrap/ExtractNewRecordState SMT removed; SOURCE ONLY, no sink)
     └─ Kafka 4.1  (KRaft — no ZooKeeper)          ← raw CDC topics, one per table
        └─ Flink 2.0.x SQL job
             ├─ source: flink-connector-kafka 4.0.1, format=debezium-json
             ├─ shared VIEWs: authors, cross_references, mesh_terms, topic_entity_tags,
             │    curation_tags, workflow_tags, reference_with_resource/copyright, mods_in_corpus …
             ├─ INSERT → references_index         (private projection/joins)
             └─ INSERT → public_references_index  (public projection + INNER JOIN mods_in_corpus)
                  └─ Flink Elasticsearch-7 sink ×2  (flink-sql-connector-elasticsearch7 v4.0.0;
                       PRIMARY KEY reference_id → upsert + delete-by-id)
                     └─ Elasticsearch 7.10  (blue/green slot indexes + alias flip — reused)
```

**No Kafka Connect sink and no joined Kafka topics** — Flink does the joins *and* indexes directly.

### Bill of materials

| Component | Version | Notes |
|---|---|---|
| PostgreSQL | 17 | done (RDS dev on 17.10) |
| Debezium | **3.5.2.Final** | Kafka Connect 4.1, Java 17, `pgoutput`; source only; 3.6 is CR-only, avoid |
| Kafka | 4.1.x | **KRaft** (combined broker/controller, no ZooKeeper) |
| Flink | **2.0.x** | pinned by the ES connector (no 2.1/2.2 ES connector yet); ForSt/S3 + bounded managed memory both present in 2.0 |
| Flink Kafka source | `flink-connector-kafka` **4.0.1** | Flink 2.0.x; `debezium-json` format |
| Flink ES sink | `flink-sql-connector-elasticsearch7` **v4.0.0** | Flink 2.0.x; ES-7 RestHighLevelClient (no ES-8 product check) |
| Search server | **Elasticsearch 7.10** (unchanged) | AWS managed; server migration deferred |
| App / external search clients | `elasticsearch-py 7.x` (**unchanged**) | no client migration — server stays ES 7.10 |

### Removed / added / kept
- **Removed:** ZooKeeper; ksqlDB + custom RocksDB config-setter jar; `kafka-connect-elasticsearch` 5.3.2; the intermediate `reference_joined`/`public_reference_joined` Kafka topics.
- **Added:** Kafka KRaft mode; Flink 2.0.x (JobManager + TaskManager + SQL Gateway) with the Kafka source + ES-7 sink connectors.
- **Kept:** Debezium *source* (now 3.5, source-only Connect 4.1 worker); **Elasticsearch 7.10** server; the app + external clients (ES 7.x); the blue/green alias-flip logic in `status_manager.sh`; the from-scratch reindex model.

## 4. Component design

**4.1 Debezium 3.5 source** — Kafka Connect 4.1 worker (Java 17), now hosting **only** the source. PostgreSQL `pgoutput`, slot + publication as today. **Change:** drop the `ExtractNewRecordState`/unwrap SMT so topics carry the full Debezium envelope (`before`/`after`/`op`) — Flink's `debezium-json` format consumes that directly. Audit 3.x property deprecations.

**4.2 Kafka 4.1 (KRaft)** — single combined broker+controller node (test scale): `KAFKA_PROCESS_ROLES=broker,controller`, `CLUSTER_ID`, controller quorum. `dbz_zookeeper` + `ZOOKEEPER_CONNECT` deleted.

**4.3 Flink 2.0.x SQL job** — JobManager + TaskManager + SQL Gateway containers; job submitted as a SQL statement set. Sources: one `CREATE TABLE … WITH ('connector'='kafka','format'='debezium-json')` per CDC topic (connector 4.0.1). Shared aggregates → Flink SQL **views** mirroring the ksqlDB intermediate tables; retract semantics mean intermediate re-emissions **cancel inside Flink**. Two `INSERT`s in one statement set so the planner shares common subgraphs.

**4.4 Flink Elasticsearch-7 sink ×2** — `flink-sql-connector-elasticsearch7` v4.0.0. Two sink tables, `PRIMARY KEY (reference_id) NOT ENFORCED` → **upsert mode** (index on INSERT/UPDATE, delete-by-`_id` on retract). Targets ES 7.10 via the ES-7 RestHighLevelClient (no ES-8 product check, so 7.10 is fine). One sink → `references_index` slot, one → `public_references_index` slot.

**4.5 Elasticsearch 7.10 (unchanged server)** — AWS managed. Blue/green slot indexes + alias flip via existing `status_manager.sh`. Server migration to OpenSearch is a deferred separate effort.

**4.6 Clients unchanged** — the API and the external consumers (curation site, public-website indexer) keep their `elasticsearch-py 7.x` / ES-7.x clients. No work here: the server stays ES 7.10, so nothing downstream changes. (CI keeps its ES 7.x container.)

## 5. The two indexes (Flink SQL mapping)

| | `references_index` (private) | `public_references_index` (public) |
|---|---|---|
| Fields | internal/curation (topic_entity_tags, curation_tags, workflow_tags, reference_emails, indexing_priorities, manual_indexing_tags, mods_needs_review…) | public-facing (copyright_license, open_access, relations, mesh_terms, resource_title…) |
| Upstream | `reference_mod_reference_types` + LEFT JOINs to curation aggregates | `reference_with_copyright` + LEFT JOINs to authors/xrefs/mesh/relations |
| Row set | all references (LEFT joins) | `INNER JOIN mods_in_corpus` → in-corpus only (~1.11M vs ~1.29M) |

Shared upstream → views; divergent tails → the two `INSERT`s into the two ES-7 sink tables.

## 6. Deletions (hard requirement)

Row retraction in Flink → the ES-7 upsert sink issues a **delete-by-`_id`** (`_id = reference_id`). Child deletes (e.g. an author removed) retract the aggregate and re-index the net doc. No tombstone topic, no `behavior.on.null.values` config. Verify author-removed and reference-removed on the test box (the SCRUM-6337 tests).

## 7. Memory strategy (goal #1)

- **Baseline (default):** Flink RocksDB state backend with **bounded managed memory** (`taskmanager.memory.managed`) — one budget across all operators, replacing ksqlDB's 55 unbounded stores. Fixes the root cause on its own.
- **Optional (Flink 2.0 ForSt):** disaggregated state — SST files on **S3**, local disk as cache, per-slot RocksDB memory hard-bounded, state size decoupled from box RAM. Decide baseline-vs-ForSt on the test box against measured state size (expected 1–5 GB).

## 8. Reindex / blue-green

- **From scratch (only mode used):** a fresh Flink job reads CDC from earliest → computes the joins → **the ES-7 sinks write to the inactive slot indexes** → gate on drain + doc parity → flip the alias (existing `status_manager.sh`). Old slot retained as rollback.
- **Steady state:** the Flink job continuously applies CDC → the sinks upsert the live indexes; deletions per §6.
- The Gate-2 "drain never settles" problem should disappear (Flink emits only net docs); reindex time to be measured (§9), not promised.

## 9. Test-box plan (debezium test instance)

Install (docker-compose): Kafka 4.1 KRaft, Debezium 3.5 Connect worker (source only), Flink 2.0.x (JM/TM/SQL Gateway) with the Kafka + ES-7 connectors, and a local **Elasticsearch 7.10** container. Add the new env vars to the box's `.env.test` (Flink heap/managed-memory + parallelism, Kafka KRaft cluster id/quorum, ES host, Debezium 3.5 image tags).

Run a full reindex and record:
- **Doc parity** vs current: `references_index` 1,291,555 / `public_references_index` 1,109,421 (±live CDC).
- **Deletions** — author-removed and reference-removed both propagate (SCRUM-6337 tests).
- **Memory** — peak + idle, bounded, no OOM.
- **Reindex time** vs ksqlDB ~3h+.

**Golden-doc capture (pre-teardown, done 2026-07-27):** mappings + sample documents pulled from the live ksqlDB-built `references_index` / `public_references_index` so Flink output can be diffed field-by-field. The current stack's docs are the parity oracle; once it's torn down we can't regenerate them.

**Gates:** **index docs byte/field-identical to golden** (both indexes) ✔, doc parity ✔, deletions ✔, memory bounded/no-OOM ✔, CI `debezium-integration-test` green.

## 10. Risks & open items

- **Flink version is pinned to 2.0.x** by the ES connector (no 2.1/2.2 ES-connector release yet). Acceptable — 2.0 has ForSt + bounded memory. Revisit when the ES connector catches up.
- **`flink-connector-kafka 4.0.1` client ↔ Kafka 4.1 broker** — expected compatible (Kafka client/broker skew), verify on box.
- **Flink ES-7 upsert/delete against ES 7.10** — verify indexing + delete-by-id on the box.
- **`optimize_and_warm_index` force-merge** — the ES-7 `wait_for_completion` incompatibility found on SCRUM-6323 must be handled here too (blue/green optimize reused).
- **AWS `Elasticsearch_7.10` is a legacy/EOL engine** (extended-support surcharge, same shape as PG13). Out of scope here by requirement, but the ES→OpenSearch server migration should be tracked as a future ticket; the Flink ES connector already has an ES-8/OpenSearch path when that day comes.

## 11. Rollout

Pilot the full stack + reindex on the debezium test box (same pattern as the SCRUM-6324 PG17 pilot) → dev/stage → prod. **No external-consumer coordination needed** — clients stay on ES 7.x because the server stays ES 7.10. PR gets blue-team reviewers. Makefile targets, runbooks, deployment docs updated.

## 12. Alternatives considered (and why not)

- **Kafka Streams** — the engine ksqlDB is built on → same RocksDB memory model, and drops SQL for imperative Java. Rejected.
- **RisingWave** — cleanest S3-decoupled-state concept, but younger/less proven and replaces Kafka Connect; Flink 2.0 ForSt closes the memory gap. Deferred.
- **Kafka Connect ES/OpenSearch sink (Aiven or Confluent), same or separate worker** — the sink connectors are built against **Kafka Connect 3.8** (Aiven v4.1.0: `kafkaVersion=3.8.1`) and are unvalidated on Debezium 3.5's **Connect 4.1**; the modern Confluent ES sink uses the ES-8 client whose product check **rejects ES 7.10**. Flink-direct via the ES-7 connector avoids all of this. Rejected.
- **Flink 2.2** — no Elasticsearch connector release exists for 2.1/2.2 (caps at 2.0.x via connector v4.0.0). Chose Flink 2.0.x; revisit when the ES connector advances.
- **Migrating the search server to OpenSearch 2.19/3.5 now** — the domains run ES 7.10 and other components depend on the ES-7 API; migrating the server + all clients is a separate future effort, not this milestone.

---
*Drafted by Claude with Valerio Arnaboldi, 2026-07-26.*
