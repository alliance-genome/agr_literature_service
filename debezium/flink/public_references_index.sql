-- SCRUM-6336: public_references_index (PUBLIC) — Flink SQL translation of ksql_queries.ksql.
-- Public doc = in-corpus references only (INNER JOIN mods_in_corpus), public-facing fields
-- (+ copyright/open_access/resource_title/relations/mesh), NO internal curation fields.
-- One multi-way join; output must be byte-identical to the ksqlDB public_reference_joined.
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'public_references_index';
-- Same tuning as references_index.sql: mini-batch coalesces aggregate+join updates so each reference
-- is written ~once with its aggregates attached (kills reindex write-amplification / temp-object churn).
-- upsert-materialize is AUTO: PK-upsert convergence only holds if the changelog reaches the sink
-- ordered per key, which is not true once parallelism > 1 (see references_index.sql).
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '30 s';
SET 'table.exec.mini-batch.size' = '200000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
-- Sink upsert-materializer: see the note in references_index.sql. NONE is unsafe at
-- parallelism > 1 (out-of-order retractions delete converged docs).
SET 'table.exec.sink.upsert-materialize' = 'AUTO';

-- ============================ SOURCE TABLES ============================
CREATE TABLE reference (
  reference_id STRING, curie STRING, abstract STRING, category STRING, citation_id STRING,
  copyright_license_id STRING, date_arrived_in_pubmed STRING, date_created BIGINT,
  date_last_modified_in_pubmed STRING, date_published STRING, issue_name STRING, keywords ARRAY<STRING>,
  `language` STRING, page_range STRING, publisher STRING, pubmed_publication_status STRING,
  pubmed_types ARRAY<STRING>, resource_id STRING, title STRING, volume STRING, retraction_status STRING,
  PRIMARY KEY (reference_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.reference','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE citation (
  citation_id STRING, citation STRING, short_citation STRING,
  PRIMARY KEY (citation_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.citation','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE resource (
  resource_id STRING, title STRING,
  PRIMARY KEY (resource_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.resource','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE copyright_license (
  copyright_license_id STRING, name STRING, open_access BOOLEAN,
  PRIMARY KEY (copyright_license_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.copyright_license','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE cross_reference (
  cross_reference_id STRING, reference_id STRING, curie STRING, is_obsolete BOOLEAN,
  PRIMARY KEY (cross_reference_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.cross_reference','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE author (
  author_id STRING, reference_id STRING, orcid STRING, name STRING, author_order INT,
  PRIMARY KEY (author_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.author','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE `mod` (
  mod_id STRING, abbreviation STRING,
  PRIMARY KEY (mod_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.mod','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE mod_corpus_association (
  mod_corpus_association_id STRING, mod_id STRING, reference_id STRING, corpus BOOLEAN,
  PRIMARY KEY (mod_corpus_association_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.mod_corpus_association','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE reference_relation (
  reference_relation_id STRING, reference_id_from STRING, reference_id_to STRING, reference_relation_type STRING,
  PRIMARY KEY (reference_relation_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.reference_relation','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE mesh_detail (
  mesh_detail_id STRING, reference_id STRING, heading_term STRING, qualifier_term STRING,
  PRIMARY KEY (mesh_detail_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.mesh_detail','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-pubidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

-- ============================ AGGREGATE VIEWS ============================
CREATE VIEW cross_references_agg AS
  SELECT reference_id, ARRAY_AGG(MAP['curie', curie, 'is_obsolete', LOWER(CAST(is_obsolete AS STRING))]) AS cross_references
  FROM cross_reference GROUP BY reference_id;

CREATE VIEW authors_agg AS
  SELECT reference_id,
         ARRAY_AGG(MAP['name', name, 'orcid', orcid,
                       'author_order', CAST(author_order AS STRING)]) AS authors
  FROM author GROUP BY reference_id;

CREATE VIEW mods_in_corpus_agg AS
  SELECT mca.reference_id, ARRAY_AGG(m.abbreviation) AS mods_in_corpus
  FROM mod_corpus_association mca JOIN `mod` m ON mca.mod_id = m.mod_id
  WHERE mca.corpus = TRUE GROUP BY mca.reference_id;

CREATE VIEW reference_relations_agg AS
  SELECT reference_id_from AS reference_id,
    ARRAY_AGG(MAP['reference_id_to', reference_id_to, 'reference_relation_type', reference_relation_type]) AS relations
  FROM reference_relation GROUP BY reference_id_from;

CREATE VIEW mesh_terms_agg AS
  SELECT reference_id, ARRAY_AGG(MAP['heading_term', heading_term, 'qualifier_term', qualifier_term]) AS mesh_terms
  FROM mesh_detail GROUP BY reference_id;

-- ============================ ES-7 SINK ============================
CREATE TABLE public_references_index_sink (
  reference_id STRING, curie STRING, title STRING, abstract STRING, category STRING,
  pubmed_types ARRAY<STRING>, resource_title STRING, volume STRING, issue_name STRING, page_range STRING,
  publisher STRING, `language` STRING, date_published STRING, pubmed_publication_status STRING,
  date_arrived_in_pubmed STRING, date_last_modified_in_pubmed STRING, date_created BIGINT,
  keywords ARRAY<STRING>, citation STRING, short_citation STRING, open_access BOOLEAN, copyright_license STRING,
  retraction_status STRING, retraction_status_name STRING,
  cross_references ARRAY<MAP<STRING,STRING>>, authors ARRAY<MAP<STRING,STRING>>,
  relations ARRAY<MAP<STRING,STRING>>, mesh_terms ARRAY<MAP<STRING,STRING>>, mods_in_corpus ARRAY<STRING>,
  PRIMARY KEY (reference_id) NOT ENFORCED
) WITH ('connector'='elasticsearch-7','hosts'='http://elasticsearch:9200','index'='flink_public_references_index',
  -- Bound the sink so a slow ES backpressures the pipeline instead of hoarding bulk requests on heap
  -- (batch reindex bursts ~1.1M docs to the sink once the join finishes; unbounded buffering OOMs the TM).
  'sink.bulk-flush.max-actions'='500','sink.bulk-flush.max-size'='2mb','sink.bulk-flush.interval'='1s',
  'sink.bulk-flush.backoff.strategy'='CONSTANT','sink.bulk-flush.backoff.max-retries'='5','sink.bulk-flush.backoff.delay'='2s');

-- ============================ FINAL ASSEMBLY ============================
-- Ascending by source size, and `reference` stays on the LEFT so its declared PK carries to the sink
-- (see the note in references_index.sql: a spine view loses key inference and turns updates into
-- delete+insert). Source rows: copyright_license 8, reference_relation 14,927, resource 45,301,
-- mod_corpus_association 1.44M, cross_reference 4.09M, author 7.78M, mesh_detail 18.16M (last).
INSERT INTO public_references_index_sink
SELECT
  r.reference_id, r.curie, r.title, r.abstract, r.category, r.pubmed_types,
  res.title AS resource_title, r.volume, r.issue_name, r.page_range, r.publisher, r.`language`,
  r.date_published, r.pubmed_publication_status,
  NULLIF(r.date_arrived_in_pubmed, '') AS date_arrived_in_pubmed,
  NULLIF(r.date_last_modified_in_pubmed, '') AS date_last_modified_in_pubmed,
  r.date_created, r.keywords, cit.citation, cit.short_citation,
  cl.open_access, cl.name AS copyright_license,
  r.retraction_status,
  CASE r.retraction_status
    WHEN 'ATP:0000346' THEN 'retracted'
    WHEN 'ATP:0000347' THEN 'partially retracted'
    WHEN 'ATP:0000348' THEN 'fully retracted'
    ELSE NULL END AS retraction_status_name,
  xref.cross_references, auth.authors, rel.relations, mesh.mesh_terms, mic.mods_in_corpus
FROM reference r
JOIN citation cit ON r.citation_id = cit.citation_id
LEFT JOIN copyright_license cl ON r.copyright_license_id = cl.copyright_license_id
LEFT JOIN reference_relations_agg rel ON r.reference_id = rel.reference_id
LEFT JOIN resource res ON r.resource_id = res.resource_id
JOIN mods_in_corpus_agg mic ON r.reference_id = mic.reference_id   -- INNER: in-corpus only
LEFT JOIN cross_references_agg xref ON r.reference_id = xref.reference_id
LEFT JOIN authors_agg auth ON r.reference_id = auth.reference_id
LEFT JOIN mesh_terms_agg mesh ON r.reference_id = mesh.reference_id;
