-- SCRUM-6336: references_index (PRIVATE) — Flink SQL translation of ksql_queries.ksql.
-- The ksqlDB version chains ~30 pairwise CTAS tables; here the whole document is ONE multi-way
-- join over 13 aggregate views + the reference base. Output must be byte-identical to the
-- ksqlDB-built references_index (validated against the golden docs).
SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'references_index';
-- Mini-batch: coalesce aggregate + join updates so each reference is written ~once with its
-- aggregates attached, instead of re-writing on every child row. This is THE lever against the
-- reindex write-amplification: the huge child aggregates (mesh_detail 18M, author 7.7M, cross 4M,
-- topic_entity_tag 2.7M) otherwise emit an ES update per row (measured ~19k ES ops/min of pure churn,
-- swamping new-reference throughput). Large windows collapse each reference's incremental ARRAY_AGG
-- updates into ~one emission upstream, WITHOUT the full-result-set materializer state that OOM/walls a
-- bounded TM (see upsert-materialize note below). 30s latency is fine for a reindex + curation index.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '30 s';
SET 'table.exec.mini-batch.size' = '200000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';
-- Disable the sink upsert-materializer entirely. In AUTO mode Flink inserts a full-result-set stateful
-- operator ahead of the ES sink that holds EVERY keyed row (~1.3M docs) in RocksDB -- on top of the
-- already-huge aggregate state (mesh_detail 18M, author 7.7M, cross 4M, tet 2.7M) this blows past the
-- managed-memory budget and RocksDB thrashes on disk compaction (reindex throughput collapses to a
-- crawl). NONE removes that operator: the ES sink upserts by PRIMARY KEY so the final doc converges
-- from the aggregate retractions anyway, and we only flip the alias after Gate 2 (catch-up) confirms a
-- stable count -- correct for a from-scratch idempotent reindex.
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- ============================ SOURCE TABLES (Debezium CDC, full envelope) ============================
CREATE TABLE reference (
  reference_id STRING, curie STRING, abstract STRING, category STRING, citation_id STRING,
  copyright_license_id STRING, date_arrived_in_pubmed STRING, date_created BIGINT,
  date_last_modified_in_pubmed STRING, date_published STRING, date_published_start STRING,
  date_published_end STRING, date_updated BIGINT, issue_name STRING, keywords ARRAY<STRING>,
  `language` STRING, page_range STRING, plain_language_abstract STRING, publisher STRING,
  pubmed_abstract_languages ARRAY<STRING>, pubmed_publication_status STRING, pubmed_types ARRAY<STRING>,
  resource_id STRING, title STRING, volume STRING, retraction_status STRING,
  can_display_image BOOLEAN, image_count INT,
  PRIMARY KEY (reference_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.reference','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE citation (
  citation_id STRING, citation STRING, short_citation STRING,
  PRIMARY KEY (citation_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.citation','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE cross_reference (
  cross_reference_id STRING, reference_id STRING, curie STRING, is_obsolete BOOLEAN,
  PRIMARY KEY (cross_reference_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.cross_reference','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE author (
  author_id STRING, reference_id STRING, orcid STRING, name STRING,
  PRIMARY KEY (author_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.author','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE `mod` (
  mod_id STRING, abbreviation STRING,
  PRIMARY KEY (mod_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.mod','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE mod_corpus_association (
  mod_corpus_association_id STRING, mod_id STRING, reference_id STRING, corpus BOOLEAN,
  PRIMARY KEY (mod_corpus_association_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.mod_corpus_association','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE obsolete_reference_curie (
  obsolete_reference_curie_id STRING, curie STRING, new_id STRING,
  PRIMARY KEY (obsolete_reference_curie_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.obsolete_reference_curie','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE referencetype (
  referencetype_id STRING, label STRING,
  PRIMARY KEY (referencetype_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.referencetype','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE mod_referencetype (
  mod_referencetype_id STRING, mod_id STRING, referencetype_id STRING, display_order STRING,
  PRIMARY KEY (mod_referencetype_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.mod_referencetype','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE reference_mod_referencetype (
  reference_mod_referencetype_id STRING, reference_id STRING, mod_referencetype_id STRING,
  PRIMARY KEY (reference_mod_referencetype_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.reference_mod_referencetype','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE topic_entity_tag (
  topic_entity_tag_id STRING, reference_id STRING, `topic` STRING, entity_type STRING,
  date_created STRING, date_updated STRING, created_by STRING, updated_by STRING, entity STRING,
  entity_published_as STRING, species STRING, display_tag STRING, confidence_level STRING,
  confidence_score DOUBLE, negated BOOLEAN, note STRING, topic_entity_tag_source_id STRING,
  data_novelty STRING, validation_by_professional_biocurator STRING, entity_id_validation STRING,
  PRIMARY KEY (topic_entity_tag_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.topic_entity_tag','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE topic_entity_tag_source (
  topic_entity_tag_source_id STRING, source_method STRING, data_provider STRING, source_evidence_assertion STRING,
  PRIMARY KEY (topic_entity_tag_source_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.topic_entity_tag_source','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE curation_status (
  curation_status_id STRING, `topic` STRING, reference_id STRING, mod_id STRING, curation_status STRING,
  note STRING, date_created STRING, date_updated STRING, created_by STRING, updated_by STRING, curation_tag STRING,
  PRIMARY KEY (curation_status_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.curation_status','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE workflow_tag (
  reference_workflow_tag_id STRING, reference_id STRING, workflow_tag_id STRING, mod_id STRING, created_by STRING,
  PRIMARY KEY (reference_workflow_tag_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.workflow_tag','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE reference_email (
  reference_email_id STRING, reference_id STRING, email_address STRING,
  PRIMARY KEY (reference_email_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.reference_email','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE indexing_priority (
  indexing_priority_id STRING, reference_id STRING, mod_id STRING, predicted_indexing_priority STRING,
  curator_indexing_priority STRING, confidence_score DOUBLE,
  PRIMARY KEY (indexing_priority_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.indexing_priority','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

CREATE TABLE manual_indexing_tag (
  manual_indexing_tag_id STRING, reference_id STRING, mod_id STRING, curation_tag STRING,
  confidence_score DOUBLE, validation_by_biocurator STRING, note STRING,
  PRIMARY KEY (manual_indexing_tag_id) NOT ENFORCED
) WITH ('connector'='kafka','topic'='abc.public.manual_indexing_tag','properties.bootstrap.servers'='dbz_kafka:9092',
  'properties.group.id'='flink-refidx','scan.startup.mode'='earliest-offset','format'='debezium-json');

-- ============================ AGGREGATE VIEWS (one per nested array) ============================
CREATE VIEW cross_references_agg AS
  SELECT reference_id, ARRAY_AGG(MAP['curie', curie, 'is_obsolete', LOWER(CAST(is_obsolete AS STRING))]) AS cross_references
  FROM cross_reference GROUP BY reference_id;

CREATE VIEW authors_agg AS
  SELECT reference_id, ARRAY_AGG(MAP['name', name, 'orcid', orcid]) AS authors
  FROM author GROUP BY reference_id;

CREATE VIEW mods_in_corpus_agg AS
  SELECT mca.reference_id, ARRAY_AGG(m.abbreviation) AS mods_in_corpus
  FROM mod_corpus_association mca JOIN `mod` m ON mca.mod_id = m.mod_id
  WHERE mca.corpus = TRUE GROUP BY mca.reference_id;

CREATE VIEW mods_needs_review_agg AS
  SELECT mca.reference_id, ARRAY_AGG(m.abbreviation) AS mods_needs_review
  FROM mod_corpus_association mca JOIN `mod` m ON mca.mod_id = m.mod_id
  WHERE mca.corpus IS NULL GROUP BY mca.reference_id;

CREATE VIEW mods_in_corpus_or_needs_review_agg AS
  SELECT mca.reference_id, ARRAY_AGG(m.abbreviation) AS mods_in_corpus_or_needs_review
  FROM mod_corpus_association mca JOIN `mod` m ON mca.mod_id = m.mod_id
  WHERE mca.corpus IS NULL OR mca.corpus = TRUE GROUP BY mca.reference_id;

CREATE VIEW obsolete_curies_agg AS
  SELECT new_id AS reference_id, ARRAY_AGG(curie) AS obsolete_curies
  FROM obsolete_reference_curie GROUP BY new_id;

CREATE VIEW mod_reference_types_agg AS
  SELECT rmr.reference_id, ARRAY_AGG(rt.label) AS mod_reference_types
  FROM reference_mod_referencetype rmr
  JOIN mod_referencetype mr ON rmr.mod_referencetype_id = mr.mod_referencetype_id
  JOIN referencetype rt ON mr.referencetype_id = rt.referencetype_id
  GROUP BY rmr.reference_id;

CREATE VIEW topic_entity_tags_agg AS
  SELECT tet.reference_id, ARRAY_AGG(MAP[
    'topic', tet.`topic`, 'entity_type', tet.entity_type, 'entity', tet.entity,
    'entity_published_as', tet.entity_published_as, 'species', tet.species, 'display_tag', tet.display_tag,
    'confidence_level', tet.confidence_level, 'confidence_score', CAST(tet.confidence_score AS STRING),
    'negated', LOWER(CAST(tet.negated AS STRING)), 'data_novelty', tet.data_novelty,
    'validation_by_professional_biocurator', tet.validation_by_professional_biocurator,
    'source_method', s.source_method, 'data_provider', s.data_provider,
    'source_evidence_assertion', s.source_evidence_assertion,
    'source_evidence_assertion_group', CASE WHEN s.source_evidence_assertion IN ('ATP:0000036','ATP:0000035')
        THEN 'ECO:0006155' ELSE 'ECO:0007669' END
  ]) AS topic_entity_tags
  FROM topic_entity_tag tet
  JOIN topic_entity_tag_source s ON tet.topic_entity_tag_source_id = s.topic_entity_tag_source_id
  GROUP BY tet.reference_id;

CREATE VIEW curation_tags_agg AS
  SELECT cs.reference_id, ARRAY_AGG(MAP['topic', cs.`topic`, 'curation_status', cs.curation_status, 'abbreviation', m.abbreviation]) AS curation_tags
  FROM curation_status cs JOIN `mod` m ON cs.mod_id = m.mod_id GROUP BY cs.reference_id;

CREATE VIEW workflow_tags_agg AS
  SELECT wt.reference_id, ARRAY_AGG(MAP['workflow_tag_id', wt.workflow_tag_id, 'mod_abbreviation', m.abbreviation]) AS workflow_tags
  FROM workflow_tag wt JOIN `mod` m ON wt.mod_id = m.mod_id GROUP BY wt.reference_id;

CREATE VIEW indexing_priorities_agg AS
  SELECT ip.reference_id, ARRAY_AGG(MAP['predicted_indexing_priority', ip.predicted_indexing_priority,
    'curator_indexing_priority', ip.curator_indexing_priority, 'mod_abbreviation', m.abbreviation]) AS indexing_priorities
  FROM indexing_priority ip JOIN `mod` m ON ip.mod_id = m.mod_id GROUP BY ip.reference_id;

CREATE VIEW manual_indexing_tags_agg AS
  SELECT mit.reference_id, ARRAY_AGG(MAP['curation_tag', mit.curation_tag, 'mod_abbreviation', m.abbreviation]) AS manual_indexing_tags
  FROM manual_indexing_tag mit JOIN `mod` m ON mit.mod_id = m.mod_id GROUP BY mit.reference_id;

CREATE VIEW reference_emails_agg AS
  SELECT reference_id, ARRAY_AGG(email_address) AS reference_emails
  FROM reference_email GROUP BY reference_id;

-- ============================ ES-7 SINK (upsert, keyed by reference_id) ============================
CREATE TABLE references_index_sink (
  reference_id STRING, curie STRING, abstract STRING, category STRING,
  date_arrived_in_pubmed STRING, date_created BIGINT, date_last_modified_in_pubmed STRING,
  date_published STRING, date_published_start STRING, date_published_end STRING, date_updated BIGINT,
  issue_name STRING, keywords ARRAY<STRING>, `language` STRING, page_range STRING,
  plain_language_abstract STRING, publisher STRING, pubmed_abstract_languages ARRAY<STRING>,
  pubmed_publication_status STRING, pubmed_types ARRAY<STRING>, resource_id STRING, title STRING,
  volume STRING, retraction_status STRING, can_display_image BOOLEAN, image_count INT,
  retraction_status_name STRING, citation STRING, short_citation STRING,
  cross_references ARRAY<MAP<STRING,STRING>>, authors ARRAY<MAP<STRING,STRING>>,
  mods_in_corpus ARRAY<STRING>, mods_needs_review ARRAY<STRING>, mods_in_corpus_or_needs_review ARRAY<STRING>,
  obsolete_curies ARRAY<STRING>, mod_reference_types ARRAY<STRING>,
  topic_entity_tags ARRAY<MAP<STRING,STRING>>, curation_tags ARRAY<MAP<STRING,STRING>>,
  workflow_tags ARRAY<MAP<STRING,STRING>>, reference_emails ARRAY<STRING>,
  indexing_priorities ARRAY<MAP<STRING,STRING>>, manual_indexing_tags ARRAY<MAP<STRING,STRING>>,
  PRIMARY KEY (reference_id) NOT ENFORCED
) WITH ('connector'='elasticsearch-7','hosts'='http://elasticsearch:9200','index'='flink_references_index',
  -- Bound the sink so a slow ES backpressures the pipeline instead of hoarding bulk requests on heap
  -- (batch reindex sends ~1.3M docs in a burst once the join finishes; unbounded buffering OOMs the TM).
  'sink.bulk-flush.max-actions'='500','sink.bulk-flush.max-size'='2mb','sink.bulk-flush.interval'='1s',
  'sink.bulk-flush.backoff.strategy'='CONSTANT','sink.bulk-flush.backoff.max-retries'='5','sink.bulk-flush.backoff.delay'='2s');

-- ============================ FINAL ASSEMBLY: one multi-way join ============================
INSERT INTO references_index_sink
SELECT
  r.reference_id, r.curie, r.abstract, r.category,
  NULLIF(r.date_arrived_in_pubmed, '') AS date_arrived_in_pubmed, r.date_created,
  NULLIF(r.date_last_modified_in_pubmed, '') AS date_last_modified_in_pubmed,
  r.date_published, r.date_published_start, r.date_published_end, r.date_updated,
  r.issue_name, r.keywords, r.`language`, r.page_range, r.plain_language_abstract, r.publisher,
  r.pubmed_abstract_languages, r.pubmed_publication_status, r.pubmed_types,
  CASE WHEN r.resource_id IS NULL THEN '__EMPTY__' ELSE r.resource_id END AS resource_id,
  r.title, r.volume, r.retraction_status, r.can_display_image, r.image_count,
  CASE r.retraction_status
    WHEN 'ATP:0000346' THEN 'retracted'
    WHEN 'ATP:0000347' THEN 'partially retracted'
    WHEN 'ATP:0000348' THEN 'fully retracted'
    ELSE NULL END AS retraction_status_name,
  cit.citation, cit.short_citation,
  xref.cross_references, auth.authors,
  mic.mods_in_corpus, mnr.mods_needs_review, mcr.mods_in_corpus_or_needs_review,
  obs.obsolete_curies, mrt.mod_reference_types,
  tet.topic_entity_tags, ctag.curation_tags, wf.workflow_tags, rem.reference_emails,
  ip.indexing_priorities, mit.manual_indexing_tags
FROM reference r
JOIN citation cit ON r.citation_id = cit.citation_id
LEFT JOIN cross_references_agg xref ON r.reference_id = xref.reference_id
LEFT JOIN authors_agg auth ON r.reference_id = auth.reference_id
LEFT JOIN mods_in_corpus_agg mic ON r.reference_id = mic.reference_id
LEFT JOIN mods_needs_review_agg mnr ON r.reference_id = mnr.reference_id
LEFT JOIN mods_in_corpus_or_needs_review_agg mcr ON r.reference_id = mcr.reference_id
LEFT JOIN obsolete_curies_agg obs ON r.reference_id = obs.reference_id
LEFT JOIN mod_reference_types_agg mrt ON r.reference_id = mrt.reference_id
LEFT JOIN topic_entity_tags_agg tet ON r.reference_id = tet.reference_id
LEFT JOIN curation_tags_agg ctag ON r.reference_id = ctag.reference_id
LEFT JOIN workflow_tags_agg wf ON r.reference_id = wf.reference_id
LEFT JOIN reference_emails_agg rem ON r.reference_id = rem.reference_id
LEFT JOIN indexing_priorities_agg ip ON r.reference_id = ip.reference_id
LEFT JOIN manual_indexing_tags_agg mit ON r.reference_id = mit.reference_id;
