#!/bin/bash
#
# Idempotent self-heal for the Debezium logical-replication publication.
#
# The unified connector (postgres-source-unified.json) uses
# publication.autocreate.mode=filtered. That mode creates the publication ONLY
# when it does not already exist; it never adds newly-included tables to a
# pre-existing publication. So when a table is added to the connector's
# table.include.list (e.g. indexing_priority, manual_indexing_tag), Postgres
# keeps publishing the old table set and the new table's row changes never
# stream into the index.
#
# A full reindex (setup.sh) fixes this because it drops and recreates the
# publication. This script is the non-destructive alternative: it adds any
# missing tables to the EXISTING publication so a running deployment starts
# streaming them immediately, without a full reindex. It is safe to run
# repeatedly.
#
# Keep the table list below in sync with "table.include.list" in
# postgres-source-unified.json.

set -euo pipefail

PUBLICATION="${DEBEZIUM_PUBLICATION:-debezium_unified}"

export PGPASSWORD=${PSQL_PASSWORD}

echo "Healing publication '${PUBLICATION}' on ${PSQL_HOST}:${PSQL_PORT}/${PSQL_DATABASE} ..."

psql -h "${PSQL_HOST}" -U "${PSQL_USERNAME}" -p "${PSQL_PORT}" -d "${PSQL_DATABASE}" \
    -v ON_ERROR_STOP=1 -c "DO \$\$
DECLARE
  tbl text;
  sch text := 'public';
  pub text := '${PUBLICATION}';
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = pub) THEN
    RAISE NOTICE 'Publication % does not exist; run setup.sh to create it.', pub;
    RETURN;
  END IF;
  IF (SELECT puballtables FROM pg_publication WHERE pubname = pub) THEN
    RAISE NOTICE 'Publication % is FOR ALL TABLES; nothing to add.', pub;
    RETURN;
  END IF;
  FOREACH tbl IN ARRAY ARRAY[
    'citation','reference','resource','mod_corpus_association','topic_entity_tag_source',
    'mod_referencetype','mod','referencetype','copyright_license','cross_reference','author',
    'obsolete_reference_curie','reference_mod_referencetype','topic_entity_tag','workflow_tag',
    'reference_relation','mesh_detail','reference_email','indexing_priority','manual_indexing_tag'
  ]
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_publication_tables
      WHERE pubname = pub AND schemaname = sch AND tablename = tbl
    ) THEN
      EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I.%I', pub, sch, tbl);
      RAISE NOTICE 'Added %.% to publication %', sch, tbl, pub;
    END IF;
  END LOOP;
END\$\$;"

echo "Publication '${PUBLICATION}' heal complete."
