# Migration Plan: Update users.id to Use person.curie

## Overview

This document outlines the plan to update the `users.id` column (currently using Okta IDs) to use `person.curie` values for human users (those with `person_id` not null).

## Current State Analysis

### Users Table Structure
```
users.id          VARCHAR  PRIMARY KEY (currently Okta ID like '00u4tk5hnbgct5lQm5d7')
users.user_id     INTEGER  UNIQUE (auto-increment)
users.person_id   INTEGER  FK -> person.person_id (nullable)
users.automation_username VARCHAR (for script/automation users)
```

### Key Statistics
- **Total users**: 1,450
- **Human users** (with person_id): **85** - these will be migrated
- **Automation users** (with automation_username): 1,365 - unchanged

### Person Table
- `person.curie` column exists but is currently **empty** (0 rows populated)
- Need to populate curie values before migration

### Foreign Key Dependencies

The `users.id` column is referenced by `created_by` and `updated_by` columns in **26 tables**:

| Table | FK Count | Affected Rows (created_by) | Affected Rows (updated_by) |
|-------|----------|---------------------------|---------------------------|
| mod_corpus_association | 4 | 60 | 22,462 |
| topic_entity_tag | 4 | 13,275 | 13,204 |
| workflow_tag | 4 | 9,925 | 5,576 |
| cross_reference | 4 | 2,848 | 2,922 |
| referencefile | 4 | 2,226 | 2,234 |
| referencefile_mod | 4 | 2,229 | 2,229 |
| reference | 4 | 39 | 2,704 |
| author | 4 | 203 | 196 |
| workflow_transition | 4 | 134 | 71 |
| person_setting | 2 | 93 | 93 |
| reference_mod_referencetype | 4 | 36 | 74 |
| curation_status | 2 | 30 | 30 |
| topic_entity_tag_source | 4 | 14 | 14 |
| mod | 4 | 9 | 9 |
| indexing_priority | 2 | 0 | 1 |
| person | 2 | 0 | 0 |
| editor | 4 | 0 | 0 |
| resource | 4 | 0 | 0 |
| email | 2 | 0 | 0 |
| dataset | 2 | 0 | 0 |
| manual_indexing_tag | 2 | 0 | 0 |
| person_cross_reference | 2 | 0 | 0 |
| person_name | 2 | 0 | 0 |
| person_note | 2 | 0 | 0 |
| reference_email | 2 | 0 | 0 |
| workflow_tag_topic | 2 | 0 | 0 |

**Note**: Tables showing 4 FK constraints have duplicate constraints (likely from migrations). This should be cleaned up.

### Total Affected Rows by Column Type
- **created_by updates**: ~31,121 rows across all tables
- **updated_by updates**: ~51,819 rows across all tables

---

## Prerequisites

### 1. Populate person.curie Values

Before the migration, all 85 persons linked to users must have their `curie` populated.

**Source options for curie values:**
- MATI identifier service (e.g., `AGR:person0000000001`)
- Or another naming convention determined by the team

**Query to identify persons needing curie:**
```sql
SELECT p.person_id, p.display_name, p.okta_id, u.id as current_user_id
FROM person p
JOIN users u ON u.person_id = p.person_id
WHERE p.curie IS NULL OR p.curie = ''
ORDER BY p.person_id;
```

---

## Migration Strategy

### Recommended Approach: Alembic Migration with Deferred Constraints

Since PostgreSQL doesn't natively support `ON UPDATE CASCADE` for primary key updates via simple ALTER, we need to:

1. **Drop all FK constraints** referencing `users.id`
2. **Update `users.id`** to new curie values
3. **Update all referencing columns** in other tables
4. **Recreate FK constraints**

All within a single transaction for data integrity.

---

## Migration Steps

### Step 1: Create Mapping Table (in migration)

```sql
-- Create temporary mapping of old_id -> new_id
CREATE TEMP TABLE user_id_mapping AS
SELECT u.id AS old_id, p.curie AS new_id
FROM users u
JOIN person p ON u.person_id = p.person_id
WHERE u.person_id IS NOT NULL
  AND p.curie IS NOT NULL
  AND p.curie != '';
```

### Step 2: Drop Foreign Key Constraints

Drop all FK constraints referencing `users.id`. There are 80 constraints across 26 tables.

```sql
-- Example for one table (repeat for all 26 tables)
ALTER TABLE reference DROP CONSTRAINT reference_created_by_fkey;
ALTER TABLE reference DROP CONSTRAINT reference_created_by_fkey1;  -- duplicate
ALTER TABLE reference DROP CONSTRAINT reference_updated_by_fkey;
ALTER TABLE reference DROP CONSTRAINT reference_updated_by_fkey1;  -- duplicate
```

**Full list of constraints to drop:**
```sql
-- author (4)
ALTER TABLE author DROP CONSTRAINT IF EXISTS author_created_by_fkey;
ALTER TABLE author DROP CONSTRAINT IF EXISTS author_created_by_fkey1;
ALTER TABLE author DROP CONSTRAINT IF EXISTS author_updated_by_fkey;
ALTER TABLE author DROP CONSTRAINT IF EXISTS author_updated_by_fkey1;

-- cross_reference (4)
ALTER TABLE cross_reference DROP CONSTRAINT IF EXISTS cross_reference_created_by_fkey;
ALTER TABLE cross_reference DROP CONSTRAINT IF EXISTS cross_reference_created_by_fkey1;
ALTER TABLE cross_reference DROP CONSTRAINT IF EXISTS cross_reference_updated_by_fkey;
ALTER TABLE cross_reference DROP CONSTRAINT IF EXISTS cross_reference_updated_by_fkey1;

-- curation_status (2)
ALTER TABLE curation_status DROP CONSTRAINT IF EXISTS curation_status_created_by_fkey;
ALTER TABLE curation_status DROP CONSTRAINT IF EXISTS curation_status_updated_by_fkey;

-- dataset (2)
ALTER TABLE dataset DROP CONSTRAINT IF EXISTS dataset_created_by_fkey;
ALTER TABLE dataset DROP CONSTRAINT IF EXISTS dataset_updated_by_fkey;

-- editor (4)
ALTER TABLE editor DROP CONSTRAINT IF EXISTS editor_created_by_fkey;
ALTER TABLE editor DROP CONSTRAINT IF EXISTS editor_created_by_fkey1;
ALTER TABLE editor DROP CONSTRAINT IF EXISTS editor_updated_by_fkey;
ALTER TABLE editor DROP CONSTRAINT IF EXISTS editor_updated_by_fkey1;

-- email (2)
ALTER TABLE email DROP CONSTRAINT IF EXISTS email_created_by_fkey;
ALTER TABLE email DROP CONSTRAINT IF EXISTS email_updated_by_fkey;

-- indexing_priority (2)
ALTER TABLE indexing_priority DROP CONSTRAINT IF EXISTS indexing_priority_created_by_fkey;
ALTER TABLE indexing_priority DROP CONSTRAINT IF EXISTS indexing_priority_updated_by_fkey;

-- manual_indexing_tag (2)
ALTER TABLE manual_indexing_tag DROP CONSTRAINT IF EXISTS manual_indexing_tag_created_by_fkey;
ALTER TABLE manual_indexing_tag DROP CONSTRAINT IF EXISTS manual_indexing_tag_updated_by_fkey;

-- mod (4)
ALTER TABLE mod DROP CONSTRAINT IF EXISTS mod_created_by_fkey;
ALTER TABLE mod DROP CONSTRAINT IF EXISTS mod_created_by_fkey1;
ALTER TABLE mod DROP CONSTRAINT IF EXISTS mod_updated_by_fkey;
ALTER TABLE mod DROP CONSTRAINT IF EXISTS mod_updated_by_fkey1;

-- mod_corpus_association (4)
ALTER TABLE mod_corpus_association DROP CONSTRAINT IF EXISTS mod_corpus_association_created_by_fkey;
ALTER TABLE mod_corpus_association DROP CONSTRAINT IF EXISTS mod_corpus_association_created_by_fkey1;
ALTER TABLE mod_corpus_association DROP CONSTRAINT IF EXISTS mod_corpus_association_updated_by_fkey;
ALTER TABLE mod_corpus_association DROP CONSTRAINT IF EXISTS mod_corpus_association_updated_by_fkey1;

-- person (2)
ALTER TABLE person DROP CONSTRAINT IF EXISTS person_created_by_fkey;
ALTER TABLE person DROP CONSTRAINT IF EXISTS person_updated_by_fkey;

-- person_cross_reference (2)
ALTER TABLE person_cross_reference DROP CONSTRAINT IF EXISTS person_cross_reference_created_by_fkey;
ALTER TABLE person_cross_reference DROP CONSTRAINT IF EXISTS person_cross_reference_updated_by_fkey;

-- person_name (2)
ALTER TABLE person_name DROP CONSTRAINT IF EXISTS person_name_created_by_fkey;
ALTER TABLE person_name DROP CONSTRAINT IF EXISTS person_name_updated_by_fkey;

-- person_note (2)
ALTER TABLE person_note DROP CONSTRAINT IF EXISTS person_note_created_by_fkey;
ALTER TABLE person_note DROP CONSTRAINT IF EXISTS person_note_updated_by_fkey;

-- person_setting (2)
ALTER TABLE person_setting DROP CONSTRAINT IF EXISTS person_setting_created_by_fkey;
ALTER TABLE person_setting DROP CONSTRAINT IF EXISTS person_setting_updated_by_fkey;

-- reference (4)
ALTER TABLE reference DROP CONSTRAINT IF EXISTS reference_created_by_fkey;
ALTER TABLE reference DROP CONSTRAINT IF EXISTS reference_created_by_fkey1;
ALTER TABLE reference DROP CONSTRAINT IF EXISTS reference_updated_by_fkey;
ALTER TABLE reference DROP CONSTRAINT IF EXISTS reference_updated_by_fkey1;

-- reference_email (2)
ALTER TABLE reference_email DROP CONSTRAINT IF EXISTS reference_email_created_by_fkey;
ALTER TABLE reference_email DROP CONSTRAINT IF EXISTS reference_email_updated_by_fkey;

-- reference_mod_referencetype (4)
ALTER TABLE reference_mod_referencetype DROP CONSTRAINT IF EXISTS reference_mod_referencetype_created_by_fkey;
ALTER TABLE reference_mod_referencetype DROP CONSTRAINT IF EXISTS reference_mod_referencetype_created_by_fkey1;
ALTER TABLE reference_mod_referencetype DROP CONSTRAINT IF EXISTS reference_mod_referencetype_updated_by_fkey;
ALTER TABLE reference_mod_referencetype DROP CONSTRAINT IF EXISTS reference_mod_referencetype_updated_by_fkey1;

-- referencefile (4)
ALTER TABLE referencefile DROP CONSTRAINT IF EXISTS referencefile_created_by_fkey;
ALTER TABLE referencefile DROP CONSTRAINT IF EXISTS referencefile_created_by_fkey1;
ALTER TABLE referencefile DROP CONSTRAINT IF EXISTS referencefile_updated_by_fkey;
ALTER TABLE referencefile DROP CONSTRAINT IF EXISTS referencefile_updated_by_fkey1;

-- referencefile_mod (4)
ALTER TABLE referencefile_mod DROP CONSTRAINT IF EXISTS referencefile_mod_created_by_fkey;
ALTER TABLE referencefile_mod DROP CONSTRAINT IF EXISTS referencefile_mod_created_by_fkey1;
ALTER TABLE referencefile_mod DROP CONSTRAINT IF EXISTS referencefile_mod_updated_by_fkey;
ALTER TABLE referencefile_mod DROP CONSTRAINT IF EXISTS referencefile_mod_updated_by_fkey1;

-- resource (4)
ALTER TABLE resource DROP CONSTRAINT IF EXISTS resource_created_by_fkey;
ALTER TABLE resource DROP CONSTRAINT IF EXISTS resource_created_by_fkey1;
ALTER TABLE resource DROP CONSTRAINT IF EXISTS resource_updated_by_fkey;
ALTER TABLE resource DROP CONSTRAINT IF EXISTS resource_updated_by_fkey1;

-- topic_entity_tag (4)
ALTER TABLE topic_entity_tag DROP CONSTRAINT IF EXISTS topic_entity_tag_created_by_fkey;
ALTER TABLE topic_entity_tag DROP CONSTRAINT IF EXISTS topic_entity_tag_created_by_fkey1;
ALTER TABLE topic_entity_tag DROP CONSTRAINT IF EXISTS topic_entity_tag_updated_by_fkey;
ALTER TABLE topic_entity_tag DROP CONSTRAINT IF EXISTS topic_entity_tag_updated_by_fkey1;

-- topic_entity_tag_source (4)
ALTER TABLE topic_entity_tag_source DROP CONSTRAINT IF EXISTS topic_entity_tag_source_created_by_fkey;
ALTER TABLE topic_entity_tag_source DROP CONSTRAINT IF EXISTS topic_entity_tag_source_created_by_fkey1;
ALTER TABLE topic_entity_tag_source DROP CONSTRAINT IF EXISTS topic_entity_tag_source_updated_by_fkey;
ALTER TABLE topic_entity_tag_source DROP CONSTRAINT IF EXISTS topic_entity_tag_source_updated_by_fkey1;

-- workflow_tag (4)
ALTER TABLE workflow_tag DROP CONSTRAINT IF EXISTS workflow_tag_created_by_fkey;
ALTER TABLE workflow_tag DROP CONSTRAINT IF EXISTS workflow_tag_created_by_fkey1;
ALTER TABLE workflow_tag DROP CONSTRAINT IF EXISTS workflow_tag_updated_by_fkey;
ALTER TABLE workflow_tag DROP CONSTRAINT IF EXISTS workflow_tag_updated_by_fkey1;

-- workflow_tag_topic (2)
ALTER TABLE workflow_tag_topic DROP CONSTRAINT IF EXISTS workflow_tag_topic_created_by_fkey;
ALTER TABLE workflow_tag_topic DROP CONSTRAINT IF EXISTS workflow_tag_topic_updated_by_fkey;

-- workflow_transition (4)
ALTER TABLE workflow_transition DROP CONSTRAINT IF EXISTS workflow_transition_created_by_fkey;
ALTER TABLE workflow_transition DROP CONSTRAINT IF EXISTS workflow_transition_created_by_fkey1;
ALTER TABLE workflow_transition DROP CONSTRAINT IF EXISTS workflow_transition_updated_by_fkey;
ALTER TABLE workflow_transition DROP CONSTRAINT IF EXISTS workflow_transition_updated_by_fkey1;
```

### Step 3: Update Referencing Columns

Update `created_by` and `updated_by` in all tables using the mapping:

```sql
-- Update all tables with created_by/updated_by referencing old user IDs
UPDATE author SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE author SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE cross_reference SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE cross_reference SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE curation_status SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE curation_status SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE dataset SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE dataset SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE editor SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE editor SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE email SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE email SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE indexing_priority SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE indexing_priority SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE manual_indexing_tag SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE manual_indexing_tag SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE mod SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE mod SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE mod_corpus_association SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE mod_corpus_association SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE person SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE person SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE person_cross_reference SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE person_cross_reference SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE person_name SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE person_name SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE person_note SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE person_note SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE person_setting SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE person_setting SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE reference SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE reference SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE reference_email SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE reference_email SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE reference_mod_referencetype SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE reference_mod_referencetype SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE referencefile SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE referencefile SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE referencefile_mod SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE referencefile_mod SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE resource SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE resource SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE topic_entity_tag SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE topic_entity_tag SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE topic_entity_tag_source SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE topic_entity_tag_source SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE workflow_tag SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE workflow_tag SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE workflow_tag_topic SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE workflow_tag_topic SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;

UPDATE workflow_transition SET created_by = m.new_id FROM user_id_mapping m WHERE created_by = m.old_id;
UPDATE workflow_transition SET updated_by = m.new_id FROM user_id_mapping m WHERE updated_by = m.old_id;
```

### Step 4: Update users.id (Primary Key)

```sql
-- Update the users.id to the new curie value
UPDATE users u
SET id = m.new_id
FROM user_id_mapping m
WHERE u.id = m.old_id;
```

### Step 5: Recreate Foreign Key Constraints

Recreate all FK constraints (only one per column, cleaning up duplicates):

```sql
-- author
ALTER TABLE author ADD CONSTRAINT author_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE author ADD CONSTRAINT author_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- cross_reference
ALTER TABLE cross_reference ADD CONSTRAINT cross_reference_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE cross_reference ADD CONSTRAINT cross_reference_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- curation_status
ALTER TABLE curation_status ADD CONSTRAINT curation_status_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE curation_status ADD CONSTRAINT curation_status_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- dataset
ALTER TABLE dataset ADD CONSTRAINT dataset_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE dataset ADD CONSTRAINT dataset_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- editor
ALTER TABLE editor ADD CONSTRAINT editor_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE editor ADD CONSTRAINT editor_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- email
ALTER TABLE email ADD CONSTRAINT email_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE email ADD CONSTRAINT email_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- indexing_priority
ALTER TABLE indexing_priority ADD CONSTRAINT indexing_priority_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE indexing_priority ADD CONSTRAINT indexing_priority_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- manual_indexing_tag
ALTER TABLE manual_indexing_tag ADD CONSTRAINT manual_indexing_tag_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE manual_indexing_tag ADD CONSTRAINT manual_indexing_tag_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- mod
ALTER TABLE mod ADD CONSTRAINT mod_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE mod ADD CONSTRAINT mod_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- mod_corpus_association
ALTER TABLE mod_corpus_association ADD CONSTRAINT mod_corpus_association_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE mod_corpus_association ADD CONSTRAINT mod_corpus_association_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- person
ALTER TABLE person ADD CONSTRAINT person_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE person ADD CONSTRAINT person_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- person_cross_reference
ALTER TABLE person_cross_reference ADD CONSTRAINT person_cross_reference_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE person_cross_reference ADD CONSTRAINT person_cross_reference_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- person_name
ALTER TABLE person_name ADD CONSTRAINT person_name_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE person_name ADD CONSTRAINT person_name_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- person_note
ALTER TABLE person_note ADD CONSTRAINT person_note_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE person_note ADD CONSTRAINT person_note_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- person_setting
ALTER TABLE person_setting ADD CONSTRAINT person_setting_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE person_setting ADD CONSTRAINT person_setting_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- reference
ALTER TABLE reference ADD CONSTRAINT reference_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE reference ADD CONSTRAINT reference_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- reference_email
ALTER TABLE reference_email ADD CONSTRAINT reference_email_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE reference_email ADD CONSTRAINT reference_email_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- reference_mod_referencetype
ALTER TABLE reference_mod_referencetype ADD CONSTRAINT reference_mod_referencetype_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE reference_mod_referencetype ADD CONSTRAINT reference_mod_referencetype_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- referencefile
ALTER TABLE referencefile ADD CONSTRAINT referencefile_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE referencefile ADD CONSTRAINT referencefile_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- referencefile_mod
ALTER TABLE referencefile_mod ADD CONSTRAINT referencefile_mod_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE referencefile_mod ADD CONSTRAINT referencefile_mod_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- resource
ALTER TABLE resource ADD CONSTRAINT resource_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE resource ADD CONSTRAINT resource_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- topic_entity_tag
ALTER TABLE topic_entity_tag ADD CONSTRAINT topic_entity_tag_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE topic_entity_tag ADD CONSTRAINT topic_entity_tag_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- topic_entity_tag_source
ALTER TABLE topic_entity_tag_source ADD CONSTRAINT topic_entity_tag_source_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE topic_entity_tag_source ADD CONSTRAINT topic_entity_tag_source_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- workflow_tag
ALTER TABLE workflow_tag ADD CONSTRAINT workflow_tag_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE workflow_tag ADD CONSTRAINT workflow_tag_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- workflow_tag_topic
ALTER TABLE workflow_tag_topic ADD CONSTRAINT workflow_tag_topic_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE workflow_tag_topic ADD CONSTRAINT workflow_tag_topic_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);

-- workflow_transition
ALTER TABLE workflow_transition ADD CONSTRAINT workflow_transition_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);
ALTER TABLE workflow_transition ADD CONSTRAINT workflow_transition_updated_by_fkey FOREIGN KEY (updated_by) REFERENCES users(id);
```

---

## Application Code Changes

### 1. Authentication/Login Flow

Update the code that creates/looks up users during login to use `person.curie` instead of Okta ID for human users.

**Files likely affected:**
- Authentication middleware
- User creation/lookup logic
- Any code that uses `users.id` directly

### 2. API Responses

Ensure API responses that return user IDs use the new curie format.

---

## Rollback Plan

If issues occur, the migration can be rolled back by:

1. Keeping a backup of the mapping table
2. Reversing the UPDATE statements using `new_id -> old_id`

```sql
-- Create persistent mapping before migration
CREATE TABLE user_id_migration_backup AS
SELECT u.id AS old_id, p.curie AS new_id
FROM users u
JOIN person p ON u.person_id = p.person_id
WHERE u.person_id IS NOT NULL
  AND p.curie IS NOT NULL;

-- Rollback (if needed)
-- 1. Drop FKs
-- 2. UPDATE users SET id = old_id FROM user_id_migration_backup WHERE id = new_id;
-- 3. UPDATE all referencing tables similarly
-- 4. Recreate FKs
```

---

## Testing Checklist

- [ ] Backup database before migration
- [ ] Verify all 85 persons have curie values populated
- [ ] Run migration on test/dev environment first
- [ ] Verify all FK constraints are recreated
- [ ] Test user login with Okta
- [ ] Test API endpoints that return user data
- [ ] Verify audit trail (created_by/updated_by) displays correctly
- [ ] Run application test suite

---

## Estimated Impact

- **Database downtime**: Minimal (single transaction)
- **Rows updated**: ~83,000 across all tables
- **Migration complexity**: High (due to PK change with many FK references)

---

## Notes

1. **Duplicate FK constraints**: The current database has duplicate FK constraints on many tables (e.g., `author_created_by_fkey` and `author_created_by_fkey1`). This migration is an opportunity to clean these up.

2. **SQLAlchemy model inconsistency**: The `UserModel` in code shows `user_id` as `primary_key=True`, but the database has `id` as the PRIMARY KEY. This should be verified and corrected.

3. **Versioned tables**: Some tables use `__versioned__`. The history tables may also need updates if they store `created_by`/`updated_by` values.
