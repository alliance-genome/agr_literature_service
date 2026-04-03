"""update_citations_procedure

Revision ID: a1b2c3d4e5f6
Revises: 78e9d4f87ed5
Create Date: 2026-04-03

Updates the update_citations stored procedure to use the new
title_abbreviation column instead of the removed iso_abbreviation
and medline_abbreviation columns.
"""
from alembic import op
from sqlalchemy import text

revision = "a1b2c3d4e5f6"
down_revision = "78e9d4f87ed5"
branch_labels = None
depends_on = None


# Updated stored procedure using title_abbreviation
citation_update = r"""
CREATE OR REPLACE PROCEDURE update_citations(
    ref_id reference.reference_id%type
)
as $$
DECLARE
-- Short citation available to A-team system
-- <first author: Last name and initial(s)>,
-- <year>,
--   <resource abbrev>
--   <volume>(<issue>):<page(s)>
   sht_citation TEXT default '';
   author_short author.name%type default '';
   s_auth record default NULL;
   ref_year reference.page_range%type;
   res_abbr TEXT default '';
   journal TEXT;
   volume reference.volume%type;
   issue_name reference.issue_name%type;
   page_range reference.page_range%type;
   citation_identifier integer;
   --- build <volume>(<issue>):<page(s)> into ref_details
   ref_details TEXT default '';
   -- used in queries for short
   title_abbr resource.title_abbreviation%type;
-- Long citation
--  citation = get_citation_from_args(authorNames, year, title, journal,
--                                     ref_db_obj.volume or '',
--                                     ref_db_obj.issue_name or '',
--                                     ref_db_obj.page_range or '')
   long_citation TEXT default '';
   -- volume, issue and page range same as short citation
   title reference.title%type;
   authors author.name%type default '';
   auth record;
BEGIN
    raise notice 'update citations for %', ref_id;
    IF ref_id is NULL THEN
        return;
    END IF;
    -- Also need to update data in short_citation column in the citation table in the database

    for auth in SELECT * FROM author
      WHERE author.reference_id = ref_id
      ORDER BY author.order asc
    loop
      -- raise notice 'Record %', auth;
      authors = CONCAT(authors, get_long_citation_author_string(auth), '; ');
      -- raise notice 'String %', authors;
      IF author_short = '' THEN
        author_short = get_short_author_string(auth);
      END IF;
    end loop;
    -- raise notice 'Author record for short is %', s_auth;
    -- raise notice 'Author for short is %', author_short;
    -- raise notice 'Authors %', authors;
    -- remove the last '; ' from the authors string
    IF authors != '' THEN
      authors := SUBSTRING(authors, 1, LENGTH(authors)-2);
    ELSE
       authors := '';
    END IF;
    -- raise notice 'Authors %', authors;
    -- Get the resource abbr
    SELECT res.title_abbreviation, res.title into title_abbr, journal
      FROM reference ref, resource res
      WHERE ref.resource_id = res.resource_id AND
            ref.reference_id = ref_id;
    IF title_abbr is not NULL THEN
        res_abbr := title_abbr;
    ELSE
        res_abbr := ' ';
    END IF;
    -- Reference details
    SELECT ref.title, ref.volume, ref.issue_name, ref.page_range, SUBSTRING(ref.date_published, 1,4), ref.citation_id
           into title, volume, issue_name, page_range, ref_year, citation_identifier
      FROM reference ref
      WHERE reference_id = ref_id;
    if title is NULL THEN
      title := '';
    END IF;
    if volume is NULL THEN
      volume := '';
    END IF;
    if issue_name is NULL THEN
      issue_name := '';
    END IF;
    if page_range is NULL THEN
      page_range := '';
    END IF;
    if ref_year is NULL THEN
      ref_year := '';
    END IF;
    if journal is NULL THEN
      journal := '';
    END IF;
    -- build the ref_details
    -- <volume>(<issue>):<page(s)>
    ref_details := volume || '(' || issue_name || '):' || page_range;
    -- Build long citation, only add period after title if it doesn't already end with punctuation
    long_citation := authors || ', (' || ref_year || ') ' || title;
    IF title != '' AND NOT (RIGHT(title, 1) IN ('.', '?', '!')) THEN
        long_citation := long_citation || '.';
    END IF;
    long_citation := long_citation || ' ' || journal || ' ' || ref_details;
    -- raise notice '%', long_citation;
    sht_citation :=  author_short || ' (' || ref_year || ') ' || res_abbr || ' ' || ref_details;
    -- raise notice '%', sht_citation;
    SELECT citation_id from reference where reference_id = ref_id into citation_identifier;
    raise notice 'citation_id from reference is %', citation_identifier;
    IF citation_identifier is NULL THEN
      -- raise notice 'sh cit: %', sht_citation;
      -- raise notice 'cit: %', long_citation;
      INSERT INTO citation (citation, short_citation) VALUES (long_citation, sht_citation)
             RETURNING citation_id into citation_identifier;
      -- raise notice 'citation inserted new id is %', citation_identifier;
      -- raise notice 'citation_id %', citation_identifier;
      UPDATE reference SET citation_id = citation_identifier WHERE reference.reference_id = ref_id;
    ELSE
      UPDATE citation SET citation = long_citation, short_citation = sht_citation
        WHERE citation.citation_id = citation_identifier;
    END IF;
END $$ language plpgsql;
"""


def upgrade():
    conn = op.get_bind()
    conn.execute(text(citation_update))


def downgrade():
    # The old procedure would reference columns that no longer exist,
    # so we can't really downgrade this. Just keep the new version.
    pass
