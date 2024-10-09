import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

citation_update = r"""
CREATE OR REPLACE PROCEDURE lit.update_citations(
    ref_id lit.reference.reference_id%type
)
as $$
DECLARE
-- Short citation available to A-team system
-- <first author: Last name and initial(s)>,
-- <year>,
--   <resource abbrev>
--   <volume>(<issue>):<page(s)>
   sht_citation TEXT default '';
   author_short lit.author.name%type default '';
   s_auth record default NULL;
   ref_year lit.reference.page_range%type;
   res_abbr TEXT default '';
   journal TEXT;
   volume lit.reference.volume%type;
   issue_name lit.reference.issue_name%type;
   page_range lit.reference.page_range%type;
   citation_identifier integer;
   --- build <volume>(<issue>):<page(s)> into ref_details
   ref_details TEXT default '';
   -- used in queries for short
   iso lit.resource.iso_abbreviation%type;
   medline lit.resource.iso_abbreviation%type;
-- Long citation
--  citation = get_citation_from_args(authorNames, year, title, journal,
--                                     ref_db_obj.volume or '',
--                                     ref_db_obj.issue_name or '',
--                                     ref_db_obj.page_range or '')
   long_citation TEXT default '';
   -- volume, issue and page range same as short citation
   title lit.reference.title%type;
   authors lit.author.name%type default '';
   auth record;
BEGIN
    raise notice 'update citations for %', ref_id;
    IF ref_id is NULL THEN
        return;
    END IF;
    -- Also need to update data in short_citation column in the citation table in the database

    for auth in SELECT * FROM lit.author
      WHERE lit.author.reference_id = ref_id
      ORDER BY lit.author.order asc
    loop
      -- raise notice 'Record %', auth;
      authors = CONCAT(authors , auth.name, '; ');
      -- raise notice 'String %', authors;
      IF author_short = '' THEN
        author_short = lit.get_short_author_string(auth);
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
    SELECT res.iso_abbreviation, res.medline_abbreviation, res.title into iso, medline, journal
      FROM lit.reference ref, lit.resource res
      WHERE ref.resource_id = res.resource_id AND
            ref.reference_id = ref_id;
    IF iso is not NULL THEN
        res_abbr := iso;
    ELSIF  medline is not NULL THEN
        res_abbr :=  medline;
    ELSE
        res_abbr := ' ';
    END IF;
    -- Reference details
    SELECT ref.title, ref.volume, ref.issue_name, ref.page_range, SUBSTRING(ref.date_published, 1,4), ref.citation_id
           into title, volume, issue_name, page_range, ref_year, citation_identifier
      FROM lit.reference ref
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
    long_citation := authors || ', (' || ref_year || ') ' || title || '.';
    long_citation := long_citation || ' ' || journal || ' ' || ref_details;
    -- raise notice '%', long_citation;
    sht_citation :=  author_short || ' (' || ref_year || ') ' || res_abbr || ' ' || ref_details;
    -- raise notice '%', sht_citation;
    SELECT citation_id from lit.reference where reference_id = ref_id into citation_identifier;
    raise notice 'citation_id from reference is %', citation_identifier;
    IF citation_identifier is NULL THEN
      -- raise notice 'sh cit: %', sht_citation;
      -- raise notice 'cit: %', long_citation;
      INSERT INTO lit.citation (citation, short_citation) VALUES (long_citation, sht_citation)
             RETURNING citation_id into citation_identifier;
      -- raise notice 'citation inserted new id is %', citation_identifier;
      -- raise notice 'citation_id %', citation_identifier;
      UPDATE lit.reference SET citation_id = citation_identifier WHERE lit.reference.reference_id = ref_id;
    ELSE
      UPDATE lit.citation SET citation = long_citation, short_citation = sht_citation
        WHERE lit.citation.citation_id = citation_identifier;
    END IF;
END $$ language plpgsql;
"""

get_short_author_string = r"""
CREATE OR REPLACE FUNCTION lit.get_short_author_string(
    author record
)
  RETURNS TEXT
  language plpgsql
as $$
DECLARE
  s_auth lit.author.name%type;
BEGIN
     IF NOT coalesce(lit.author.first_initial, '') = '' THEN
        IF NOT coalesce(lit.author.last_name, '') = '' THEN
            return CONCAT(lit.author.last_name, ' ', lit.author.first_initial);
        END IF;
    END IF;
     IF NOT coalesce(lit.author.first_name, '') = '' THEN
        IF NOT coalesce(lit.author.last_name, '') = '' THEN
            return CONCAT(lit.author.last_name, ' ', lit.author.first_name);
        END IF;
    END IF;
    return CONCAT(lit.author.name, '');
END;
$$;
"""

citation_seq = r"""
CREATE OR REPLACE FUNCTION lit.get_next_citation_id()
  RETURNS int
  language plpgsql
as $$
DECLARE
  cit_id integer;
BEGIN
    SELECT into cit_id currval('lit.citation_citation_id_seq');
    raise notice 'cit_id: %', cit_id;
    return cit_id;
END;
$$;
"""


def add_citation_methods(db_session):
    db_session.execute(text(get_short_author_string))
    db_session.execute(text(citation_update))
    db_session.execute(text(citation_seq))
    db_session.commit()
