import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

citation_update = r"""
CREATE OR REPLACE PROCEDURE update_citations(
    ref_id reference.reference_id%type
)
as $$
DECLARE
-- Short citation available to A-team system
-- <first author: Last name and initial(s)> (<year>) <resource abbrev> <volume>(<issue>):<page(s)>
-- Empty parts are omitted along with their separators. When the reference has
-- no resource abbreviation, the full resource title is used; when there is no
-- resource at all (e.g. category Internal_Process_Reference), the reference
-- title is used instead.
   sht_citation TEXT default '';
   author_short author.name%type default '';
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
-- <authors>, (<year>) <title>. <journal> <volume>(<issue>):<page(s)>
-- with empty parts and their separators omitted as for the short citation
   long_citation TEXT default '';
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
        AND author.author_order IS NOT NULL
      ORDER BY author.author_order asc
    loop
      authors = CONCAT(authors, get_long_citation_author_string(auth), '; ');
      IF author_short = '' THEN
        author_short = get_short_author_string(auth);
      END IF;
    end loop;
    -- remove the last '; ' from the authors string
    IF authors != '' THEN
      authors := SUBSTRING(authors, 1, LENGTH(authors)-2);
    END IF;
    -- Get the resource abbr
    SELECT res.title_abbreviation, res.title into title_abbr, journal
      FROM reference ref, resource res
      WHERE ref.resource_id = res.resource_id AND
            ref.reference_id = ref_id;
    -- Reference details
    SELECT ref.title, ref.volume, ref.issue_name, ref.page_range, SUBSTRING(ref.date_published, 1,4), ref.citation_id
           into title, volume, issue_name, page_range, ref_year, citation_identifier
      FROM reference ref
      WHERE reference_id = ref_id;
    title := coalesce(title, '');
    volume := coalesce(volume, '');
    issue_name := coalesce(issue_name, '');
    page_range := coalesce(page_range, '');
    ref_year := coalesce(ref_year, '');
    journal := coalesce(journal, '');
    res_abbr := coalesce(title_abbr, '');
    author_short := coalesce(author_short, '');
    -- build the ref_details
    -- <volume>(<issue>):<page(s)>, omitting empty parts and their separators
    ref_details := volume;
    IF issue_name != '' THEN
        ref_details := ref_details || '(' || issue_name || ')';
    END IF;
    IF page_range != '' THEN
        IF ref_details != '' THEN
            ref_details := ref_details || ':' || page_range;
        ELSE
            ref_details := page_range;
        END IF;
    END IF;
    -- Build long citation, only add period after title if it doesn't already end with punctuation
    IF authors != '' THEN
        long_citation := authors || ',';
    END IF;
    IF ref_year != '' THEN
        long_citation := long_citation || ' (' || ref_year || ')';
    END IF;
    IF title != '' THEN
        long_citation := long_citation || ' ' || title;
        IF NOT (RIGHT(title, 1) IN ('.', '?', '!')) THEN
            long_citation := long_citation || '.';
        END IF;
    END IF;
    IF journal != '' THEN
        long_citation := long_citation || ' ' || journal;
    END IF;
    IF ref_details != '' THEN
        long_citation := long_citation || ' ' || ref_details;
    END IF;
    long_citation := LTRIM(long_citation);
    -- raise notice '%', long_citation;
    -- Build short citation; fall back to the full journal title, then the
    -- reference title, when there is no resource abbreviation
    sht_citation := author_short;
    IF ref_year != '' THEN
        sht_citation := sht_citation || ' (' || ref_year || ')';
    END IF;
    IF res_abbr != '' THEN
        sht_citation := sht_citation || ' ' || res_abbr;
    ELSIF journal != '' THEN
        sht_citation := sht_citation || ' ' || journal;
    ELSIF title != '' THEN
        sht_citation := sht_citation || ' ' || title;
    END IF;
    IF ref_details != '' THEN
        sht_citation := sht_citation || ' ' || ref_details;
    END IF;
    sht_citation := LTRIM(sht_citation);
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

get_short_author_string = r"""
CREATE OR REPLACE FUNCTION get_short_author_string(
    author record
)
  RETURNS TEXT
  language plpgsql
as $$
DECLARE
  s_auth author.name%type;
BEGIN
     IF NOT coalesce(author.first_initial, '') = '' THEN
        IF NOT coalesce(author.last_name, '') = '' THEN
            return CONCAT(author.last_name, ' ', author.first_initial);
        END IF;
    END IF;
     IF NOT coalesce(author.first_name, '') = '' THEN
        IF NOT coalesce(author.last_name, '') = '' THEN
            return CONCAT(author.last_name, ' ', author.first_name);
        END IF;
    END IF;
    return CONCAT(author.name, '');
END;
$$;
"""

get_long_citation_author_string = r"""
CREATE OR REPLACE FUNCTION get_long_citation_author_string(
    author record
)
  RETURNS TEXT
  language plpgsql
as $$
DECLARE
  initials TEXT default '';
  first_name_val TEXT;
  word TEXT;
  words TEXT[];
BEGIN
    -- If we have last_name and first_name, build "Last name First initials"
    IF NOT coalesce(author.last_name, '') = '' THEN
        IF NOT coalesce(author.first_name, '') = '' THEN
            first_name_val := author.first_name;
            -- Replace hyphens with spaces to treat hyphenated names as separate words
            first_name_val := REPLACE(first_name_val, '-', ' ');
            -- Split by spaces and get first character of each word
            words := string_to_array(first_name_val, ' ');
            FOREACH word IN ARRAY words
            LOOP
                IF LENGTH(TRIM(word)) > 0 THEN
                    initials := initials || UPPER(LEFT(TRIM(word), 1));
                END IF;
            END LOOP;
            IF LENGTH(initials) > 0 THEN
                return CONCAT(author.last_name, ' ', initials);
            ELSE
                return author.last_name;
            END IF;
        ELSE
            return author.last_name;
        END IF;
    END IF;
    -- Fallback to name if no last_name
    return COALESCE(author.name, '');
END;
$$;
"""

citation_seq = r"""
CREATE OR REPLACE FUNCTION get_next_citation_id()
  RETURNS int
  language plpgsql
as $$
DECLARE
  cit_id integer;
BEGIN
    SELECT into cit_id currval('citation_citation_id_seq');
    return cit_id;
END;
$$;
"""


def add_citation_methods(db_session):
    db_session.execute(text(get_short_author_string))
    db_session.execute(text(get_long_citation_author_string))
    db_session.execute(text(citation_update))
    db_session.execute(text(citation_seq))
    db_session.commit()
