citation_update = """
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
   ref_year reference.page_range%type;
   res_abbr TEXT default '';
   journal TEXT;
   volume reference.volume%type;
   issue_name reference.issue_name%type;
   page_range reference.page_range%type;
   citation_identifier integer;
   --- build <volume>(<issue>):<page(s)> into ref_details
   ref_details TEXT default '';
   -- used in querys for short
   iso resource.iso_abbreviation%type;
   medline resource.iso_abbreviation%type;
-- Long citation
--  citation = get_citation_from_args(authorNames, year, title, journal,
--                                     ref_db_obj.volume or '',
--                                     ref_db_obj.issue_name or '',
--                                     ref_db_obj.page_range or '')
   long_citation TEXT default '';
   -- volume, issue and page range same as short citation
   title reference.title%type;
   authors author.name%type default ' ';
   auth record;
BEGIN
    raise notice 'update citations for %', ref_id;
    IF ref_id is NULL THEN
        return;
    END IF;
    -- Get first author for short citation
    SELECT CONCAT(SUBSTRING(author.first_name, 1 ,1), ' ', author.last_name) FROM author into author_short
      WHERE author.reference_id = ref_id AND
            author.first_author = 't'
        LIMIT 1;
    -- raise notice 'auth add is %', author_short;
    IF author_short is NULL THEN
      SELECT CONCAT(SUBSTRING(author.first_name, 1 ,1), ' ', author.last_name) FROM author into author_short
        WHERE author.reference_id = ref_id
          ORDER BY author.author_id
          LIMIT 1;
    END IF;
    IF author_short is NULL THEN
      author_short := ' ';
    END IF;
    -- raise notice 'Author for short is %', author_short;
    -- Get list of authors for long citation
    for auth in SELECT * FROM author
      WHERE author.reference_id = ref_id and
            author.first_author = 't'
    loop
      raise notice 'Record %', auth;
      authors := authors || auth.name || '; ';
    end loop;
    for auth in SELECT * FROM author
      WHERE author.reference_id = ref_id and
            author.first_author is distinct from 't'
      ORDER BY author.author_id asc
    loop
      raise notice 'Record %', auth;
      authors := authors || auth.name || '; ';
    end loop;
    raise notice 'Authors %', authors;
    IF authors != ' ' THEN
      authors := SUBSTRING(authors, 1, LENGTH(authors)-2);
    ELSE
       authors := '';
    END IF;
    -- raise notice 'Authors %', authors;
    -- Get the resource abbr
    SELECT res.iso_abbreviation, res.medline_abbreviation, res.title into iso, medline, journal
      FROM reference ref, resource res
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
    SELECT ref.title, ref.volume, ref.issue_name, ref.page_range, SUBSTRING(ref.date_published, 1,4), ref.citation_id into title, volume, issue_name, page_range, ref_year, citation_identifier
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
    ref_details := volume || ' (' || issue_name || '): ' || page_range;
    raise notice 'rd: %', ref_details;
    raise notice 'tit: %', title;
    long_citation := authors || ', (' || ref_year || ') ' || title;
    long_citation := long_citation || ' ' || journal || ' ' || ref_details;
    raise notice '%', long_citation;
    sht_citation :=  author_short || ', ' || ref_year || ', ' || res_abbr || ', ' || ref_details;
    -- raise notice '%', sht_citation;
    IF citation_identifier is NULL THEN
      raise notice 'sh cit: %', sht_citation;
      raise notice 'cit: %', long_citation;
      INSERT INTO citation (citation, short_citation) VALUES (long_citation, sht_citation);
      citation_identifier := (SELECT currval('citation_citation_id_seq'));
      raise notice 'citation_id %', citation_identifier;
      --RETURNING citation_id into citation_id;
      UPDATE reference SET citation_id = get_next_citation_id() WHERE reference.reference_id = ref_id;
    ELSE
      UPDATE citation SET citation = long_citation, short_citation = sht_citation
        WHERE citation.citation_id = citation_identifier;
    END IF;
    COMMIT;
END $$ language plpgsql;
"""

citation_seq = """
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
    db_session.execute(citation_update)
    db_session.execute(citation_seq)
