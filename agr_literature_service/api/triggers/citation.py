"""
Use triggers and stored procedures to update citations.
"""
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger


get_next_citation_id = PGFunction(
    schema="public",
    signature="get_next_citation_id()",
    definition="""
returns integer
as $$
BEGIN
    return (SELECT currval('citation_citation_id_seq'));
END $$ language plpgsql;
""")

update_citations = PGFunction(
    schema="public",
    signature="update_citations(ref_id integer)",
    definition="""
returns text
as $$
DECLARE
-- Short citation available to A-team system
--   <first author: Last name and initial(s)>,
--   <year>,
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
    -- Get first author for short citation
    SELECT CONCAT(SUBSTRING(author.first_name, 1 ,1), ' ', author.last_name) FROM author into author_short
      WHERE author.reference_id = ref_id AND
            author.first_author = 't'
        LIMIT 1;
    -- raise notice 'auth add is %', author_short;
    IF author_short is NULL THEN
      SELECT CONCAT(SUBSTRING(author.first_name, 1 ,1), ' ', author.last_name) FROM author into author_short
        WHERE author.reference_id = ref_id AND
              author.first_author = 'f'
          ORDER BY author.author_id
          LIMIT 1;
    END IF;
    IF author_short is NULL THEN
      author_short := ' ';
    END IF;
    -- raise notice 'Author for short is %', author_short;
    -- Get list of authors for long citation
    for auth in SELECT * FROM author
      WHERE author.reference_id = ref_id
    loop
      -- raise notice 'Record %', auth;
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
    return 'DONE';
END $$ language plpgsql;
""")

trgfunc_author_update_citation = PGFunction(
    schema="public",
    signature="trgfunc_author_update_citation()",
    definition="""
returns TRIGGER
as $$
BEGIN
    PERFORM update_citations(NEW.reference_id);
    return NEW;
END $$ language plpgsql;
""")

trg_author_update_citation = PGTrigger(
    schema="public",
    signature="trg_author_update_citation",
    on_entity="public.author",
    is_constraint=False,
    definition="""AFTER UPDATE ON pubic.author
        FOR EACH ROW
        EXECUTE FUNCTION public.trgfunc_author_update_citation()""",
)

trg_author_create_citation = PGTrigger(
    schema="public",
    signature="trg_author_create_citation",
    on_entity="public.author",
    is_constraint=False,
    definition="""AFTER INSERT ON public.author
        FOR EACH ROW
        EXECUTE FUNCTION public.trgfunc_author_update_citation()""",
)

trg_author_delete_citation = PGTrigger(
    schema="public",
    signature="trg_author_delete_citation",
    on_entity="public.author",
    is_constraint=False,
    definition="""AFTER DELETE ON public.author
        FOR EACH ROW
        EXECUTE FUNCTION public.trgfunc_author_update_citation()""",
)

trgfunc_reference_update_citation = PGFunction(
    schema="public",
    signature="trgfunc_reference_update_citation()",
    definition="""
returns TRIGGER
as $$
BEGIN
    raise notice 'trgfunc_reference_update_citation: %: % % %', NEW.reference_id, NEW.title, OLD.title, OLD.curie;
    IF NEW.title != OLD.title OR
       NEW.volume != OLD.volume OR
       NEW.issue_name != OLD.issue_name OR
       NEW.page_range != OLD.page_range OR
       NEW.date_published != OLD.date_published THEN
         raise notice 'Calling update_citations with %', NEW.title;
         PERFORM update_citations(NEW.reference_id);
    ELSE
       raise notice 'Failed update critieria?';
    END IF;
    raise notice 'Exiting trgfunc_reference_update_citation';
    return NEW;
END $$ language plpgsql;
""")

trgfunc_reference_create_citation = PGFunction(
    schema="public",
    signature="trgfunc_reference_create_citation()",
    definition="""
returns TRIGGER
as $$
BEGIN
    raise notice 'trgfunc_reference_create_citation: %: % % %', NEW.reference_id, NEW.title, OLD.title, OLD.curie;
    PERFORM update_citations(NEW.reference_id);
    raise notice 'Exiting trgfunc_reference_create_citation';
    return NEW;
END $$ language plpgsql;
""")

trg_reference_update_citation = PGTrigger(
    schema="public",
    signature="trg_reference_update_citation",
    on_entity="public.reference",
    is_constraint=False,
    definition="""AFTER UPDATE ON reference
        FOR EACH ROW
        EXECUTE FUNCTION public.trgfunc_reference_update_citation()""",
)

trg_reference_create_citation = PGTrigger(
    schema="public",
    signature="trg_reference_create_citation",
    on_entity="public.reference",
    is_constraint=False,
    definition="""AFTER INSERT ON reference
        FOR EACH ROW
        EXECUTE FUNCTION trgfunc_reference_create_citation()""",
)

trgfunc_resource_update_citation = PGFunction(
    schema="public",
    signature="trgfunc_resource_update_citation()",
    definition="""
returns TRIGGER
as $$
BEGIN
    IF NEW.iso_abbreviation != OLD.iso_abbreviation OR
       NEW.medline_abbreviation != OLD.medline_abbreviation OR
       NEW.title != OLD.title THEN
         PERFORM update_citations(NEW.reference_id);
    END IF;
    return NEW;
END $$ language plpgsql;
""")

trg_resource_update_citation = PGTrigger(
    schema="public",
    signature="trg_reference_update_citation",
    on_entity="public.resource",
    is_constraint=False,
    definition="""AFTER UPDATE ON resource
        FOR EACH ROW
        EXECUTE FUNCTION trgfunc_resource_update_citation()""",
)
