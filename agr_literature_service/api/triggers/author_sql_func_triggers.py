
author_update_function = """
CREATE OR REPLACE FUNCTION author_update_citation()
  returns TRIGGER
  language plpgsql
as $$
BEGIN
    raise notice 'Author update citations for %', NEW;
    raise notice 'OLD Author update citations for %', OLD;
    raise notice 'Author update citations for ref %', NEW.reference_id;
    IF NEW.reference_id is NULL THEN
      call update_citations(OLD.reference_id);
    ELSE
      call update_citations(NEW.reference_id);
    END IF;
    return NEW;
END;
$$;
"""

author_update_trigger = """
DROP TRIGGER IF EXISTS author_citation_trigger on public.author;
CREATE TRIGGER author_citation_trigger
AFTER UPDATE OR INSERT OR DELETE ON author
        FOR EACH ROW
        EXECUTE FUNCTION public.author_update_citation();
"""


def add_author_triggers(db_session):
    db_session.execute(author_update_function)
    db_session.execute(author_update_trigger)
