from sqlalchemy import text

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

# author_update_trigger = """
# DROP TRIGGER IF EXISTS author_citation_trigger on public.author;
# CREATE TRIGGER author_citation_trigger
# AFTER UPDATE OR INSERT OR DELETE ON author
#        FOR EACH ROW
#        EXECUTE FUNCTION public.author_update_citation();
# """

## we can combine insert and delete ones, but will keep these as placeholder
## since we want to re-work on it and combine all three into one later anyway
author_citation_insert_trigger = """
DROP TRIGGER IF EXISTS author_citation_insert_trigger on public.author;
CREATE TRIGGER author_citation_insert_trigger
AFTER INSERT ON author
FOR EACH ROW
EXECUTE FUNCTION public.author_update_citation();
"""

author_citation_delete_trigger = """
DROP TRIGGER IF EXISTS author_citation_delete_trigger on public.author;
CREATE TRIGGER author_citation_delete_trigger
AFTER DELETE ON author
FOR EACH ROW
EXECUTE FUNCTION public.author_update_citation();
"""

author_citation_update_trigger = """
DROP TRIGGER IF EXISTS author_citation_update_trigger on public.author;
CREATE TRIGGER author_citation_update_trigger
AFTER UPDATE ON author
FOR EACH ROW
WHEN (
    NEW.name IS DISTINCT FROM OLD.name OR
    NEW.order IS DISTINCT FROM OLD.order
)
EXECUTE FUNCTION public.author_update_citation();
"""


def add_author_triggers(db_session):
    with db_session.begin:
        db_session.execute(text(author_update_function))
        # db_session.execute(text(author_update_trigger))
        db_session.execute(text(author_citation_insert_trigger))
        db_session.execute(text(author_citation_delete_trigger))
        db_session.execute(text(author_citation_update_trigger))
