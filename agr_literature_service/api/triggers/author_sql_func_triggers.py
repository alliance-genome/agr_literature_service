from sqlalchemy import text

author_update_function = """
CREATE OR REPLACE FUNCTION lit.author_update_citation()
  returns TRIGGER
  language plpgsql
as $$
BEGIN
    raise notice 'Author update citations for %', NEW;
    raise notice 'OLD Author update citations for %', OLD;
    raise notice 'Author update citations for ref %', NEW.reference_id;
    -- Call the update_citations function based on the reference_id
    PERFORM update_citations(COALESCE(NEW.reference_id, OLD.reference_id));
    return NEW;
END;
$$;
"""

# author_update_trigger = """
# DROP TRIGGER IF EXISTS lit.author_citation_trigger on public.author;
# CREATE TRIGGER lit.author_citation_trigger
# AFTER UPDATE OR INSERT OR DELETE ON lit.author
#        FOR EACH ROW
#        EXECUTE FUNCTION lit.author_update_citation();
# """

## we can combine insert and delete ones, but will keep these as placeholder
## since we want to re-work on it and combine all three into one later anyway
author_citation_insert_trigger = """
DROP TRIGGER IF EXISTS author_citation_insert_trigger on lit.author;
CREATE TRIGGER author_citation_insert_trigger
AFTER INSERT ON lit.author
FOR EACH ROW
EXECUTE FUNCTION lit.author_update_citation();
"""

author_citation_delete_trigger = """
DROP TRIGGER IF EXISTS author_citation_delete_trigger on lit.author;
CREATE TRIGGER author_citation_delete_trigger
AFTER DELETE ON lit.author
FOR EACH ROW
EXECUTE FUNCTION lit.author_update_citation();
"""

author_citation_update_trigger = """
DROP TRIGGER IF EXISTS author_citation_update_trigger on lit.author;
CREATE TRIGGER author_citation_update_trigger
AFTER UPDATE ON lit.author
FOR EACH ROW
WHEN (
    NEW.name IS DISTINCT FROM OLD.name OR
    NEW.order IS DISTINCT FROM OLD.order
)
EXECUTE FUNCTION lit.author_update_citation();
"""


def add_author_triggers(db_session):
    db_session.execute(text(author_update_function))
    # db_session.execute(text(author_update_trigger))
    db_session.execute(text(author_citation_insert_trigger))
    db_session.execute(text(author_citation_delete_trigger))
    db_session.execute(text(author_citation_update_trigger))
    db_session.commit()
