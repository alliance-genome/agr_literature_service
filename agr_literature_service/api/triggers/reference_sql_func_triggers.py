from sqlalchemy import text

reference_update_function = """
CREATE OR REPLACE FUNCTION reference_update_citation()
  returns TRIGGER
  language plpgsql
as $$
BEGIN
  raise notice 'reference_update_citation: % % : % %', NEW.reference_id, NEW.title, OLD.title, OLD.curie;
  call update_citations(NEW.reference_id);
  raise notice 'Exiting reference_update_citation';
  return NEW;
  END;
$$;
"""

# reference_update_trigger = """
# DROP TRIGGER IF EXISTS reference_citation_trigger on public.reference;
# CREATE TRIGGER reference_citation_trigger
# AFTER UPDATE OR INSERT ON reference
#        FOR EACH ROW
#        EXECUTE FUNCTION public.reference_update_citation();
# """

reference_citation_insert_trigger = """
DROP TRIGGER IF EXISTS reference_citation_insert_trigger on public.reference;
CREATE TRIGGER reference_citation_insert_trigger
AFTER INSERT ON reference
    FOR EACH ROW
    EXECUTE FUNCTION public.reference_update_citation();
"""

reference_citation_update_trigger = """
DROP TRIGGER IF EXISTS reference_citation_update_trigger on public.reference;
CREATE TRIGGER reference_citation_update_trigger
AFTER UPDATE ON reference
    FOR EACH ROW
    WHEN (
        COALESCE(OLD.title, '') IS DISTINCT FROM COALESCE(NEW.title, '') OR
        COALESCE(OLD.volume, '') IS DISTINCT FROM COALESCE(NEW.volume, '') OR
        COALESCE(OLD.issue_name, '') IS DISTINCT FROM COALESCE(NEW.issue_name, '') OR
        COALESCE(OLD.page_range, '') IS DISTINCT FROM COALESCE(NEW.page_range, '') OR
        COALESCE(OLD.date_published, '') IS DISTINCT FROM COALESCE(NEW.date_published, '') OR
        COALESCE(OLD.resource_id, 0) IS DISTINCT FROM COALESCE(NEW.resource_id, 0)
    )
    EXECUTE FUNCTION public.reference_update_citation();
"""


def add_reference_triggers(db_session):
    with db_session.begin():
        db_session.execute(text(reference_update_function))
        # db_session.execute(text(reference_update_trigger))
        db_session.execute(text(reference_citation_insert_trigger))
        db_session.execute(text(reference_citation_update_trigger))
