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
reference_update_trigger = """
DROP TRIGGER IF EXISTS reference_citation_trigger on public.reference;
CREATE TRIGGER reference_citation_trigger
AFTER UPDATE OR INSERT ON reference
        FOR EACH ROW
        EXECUTE FUNCTION public.reference_update_citation();
"""


def add_reference_triggers(db_session):
    db_session.execute(reference_update_function)
    db_session.execute(reference_update_trigger)
