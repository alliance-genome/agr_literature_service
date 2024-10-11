from sqlalchemy import text

resource_update_function = """
CREATE OR REPLACE FUNCTION resource_update_citation()
  returns TRIGGER
  language plpgsql
as $$
DECLARE
  refs record;
BEGIN
  raise notice 'resource_update_citation: %', NEW.resource_id;
  for refs in SELECT reference_id FROM reference
      WHERE reference.resource_id = NEW.resource_id
    loop
      raise notice 'Record %', refs;
      call update_citations(refs.reference_id);
    end loop;
  raise notice 'Exiting resource_update_citation';
  return NEW;
  END;
$$;
"""
resource_update_trigger = """
DROP TRIGGER IF EXISTS resource_citation_trigger on public.resource;
CREATE TRIGGER resource_citation_trigger
AFTER UPDATE ON resource
        FOR EACH ROW
        EXECUTE FUNCTION public.resource_update_citation();
"""


def add_resource_triggers(db_session):
    db_session.execute(text(resource_update_function))
    db_session.execute(text(resource_update_trigger))
    db_session.commit()
