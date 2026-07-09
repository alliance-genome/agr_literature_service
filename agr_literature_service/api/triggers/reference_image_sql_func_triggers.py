"""
reference_image_sql_func_triggers.py
====================================

SQL functions and triggers that keep the denormalized reference columns
``can_display_image`` and ``image_count`` up to date.

``image_count`` is the number of referencefile rows with a ``file_class``
containing 'figure' for the reference.

``can_display_image`` mirrors the priority logic of
``get_effective_image_permission()`` in
``agr_literature_service/api/crud/reference_crud.py``:

1. Reference-level copyright license (curator/PMC override).
2. Resource-level copyright license, when the publication year meets the
   resource ``license_start_year``.
3. Resource image permission matching the publication year.
4. Default: false.

Because the effective permission depends on data in several tables, triggers
are installed on all of them: reference, referencefile, resource,
copyright_license, resource_image_permission and image_permission. Changes to
the upstream tables fan out and refresh the affected reference rows. The
fan-out updates only touch ``can_display_image``/``image_count``, so they do
not re-fire the reference triggers (their WHEN clauses only watch the
license/resource/date columns).
"""
from sqlalchemy import text

extract_publication_year_function = r"""
CREATE OR REPLACE FUNCTION extract_publication_year(
    p_date_published_start TEXT,
    p_date_published TEXT,
    p_date_published_end TEXT)
RETURNS INTEGER
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    date_value TEXT;
    year_match TEXT;
BEGIN
    -- Same field order and regex as _extract_publication_year() in reference_crud.py
    FOREACH date_value IN ARRAY ARRAY[p_date_published_start, p_date_published, p_date_published_end] LOOP
        IF date_value IS NOT NULL AND date_value <> '' THEN
            year_match := substring(date_value FROM '(1[89]\d{2}|20\d{2})');
            IF year_match IS NOT NULL THEN
                RETURN year_match::INTEGER;
            END IF;
        END IF;
    END LOOP;
    RETURN NULL;
END;
$$;
"""

compute_can_display_image_function = """
CREATE OR REPLACE FUNCTION compute_can_display_image(
    p_copyright_license_id INTEGER,
    p_resource_id INTEGER,
    p_publication_year INTEGER)
RETURNS BOOLEAN
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_open_access BOOLEAN;
    v_license_start_year INTEGER;
    v_resource_open_access BOOLEAN;
    v_can_display BOOLEAN;
BEGIN
    -- Priority 1: reference copyright license (curator/PMC override)
    IF p_copyright_license_id IS NOT NULL THEN
        SELECT cl.open_access INTO v_open_access
        FROM copyright_license cl
        WHERE cl.copyright_license_id = p_copyright_license_id;
        IF FOUND THEN
            RETURN COALESCE(v_open_access, FALSE);
        END IF;
    END IF;

    IF p_resource_id IS NOT NULL THEN
        -- Priority 2: resource copyright license (if publication year >= license_start_year)
        SELECT r.license_start_year, cl.open_access
        INTO v_license_start_year, v_resource_open_access
        FROM resource r
        JOIN copyright_license cl ON cl.copyright_license_id = r.copyright_license_id
        WHERE r.resource_id = p_resource_id;
        IF FOUND AND (v_license_start_year IS NULL
                      OR (p_publication_year IS NOT NULL
                          AND p_publication_year >= v_license_start_year)) THEN
            RETURN COALESCE(v_resource_open_access, FALSE);
        END IF;

        -- Priority 3: resource image permission matching the publication year;
        -- row selection mirrors _resource_image_permission_for_reference()
        SELECT ip.can_display_images INTO v_can_display
        FROM resource_image_permission rip
        JOIN image_permission ip ON ip.image_permission_id = rip.image_permission_id
        WHERE rip.resource_id = p_resource_id
          AND ((p_publication_year IS NULL
                AND rip.start_year IS NULL AND rip.end_year IS NULL)
               OR (p_publication_year IS NOT NULL
                   AND (rip.start_year IS NULL OR rip.start_year <= p_publication_year)
                   AND (rip.end_year IS NULL OR rip.end_year >= p_publication_year)))
        ORDER BY (rip.start_year IS NULL),
                 COALESCE(rip.start_year, 0) DESC,
                 (rip.end_year IS NULL),
                 COALESCE(rip.end_year, 9999),
                 rip.resource_image_permission_id
        LIMIT 1;
        IF FOUND THEN
            RETURN COALESCE(v_can_display, FALSE);
        END IF;
    END IF;

    -- Default: no permission
    RETURN FALSE;
END;
$$;
"""

compute_reference_can_display_image_function = """
CREATE OR REPLACE FUNCTION compute_reference_can_display_image(p_reference_id INTEGER)
RETURNS BOOLEAN
LANGUAGE SQL STABLE
AS $$
    SELECT compute_can_display_image(
        r.copyright_license_id,
        r.resource_id,
        extract_publication_year(r.date_published_start, r.date_published, r.date_published_end))
    FROM reference r
    WHERE r.reference_id = p_reference_id;
$$;
"""

compute_reference_image_count_function = """
CREATE OR REPLACE FUNCTION compute_reference_image_count(p_reference_id INTEGER)
RETURNS INTEGER
LANGUAGE SQL STABLE
AS $$
    SELECT count(*)::INTEGER
    FROM referencefile
    WHERE reference_id = p_reference_id
      AND file_class LIKE '%figure%';
$$;
"""

refresh_can_display_image_for_resource_function = """
CREATE OR REPLACE FUNCTION refresh_can_display_image_for_resource(p_resource_id INTEGER)
RETURNS VOID
LANGUAGE SQL
AS $$
    UPDATE reference r
    SET can_display_image = sub.new_value
    FROM (
        SELECT reference_id, compute_reference_can_display_image(reference_id) AS new_value
        FROM reference
        WHERE resource_id = p_resource_id
    ) sub
    WHERE r.reference_id = sub.reference_id
      AND r.can_display_image IS DISTINCT FROM sub.new_value;
$$;
"""

reference_set_can_display_image_function = """
CREATE OR REPLACE FUNCTION reference_set_can_display_image()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
    NEW.can_display_image := compute_can_display_image(
        NEW.copyright_license_id,
        NEW.resource_id,
        extract_publication_year(NEW.date_published_start, NEW.date_published, NEW.date_published_end));
    RETURN NEW;
END;
$$;
"""

reference_can_display_image_insert_trigger = """
DROP TRIGGER IF EXISTS reference_can_display_image_insert_trigger ON public.reference;
CREATE TRIGGER reference_can_display_image_insert_trigger
BEFORE INSERT ON reference
    FOR EACH ROW
    EXECUTE FUNCTION public.reference_set_can_display_image();
"""

reference_can_display_image_update_trigger = """
DROP TRIGGER IF EXISTS reference_can_display_image_update_trigger ON public.reference;
CREATE TRIGGER reference_can_display_image_update_trigger
BEFORE UPDATE ON reference
    FOR EACH ROW
    WHEN (
        OLD.copyright_license_id IS DISTINCT FROM NEW.copyright_license_id OR
        OLD.resource_id IS DISTINCT FROM NEW.resource_id OR
        OLD.date_published_start IS DISTINCT FROM NEW.date_published_start OR
        OLD.date_published IS DISTINCT FROM NEW.date_published OR
        OLD.date_published_end IS DISTINCT FROM NEW.date_published_end
    )
    EXECUTE FUNCTION public.reference_set_can_display_image();
"""

referencefile_update_image_count_function = """
CREATE OR REPLACE FUNCTION referencefile_update_image_count()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        UPDATE reference
        SET image_count = compute_reference_image_count(NEW.reference_id)
        WHERE reference_id = NEW.reference_id;
    END IF;
    IF TG_OP = 'DELETE'
       OR (TG_OP = 'UPDATE' AND OLD.reference_id IS DISTINCT FROM NEW.reference_id) THEN
        UPDATE reference
        SET image_count = compute_reference_image_count(OLD.reference_id)
        WHERE reference_id = OLD.reference_id;
    END IF;
    RETURN NULL;
END;
$$;
"""

referencefile_image_count_insert_trigger = """
DROP TRIGGER IF EXISTS referencefile_image_count_insert_trigger ON public.referencefile;
CREATE TRIGGER referencefile_image_count_insert_trigger
AFTER INSERT ON referencefile
    FOR EACH ROW
    EXECUTE FUNCTION public.referencefile_update_image_count();
"""

referencefile_image_count_update_trigger = """
DROP TRIGGER IF EXISTS referencefile_image_count_update_trigger ON public.referencefile;
CREATE TRIGGER referencefile_image_count_update_trigger
AFTER UPDATE ON referencefile
    FOR EACH ROW
    WHEN (
        OLD.file_class IS DISTINCT FROM NEW.file_class OR
        OLD.reference_id IS DISTINCT FROM NEW.reference_id
    )
    EXECUTE FUNCTION public.referencefile_update_image_count();
"""

referencefile_image_count_delete_trigger = """
DROP TRIGGER IF EXISTS referencefile_image_count_delete_trigger ON public.referencefile;
CREATE TRIGGER referencefile_image_count_delete_trigger
AFTER DELETE ON referencefile
    FOR EACH ROW
    EXECUTE FUNCTION public.referencefile_update_image_count();
"""

resource_refresh_can_display_image_function = """
CREATE OR REPLACE FUNCTION resource_refresh_can_display_image()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM refresh_can_display_image_for_resource(NEW.resource_id);
    RETURN NULL;
END;
$$;
"""

resource_can_display_image_update_trigger = """
DROP TRIGGER IF EXISTS resource_can_display_image_update_trigger ON public.resource;
CREATE TRIGGER resource_can_display_image_update_trigger
AFTER UPDATE ON resource
    FOR EACH ROW
    WHEN (
        OLD.copyright_license_id IS DISTINCT FROM NEW.copyright_license_id OR
        OLD.license_start_year IS DISTINCT FROM NEW.license_start_year
    )
    EXECUTE FUNCTION public.resource_refresh_can_display_image();
"""

copyright_license_refresh_can_display_image_function = """
CREATE OR REPLACE FUNCTION copyright_license_refresh_can_display_image()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
    -- references that use this license directly
    UPDATE reference r
    SET can_display_image = sub.new_value
    FROM (
        SELECT reference_id, compute_reference_can_display_image(reference_id) AS new_value
        FROM reference
        WHERE copyright_license_id = NEW.copyright_license_id
    ) sub
    WHERE r.reference_id = sub.reference_id
      AND r.can_display_image IS DISTINCT FROM sub.new_value;
    -- references whose resource uses this license
    PERFORM refresh_can_display_image_for_resource(res.resource_id)
    FROM resource res
    WHERE res.copyright_license_id = NEW.copyright_license_id;
    RETURN NULL;
END;
$$;
"""

copyright_license_can_display_image_update_trigger = """
DROP TRIGGER IF EXISTS copyright_license_can_display_image_update_trigger ON public.copyright_license;
CREATE TRIGGER copyright_license_can_display_image_update_trigger
AFTER UPDATE ON copyright_license
    FOR EACH ROW
    WHEN (OLD.open_access IS DISTINCT FROM NEW.open_access)
    EXECUTE FUNCTION public.copyright_license_refresh_can_display_image();
"""

resource_image_permission_refresh_can_display_image_function = """
CREATE OR REPLACE FUNCTION resource_image_permission_refresh_can_display_image()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        PERFORM refresh_can_display_image_for_resource(NEW.resource_id);
    END IF;
    IF TG_OP = 'DELETE'
       OR (TG_OP = 'UPDATE' AND OLD.resource_id IS DISTINCT FROM NEW.resource_id) THEN
        PERFORM refresh_can_display_image_for_resource(OLD.resource_id);
    END IF;
    RETURN NULL;
END;
$$;
"""

resource_image_permission_can_display_image_trigger = """
DROP TRIGGER IF EXISTS resource_image_permission_can_display_image_trigger ON public.resource_image_permission;
CREATE TRIGGER resource_image_permission_can_display_image_trigger
AFTER INSERT OR UPDATE OR DELETE ON resource_image_permission
    FOR EACH ROW
    EXECUTE FUNCTION public.resource_image_permission_refresh_can_display_image();
"""

image_permission_refresh_can_display_image_function = """
CREATE OR REPLACE FUNCTION image_permission_refresh_can_display_image()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM refresh_can_display_image_for_resource(sub.resource_id)
    FROM (
        SELECT DISTINCT resource_id
        FROM resource_image_permission
        WHERE image_permission_id = NEW.image_permission_id
    ) sub;
    RETURN NULL;
END;
$$;
"""

image_permission_can_display_image_update_trigger = """
DROP TRIGGER IF EXISTS image_permission_can_display_image_update_trigger ON public.image_permission;
CREATE TRIGGER image_permission_can_display_image_update_trigger
AFTER UPDATE ON image_permission
    FOR EACH ROW
    WHEN (OLD.can_display_images IS DISTINCT FROM NEW.can_display_images)
    EXECUTE FUNCTION public.image_permission_refresh_can_display_image();
"""

# Ordered so that every function exists before anything that calls it.
reference_image_sql_statements = [
    extract_publication_year_function,
    compute_can_display_image_function,
    compute_reference_can_display_image_function,
    compute_reference_image_count_function,
    refresh_can_display_image_for_resource_function,
    reference_set_can_display_image_function,
    reference_can_display_image_insert_trigger,
    reference_can_display_image_update_trigger,
    referencefile_update_image_count_function,
    referencefile_image_count_insert_trigger,
    referencefile_image_count_update_trigger,
    referencefile_image_count_delete_trigger,
    resource_refresh_can_display_image_function,
    resource_can_display_image_update_trigger,
    copyright_license_refresh_can_display_image_function,
    copyright_license_can_display_image_update_trigger,
    resource_image_permission_refresh_can_display_image_function,
    resource_image_permission_can_display_image_trigger,
    image_permission_refresh_can_display_image_function,
    image_permission_can_display_image_update_trigger,
]


def add_reference_image_triggers(db_session):
    for statement in reference_image_sql_statements:
        db_session.execute(text(statement))
    db_session.commit()
