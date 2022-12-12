--
---
 CREATE FUNCTION public.table_notify() RETURNS trigger
     LANGUAGE plpgsql
     AS $$
 DECLARE
   channel TEXT;
   old_row JSON;
   new_row JSON;
   notification JSON;
   xmin BIGINT;
   _primary_keys TEXT [];
   _foreign_keys TEXT [];
 
 BEGIN
     -- database is also the channel name.
     channel := CURRENT_DATABASE();
 
     IF TG_OP = 'DELETE' THEN
 
         SELECT primary_keys
         INTO _primary_keys
         FROM public._view
         WHERE table_name = TG_TABLE_NAME;
 
         old_row = ROW_TO_JSON(OLD);
         old_row := (
             SELECT JSONB_OBJECT_AGG(key, value)
             FROM JSON_EACH(old_row)
             WHERE key = ANY(_primary_keys)
         );
         xmin := OLD.xmin;
     ELSE
         IF TG_OP <> 'TRUNCATE' THEN
 
             SELECT primary_keys, foreign_keys
             INTO _primary_keys, _foreign_keys
             FROM public._view
             WHERE table_name = TG_TABLE_NAME;
 
             new_row = ROW_TO_JSON(NEW);
             new_row := (
                 SELECT JSONB_OBJECT_AGG(key, value)
                 FROM JSON_EACH(new_row)
                 WHERE key = ANY(_primary_keys || _foreign_keys)
             );
             IF TG_OP = 'UPDATE' THEN
                 old_row = ROW_TO_JSON(OLD);
                 old_row := (
                     SELECT JSONB_OBJECT_AGG(key, value)
                     FROM JSON_EACH(old_row)
                     WHERE key = ANY(_primary_keys || _foreign_keys)
                 );
             END IF;
             xmin := NEW.xmin;
         END IF;
     END IF;
 
     -- construct the notification as a JSON object.
     notification = JSON_BUILD_OBJECT(
         'xmin', xmin,
         'new', new_row,
         'old', old_row,
         'tg_op', TG_OP,
         'table', TG_TABLE_NAME,
         'schema', TG_TABLE_SCHEMA
     );
 
     -- Notify/Listen updates occur asynchronously,
     -- so this doesn't block the Postgres trigger procedure.
     PERFORM PG_NOTIFY(channel, notification::TEXT);
 
   RETURN NEW;
 END;
 $$;
 
 
 ALTER FUNCTION public.table_notify() OWNER TO postgres;
 
 SET default_tablespace = '';
 
 SET default_table_access_method = heap;
 
 --
 -- Name: _view; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
 --
 
 CREATE MATERIALIZED VIEW public._view AS
  SELECT t.table_name,
     t.primary_keys,
     t.foreign_keys
    FROM ( VALUES ('authors'::text,ARRAY['author_id'::text],ARRAY['person_id'::text, 'reference_id'::text, 'orcid'::text, 'resource_id'::text]), ('cross_references'::text,ARRAY['curie'::text],ARRAY['reference_id'::text, 'resource_id'::text]), ('references'::text,ARRAY['reference_id'::text],ARRAY['resource_id'::text, 'merged_into_id'::text]), ('resources'::text,ARRAY['resource_id'::text],ARRAY['resource_id'::text]), ('author'::text,ARRAY['author_id'::text],ARRAY['reference_id'::text, 'orcid'::text]), ('cross_reference'::text,ARRAY['curie'::text],ARRAY['reference_id'::text, 'resource_id'::text]), ('reference'::text,ARRAY['reference_id'::text],ARRAY['resource_id'::text, 'merged_into_id'::text]), ('resource'::text,ARRAY['resource_id'::text],ARRAY['resource_id'::text])) t(table_name, primary_keys, foreign_keys)
   WITH NO DATA;
 
 
 ALTER TABLE public._view OWNER TO postgres;

 CREATE UNIQUE INDEX _idx ON public._view USING btree (table_name);
 
 