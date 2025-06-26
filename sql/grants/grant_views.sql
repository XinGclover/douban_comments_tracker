-- Grant SELECT on all views in the public schema to looker_user
GRANT CONNECT ON DATABASE zhaoxuelu TO looker_user;

-- Revoke all privileges on all tables and sequences in the public schema from looker_user
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM looker_user;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM looker_user;

-- Grant SELECT on all views in the public schema to looker_user 
DO
$$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_schema = 'public'
    LOOP
        EXECUTE format('GRANT SELECT ON %I.%I TO looker_user;', r.table_schema, r.table_name);
    END LOOP;
END;
$$;


