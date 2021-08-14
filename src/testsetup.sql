-- Run with a superuser
-- > sudo su postgres
-- > psql < setup.sql

DO
$do$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE  rolname = 'riverdb_test') THEN
            CREATE ROLE riverdb_test LOGIN PASSWORD '1234';
            CREATE DATABASE riverdb_test;
            GRANT ALL PRIVILEGES ON DATABASE riverdb_test TO riverdb_test;
            CREATE ROLE riverdb_test_ro LOGIN PASSWORD 'openseasame';
            GRANT CONNECT ON DATABASE riverdb_test TO riverdb_test_ro;
            GRANT USAGE ON SCHEMA public TO riverdb_test_ro;
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO riverdb_test_ro;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
                GRANT SELECT ON TABLES TO riverdb_test_ro;
        END IF;
    END
$do$;