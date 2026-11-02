-- M0 baseline. Real tables start in M1 (V2__auth.sql, V3__catalog.sql, ...).
-- gen_random_uuid() is built into Postgres 13+, so no extension is needed for UUID keys.
SELECT 1;
