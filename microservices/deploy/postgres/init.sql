-- One server, four databases: each service owns exactly one and has no access to the others'.
-- (Logical isolation only. Separate servers would not fit this laptop; see docs/PHASE2_DESIGN.md section 8.)
CREATE DATABASE auth_db;
CREATE DATABASE catalog_db;
CREATE DATABASE reservation_db;
CREATE DATABASE booking_db;
