docker exec -it bookingingestion-oracle sqlplus books/books@FREEPDB1

-- Check current user
SHOW USER;

-- List tables (should be empty initially)
SELECT table_name FROM user_tables;

-- Verify privileges
SELECT * FROM user_sys_privs;

-- Exit
EXIT;