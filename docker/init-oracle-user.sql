-- Initialize Oracle user for booking ingestion service
-- This script handles the case where the user might already exist

-- Connect to the pluggable database
ALTER SESSION SET CONTAINER = FREEPDB1;

-- Drop user if exists (for clean re-initialization)
BEGIN
   EXECUTE IMMEDIATE 'DROP USER books CASCADE';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -1918 THEN
         RAISE;
      END IF;
END;
/

-- Create the books user
CREATE USER books IDENTIFIED BY books;

-- Grant necessary privileges
GRANT CONNECT, RESOURCE TO books;
GRANT CREATE SESSION TO books;
GRANT CREATE TABLE TO books;
GRANT CREATE SEQUENCE TO books;
GRANT CREATE TRIGGER TO books;

-- Allow unlimited tablespace (for development)
ALTER USER books QUOTA UNLIMITED ON USERS;

COMMIT;
EXIT;

