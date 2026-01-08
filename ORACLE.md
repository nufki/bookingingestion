# Oracle Database Reference

## Connection Info
- **Container:** `bookingingestion-oracle`
- **Database:** `FREE` (PDB: `FREEPDB1`)
- **User:** `books` / **Password:** `books`
- **Port:** `1521`

## Connect to Database

```bash
# As application user
docker exec -it bookingingestion-oracle sqlplus books/books@FREEPDB1

# As SYS admin
docker exec -it bookingingestion-oracle sqlplus sys/books@FREEPDB1 as sysdba

# Exit
EXIT;
```

## Common Queries

```sql
-- Show current user
SHOW USER;

-- List tables
SELECT table_name FROM user_tables;

-- Describe table structure
DESC table_name;

-- List all objects
SELECT object_name, object_type FROM user_objects ORDER BY object_type;

-- List sequences
SELECT sequence_name FROM user_sequences;

-- Show privileges
SELECT * FROM user_sys_privs;
```

## Troubleshooting

```bash
# Check container status
docker ps | grep bookingingestion-oracle

# View logs
docker logs bookingingestion-oracle

# Reset database (WARNING: Deletes all data!)
docker compose down
docker volume rm bookingingestion_oracle-data
docker compose up -d
```

## Notes

- Data persists in `oracle-data` Docker volume
- Tables exist after restart because volume is preserved
- User `books` auto-created via `APP_USER` environment variable in docker-compose
- System views (`v$database`) require SYS privileges