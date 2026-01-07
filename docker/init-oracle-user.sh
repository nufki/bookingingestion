#!/bin/bash
# Helper script to initialize Oracle user for booking ingestion service
# Run this after the Oracle container is healthy:
# ./init-oracle-user.sh

echo "Waiting for Oracle container to be ready..."
sleep 5

echo "Initializing Oracle user 'books'..."

docker exec -i bookingingestion-oracle sqlplus sys/books@FREEPDB1 as sysdba < init-oracle-user.sql

if [ $? -eq 0 ]; then
    echo "Oracle user 'books' created successfully!"
else
    echo "Failed to create Oracle user. Make sure the container is running and healthy."
    exit 1
fi

