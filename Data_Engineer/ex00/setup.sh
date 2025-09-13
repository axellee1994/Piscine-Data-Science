#!/bin/bash

sudo systemctl start postgresql

# To exit on error
set -e

DB_USER="axlee"
DB_NAME="piscineds"
DB_OWNER="axlee"
DB_PASSWORD="mysecretpassword"

echo "Creating database"

# Drop the database if it already exists (optional)
(sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB_NAME;") 2>/dev/null
(sudo -u postgres psql -c "DROP USER IF EXISTS $DB_USER;") 2>/dev/null

(sudo -u postgres psql -c "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';") 2>/dev/null
(sudo -u postgres psql -c "CREATE DATABASE $DB_NAME WITH OWNER $DB_OWNER;") 2>/dev/null
(sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;") 2>/dev/null

echo "Database created"

# To check for the created database and user
# sudo -u postgres psql -c "\l"