#!/bin/bash
###############################################################################
#
# $Id$
#
# this script creates read only database role on pnfs database
#
###############################################################################

psql -U postgres -t -c "create role enstore_reader NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN;"
psql -U postgres -c "GRANT USAGE ON SCHEMA public TO enstore_reader"

dbs=`psql  -U enstore -l  -t | sed "/^\s*\:/d;/^\s*$/d" | cut -d"|" -f1`
for db in ${dbs}; do
    psql -U enstore -qAt -c "SELECT 'GRANT SELECT,UPDATE ON ' || relname || ' TO enstore_reader;' FROM pg_class JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE nspname = 'public' AND relkind IN ('r', 'v');"  $db | grep "^GRANT" | psql -U enstore $db
done