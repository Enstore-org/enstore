#!/bin/bash -eu

# Create a local PostgreSQL base backup, copy it to a remote host, and delete
# those older than 4 days from both locations.
#
# Usage (by cron):
# M H * * * enstore pg_base_backup.sh <db_cluster>
# <db_cluster> = accounting|drivestat|enstore
#
# Requirements:
# - postgresql.conf must permit base backup.
# - pg_hba.conf must have replication privilege for the corresponding "dbuser"
#   or for "all".
# - Multiple tablespaces in any database are incompatible with
#   "pg_basebackup --pgdata=- --format=tar".
# - Enstore config keys "PITR_area", "dbport", and "dbuser" for the
#   corresponding server must be available.
# - Local directory corresponding to "PITR_area" must exist and be writable.
# - Enstore config for "crons" with keys "backup_node" and "backup_dir" must be
#   available.
# - Remote directory corresponding to "backup_dir" on "backup_node" must exist
#   and be writable.
#
# Related:
# pg_xlog_archive.sh

source /usr/local/etc/setups.sh
setup enstore

get_value_for_key(){ python -c "import ast,sys; print(dict(ast.literal_eval(sys.stdin.read())))['${1}']" ; }  # Expects dict as stdin.

DB="${1}"
DIR_SUFFIX="pg_base_backup/${DB}"

# Get local destination
declare -A CONFIG_KEYS=( [accounting]=accounting_server [drivestat]=drivestat_server [enstore]=database )
DB_CONFIG="$(enstore config --show ${CONFIG_KEYS[${DB}]})"
db_cfg(){ echo "${DB_CONFIG}" | get_value_for_key "${1}" ; }
OUT_DIR="$(db_cfg PITR_area)/${DIR_SUFFIX}"
OUT="${OUT_DIR}/$(date +%F_%H-%M-%S).tar.xz"

# Write local backup
mkdir -m 0755 -p "${OUT_DIR}"
# Check if pg_basebackup supports "xlog" option
set +e
pg_basebackup --help | grep "\-\-xlog" > /dev/null 2>&1
rc=$?
set -e
if [ ${rc} -ne 0 ]; then
    pg_basebackup --pgdata=- --format=tar --wal-method=fetch --checkpoint=fast --port="$(db_cfg dbport)" --username="$(db_cfg dbuser)" | xz -1 >"${OUT}"
else
    pg_basebackup --pgdata=- --format=tar --xlog --checkpoint=fast --port="$(db_cfg dbport)" --username="$(db_cfg dbuser)" | xz -1 >"${OUT}"
fi

tmpwatch -f -q -m 4d "${OUT_DIR}"

# Get remote destination
CRONS_CONFIG=$(enstore config --show crons)
crons_cfg(){ echo "$CRONS_CONFIG" | get_value_for_key "${1}" ; }
RNODE="$(crons_cfg backup_node)"
RDIR="$(crons_cfg backup_dir)/${DIR_SUFFIX}"

# Write remote backup
enrsh ${RNODE} "mkdir -m 0755 -p \"${RDIR}\""
enrsync "${OUT}" "${RNODE}:${RDIR}"/ >/dev/null 2>&1
enrsh ${RNODE} "tmpwatch -f -q -m 4d \"${RDIR}\""
