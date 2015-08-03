#!/bin/bash -eu

# Archive a PostgreSQL WAL file locally, copy it to a remote host, and
# frequently delete those older than 4 days from both locations.
#
# Usage (by PostgreSQL):
# archive_command = 'pg_xlog_archive.sh <db_cluster> "%p" "%f"'
# <db_cluster> = accounting|drivestat|enstore
#
# Requirements:
# - postgresql.conf must have WAL archiving enabled.
# - Enstore config key "PITR_area" for the corresponding server must be
#   available.
# - Local directory corresponding to "PITR_area" must exist and be writable by
#   the user of the database server process.
# - Enstore config for "crons" with keys "backup_node" and "backup_dir" must be
#   available.
# - Remote directory corresponding to "backup_dir" on "backup_node" must exist
#   and be writable.
#
# Related:
# pg_base_backup.sh

source /usr/local/etc/setups.sh
setup enstore

get_value_for_key(){ python -c "import ast,sys; print(dict(ast.literal_eval(sys.stdin.read())))['${1}']" ; }  # Expects dict as stdin.

DB="${1}"
IN="${2}"
OUT_FILE="${3}"
DIR_SUFFIX="pg_xlog_archive/${DB}"

# Get local destination
declare -A CONFIG_KEYS=( [accounting]=accounting_server [drivestat]=drivestat_server [enstore]=database )
DB_CONFIG="$(enstore config --show ${CONFIG_KEYS[${DB}]})"
db_cfg(){ echo "${DB_CONFIG}" | get_value_for_key "${1}" ; }
OUT_DIR="$(db_cfg PITR_area)/${DIR_SUFFIX}"
OUT="${OUT_DIR}/${OUT_FILE}.xz"

# Write local WAL file
mkdir -m 0755 -p "${OUT_DIR}"
xz -2 <"${IN}" >"${OUT}"  # File existence check is intentionally skipped.
# NOTE: Relying on a file existence check would not address uncertainties about
# the correctness of and also the consistency between the existing local and
# remote output files. Skipping the check allows retries; it is unsafe only
# if multiple database clusters write to the same timeline in the same archive
# directory, although this is reliably avoided by setting "archive_command"
# appropriately.
if (( RANDOM % 16 == 0 )); then tmpwatch -f -q -m 4d "${OUT_DIR}"; fi

# Get remote destination
CRONS_CONFIG=$(enstore config --show crons)
crons_cfg(){ echo "$CRONS_CONFIG" | get_value_for_key "${1}" ; }
RNODE="$(crons_cfg backup_node)"
RDIR="$(crons_cfg backup_dir)/${DIR_SUFFIX}"

# Write remote WAL file
enrsh ${RNODE} "mkdir -m 0755 -p \"${RDIR}\""
enrsync "${OUT}" "${RNODE}:${RDIR}"/ >/dev/null 2>&1
if (( RANDOM % 16 == 0 )); then enrsh ${RNODE} "tmpwatch -f -q -m 4d \"${RDIR}\""; fi
