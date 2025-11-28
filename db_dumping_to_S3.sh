#!/usr/bin/env bash
set -euo pipefail

################################################################################
# SCRIPT: collect_and_dump.sh
#
# PURPOSE: 
#   1. Read the manager table (table_name, key_column)
#   2. Identify rows in each table within a timestamp window
#   3. Dump those rows into a .sql file per table
#
# ONLY collecting + dumping
################################################################################

########################################
# LOAD .env IF PRESENT
########################################
if [[ -f ".env" ]]; then
  export $(grep -v '^\s*#' .env | xargs) || true
fi

################################################################################
# CONFIGURATION
################################################################################

# Source DB Credentials
: "${DB_HOST:=}"
: "${DB_PORT:=}"
: "${DB_USER:=}"
: "${DB_PASS:=}"
: "${DB_NAME:=}"

# Manager Table
: "${MANAGER_TABLE:=archieve_table_manager}"
: "${TIMESTAMP_COL:=insert_ts}"

# Archive Window
: "${ARCHIVE_START:=2023-01-01 00:00:00}"

if [[ -z "${ARCHIVE_END:-}" ]]; then
  ARCHIVE_END="$(date -u -d "30 days ago" +"%Y-%m-%d 00:00:00")"
fi

# ----------------------------------------
# ⭐ USE IST DATE FORMAT (dd-mm-yyyy)
# ----------------------------------------
ARCHIVE_TAG="$(TZ='Asia/Kolkata' date +'%d-%m-%Y')"

# Dump Directory
: "${WORK_DIR:=/home/pc/scripts/utils/dumped_files}"
DUMP_DIR="${WORK_DIR}/dumps"
mkdir -p "${DUMP_DIR}"

################################################################################
# CHECK MYSQL CLIENTS
################################################################################
if ! command -v mysql >/dev/null 2>&1; then
  echo "ERROR: 'mysql' client not found." >&2
  exit 1
fi

if ! command -v mysqldump >/dev/null 2>&1; then
  echo "ERROR: 'mysqldump' not found." >&2
  exit 1
fi

################################################################################
# MYSQL COMMAND ARRAYS
################################################################################
MYSQL_SRC=(mysql --host="${DB_HOST}" --port="${DB_PORT}" --database="${DB_NAME}" --batch --skip-column-names --default-character-set=utf8mb4)
MYSQLDUMP_SRC=(mysqldump --host="${DB_HOST}" --port="${DB_PORT}" --single-transaction --set-gtid-purged=OFF --skip-triggers --default-character-set=utf8mb4)

[[ -n "${DB_USER:-}" ]] && MYSQL_SRC+=(--user="${DB_USER}") && MYSQLDUMP_SRC+=(--user="${DB_USER}")
[[ -n "${DB_PASS:-}" ]] && MYSQL_SRC+=(--password="${DB_PASS}") && MYSQLDUMP_SRC+=(--password="${DB_PASS}")

################################################################################
# START LOG
################################################################################
echo "=============================================="
echo "  STARTING DATA COLLECTION & DUMP PROCESS"
echo "----------------------------------------------"
echo "Source DB : ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "Manager   : ${MANAGER_TABLE}"
echo "Window    : ${ARCHIVE_START} → ${ARCHIVE_END}"
echo "Dump Path : ${DUMP_DIR}"
echo "Date Tag  : ${ARCHIVE_TAG}"
echo "=============================================="
echo ""

################################################################################
# FETCH TABLE LIST
################################################################################
SQL_LIST="SELECT table_name, key_column FROM \`${MANAGER_TABLE}\`;"
mapfile -t ITEMS < <("${MYSQL_SRC[@]}" -N -e "${SQL_LIST}" || true)

if [[ ${#ITEMS[@]} -eq 0 ]]; then
  echo "No tables found in manager table."
  exit 0
fi

################################################################################
# PROCESS EACH TABLE
################################################################################
for ROW in "${ITEMS[@]}"; do
  IFS=$'\t' read -r TABLE_NAME KEY_COL <<< "${ROW}"

  if [[ -z "${TABLE_NAME}" ]]; then
    echo "Skipping invalid manager row: ${ROW}"
    continue
  fi

  echo "---- TABLE: ${TABLE_NAME} -------------------------------------"

  # Skip Views
  TABLE_TYPE="$("${MYSQL_SRC[@]}" -N -e "SELECT TABLE_TYPE FROM information_schema.tables WHERE table_schema='${DB_NAME}' AND table_name='${TABLE_NAME}';" || echo "")"
  if [[ "${TABLE_TYPE}" == "VIEW" ]]; then
    echo "Skipping view: ${TABLE_NAME}"
    echo ""
    continue
  fi

  # Count rows within date window
  WHERE_CLAUSE="\`${TIMESTAMP_COL}\` >= '${ARCHIVE_START}' AND \`${TIMESTAMP_COL}\` < '${ARCHIVE_END}'"
  COUNT_SQL="SELECT COUNT(*) FROM \`${TABLE_NAME}\` WHERE ${WHERE_CLAUSE};"

  MATCHING_COUNT="$("${MYSQL_SRC[@]}" -N -e "${COUNT_SQL}" || echo 0)"
  MATCHING_COUNT="${MATCHING_COUNT:-0}"

  echo "Rows to archive: ${MATCHING_COUNT}"

  if [[ "${MATCHING_COUNT}" -eq 0 ]]; then
    echo "Nothing to dump"
    echo ""
    continue
  fi

  # Prepare folder path (now uses IST date)
  TARGET_DIR="${DUMP_DIR}/${DB_NAME}/${TABLE_NAME}/${ARCHIVE_TAG}"
  mkdir -p "${TARGET_DIR}"

  # File name also uses IST date
  DUMP_FILE="${TARGET_DIR}/${TABLE_NAME}_${ARCHIVE_TAG}.sql"

  echo "Dumping → ${DUMP_FILE}"

  # Create dump
  if "${MYSQLDUMP_SRC[@]}" "${DB_NAME}" "${TABLE_NAME}" \
      --complete-insert \
      --tz-utc \
      --where="${WHERE_CLAUSE}" > "${DUMP_FILE}"; then
    echo "Dump created successfully."
  else
    echo "ERROR: Dump failed for ${TABLE_NAME}"
    rm -f "${DUMP_FILE}" || true
  fi

  echo ""
done

echo "=============================================="
echo "   DATA COLLECTION + DUMPING COMPLETED"
echo "=============================================="