#!/usr/bin/env bash
set -euo pipefail

#############################################
# LOAD ENV
#############################################
if [[ -f ".env" ]]; then
  export $(grep -v '^#' .env | xargs)
fi

#############################################
# CONFIGURATION
#############################################
# Source DB (dump from)
: "${DB_HOST:?DB_HOST not set}"
: "${DB_PORT:=3306}"
: "${DB_USER:?DB_USER not set}"
: "${DB_PASS:?DB_PASS not set}"
: "${DB_NAME:?DB_NAME not set}"
: "${MANAGER_TABLE:=archieve_table_manager}"
: "${TIMESTAMP_COL:=insert_ts}"  
: "${DRY_RUN:=true}"               

# Destination DB (restore to)
: "${RESTORE_DB_HOST:?RESTORE_DB_HOST not set}"
: "${RESTORE_DB_PORT:=3306}"
: "${RESTORE_DB_USER:?RESTORE_DB_USER not set}"
: "${RESTORE_DB_PASS:?RESTORE_DB_PASS not set}"
: "${RESTORE_DB_NAME:?RESTORE_DB_NAME not set}"

# Paths ------------------------------------------------------------------------
WORK_DIR="___working-dir-path___"
LOG_DIR="${WORK_DIR}/logs"
DUMP_DIR="${WORK_DIR}/dumps"
mkdir -p "${LOG_DIR}" "${DUMP_DIR}"

RUN_ID="$(date -u +'%Y%m%dT%H%M%SZ')"
LOG_FILE="${LOG_DIR}/maintenance.log"

log() { echo "[$(date -u +'%F %T')] $*" | tee -a "${LOG_FILE}"; }

MYSQL_SRC="mysql --host=${DB_HOST} --port=${DB_PORT} --database=${DB_NAME} --default-character-set=utf8mb4"
[[ -n "${DB_USER:-}" ]] && MYSQL_SRC+=" --user=${DB_USER}"
[[ -n "${DB_PASS:-}" ]] && MYSQL_SRC+=" --password=${DB_PASS}"

MYSQLDUMP_SRC="mysqldump --host=${DB_HOST} --port=${DB_PORT} --single-transaction --set-gtid-purged=OFF --skip-triggers --default-character-set=utf8mb4"
[[ -n "${DB_USER:-}" ]] && MYSQLDUMP_SRC+=" --user=${DB_USER}"
[[ -n "${DB_PASS:-}" ]] && MYSQLDUMP_SRC+=" --password=${DB_PASS}"

MYSQL_DEST="mysql --host=${RESTORE_DB_HOST} --port=${RESTORE_DB_PORT} --database=${RESTORE_DB_NAME} --default-character-set=utf8mb4"
[[ -n "${RESTORE_DB_USER:-}" ]] && MYSQL_DEST+=" --user=${RESTORE_DB_USER}"
[[ -n "${RESTORE_DB_PASS:-}" ]] && MYSQL_DEST+=" --password=${RESTORE_DB_PASS}"

#archiving time interval -----
ARCHIVE_START="2023-01-01 00:00:00"
ARCHIVE_END=$(date -u -d "30 days ago" +"%Y-%m-%d 00:00:00")
ARCHIVE_TAG=$(date -u +"%Y%m%d")

log "=== Starting Archive + Restore Run: ${RUN_ID} ==="
log "Source DB: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
log "Destination DB: ${RESTORE_DB_HOST}:${RESTORE_DB_PORT}/${RESTORE_DB_NAME}"
log "Manager table: ${MANAGER_TABLE}"
log "Time window: [${ARCHIVE_START} .. ${ARCHIVE_END})"
log "Dry-run: ${DRY_RUN}"
log "Dump location: ${DUMP_DIR}"

#############################################
# FETCH TABLES FROM MANAGER TABLE
#############################################
SQL_LIST="SELECT table_name, key_column FROM \`${MANAGER_TABLE}\`;"
readarray -t ITEMS < <(echo "${SQL_LIST}" | ${MYSQL_SRC} | sed '1d' || true)

[[ ${#ITEMS[@]} -eq 0 ]] && { log "No rows in ${MANAGER_TABLE}. Nothing to do."; exit 0; }

OVERALL_COUNT_ARCHIVED=0

for LINE in "${ITEMS[@]}"; do
  IFS=$'\t' read -r TABLE_NAME KEY_COL <<< "${LINE}"
  [[ -z "${TABLE_NAME}" || -z "${KEY_COL}" ]] && { log "Skipping malformed row: ${LINE}"; continue; }

  IS_VIEW=$(${MYSQL_SRC} -N -e "SELECT TABLE_TYPE FROM information_schema.tables WHERE table_schema='${DB_NAME}' AND table_name='${TABLE_NAME}';" || echo "UNKNOWN")
  [[ "${IS_VIEW}" == "VIEW" ]] && { log "Skipping view: ${TABLE_NAME}"; continue; }

  TABLE_DIR="${DUMP_DIR}/${DB_NAME}/${TABLE_NAME}/${ARCHIVE_TAG}"
  mkdir -p "${TABLE_DIR}"
  DUMP_SQL="${TABLE_DIR}/${TABLE_NAME}_${ARCHIVE_TAG}.sql"

  log "--- Processing ${TABLE_NAME} ---"

  SQL_COUNT="SELECT COUNT(*) FROM \`${TABLE_NAME}\` WHERE \`${TIMESTAMP_COL}\` >= '${ARCHIVE_START}' AND \`${TIMESTAMP_COL}\` < '${ARCHIVE_END}';"
  COUNT_TO_ARCHIVE="$(${MYSQL_SRC} -N -e "${SQL_COUNT}" || echo 0)"
  log "Rows identified for archive: ${COUNT_TO_ARCHIVE}"

  [[ "${COUNT_TO_ARCHIVE}" -eq 0 ]] && { log "Nothing to archive for ${TABLE_NAME}"; continue; }

  log "Creating dump: ${DUMP_SQL}"
  ${MYSQLDUMP_SRC} "${DB_NAME}" "${TABLE_NAME}" --complete-insert --tz-utc --where="${TIMESTAMP_COL} >= '${ARCHIVE_START}' AND ${TIMESTAMP_COL} < '${ARCHIVE_END}'" > "${DUMP_SQL}" || { log "Error dumping ${TABLE_NAME}"; continue; }

  log "Dump created successfully: ${DUMP_SQL}"

  [[ "${DRY_RUN}" == "false" ]] && {
    log "Deleting archived rows from ${TABLE_NAME}..."
    DELETE_SQL="DELETE FROM \`${TABLE_NAME}\` WHERE \`${TIMESTAMP_COL}\` >= '${ARCHIVE_START}' AND \`${TIMESTAMP_COL}\` < '${ARCHIVE_END}';"
    ${MYSQL_SRC} -N -e "${DELETE_SQL}" || log "Error deleting rows from ${TABLE_NAME}"
    log "Deleted ${COUNT_TO_ARCHIVE} rows from ${TABLE_NAME}"
  } || log "[DRY-RUN] Would delete ${COUNT_TO_ARCHIVE} rows from ${TABLE_NAME}"

  #############################################
  # RESTORE TO DESTINATION DB
  #############################################
  log "Restoring ${TABLE_NAME} to destination DB..."
  if ${MYSQL_DEST} < "${DUMP_SQL}"; then
    log "--- Restored ${TABLE_NAME}"
    rm -f "${DUMP_SQL}" && log "--- Deleted dump file: ${DUMP_SQL}"
  else
    log "--- Failed to restore ${TABLE_NAME} (keeping dump file)"
  fi

  OVERALL_COUNT_ARCHIVED=$((OVERALL_COUNT_ARCHIVED + COUNT_TO_ARCHIVE))
done

# Optional: remove empty directories
find "${DUMP_DIR}" -type d -empty -delete && log "--- Cleaned up empty folders"

log "=== Archive + Restore DONE ==="
log "Total archived/restored: ${OVERALL_COUNT_ARCHIVED}"
log "Logs: ${LOG_FILE}"