#!/usr/bin/env bash
set -euo pipefail

################################################################################
# SCRIPT: Db_Backup_to_S3.sh
#
# PURPOSE:
#   1. Read the manager table (table_name, key_column)
#   2. Identify rows in each table within a timestamp window
#   3. Dump those rows into a .sql file per table
#   4. Upload dumps to S3 (portable & env-driven)
################################################################################

########################################
# LOAD .env IF PRESENT (robust)
########################################
if [[ -f ".env" ]]; then
  # export all vars defined in .env (ignores comments and blank lines)
  # compatible with values containing spaces and quotes
  set -o allexport
  # shellcheck disable=SC1090
  source <(awk 'BEGIN{FS="#"} /^[[:space:]]*[^#[:space:]]/ {print $1}' .env)
  set +o allexport
fi

################################################################################
# CONFIGURATION (DB defaults)
################################################################################

# Source DB Credentials (can be provided via .env)
: "${DB_HOST:=}"
: "${DB_PORT:=3306}"
: "${DB_USER:=armaan_usr}"
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

# Dump Directory (ensure it exists)
: "${WORK_DIR:=/home/armaan/scripts/utils/dumped_files}"
DUMP_DIR="${WORK_DIR}/dumps"
mkdir -p "${DUMP_DIR}"

################################################################################
# AWS / S3 CONFIG (set via .env or defaults)
# Example .env keys:
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_DEFAULT_REGION=ap-south-1
# S3_BUCKET=testing-archiving-enr
# S3_PREFIX=optional/prefix
# STORAGE_CLASS=STANDARD_IA
# DELETE_LOCAL_AFTER_UPLOAD=false
# RETENTION_DAYS=30
# AWS_PROFILE=optional_profile_name
################################################################################
: "${AWS_DEFAULT_REGION:=ap-south-1}"
: "${S3_BUCKET:=testing-archiving-enr}"
: "${S3_PREFIX:=db-dumps}"
: "${STORAGE_CLASS:=STANDARD_IA}"
: "${DELETE_LOCAL_AFTER_UPLOAD:=false}"
: "${RETENTION_DAYS:=30}"

################################################################################
# CHECK PREREQUISITES
################################################################################
if ! command -v mysql >/dev/null 2>&1; then
  echo "ERROR: 'mysql' client not found." >&2
  exit 1
fi

if ! command -v mysqldump >/dev/null 2>&1; then
  echo "ERROR: 'mysqldump' not found." >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: 'aws' CLI not found. Please install AWS CLI v2." >&2
  exit 1
fi

################################################################################
# AWS Credential check helper
################################################################################
function aws_has_valid_credentials() {
  # optionally set AWS_PROFILE if provided
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    export AWS_PROFILE="${AWS_PROFILE}"
  fi

  # call STS to ensure credentials/role/profile are valid
  if aws sts get-caller-identity --output json >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

# Try to confirm credentials are usable
if ! aws_has_valid_credentials; then
  echo "No valid AWS credentials detected for this environment (env/.aws/profile/instance-role)."
  echo "If you want to provide credentials via .env, add AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
  echo "Or configure a profile with 'aws configure' or attach an instance role to the host."
  echo "Exiting."
  exit 1
fi

AWS_CALLER="$(aws sts get-caller-identity --query 'Arn' --output text || echo 'unknown')"
echo "Authenticated to AWS as: ${AWS_CALLER}"
echo "S3 bucket target: s3://${S3_BUCKET}/${S3_PREFIX}"

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
echo ""

################################################################################
# UPLOAD ALL DUMPED FILES TO S3
################################################################################

# Build S3 destination path (handle optional prefix)
S3_DEST="s3://${S3_BUCKET}"
if [[ -n "${S3_PREFIX// }" && "${S3_PREFIX}" != "''" ]]; then
  # remove leading/trailing slashes then append
  S3_DEST="${S3_DEST%/}/${S3_PREFIX%/}/"
else
  S3_DEST="${S3_DEST%/}/"
fi

echo "=============================================="
echo "  STARTING S3 UPLOAD"
echo "     From: ${DUMP_DIR}"
echo "     To  : ${S3_DEST}"
echo "     Storage class: ${STORAGE_CLASS}"
echo "=============================================="

MAX_RETRIES=1
attempt=0
upload_ok=false

while [[ $attempt -le $MAX_RETRIES ]]; do
  if aws s3 sync "${DUMP_DIR}" "${S3_DEST}" --storage-class "${STORAGE_CLASS}"; then
    upload_ok=true
    break
  else
    echo "S3 sync failed (attempt $((attempt+1))). Retrying..."
    attempt=$((attempt+1))
    sleep 2
  fi
done

if [[ "${upload_ok}" != "true" ]]; then
  echo "ERROR: failed to upload dumps to S3 after $((MAX_RETRIES+1)) attempts." >&2
  exit 1
fi

echo "S3 upload completed successfully."

# Optional: remove local files older than RETENTION_DAYS if configured
if [[ "${DELETE_LOCAL_AFTER_UPLOAD}" == "true" || "${DELETE_LOCAL_AFTER_UPLOAD}" == "1" ]]; then
  echo "Removing local dump files older than ${RETENTION_DAYS} days from ${DUMP_DIR} ..."
  find "${DUMP_DIR}" -type f -mtime +"${RETENTION_DAYS}" -print -delete || true
  echo "Local cleanup finished."
fi

echo "All done."
