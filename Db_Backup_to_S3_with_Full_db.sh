#!/usr/bin/env bash
set -euo pipefail



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


########################################
# CONFIG
########################################
DB_HOST="___"
DB_PORT="3306"
DB_USER="___"
DB_PASS="___"

TIMESTAMP_COL="insert_ts"

AWS_BUCKET="___"
AWS_PREFIX="___"
AWS_REGION="ap-south-1"
STORAGE_CLASS="___"

WORK_DIR="/home/armaan/backup_scripts/utils"
BASE_DUMP_DIR="${WORK_DIR}/db_dumps"
ARCHIVE_TAG="$(TZ='Asia/Kolkata' date +'%d-%m-%Y')"

########################################
# MYSQL COMMANDS
########################################
MYSQL_NO_DB=(mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" --batch --skip-column-names)
MYSQL_DB=(mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" --batch --skip-column-names -D)

MYSQLDUMP=(mysqldump -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" \
  --single-transaction --set-gtid-purged=OFF --no-tablespaces)

########################################
# CHECK AWS AUTH
########################################
aws sts get-caller-identity >/dev/null || {
  echo "ERROR: AWS credentials not configured"
  exit 1
}

########################################
# HELPER: CHECK COLUMN EXISTS
########################################
has_timestamp_column() {
  local db="$1"
  local table="$2"
  local col="$3"

  "${MYSQL_DB[@]}" "$db" -e "
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema='${db}'
      AND table_name='${table}'
      AND column_name='${col}';
  "
}

########################################
# GET DATABASE LIST
########################################
DATABASES=$("${MYSQL_NO_DB[@]}" -e "SHOW DATABASES" \
  | grep -Ev "^(mysql|sys|performance_schema|information_schema)$")

########################################
# PROCESS EACH DATABASE
########################################
for DB_NAME in $DATABASES; do
  echo "=== DATABASE: $DB_NAME ==="

  DB_ROOT="${BASE_DUMP_DIR}/${DB_NAME}"
  mkdir -p "$DB_ROOT"

  ####################################
  # CHECKPOINT TABLE
  ####################################
  "${MYSQL_DB[@]}" "$DB_NAME" -e "
    CREATE TABLE IF NOT EXISTS backup_checkpoint (
      scope ENUM('db','table') NOT NULL,
      object_name VARCHAR(255) NOT NULL,
      last_backup_ts DATETIME NOT NULL,
      PRIMARY KEY (scope, object_name)
    );
  "

  ####################################
  # FULL DATABASE BACKUP
  ####################################
  DB_FULL_DIR="${DB_ROOT}/db/full/${ARCHIVE_TAG}"
  mkdir -p "$DB_FULL_DIR"

  "${MYSQLDUMP[@]}" "$DB_NAME" \
    --routines --triggers --events \
    > "${DB_FULL_DIR}/${DB_NAME}_full_${ARCHIVE_TAG}.sql"

  ####################################
  # INCREMENTAL DATABASE BACKUP (DATA ONLY)
  ####################################
  DB_INC_DIR="${DB_ROOT}/db/incremental/${ARCHIVE_TAG}"
  mkdir -p "$DB_INC_DIR"

  "${MYSQLDUMP[@]}" "$DB_NAME" \
    --no-create-info \
    > "${DB_INC_DIR}/${DB_NAME}_inc_${ARCHIVE_TAG}.sql"

  "${MYSQL_DB[@]}" "$DB_NAME" -e "
    INSERT INTO backup_checkpoint (scope, object_name, last_backup_ts)
    VALUES ('db','$DB_NAME',NOW())
    ON DUPLICATE KEY UPDATE last_backup_ts=NOW();
  "

  ####################################
  # TABLE LIST (EXCLUDE CONTROL TABLE)
  ####################################
  TABLES=$("${MYSQL_DB[@]}" "$DB_NAME" -e "
    SHOW TABLES WHERE Tables_in_${DB_NAME} != 'backup_checkpoint'
  ")

  ####################################
  # TABLE BACKUPS
  ####################################
  for TABLE in $TABLES; do
    echo "  → Table: $TABLE"

    ################################
    # FULL TABLE BACKUP (ALWAYS)
    ################################
    T_FULL_DIR="${DB_ROOT}/tables/full/${TABLE}/${ARCHIVE_TAG}"
    mkdir -p "$T_FULL_DIR"

    "${MYSQLDUMP[@]}" "$DB_NAME" "$TABLE" \
      > "${T_FULL_DIR}/${TABLE}_full_${ARCHIVE_TAG}.sql"

    ################################
    # CHECK TIMESTAMP COLUMN
    ################################
    HAS_COL=$(has_timestamp_column "$DB_NAME" "$TABLE" "$TIMESTAMP_COL")

    if [[ "$HAS_COL" -eq 0 ]]; then
      echo "    ↳ No ${TIMESTAMP_COL} column, skipping incremental"
      continue
    fi

    ################################
    # INCREMENTAL TABLE BACKUP
    ################################
    LAST_TS=$("${MYSQL_DB[@]}" "$DB_NAME" -e "
      SELECT last_backup_ts
      FROM backup_checkpoint
      WHERE scope='table'
        AND object_name='${DB_NAME}.${TABLE}'
    ")

    if [[ -n "$LAST_TS" ]]; then
      T_INC_DIR="${DB_ROOT}/tables/incremental/${TABLE}/${ARCHIVE_TAG}"
      mkdir -p "$T_INC_DIR"

      "${MYSQLDUMP[@]}" "$DB_NAME" "$TABLE" \
        --where="\`${TIMESTAMP_COL}\` > '${LAST_TS}'" \
        > "${T_INC_DIR}/${TABLE}_inc_${ARCHIVE_TAG}.sql"
    fi

    "${MYSQL_DB[@]}" "$DB_NAME" -e "
      INSERT INTO backup_checkpoint (scope, object_name, last_backup_ts)
      VALUES ('table','${DB_NAME}.${TABLE}',NOW())
      ON DUPLICATE KEY UPDATE last_backup_ts=NOW();
    "
  done

  ####################################
  # S3 UPLOAD
  ####################################
  aws s3 sync \
    "${DB_ROOT}" \
    "s3://${AWS_BUCKET}/${AWS_PREFIX}/${DB_NAME}/" \
    --storage-class "${STORAGE_CLASS}"

done

echo "=== ALL DATABASE BACKUPS COMPLETED SUCCESSFULLY ==="