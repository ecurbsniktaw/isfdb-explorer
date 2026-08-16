#!/usr/bin/env bash
# Prepare for a new ISFDB database version.
#
# This script does three things:
#   1. Exports all custom data (users + collections) to a backup file
#   2. Renames the current database to isfdb_prev  (instant — enables quick rollback)
#   3. Creates a fresh empty isfdb ready to receive the new backup
#
# Usage:
#   ./scripts/export_custom_data.sh
#   ./scripts/export_custom_data.sh --db isfdb --user root --pass secret
#
# After this script completes:
#   mysql -u root isfdb < new_isfdb_backup.sql
#   python3 manage.py migrate
#   ./scripts/import_custom_data.sh --input <output file printed below>
#
# ROLLBACK (if something goes wrong after switching):
#   1. Edit isfdb_site/settings.py  — change  'NAME': 'isfdb'  to  'NAME': 'isfdb_prev'
#   2. Restart the server:  sudo systemctl restart gunicorn   (or python3 manage.py runserver)
#   The site will immediately use the old database again.
#
# CLEANUP (once the new database is confirmed working):
#   mysql -u root -e "DROP DATABASE isfdb_prev;"

set -euo pipefail

DB="isfdb"
DB_PREV="${DB}_prev"
MYSQL_USER="root"
MYSQL_PASS="Bpw591968$"
OUTPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)     DB="$2";         DB_PREV="${DB}_prev"; shift 2 ;;
    --user)   MYSQL_USER="$2"; shift 2 ;;
    --pass)   MYSQL_PASS="$2"; shift 2 ;;
    --output) OUTPUT="$2";     shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$OUTPUT" ]]; then
  OUTPUT="custom_data_$(date +%Y%m%d_%H%M%S).sql"
fi

MYSQL_OPTS="-u $MYSQL_USER"
if [[ -n "$MYSQL_PASS" ]]; then
  MYSQL_OPTS="$MYSQL_OPTS -p$MYSQL_PASS"
fi

# ── Step 1: Export custom data ─────────────────────────────────────────────
echo "Step 1: Exporting custom data from '$DB' to '$OUTPUT' ..."

# auth_user: data only (migrate will recreate the table structure)
mysqldump $MYSQL_OPTS --no-create-info --skip-add-drop-table \
  "$DB" auth_user > "$OUTPUT"

# Our custom tables: full DDL + data so they are recreated from scratch
mysqldump $MYSQL_OPTS --add-drop-table \
  "$DB" collection_tokens collection_items user_collection_items >> "$OUTPUT"

echo "  Custom data saved to: $OUTPUT"

# ── Step 2: Rename current database to isfdb_prev ─────────────────────────
echo "Step 2: Renaming '$DB' to '$DB_PREV' (preserving old data for rollback) ..."

# MySQL has no RENAME DATABASE command, so we rename every table across databases.
# This is a metadata-only operation — instant regardless of database size.
mysql $MYSQL_OPTS -e "CREATE DATABASE IF NOT EXISTS \`$DB_PREV\` CHARACTER SET latin1;"

TABLES=$(mysql $MYSQL_OPTS "$DB" -e "SHOW TABLES;" --batch --skip-column-names)
if [[ -n "$TABLES" ]]; then
  RENAME_SQL=""
  for t in $TABLES; do
    RENAME_SQL="$RENAME_SQL RENAME TABLE \`$DB\`.\`$t\` TO \`$DB_PREV\`.\`$t\`;"
  done
  mysql $MYSQL_OPTS -e "$RENAME_SQL"
fi

echo "  '$DB' is now empty and '$DB_PREV' holds the previous data."

# ── Step 3: The empty database is already there — nothing left to do ───────
echo ""
echo "Done. Next steps:"
echo "  1. Load the new ISFDB backup:  mysql $MYSQL_OPTS $DB < new_isfdb_backup.sql"
echo "  2. Run Django migrations:      python3 manage.py migrate"
echo "  3. Restore custom data:        ./scripts/import_custom_data.sh --input $OUTPUT"
echo ""
echo "ROLLBACK if needed:"
echo "  Edit isfdb_site/settings.py: change 'NAME': '$DB' to 'NAME': '$DB_PREV'"
echo "  Then restart the server."
