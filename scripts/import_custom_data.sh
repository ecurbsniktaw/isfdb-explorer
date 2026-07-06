#!/usr/bin/env bash
# Restore custom data (users + collections) after loading a new ISFDB database.
#
# Usage:
#   ./scripts/import_custom_data.sh --input custom_data_YYYYMMDD_HHMMSS.sql
#   ./scripts/import_custom_data.sh --input custom_data.sql --db isfdb --user root
#
# Run this AFTER:
#   1. export_custom_data.sh          (saved users/collections, renamed old DB)
#   2. mysql ... isfdb < new_backup.sql  (loaded new ISFDB data)
#   3. python3 manage.py migrate      (recreated Django auth/session tables)
#
# ── ROLLBACK ──────────────────────────────────────────────────────────────
# If anything looks wrong, you can immediately revert to the previous database:
#   1. Edit isfdb_site/settings.py: change 'NAME': 'isfdb' to 'NAME': 'isfdb_prev'
#   2. Restart:  sudo systemctl restart gunicorn   (or python3 manage.py runserver)
#
# Once the new database is confirmed working, free the disk space:
#   mysql -u root -e "DROP DATABASE isfdb_prev;"
# ──────────────────────────────────────────────────────────────────────────

set -euo pipefail

DB="isfdb"
MYSQL_USER="root"
MYSQL_PASS=""
INPUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)    DB="$2";         shift 2 ;;
    --user)  MYSQL_USER="$2"; shift 2 ;;
    --pass)  MYSQL_PASS="$2"; shift 2 ;;
    --input) INPUT="$2";      shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$INPUT" ]]; then
  echo "Error: --input <file> is required."
  exit 1
fi

if [[ ! -f "$INPUT" ]]; then
  echo "Error: input file not found: $INPUT"
  exit 1
fi

MYSQL_OPTS="-u $MYSQL_USER"
if [[ -n "$MYSQL_PASS" ]]; then
  MYSQL_OPTS="$MYSQL_OPTS -p$MYSQL_PASS"
fi

echo "Restoring custom data from '$INPUT' into '$DB' ..."
mysql $MYSQL_OPTS "$DB" < "$INPUT"
echo "Done. Users and collection data restored."
echo ""
echo "Next steps:"
echo "  - Test the site:  python3 manage.py runserver"
echo "  - Check key pages: author, book, issue, My Collection"
echo "  - If deploying to the server:"
echo "      sudo find /var/cache/nginx/isfdb/ -type f -delete"
echo "      sudo systemctl restart gunicorn"
echo "  - Once confirmed working, free disk space:"
echo "      mysql $MYSQL_OPTS -e \"DROP DATABASE ${DB}_prev;\""
