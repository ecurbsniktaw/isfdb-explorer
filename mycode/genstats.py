import mysql.connector

CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Bpw591968$",
    "database": "isfdb",
}

def get_connection():
    return mysql.connector.connect(**CONFIG)

def get_db_stats(cursor) -> dict:
    """Return live database statistics for the About page."""
    cursor.execute("""
        SELECT
            (SELECT COUNT(DISTINCT author_id) FROM canonical_author)                    AS authors_with_works,
            (SELECT COUNT(*) FROM titles)                                               AS total_titles,
            (SELECT COUNT(*) FROM titles WHERE title_ttype = 'NOVEL')                  AS novels,
            (SELECT COUNT(*) FROM titles WHERE title_ttype = 'SHORTFICTION')           AS short_fiction,
            (SELECT COUNT(*) FROM titles WHERE title_ttype = 'COLLECTION')             AS collections,
            (SELECT COUNT(*) FROM titles WHERE title_ttype = 'ANTHOLOGY')              AS anthologies,
            (SELECT COUNT(*) FROM titles WHERE title_ttype = 'NONFICTION')             AS nonfiction,
            (SELECT COUNT(*) FROM series)                                               AS series_count,
            (SELECT COUNT(*) FROM pubs WHERE pub_ctype = 'MAGAZINE')                   AS magazine_issues,
            (SELECT COUNT(DISTINCT SUBSTRING_INDEX(pub_title, ',', 1))
             FROM pubs WHERE pub_ctype = 'MAGAZINE')                                   AS distinct_magazines,
            (SELECT COUNT(*) FROM pubs WHERE pub_ctype != 'MAGAZINE')                  AS book_pubs,
            (SELECT COUNT(*) FROM publishers)                                           AS publishers,
            (SELECT COUNT(*) FROM pubs
             WHERE pub_frontimage IS NOT NULL AND pub_frontimage != '')                AS pubs_with_images,
            (SELECT COUNT(DISTINCT title_id) FROM webpages
             WHERE title_id IS NOT NULL AND url LIKE '%%wikipedia.org%%')              AS wikipedia_titles,
            (SELECT COUNT(*) FROM award_types)                                          AS award_types,
            (SELECT COUNT(*) FROM awards)                                               AS total_awards,
            (SELECT COUNT(DISTINCT title_id) FROM title_awards WHERE title_id > 0)     AS awarded_titles
    """)
    return cursor.fetchone()

class _DictCursorWrapper:
    """Thin wrapper that makes a Django cursor behave like dictionary=True."""
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, params=None):
        self._cursor.execute(query, params)

    def fetchall(self):
        cols = [col[0] for col in self._cursor.description]
        return [dict(zip(cols, row)) for row in self._cursor.fetchall()]

    def fetchone(self):
        cols = [col[0] for col in self._cursor.description]
        row = self._cursor.fetchone()
        return dict(zip(cols, row)) if row else None

    def close(self):
        self._cursor.close()

def _dict_cursor():
    """Return a Django database cursor that yields rows as dicts."""
    return _DictCursorWrapper(connection.cursor())

def about(request):
    cursor = _dict_cursor()
    try:
        stats = get_db_stats(cursor)
    finally:
        cursor.close()
        # July 2026: added test_value
    return render(request, "magazine/about.html", {
        "stats": stats,
        "snapshot_date": django_settings.ISFDB_SNAPSHOT_DATE,
        "test_value": django_settings.TEST_VALUE,
    })

#--------------------------------------------------------

connection = get_connection()
cursor     = _dict_cursor()
stats      = get_db_stats(cursor)

# with open("setstats.py", "w", encoding="utf-8") as file:
#     for key, value in stats.items():
#         file.write(f'{key.upper()} = "{value}"\n')

with open("setstats.py", "w", encoding="utf-8") as file:
    file.write('ABOUT_STATS = {')

    for key, value in stats.items():
        file.write(f'"{key}": "{value}",\n')

    file.write('}\n')



