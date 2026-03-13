"""
Shared SQL queries for magazine data.

All functions accept a Django database cursor (dictionary mode) and return
lists of dicts.  The same queries power both the CLI scripts and the web views.

Important: pub_year is stored as YYYY-MM-00 (day=0) so Python sees it as None.
Always use YEAR() / MONTH() for filtering — never DATE_FORMAT (the
mysql-connector-python driver sends '%%Y' to MySQL as a literal '%Y' string).
"""

MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

TITLE_TYPE_LABELS = {
    "SHORTFICTION": "Short Fiction",
    "NOVEL":        "Novel",
    "SERIAL":       "Serial",
    "POEM":         "Poem",
    "ESSAY":        "Essay",
    "INTERVIEW":    "Interview",
    "REVIEW":       "Review",
    "NONFICTION":   "Nonfiction",
    "COVERART":     "Cover Art",
    "BACKCOVERART": "Back Cover Art",
    "INTERIORART":  "Interior Art",
    "EDITOR":       "Editor",
    "ANTHOLOGY":    "Anthology",
    "COLLECTION":   "Collection",
    "OMNIBUS":      "Omnibus",
    "CHAPBOOK":     "Chapbook",
}

# Content types shown in the main narrative section
NARRATIVE_TYPES = {
    "SHORTFICTION", "NOVEL", "SERIAL", "POEM",
    "ESSAY", "INTERVIEW", "REVIEW", "NONFICTION", "CHAPBOOK",
}

# Fiction types only (excludes essays/reviews/etc.)
FICTION_TYPES = ("SHORTFICTION", "NOVEL", "SERIAL", "POEM", "CHAPBOOK")


def format_date(year, month) -> str:
    """Convert numeric year + month to a human-readable string."""
    month_name = MONTH_NAMES[month] if month and 1 <= month <= 12 else ""
    return f"{month_name} {year}" if month_name else str(year)


def find_issues(cursor, magazine_name: str, date_filter: str) -> list:
    """
    Return magazine issues whose title contains magazine_name and whose
    date matches date_filter (YYYY or YYYY-MM).

    Returns a list of dicts with keys:
        pub_id, pub_title, pub_year (int), pub_month (int)
    """
    if len(date_filter) == 4 and date_filter.isdigit():
        date_clause = "YEAR(p.pub_year) = %s"
        date_params = (int(date_filter),)
    elif len(date_filter) == 7 and date_filter[4] == "-":
        year, month = date_filter.split("-")
        date_clause = "YEAR(p.pub_year) = %s AND MONTH(p.pub_year) = %s"
        date_params = (int(year), int(month))
    else:
        raise ValueError(f"date must be YYYY or YYYY-MM, got {date_filter!r}")

    query = f"""
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)  AS pub_year,
            MONTH(p.pub_year) AS pub_month
        FROM pubs p
        WHERE p.pub_ctype = 'MAGAZINE'
          AND p.pub_title LIKE %s
          AND {date_clause}
        ORDER BY p.pub_year, MONTH(p.pub_year), p.pub_title
    """
    cursor.execute(query, (f"%{magazine_name}%", *date_params))
    return cursor.fetchall()


def get_issue_meta(cursor, pub_id: int) -> dict | None:
    """Return basic metadata for one issue (pub_id, pub_title, year, month)."""
    query = """
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)  AS pub_year,
            MONTH(p.pub_year) AS pub_month
        FROM pubs p
        WHERE p.pub_id = %s AND p.pub_ctype = 'MAGAZINE'
    """
    cursor.execute(query, (pub_id,))
    return cursor.fetchone()


def get_contents(cursor, pub_id: int) -> list:
    """
    Return the table of contents for a publication, sorted by page number.

    Returns a list of dicts with keys:
        title_id, title_title, title_ttype, title_storylen, pubc_page, authors
    """
    query = """
        SELECT
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_storylen,
            pc.pubc_page,
            GROUP_CONCAT(
                a.author_canonical
                ORDER BY ca.ca_id
                SEPARATOR ' & '
            ) AS authors
        FROM pub_content pc
        JOIN titles t ON pc.title_id = t.title_id
        LEFT JOIN canonical_author ca ON t.title_id = ca.title_id
        LEFT JOIN authors a ON ca.author_id = a.author_id
        WHERE pc.pub_id = %s
        GROUP BY
            t.title_id, t.title_title, t.title_ttype,
            t.title_storylen, pc.pubc_page
        ORDER BY
            CASE
                WHEN pc.pubc_page REGEXP '^[0-9]+$'
                THEN LPAD(pc.pubc_page, 6, '0')
                ELSE pc.pubc_page
            END,
            t.title_title
    """
    cursor.execute(query, (pub_id,))
    rows = cursor.fetchall()

    # Annotate each row with a human-readable type label
    fiction_set = set(FICTION_TYPES)
    for row in rows:
        row["type_label"] = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["is_narrative"] = row["title_ttype"] in NARRATIVE_TYPES
        row["kind"] = "Fiction" if row["title_ttype"] in fiction_set else "Non Fiction"

    return rows


def get_author_fiction(cursor, magazine_name: str, author_name: str) -> list:
    """
    Return all fiction published in a magazine by a given author.

    Uses a two-step approach to avoid slow leading-wildcard LIKE on a large table:
      1. Resolve author_name → author_ids via a fast scan of the authors table.
      2. Main query joins canonical_author by author_id (indexed), avoiding any
         wildcard-driven temp-table materialisation in the main query plan.

    Returns a list of dicts with keys:
        pub_id, pub_title, pub_year (int), pub_month (int),
        title_id, title_title, title_ttype, type_label,
        title_storylen, pubc_page, authors, formatted_date
    """
    # Step 1 — resolve author name to IDs (fast: small table, result cached by MySQL)
    cursor.execute(
        "SELECT author_id FROM authors WHERE author_canonical LIKE %s",
        (f"%{author_name}%",),
    )
    author_ids = [row["author_id"] for row in cursor.fetchall()]
    if not author_ids:
        return []

    # Step 2 — main query using exact author_id IN (...), no wildcard in the join
    id_placeholders   = ", ".join(["%s"] * len(author_ids))
    type_placeholders = ", ".join(["%s"] * len(FICTION_TYPES))

    query = f"""
        SELECT
            MIN(p.pub_id)             AS pub_id,
            MIN(p.pub_title)          AS pub_title,
            YEAR(MIN(p.pub_year))     AS pub_year,
            MONTH(MIN(p.pub_year))    AS pub_month,
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_storylen,
            MIN(pc.pubc_page)         AS pubc_page,
            GROUP_CONCAT(
                DISTINCT a_all.author_canonical
                ORDER BY ca_all.ca_id
                SEPARATOR ' & '
            ) AS authors
        FROM pubs p
        JOIN pub_content pc               ON pc.pub_id    = p.pub_id
        JOIN titles t                     ON t.title_id   = pc.title_id
        JOIN canonical_author ca          ON ca.title_id  = t.title_id
                                         AND ca.author_id IN ({id_placeholders})
        LEFT JOIN canonical_author ca_all ON ca_all.title_id = t.title_id
        LEFT JOIN authors a_all           ON a_all.author_id = ca_all.author_id
        JOIN languages lang               ON lang.lang_id = t.title_language
        WHERE p.pub_ctype = 'MAGAZINE'
          AND p.pub_title LIKE %s
          AND t.title_ttype IN ({type_placeholders})
          AND lang.lang_code = 'eng'
        GROUP BY
            t.title_id, t.title_title, t.title_ttype,
            t.title_storylen
        ORDER BY
            YEAR(MIN(p.pub_year)),
            MONTH(MIN(p.pub_year)),
            CASE
                WHEN MIN(pc.pubc_page) REGEXP '^[0-9]+$'
                THEN LPAD(MIN(pc.pubc_page), 6, '0')
                ELSE MIN(pc.pubc_page)
            END
    """
    cursor.execute(query, (*author_ids, f"%{magazine_name}%", *FICTION_TYPES))
    rows = cursor.fetchall()

    fiction_set = set(FICTION_TYPES)
    for row in rows:
        row["type_label"]     = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["formatted_date"] = format_date(row["pub_year"], row["pub_month"])
        row["kind"] = "Fiction" if row["title_ttype"] in fiction_set else "Non Fiction"

    return rows
