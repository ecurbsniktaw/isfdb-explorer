"""
Shared SQL queries for magazine data.

All functions accept a Django database cursor (dictionary mode) and return
lists of dicts.  The same queries power both the CLI scripts and the web views.

Important: pub_year is stored as YYYY-MM-00 (day=0) so Python sees it as None.
Always use YEAR() / MONTH() for filtering — never DATE_FORMAT (the
mysql-connector-python driver sends '%%Y' to MySQL as a literal '%Y' string).
"""

import re
from urllib.parse import urlparse

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

# Human-readable labels for well-known domains used in the webpages table
_WEBPAGE_LABELS = {
    "en.wikipedia.org":        "Wikipedia",
    "imdb.com":                "IMDb",
    "www.imdb.com":            "IMDb",
    "sf-encyclopedia.com":     "SF Encyclopedia",
    "www.sf-encyclopedia.com": "SF Encyclopedia",
    "isfdb.org":               "ISFDB",
    "www.isfdb.org":           "ISFDB",
}


def _clean_author_note(note: str) -> str:
    """Strip ISFDB wiki markup from a biographical note."""
    if not note:
        return ""
    note = re.sub(r"\{\{A\|([^}]+)\}\}", r"\1", note)   # {{A|name}} → name
    note = re.sub(r"\{\{[^}]+\}\}", "", note)             # other {{…}} → drop
    # Rewrite isfdb author page links to local author pages
    note = re.sub(
        r'href="https?://(?:www\.)?isfdb\.org/cgi-bin/ea\.cgi\?(\d+)"',
        r'href="/author/\1/"',
        note,
        flags=re.IGNORECASE,
    )
    # Open remaining external links in a new tab
    note = re.sub(
        r'<a (href="https?://)',
        r'<a target="_blank" rel="noopener" \1',
        note,
        flags=re.IGNORECASE,
    )
    return note.strip()


def _webpage_label(url: str) -> str:
    """Return a short human-readable label for a URL."""
    try:
        domain = urlparse(url).netloc.lower()
        if domain in _WEBPAGE_LABELS:
            return _WEBPAGE_LABELS[domain]
        return domain.removeprefix("www.")
    except Exception:
        return url


def _make_author_list(authors_str, author_ids_str) -> list:
    """
    Convert parallel GROUP_CONCAT strings into a structured list.

    authors_str     e.g. "Isaac Asimov & Robert Silverberg"
    author_ids_str  e.g. "5,54"
    Returns [{'name': str, 'id': int|None}, …]
    """
    if not authors_str:
        return []
    names = authors_str.split(" & ")
    ids   = (author_ids_str or "").split(",")
    result = []
    for i, name in enumerate(names):
        if name.strip():
            raw_id = ids[i].strip() if i < len(ids) else ""
            result.append({
                "name": name.strip(),
                "id":   int(raw_id) if raw_id.isdigit() else None,
            })
    return result


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
            MONTH(p.pub_year) AS pub_month,
            p.pub_frontimage
        FROM pubs p
        WHERE p.pub_id = %s AND p.pub_ctype = 'MAGAZINE'
    """
    cursor.execute(query, (pub_id,))
    return cursor.fetchone()


def get_archive_links(cursor, pub_id: int) -> list:
    """Return archive.org URLs for a publication (may be empty or multiple)."""
    cursor.execute(
        "SELECT url FROM webpages WHERE pub_id = %s AND url LIKE '%%archive.org%%'",
        (pub_id,)
    )
    return [row["url"] for row in cursor.fetchall()]


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
            ) AS authors,
            GROUP_CONCAT(
                a.author_id
                ORDER BY ca.ca_id
                SEPARATOR ','
            ) AS author_ids
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
        row["type_label"]  = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["is_narrative"] = row["title_ttype"] in NARRATIVE_TYPES
        row["kind"]        = "Fiction" if row["title_ttype"] in fiction_set else "Non Fiction"
        row["author_list"] = _make_author_list(row.get("authors"), row.get("author_ids"))

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
            ) AS authors,
            GROUP_CONCAT(
                DISTINCT a_all.author_id
                ORDER BY ca_all.ca_id
                SEPARATOR ','
            ) AS author_ids
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
        row["author_list"]    = _make_author_list(row.get("authors"), row.get("author_ids"))

    return rows


def _earliest_pub_id_expr(alias="sort_key"):
    """
    SQL expression that returns the pub_id of the earliest pub for a grouped set,
    encoding year+month+pub_id into a zero-padded string so MIN() picks the
    right row. Returns a 16-char string: YYYY MM pub_id(10). Use RIGHT(..., 10)
    to extract the pub_id.
    """
    return (
        "RIGHT(MIN(CONCAT("
        "LPAD(YEAR(p.pub_year), 4, '0'),"
        "LPAD(MONTH(p.pub_year), 2, '0'),"
        "LPAD(p.pub_id, 10, '0')"
        ")), 10)"
    )


def get_author_works(cursor, author_id: int) -> list:
    """
    Return all works by a specific author (by ID), in chronological order,
    deduplicated across editions (grouped by title_id).

    pub_id and pub_title are always from the *earliest* publication of each
    title (by pub_year then pub_id), not the alphabetically-first one.
    """
    # Step 1 — get title-level data with the correct earliest pub_id encoded
    # in a sortable CONCAT string. We extract year/month/pub_id from it.
    query = """
        SELECT
            CAST(RIGHT(MIN(CONCAT(
                LPAD(YEAR(p.pub_year), 4, '0'),
                LPAD(MONTH(p.pub_year), 2, '0'),
                LPAD(p.pub_id, 10, '0')
            )), 10) AS UNSIGNED)           AS pub_id,
            CAST(LEFT(MIN(CONCAT(
                LPAD(YEAR(p.pub_year), 4, '0'),
                LPAD(MONTH(p.pub_year), 2, '0'),
                LPAD(p.pub_id, 10, '0')
            )), 4) AS UNSIGNED)            AS pub_year,
            CAST(SUBSTRING(MIN(CONCAT(
                LPAD(YEAR(p.pub_year), 4, '0'),
                LPAD(MONTH(p.pub_year), 2, '0'),
                LPAD(p.pub_id, 10, '0')
            )), 5, 2) AS UNSIGNED)         AS pub_month,
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_storylen,
            MIN(pc.pubc_page)             AS pubc_page,
            GROUP_CONCAT(
                DISTINCT a_all.author_canonical
                ORDER BY ca_all.ca_id
                SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                DISTINCT a_all.author_id
                ORDER BY ca_all.ca_id
                SEPARATOR ','
            ) AS author_ids
        FROM pubs p
        JOIN pub_content pc               ON pc.pub_id    = p.pub_id
        JOIN titles t                     ON t.title_id   = pc.title_id
        JOIN canonical_author ca          ON ca.title_id  = t.title_id
                                         AND ca.author_id = %s
        LEFT JOIN canonical_author ca_all ON ca_all.title_id = t.title_id
        LEFT JOIN authors a_all           ON a_all.author_id = ca_all.author_id
        JOIN languages lang               ON lang.lang_id = t.title_language
        WHERE p.pub_ctype = 'MAGAZINE'
          AND lang.lang_code = 'eng'
        GROUP BY
            t.title_id, t.title_title, t.title_ttype, t.title_storylen
        ORDER BY
            pub_year, pub_month, t.title_title
    """
    cursor.execute(query, (author_id,))
    rows = cursor.fetchall()

    if not rows:
        return rows

    # Step 2 — bulk-fetch pub_titles for the earliest pub_ids in one query
    pub_ids = [row["pub_id"] for row in rows]
    placeholders = ", ".join(["%s"] * len(pub_ids))
    cursor.execute(
        f"SELECT pub_id, pub_title FROM pubs WHERE pub_id IN ({placeholders})",
        pub_ids,
    )
    pub_title_map = {r["pub_id"]: r["pub_title"] for r in cursor.fetchall()}

    fiction_set = set(FICTION_TYPES)
    for row in rows:
        row["pub_title"]      = pub_title_map.get(row["pub_id"], "")
        row["type_label"]     = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["formatted_date"] = format_date(row["pub_year"], row["pub_month"])
        row["kind"]           = "Fiction" if row["title_ttype"] in fiction_set else "Non Fiction"
        row["author_list"]    = _make_author_list(row.get("authors"), row.get("author_ids"))

    return rows


BOOK_TYPES = ("NOVEL", "COLLECTION", "ANTHOLOGY", "OMNIBUS", "NONFICTION", "CHAPBOOK")


def get_author_books(cursor, author_id: int) -> list:
    """
    Return all English-language books by a specific author (by ID), deduplicated
    across editions (grouped by title_id), in chronological order.

    Only includes the 'parent' title for each pub (where title_ttype = pub_ctype).

    Returns a list of dicts with keys:
        pub_id, pub_year (int), pub_month (int), title_id, title_title,
        title_ttype, type_label, formatted_date, authors, author_list, edition_count
    """
    type_placeholders = ", ".join(["%s"] * len(BOOK_TYPES))
    query = f"""
        SELECT
            MIN(p.pub_id)             AS pub_id,
            YEAR(MIN(p.pub_year))     AS pub_year,
            MONTH(MIN(p.pub_year))    AS pub_month,
            t.title_id,
            t.title_title,
            t.title_ttype,
            COUNT(DISTINCT p.pub_id)  AS edition_count,
            GROUP_CONCAT(
                DISTINCT a_all.author_canonical
                ORDER BY ca_all.ca_id
                SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                DISTINCT a_all.author_id
                ORDER BY ca_all.ca_id
                SEPARATOR ','
            ) AS author_ids
        FROM pubs p
        JOIN pub_content pc               ON pc.pub_id   = p.pub_id
        JOIN titles t                     ON t.title_id  = pc.title_id
                                         AND t.title_ttype = p.pub_ctype
        JOIN canonical_author ca          ON ca.title_id = t.title_id
                                         AND ca.author_id = %s
        LEFT JOIN canonical_author ca_all ON ca_all.title_id = t.title_id
        LEFT JOIN authors a_all           ON a_all.author_id = ca_all.author_id
        JOIN languages lang               ON lang.lang_id = t.title_language
        WHERE p.pub_ctype IN ({type_placeholders})
          AND lang.lang_code = 'eng'
          AND YEAR(p.pub_year) > 0
        GROUP BY t.title_id, t.title_title, t.title_ttype
        ORDER BY YEAR(MIN(p.pub_year)), t.title_title
    """
    cursor.execute(query, (author_id, *BOOK_TYPES))
    rows = cursor.fetchall()

    for row in rows:
        row["type_label"]     = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["formatted_date"] = str(row["pub_year"]) if row["pub_year"] else ""
        row["author_list"]    = _make_author_list(row.get("authors"), row.get("author_ids"))

    return rows


_PUB_PTYPE_LABELS = {
    "hc":      "Hardcover",
    "pb":      "Paperback",
    "tp":      "Trade Paperback",
    "ebook":   "eBook",
    "digest":  "Digest",
    "ph":      "Pamphlet",
    "audio":   "Audio",
    "unknown": "Unknown",
}


def get_book_detail(cursor, title_id: int) -> dict | None:
    """
    Return details for the first English-language publication of a given title_id.
    Includes cover image, pub date, catalog id, publisher, format, and cover artist.
    """
    type_placeholders = ", ".join(["%s"] * len(BOOK_TYPES))
    query = f"""
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)   AS pub_year,
            MONTH(p.pub_year)  AS pub_month,
            p.pub_catalog,
            p.pub_isbn,
            p.pub_ptype,
            p.pub_pages,
            p.pub_frontimage,
            pub.publisher_name,
            n.note_note        AS pub_note,
            tn.note_note       AS title_note,
            t.title_id,
            t.title_title,
            t.title_ttype,
            GROUP_CONCAT(
                DISTINCT a_all.author_canonical
                ORDER BY ca_all.ca_id
                SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                DISTINCT a_all.author_id
                ORDER BY ca_all.ca_id
                SEPARATOR ','
            ) AS author_ids,
            GROUP_CONCAT(
                DISTINCT a_cv.author_canonical
                ORDER BY ca_cv.ca_id
                SEPARATOR ' & '
            ) AS cover_artist,
            GROUP_CONCAT(
                DISTINCT a_cv.author_id
                ORDER BY ca_cv.ca_id
                SEPARATOR ','
            ) AS cover_artist_ids
        FROM pubs p
        JOIN pub_content pc           ON pc.pub_id  = p.pub_id
        JOIN titles t                 ON t.title_id = pc.title_id
                                     AND t.title_ttype = p.pub_ctype
        LEFT JOIN publishers pub      ON pub.publisher_id = p.publisher_id
        LEFT JOIN notes n             ON n.note_id = p.note_id
        LEFT JOIN notes tn            ON tn.note_id = t.note_id
        LEFT JOIN canonical_author ca_all ON ca_all.title_id = t.title_id
        LEFT JOIN authors a_all       ON a_all.author_id = ca_all.author_id
        LEFT JOIN pub_content pc_cv   ON pc_cv.pub_id = p.pub_id
        LEFT JOIN titles t_cv         ON t_cv.title_id = pc_cv.title_id
                                     AND t_cv.title_ttype = 'COVERART'
        LEFT JOIN canonical_author ca_cv ON ca_cv.title_id = t_cv.title_id
        LEFT JOIN authors a_cv        ON a_cv.author_id = ca_cv.author_id
        JOIN languages lang           ON lang.lang_id = t.title_language
        WHERE t.title_id = %s
          AND p.pub_ctype IN ({type_placeholders})
          AND lang.lang_code = 'eng'
          AND YEAR(p.pub_year) > 0
        GROUP BY p.pub_id, p.pub_title, p.pub_year,
                 p.pub_catalog, p.pub_isbn, p.pub_ptype, p.pub_pages, p.pub_frontimage,
                 pub.publisher_name, n.note_note, tn.note_note, t.title_id, t.title_title, t.title_ttype
        ORDER BY p.pub_year, p.pub_id
        LIMIT 1
    """
    cursor.execute(query, (title_id, *BOOK_TYPES))
    row = cursor.fetchone()
    if not row:
        return None

    row["type_label"]        = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
    row["format_label"]      = _PUB_PTYPE_LABELS.get(row["pub_ptype"] or "", row["pub_ptype"] or "")
    row["formatted_date"]    = str(row["pub_year"]) if row["pub_year"] else ""
    row["author_list"]       = _make_author_list(row.get("authors"), row.get("author_ids"))
    row["cover_artist_list"] = _make_author_list(row.get("cover_artist"), row.get("cover_artist_ids"))

    # External links
    cursor.execute("SELECT url FROM webpages WHERE title_id = %s ORDER BY webpage_id", (title_id,))
    row["webpages"] = [
        {"url": r["url"], "label": _webpage_label(r["url"])}
        for r in cursor.fetchall()
        if r.get("url")
    ]

    # Awards
    _AWARD_LEVEL = {"1": "Winner", "2": "Runner-up"}
    cursor.execute("""
        SELECT at2.award_type_name, ac.award_cat_name, a.award_level
        FROM title_awards ta
        JOIN awards a      ON a.award_id      = ta.award_id
        JOIN award_types at2 ON at2.award_type_id = a.award_type_id
        JOIN award_cats ac ON ac.award_cat_id  = a.award_cat_id
        WHERE ta.title_id = %s
        ORDER BY at2.award_type_name, ac.award_cat_name
    """, (title_id,))
    row["awards"] = [
        {
            "award_name": r["award_type_name"],
            "category":   r["award_cat_name"],
            "level":      _AWARD_LEVEL.get(str(r["award_level"]), "Nominee/Finalist"),
        }
        for r in cursor.fetchall()
    ]

    return row


def get_book_editions(cursor, title_id: int, exclude_pub_id: int) -> list:
    """
    Return all English-language editions of a title except the one already
    shown as the first/primary edition.
    """
    type_placeholders = ", ".join(["%s"] * len(BOOK_TYPES))
    query = f"""
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)  AS pub_year,
            p.pub_catalog,
            p.pub_ptype,
            p.pub_pages,
            pub.publisher_name,
            t.title_ttype,
            GROUP_CONCAT(
                DISTINCT a_cv.author_canonical
                ORDER BY ca_cv.ca_id
                SEPARATOR ' & '
            ) AS cover_artist,
            GROUP_CONCAT(
                DISTINCT a_cv.author_id
                ORDER BY ca_cv.ca_id
                SEPARATOR ','
            ) AS cover_artist_ids
        FROM pubs p
        JOIN pub_content pc        ON pc.pub_id  = p.pub_id
        JOIN titles t              ON t.title_id = pc.title_id
                                  AND t.title_ttype = p.pub_ctype
        LEFT JOIN publishers pub   ON pub.publisher_id = p.publisher_id
        LEFT JOIN pub_content pc_cv  ON pc_cv.pub_id = p.pub_id
        LEFT JOIN titles t_cv        ON t_cv.title_id = pc_cv.title_id
                                    AND t_cv.title_ttype = 'COVERART'
        LEFT JOIN canonical_author ca_cv ON ca_cv.title_id = t_cv.title_id
        LEFT JOIN authors a_cv           ON a_cv.author_id = ca_cv.author_id
        JOIN languages lang        ON lang.lang_id = t.title_language
        WHERE t.title_id = %s
          AND p.pub_id   != %s
          AND p.pub_ctype IN ({type_placeholders})
          AND lang.lang_code = 'eng'
          AND YEAR(p.pub_year) > 0
        GROUP BY p.pub_id, p.pub_title, p.pub_year, p.pub_catalog,
                 p.pub_ptype, p.pub_pages, pub.publisher_name, t.title_ttype
        ORDER BY p.pub_year, p.pub_id
    """
    cursor.execute(query, (title_id, exclude_pub_id, *BOOK_TYPES))
    rows = cursor.fetchall()
    for row in rows:
        row["type_label"]        = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["format_label"]      = _PUB_PTYPE_LABELS.get(row["pub_ptype"] or "", row["pub_ptype"] or "")
        row["cover_artist_list"] = _make_author_list(row.get("cover_artist"), row.get("cover_artist_ids"))
    return rows


# Where Mag_Name in the magazine table doesn't match all pub_title variants,
# supply the correct LIKE pattern(s).  Each entry is a tuple of one or more
# patterns OR-ed together in the query.
_MAG_TITLE_PATTERNS = {
    "AMF":      ("A. Merritt's Fantasy Magazine%",),
    "AST":      (                               # all Astounding names (not Frontiers)
                    "Astounding,%",             # bare "Astounding" issues
                    "Astounding Stories%",      # Stories / Super-Science / Yearbook
                    "Astounding Science%",      # Science Fiction & Science-Fiction
                    "Astounding/Analog%",       # transition issues
                    "Astounding SF%",
                ),
    "ANLG":     ("Analog%",),               # all title variants 1960–present
    "AVONFR":   ("Avon Fantasy Reader%",),
    "FAN":      (                               # Fantastic (not Adventures/Universe/Story)
                    "Fantastic,%",
                    "Fantastic Science Fiction Stories%",
                    "Fantastic Stories of Imagination%",
                    "Fantastic Stories of the Imagination%",
                    "Fantastic Stories,%",
                ),
    "FANTUNIV": ("Fantastic Universe%",),       # separate from FAN
    "FSF":      ("The Magazine of Fantasy%",),  # catches & and "and" variants
    "GAL":      (                               # Galaxy (not Galaxy's Edge)
                    "Galaxy Science Fiction%",
                    "Galaxy,%",
                    "Galaxy Magazine%",
                ),
    "INTZ":     ("Interzone%",),
    "MGZNHRR":  ("Magazine of Horror%",),
    "NEWWR":    ("New Worlds%",),               # New Worlds / Science Fiction / SF
    "SCIFANT":  ("Science Fantasy%",),
    "SCIFIQ":   ("Science Fiction Quarterly%",),
    "SPRSCIS":  ("Super Science Stories%",),    # incl. Canadian & numbered issues
    "TRMNLF":   ("Terminal Fright%",),
    "TWONS":    ("Thrilling Wonder Stories%",),
    "UNKNOWN":  ("Unknown%",),                  # Worlds / Fantasy Fiction / UK
    "WOFFH":    ("Worlds of Fantasy%",),
    "WONDST":   ("Wonder Stories%",),           # incl. Wonder Stories Quarterly
}


def _mag_patterns(mag_code: str, mag_name: str) -> tuple:
    """Return the LIKE pattern(s) to match pub_title for this magazine."""
    return _MAG_TITLE_PATTERNS.get(mag_code, (f"{mag_name}%",))


def get_all_magazines(cursor) -> list:
    """
    Return all magazines derived directly from pubs, grouped by the name
    portion of pub_title (everything before the first comma).
    Sorted alphabetically.
    """
    cursor.execute("""
        SELECT
            SUBSTRING_INDEX(pub_title, ',', 1)  AS mag_name,
            COUNT(*)                             AS issue_count,
            MIN(CASE WHEN YEAR(pub_year) > 0 AND YEAR(pub_year) < 8888
                     THEN YEAR(pub_year) END)    AS first_year,
            MAX(CASE WHEN YEAR(pub_year) > 0 AND YEAR(pub_year) < 8888
                     THEN YEAR(pub_year) END)    AS last_year
        FROM pubs
        WHERE pub_ctype = 'MAGAZINE'
        GROUP BY SUBSTRING_INDEX(pub_title, ',', 1)
        HAVING mag_name NOT LIKE '%%&#%%'
        ORDER BY mag_name
    """)
    return cursor.fetchall()


def search_magazines(cursor, query: str) -> list:
    """Return magazines whose name contains the given string (case-insensitive)."""
    cursor.execute("""
        SELECT
            SUBSTRING_INDEX(pub_title, ',', 1)  AS mag_name,
            COUNT(*)                             AS issue_count,
            MIN(CASE WHEN YEAR(pub_year) > 0 AND YEAR(pub_year) < 8888
                     THEN YEAR(pub_year) END)    AS first_year,
            MAX(CASE WHEN YEAR(pub_year) > 0 AND YEAR(pub_year) < 8888
                     THEN YEAR(pub_year) END)    AS last_year
        FROM pubs
        WHERE pub_ctype = 'MAGAZINE'
        GROUP BY SUBSTRING_INDEX(pub_title, ',', 1)
        HAVING mag_name NOT LIKE '%%&#%%'
           AND mag_name LIKE %s
        ORDER BY mag_name
    """, (f"%{query}%",))
    return cursor.fetchall()


def get_magazine_issues_by_name(cursor, mag_name: str) -> list:
    """
    Return all issues whose pub_title starts with mag_name followed by a comma,
    in chronological order.
    """
    cursor.execute("""
        SELECT
            pub_id,
            pub_title,
            YEAR(pub_year)  AS pub_year,
            MONTH(pub_year) AS pub_month
        FROM pubs
        WHERE pub_ctype = 'MAGAZINE'
          AND pub_title LIKE %s
        ORDER BY pub_year, MONTH(pub_year), pub_title
    """, (f"{mag_name},%",))
    rows = cursor.fetchall()
    for r in rows:
        r["formatted_date"] = format_date(r["pub_year"], r["pub_month"])
    return rows


def get_magazine_issues(cursor, mag_code: str) -> tuple:
    """
    Return the magazine name and a list of all its issues in chronological order.

    Returns (mag_name, rows) where rows have keys:
        pub_id, pub_title, pub_year (int), pub_month (int), formatted_date
    """
    cursor.execute(
        "SELECT Mag_Name AS mag_name FROM magazine WHERE Mag_Code = %s",
        (mag_code,),
    )
    row = cursor.fetchone()
    if not row:
        return None, []
    mag_name = row["mag_name"]

    patterns = _mag_patterns(mag_code, mag_name)
    or_clause = " OR ".join(["p.pub_title LIKE %s"] * len(patterns))
    cursor.execute(f"""
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)  AS pub_year,
            MONTH(p.pub_year) AS pub_month
        FROM pubs p
        WHERE p.pub_ctype = 'MAGAZINE' AND ({or_clause})
        ORDER BY p.pub_year, MONTH(p.pub_year), p.pub_title
    """, patterns)
    rows = cursor.fetchall()

    for r in rows:
        r["formatted_date"] = format_date(r["pub_year"], r["pub_month"])

    return mag_name, rows


def find_authors(cursor, name: str) -> list:
    """
    Return authors whose canonical name contains the given string.

    Returns a list of dicts with keys:
        author_id, author_canonical, author_legalname,
        birth_year (int|None), death_year (int|None), title_count (int|None)
    Ordered alphabetically by author_canonical.
    """
    cursor.execute("""
        SELECT
            a.author_id,
            a.author_canonical,
            a.author_legalname,
            YEAR(a.author_birthdate) AS birth_year,
            YEAR(a.author_deathdate) AS death_year,
            abd.title_count
        FROM authors a
        LEFT JOIN authors_by_debut_date abd ON abd.author_id = a.author_id
        WHERE REPLACE(a.author_canonical, '.', '') LIKE %s
        ORDER BY a.author_canonical
    """, (f"%{name.replace('.', '')}%",))
    return cursor.fetchall()


def get_author_detail(cursor, author_id: int) -> dict | None:
    """
    Return full author info for the author detail page, or None if not found.

    Returned dict keys:
        author_id, author_canonical, author_legalname, author_birthplace,
        author_birthdate (date|None), author_deathdate (date|None),
        author_image, author_note (wiki markup stripped),
        debut_year (int|None), title_count (int|None),
        real_author (dict|None — set when this record is itself a pen name),
        pseudonyms (list of {author_id, author_canonical} — Latin script only),
        webpages (list of {url, label})
    """
    cursor.execute("""
        SELECT
            a.author_id,
            a.author_canonical,
            a.author_legalname,
            a.author_birthplace,
            a.author_birthdate,
            a.author_deathdate,
            a.author_image,
            a.author_note,
            abd.debut_year,
            abd.title_count
        FROM authors a
        LEFT JOIN authors_by_debut_date abd ON abd.author_id = a.author_id
        WHERE a.author_id = %s
    """, (author_id,))
    author = cursor.fetchone()
    if not author:
        return None

    author["author_note"] = _clean_author_note(author.get("author_note"))

    birth = author.get("author_birthdate")
    death = author.get("author_deathdate")
    if birth and death:
        age = death.year - birth.year
        if (death.month, death.day) < (birth.month, birth.day):
            age -= 1
        author["age_at_death"] = age
    else:
        author["age_at_death"] = None

    # Is this record itself a pen name? If so, surface the canonical author.
    cursor.execute("""
        SELECT a2.author_id, a2.author_canonical
        FROM pseudonyms p
        JOIN authors a2 ON a2.author_id = p.author_id
        WHERE p.pseudonym = %s
    """, (author_id,))
    author["real_author"] = cursor.fetchone()   # None when this is a canonical name

    # Pen names (this is the canonical author — fetch its pseudonyms).
    # Filter to Latin-script names only; foreign-script transliterations are noise.
    cursor.execute("""
        SELECT a2.author_id, a2.author_canonical
        FROM pseudonyms p
        JOIN authors a2 ON a2.author_id = p.pseudonym
        WHERE p.author_id = %s
        ORDER BY a2.author_canonical
    """, (author_id,))
    author["pseudonyms"] = [
        p for p in cursor.fetchall()
        if p.get("author_canonical")
        and "&#" not in p["author_canonical"]
        and p["author_canonical"].isascii()
    ]

    # External web links with human-readable labels.
    cursor.execute("""
        SELECT url FROM webpages
        WHERE author_id = %s
        ORDER BY webpage_id
    """, (author_id,))
    author["webpages"] = [
        {"url": row["url"], "label": _webpage_label(row["url"])}
        for row in cursor.fetchall()
    ]

    return author


def get_random_author_id(cursor) -> int | None:
    """Return a random author_id from authors who have at least one title."""
    cursor.execute("""
        SELECT author_id FROM canonical_author
        ORDER BY RAND() LIMIT 1
    """)
    row = cursor.fetchone()
    return row["author_id"] if row else None


def get_random_issue_id(cursor) -> int | None:
    """Return a random pub_id from magazine issues."""
    cursor.execute("""
        SELECT pub_id FROM pubs WHERE pub_ctype = 'MAGAZINE'
        ORDER BY RAND() LIMIT 1
    """)
    row = cursor.fetchone()
    return row["pub_id"] if row else None


def get_random_book_title_id(cursor) -> int | None:
    """
    Return a random title_id for an English-language book.

    Queries the titles table only (no pub join) for speed — the caller is
    responsible for verifying a pub exists via get_book_detail().  Uses a
    random starting point against the primary-key index instead of
    ORDER BY RAND() to avoid a full-table sort.
    """
    import random as _random

    type_placeholders = ", ".join(["%s"] * len(BOOK_TYPES))

    # Resolve the English language id once.
    cursor.execute("SELECT lang_id FROM languages WHERE lang_code = 'eng'")
    lang_row = cursor.fetchone()
    if not lang_row:
        return None
    lang_id = lang_row["lang_id"]

    # Upper bound: absolute max title_id — no filter so it uses the index
    # directly.  Non-book IDs picked at random are simply skipped.
    cursor.execute("SELECT MAX(title_id) AS max_id FROM titles")
    bound_row = cursor.fetchone()
    if not bound_row or not bound_row["max_id"]:
        return None
    max_id = bound_row["max_id"]

    # Try up to 10 random starting points.  Each attempt uses the primary-key
    # index so it returns almost instantly.  We randomly go either forward
    # (>=) or backward (<=) from the chosen id to avoid always landing on
    # the same "first book after a large gap" in the id space.
    for _ in range(10):
        rand_id = _random.randint(1, max_id)
        if _random.random() < 0.5:
            op, order = ">=", "ASC"
        else:
            op, order = "<=", "DESC"
        cursor.execute(
            f"""SELECT title_id FROM titles
                WHERE title_ttype   IN ({type_placeholders})
                  AND title_language = %s
                  AND title_id      {op} %s
                ORDER BY title_id {order}
                LIMIT 1""",
            (*BOOK_TYPES, lang_id, rand_id),
        )
        row = cursor.fetchone()
        if row:
            return row["title_id"]

    return None
