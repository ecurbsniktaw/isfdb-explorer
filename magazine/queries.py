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


def _rewrite_isfdb_links(note: str) -> str:
    """
    Rewrite isfdb.org links in HTML note text to local site URLs, and
    open any remaining external links in a new tab.
    """
    if not note:
        return ""
    # author pages: ea.cgi?N → /author/N/
    note = re.sub(
        r'href="https?://(?:www\.)?isfdb\.org/cgi-bin/ea\.cgi\?(\d+)"',
        r'href="/author/\1/"',
        note, flags=re.IGNORECASE,
    )
    # title pages: title.cgi?N → /story/N/
    note = re.sub(
        r'href="https?://(?:www\.)?isfdb\.org/cgi-bin/title\.cgi\?(\d+)"',
        r'href="/story/\1/"',
        note, flags=re.IGNORECASE,
    )
    # publication pages: pl.cgi?N → /issue/N/
    note = re.sub(
        r'href="https?://(?:www\.)?isfdb\.org/cgi-bin/pl\.cgi\?(\d+)"',
        r'href="/issue/\1/"',
        note, flags=re.IGNORECASE,
    )
    # open remaining external links in a new tab
    note = re.sub(
        r'<a (href="https?://)',
        r'<a target="_blank" rel="noopener" \1',
        note, flags=re.IGNORECASE,
    )
    return note


def _clean_author_note(note: str) -> str:
    """Strip ISFDB wiki markup from a biographical note, then rewrite links."""
    if not note:
        return ""
    note = re.sub(r"\{\{A\|([^}]+)\}\}", r"\1", note)   # {{A|name}} → name
    note = re.sub(r"\{\{[^}]+\}\}", "", note)             # other {{…}} → drop
    return _rewrite_isfdb_links(note).strip()


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


def _clean_issue_note(note: str) -> str:
    """
    Strip the ISFDB-generated prev/next navigation block from an issue note,
    then rewrite any remaining internal links.

    The raw note format is:
        <!--isfdb specific-->
        <nav HTML>
        <!--isfdb specific-->
        actual note text...
    """
    if not note:
        return ""
    parts = note.split("<!--isfdb specific-->")
    if len(parts) >= 3:
        note = parts[2].strip()
    elif len(parts) == 2:
        note = parts[1].strip()
    # Strip leading <br/> artifacts
    note = re.sub(r'^(<br\s*/?>[\s\n]*)+', '', note).strip()
    return _rewrite_isfdb_links(note)


def get_issue_meta(cursor, pub_id: int) -> dict | None:
    """
    Return metadata for one magazine issue: title, date, cover image,
    publisher, price, pages, note, editor(s), and cover artist(s).
    """
    cursor.execute("""
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)  AS pub_year,
            MONTH(p.pub_year) AS pub_month,
            p.pub_frontimage,
            p.pub_price,
            p.pub_pages,
            pub2.publisher_name,
            n.note_note AS pub_note
        FROM pubs p
        LEFT JOIN publishers pub2 ON pub2.publisher_id = p.publisher_id
        LEFT JOIN notes n         ON n.note_id         = p.note_id
        WHERE p.pub_id = %s AND p.pub_ctype = 'MAGAZINE'
    """, (pub_id,))
    row = cursor.fetchone()
    if not row:
        return None

    row["pub_note"] = _clean_issue_note(row.get("pub_note") or "")

    # Editor(s)
    cursor.execute("""
        SELECT a.author_id, a.author_canonical
        FROM pub_content pc
        JOIN titles t            ON t.title_id  = pc.title_id AND t.title_ttype = 'EDITOR'
        JOIN canonical_author ca ON ca.title_id = t.title_id
        JOIN authors a           ON a.author_id  = ca.author_id
        WHERE pc.pub_id = %s
        ORDER BY ca.ca_id
    """, (pub_id,))
    row["editors"] = cursor.fetchall()

    # Cover artist(s)
    cursor.execute("""
        SELECT a.author_id, a.author_canonical
        FROM pub_content pc
        JOIN titles t            ON t.title_id  = pc.title_id AND t.title_ttype = 'COVERART'
        JOIN canonical_author ca ON ca.title_id = t.title_id
        JOIN authors a           ON a.author_id  = ca.author_id
        WHERE pc.pub_id = %s
        ORDER BY ca.ca_id
    """, (pub_id,))
    row["cover_artists"] = cursor.fetchall()

    return row


def get_adjacent_issues(cursor, pub_id: int) -> tuple:
    """
    Return (prev_issue, next_issue) for the same magazine, where each is a
    dict with pub_id and pub_title, or None if there is no adjacent issue.

    Uses the leading portion of pub_title (up to the first comma) as the
    magazine identifier.
    """
    cursor.execute("""
        SELECT SUBSTRING_INDEX(pub_title, ',', 1) AS mag_prefix,
               YEAR(pub_year)  AS pub_year,
               MONTH(pub_year) AS pub_month
        FROM pubs WHERE pub_id = %s
    """, (pub_id,))
    row = cursor.fetchone()
    if not row:
        return None, None

    prefix   = row["mag_prefix"]
    yr_mo    = row["pub_year"] * 12 + row["pub_month"]

    cursor.execute("""
        SELECT pub_id, pub_title
        FROM pubs
        WHERE pub_ctype = 'MAGAZINE'
          AND SUBSTRING_INDEX(pub_title, ',', 1) = %s
          AND YEAR(pub_year) * 12 + MONTH(pub_year) < %s
        ORDER BY pub_year DESC, pub_id DESC
        LIMIT 1
    """, (prefix, yr_mo))
    prev_issue = cursor.fetchone()

    cursor.execute("""
        SELECT pub_id, pub_title
        FROM pubs
        WHERE pub_ctype = 'MAGAZINE'
          AND SUBSTRING_INDEX(pub_title, ',', 1) = %s
          AND YEAR(pub_year) * 12 + MONTH(pub_year) > %s
        ORDER BY pub_year ASC, pub_id ASC
        LIMIT 1
    """, (prefix, yr_mo))
    next_issue = cursor.fetchone()

    return prev_issue, next_issue


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
            p.pub_price,
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
                 p.pub_catalog, p.pub_isbn, p.pub_price, p.pub_ptype, p.pub_pages, p.pub_frontimage,
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
    row["pub_note"]          = _rewrite_isfdb_links(row.get("pub_note") or "")
    row["title_note"]        = _rewrite_isfdb_links(row.get("title_note") or "")

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
        SELECT at2.award_type_name, ac.award_cat_name, a.award_level,
               YEAR(a.award_year) AS award_year
        FROM title_awards ta
        JOIN awards a        ON a.award_id       = ta.award_id
        JOIN award_types at2 ON at2.award_type_id = a.award_type_id
        JOIN award_cats ac   ON ac.award_cat_id   = a.award_cat_id
        WHERE ta.title_id = %s
        ORDER BY YEAR(a.award_year), at2.award_type_name, ac.award_cat_name
    """, (title_id,))
    row["awards"] = [
        {
            "award_name": r["award_type_name"],
            "category":   r["award_cat_name"],
            "level":      _AWARD_LEVEL.get(str(r["award_level"]), "Nominee/Finalist"),
            "year":       r["award_year"] if r["award_year"] else "",
        }
        for r in cursor.fetchall()
    ]

    return row


_STORY_LENGTH_LABELS = {
    "short story": "Short Story",
    "novelette":   "Novelette",
    "novella":     "Novella",
}

# Publication types that are not magazines
_BOOK_PUB_CTYPES = ("NOVEL", "COLLECTION", "ANTHOLOGY", "OMNIBUS", "NONFICTION",
                    "CHAPBOOK", "NONGENRE", "FANZINE", "NEWSLETTER")


def get_story_detail(cursor, title_id: int) -> dict | None:
    """
    Return details for a fiction/essay/poem title: metadata, authors, first pub,
    series, note, synopsis, webpages, awards, and all publications.
    """
    query = """
        SELECT
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_storylen,
            t.series_id,
            t.title_seriesnum,
            t.title_seriesnum_2,
            t.note_id,
            t.title_synopsis,
            GROUP_CONCAT(
                a.author_canonical ORDER BY ca.ca_id SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                a.author_id ORDER BY ca.ca_id SEPARATOR ','
            ) AS author_ids
        FROM titles t
        LEFT JOIN canonical_author ca ON ca.title_id = t.title_id
        LEFT JOIN authors a           ON a.author_id  = ca.author_id
        WHERE t.title_id = %s
        GROUP BY t.title_id, t.title_title, t.title_ttype, t.title_storylen,
                 t.series_id, t.title_seriesnum, t.title_seriesnum_2,
                 t.note_id, t.title_synopsis
    """
    cursor.execute(query, (title_id,))
    row = cursor.fetchone()
    if not row:
        return None

    row["type_label"]   = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
    row["length_label"] = _STORY_LENGTH_LABELS.get(row.get("title_storylen") or "", "")
    row["author_list"]  = _make_author_list(row.get("authors"), row.get("author_ids"))

    # Series
    if row.get("series_id"):
        cursor.execute("""
            SELECT s.series_title, s.series_parent, sp.series_title AS parent_title
            FROM series s
            LEFT JOIN series sp ON sp.series_id = s.series_parent
            WHERE s.series_id = %s
        """, (row["series_id"],))
        row["series"] = cursor.fetchone()
    else:
        row["series"] = None

    # Note
    if row.get("note_id"):
        cursor.execute("SELECT note_note FROM notes WHERE note_id = %s", (row["note_id"],))
        n = cursor.fetchone()
        row["title_note"] = _rewrite_isfdb_links(n["note_note"] if n else "")
    else:
        row["title_note"] = ""

    # Synopsis
    if row.get("title_synopsis"):
        cursor.execute("SELECT note_note FROM notes WHERE note_id = %s", (row["title_synopsis"],))
        n = cursor.fetchone()
        row["synopsis"] = _rewrite_isfdb_links(n["note_note"] if n else "")
    else:
        row["synopsis"] = ""

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
        SELECT at2.award_type_name, ac.award_cat_name, a.award_level,
               YEAR(a.award_year) AS award_year
        FROM title_awards ta
        JOIN awards a        ON a.award_id       = ta.award_id
        JOIN award_types at2 ON at2.award_type_id = a.award_type_id
        JOIN award_cats ac   ON ac.award_cat_id   = a.award_cat_id
        WHERE ta.title_id = %s
        ORDER BY YEAR(a.award_year), at2.award_type_name
    """, (title_id,))
    row["awards"] = [
        {
            "award_name": r["award_type_name"],
            "category":   r["award_cat_name"],
            "level":      _AWARD_LEVEL.get(str(r["award_level"]), "Nominee/Finalist"),
            "year":       r["award_year"] if r["award_year"] else "",
        }
        for r in cursor.fetchall()
    ]

    # All publications (where and when this story appeared)
    cursor.execute("""
        SELECT
            p.pub_id,
            p.pub_title,
            YEAR(p.pub_year)  AS pub_year,
            MONTH(p.pub_year) AS pub_month,
            p.pub_ctype,
            p.pub_frontimage,
            pub2.publisher_name,
            pc.pubc_page
        FROM pub_content pc
        JOIN pubs p ON p.pub_id = pc.pub_id
        LEFT JOIN publishers pub2 ON pub2.publisher_id = p.publisher_id
        WHERE pc.title_id = %s
          AND YEAR(p.pub_year) > 0
        ORDER BY p.pub_year, p.pub_id
    """, (title_id,))
    pubs = cursor.fetchall()
    for p in pubs:
        p["formatted_date"] = format_date(p["pub_year"], p["pub_month"])
        p["is_magazine"]    = p["pub_ctype"] == "MAGAZINE"
        p["type_label"]     = TITLE_TYPE_LABELS.get(p["pub_ctype"], p["pub_ctype"] or "")
    row["publications"] = pubs

    # First publication date (earliest entry)
    if pubs:
        first = pubs[0]
        row["first_pub_date"] = first["formatted_date"]
        row["first_pub_id"]   = first["pub_id"]
    else:
        row["first_pub_date"] = ""
        row["first_pub_id"]   = None

    return row


_TOC_TYPES = (
    "SHORTFICTION", "NOVEL", "SERIAL", "POEM",
    "ESSAY", "INTERVIEW", "REVIEW", "NONFICTION", "CHAPBOOK",
)


def get_book_contents(cursor, pub_id: int) -> list:
    """
    Return the table of contents for a book (collection, anthology, omnibus, etc.).

    Excludes the top-level title matching the book itself (same ttype as pub_ctype)
    and all art/editorial entries.  Returns rows ordered by page number.
    """
    # Get the pub_ctype so we can exclude the title that is the book itself
    cursor.execute("SELECT pub_ctype FROM pubs WHERE pub_id = %s", (pub_id,))
    row = cursor.fetchone()
    if not row:
        return []
    pub_ctype = row["pub_ctype"]

    type_placeholders = ", ".join(["%s"] * len(_TOC_TYPES))
    cursor.execute(f"""
        SELECT
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_storylen,
            pc.pubc_page,
            GROUP_CONCAT(
                a.author_canonical ORDER BY ca.ca_id SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                a.author_id ORDER BY ca.ca_id SEPARATOR ','
            ) AS author_ids
        FROM pub_content pc
        JOIN titles t            ON t.title_id  = pc.title_id
        LEFT JOIN canonical_author ca ON ca.title_id = t.title_id
        LEFT JOIN authors a       ON a.author_id  = ca.author_id
        WHERE pc.pub_id = %s
          AND t.title_ttype IN ({type_placeholders})
          AND t.title_ttype != %s
        GROUP BY t.title_id, t.title_title, t.title_ttype, t.title_storylen, pc.pubc_page
        ORDER BY
            CASE WHEN pc.pubc_page REGEXP '^[0-9]+$'
                 THEN LPAD(pc.pubc_page, 6, '0')
                 ELSE pc.pubc_page END,
            t.title_title
    """, (pub_id, *_TOC_TYPES, pub_ctype))
    rows = cursor.fetchall()
    for row in rows:
        row["type_label"]   = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["length_label"] = _STORY_LENGTH_LABELS.get(row.get("title_storylen") or "", "")
        row["author_list"]  = _make_author_list(row.get("authors"), row.get("author_ids"))
    return rows


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
            p.pub_frontimage,
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


_SEARCHABLE_TYPES = (
    "NOVEL", "COLLECTION", "ANTHOLOGY", "OMNIBUS", "NONFICTION", "CHAPBOOK",
    "SHORTFICTION", "SERIAL", "ESSAY", "POEM",
)
_BOOK_SEARCH_TYPES    = ("NOVEL", "COLLECTION", "ANTHOLOGY", "OMNIBUS", "NONFICTION", "CHAPBOOK")
_FICTION_SEARCH_TYPES = ("SHORTFICTION", "SERIAL")


def find_titles(cursor, title: str, match_type: str = "exact",
                content_type: str = "all") -> list:
    """
    Search for titles by name.

    match_type:   'exact'   — full title match (case-insensitive)
                  'partial' — substring match
    content_type: 'all'     — novels, collections, short fiction, etc.
                  'book'    — books only (NOVEL, COLLECTION, ANTHOLOGY, …)
                  'fiction' — short fiction / serials only

    Returns up to 200 results sorted by title then first-pub year.
    """
    if content_type == "book":
        type_list = _BOOK_SEARCH_TYPES
    elif content_type == "fiction":
        type_list = _FICTION_SEARCH_TYPES
    else:
        type_list = _SEARCHABLE_TYPES

    type_placeholders = ", ".join(["%s"] * len(type_list))

    if match_type == "partial":
        title_clause = "LOWER(t.title_title) LIKE LOWER(%s)"
        title_param  = f"%{title}%"
    else:
        title_clause = "LOWER(t.title_title) = LOWER(%s)"
        title_param  = title

    query = f"""
        SELECT
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_storylen,
            MIN(YEAR(p.pub_year)) AS first_year,
            GROUP_CONCAT(
                DISTINCT a.author_canonical ORDER BY ca.ca_id SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                DISTINCT a.author_id ORDER BY ca.ca_id SEPARATOR ','
            ) AS author_ids
        FROM titles t
        LEFT JOIN canonical_author ca ON ca.title_id  = t.title_id
        LEFT JOIN authors a           ON a.author_id   = ca.author_id
        LEFT JOIN pub_content pc      ON pc.title_id   = t.title_id
        LEFT JOIN pubs p              ON p.pub_id      = pc.pub_id
                                     AND YEAR(p.pub_year) > 0
        JOIN languages lang           ON lang.lang_id  = t.title_language
        WHERE {title_clause}
          AND t.title_ttype IN ({type_placeholders})
          AND lang.lang_code = 'eng'
        GROUP BY t.title_id, t.title_title, t.title_ttype, t.title_storylen
        ORDER BY t.title_title, first_year
        LIMIT 200
    """
    cursor.execute(query, (title_param, *type_list))
    rows = cursor.fetchall()
    for row in rows:
        row["type_label"]   = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        row["length_label"] = _STORY_LENGTH_LABELS.get(row.get("title_storylen") or "", "")
        row["author_list"]  = _make_author_list(row.get("authors"), row.get("author_ids"))
        row["is_book"]      = row["title_ttype"] in _BOOK_SEARCH_TYPES
    return rows


def find_authors(cursor, name: str, search_type: str = "full") -> list:
    """
    Return authors whose canonical name matches the given string.

    search_type:
        'full' — match anywhere in the full name (default)
        'last' — match against the last word of the name only

    Returns a list of dicts with keys:
        author_id, author_canonical, author_legalname,
        birth_year (int|None), death_year (int|None), title_count (int|None)
    Ordered alphabetically by author_canonical.
    """
    clean = name.replace(".", "")
    if search_type == "last":
        where = "REPLACE(SUBSTRING_INDEX(a.author_canonical, ' ', -1), '.', '') LIKE %s"
        param = f"%{clean}%"
    else:
        where = "REPLACE(a.author_canonical, '.', '') LIKE %s"
        param = f"%{clean}%"

    cursor.execute(f"""
        SELECT
            a.author_id,
            a.author_canonical,
            a.author_legalname,
            YEAR(a.author_birthdate) AS birth_year,
            YEAR(a.author_deathdate) AS death_year,
            abd.title_count
        FROM authors a
        LEFT JOIN authors_by_debut_date abd ON abd.author_id = a.author_id
        WHERE {where}
        ORDER BY a.author_canonical
    """, (param,))
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


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------

_MAJOR_SERIES_IDS = [
    165, 11826, 504, 11680, 12290, 518, 24708,
    869, 1023, 4543, 684, 1154, 875, 13661, 2294,
]

_MAJOR_SERIES_INFO = {
    165:   {"name": "Hitchhiker's Guide to the Galaxy",    "author": "Douglas Adams"},
    11826: {"name": "Foundation Universe",                 "author": "Isaac Asimov"},
    504:   {"name": "Culture",                             "author": "Iain M. Banks"},
    11680: {"name": "Parable of the Sower / Earthseed",   "author": "Octavia E. Butler"},
    12290: {"name": "Ender's Universe",                    "author": "Orson Scott Card"},
    518:   {"name": "Riverworld",                          "author": "Philip José Farmer"},
    24708: {"name": "Future History",                      "author": "Robert A. Heinlein"},
    869:   {"name": "Dune",                                "author": "Frank Herbert"},
    1023:  {"name": "The Dark Tower",                      "author": "Stephen King"},
    4543:  {"name": "A Song of Ice and Fire",              "author": "George R.R. Martin"},
    684:   {"name": "Known Space",                         "author": "Larry Niven"},
    1154:  {"name": "Witch World Universe",                "author": "Andre Norton"},
    875:   {"name": "Heechee",                             "author": "Frederik Pohl"},
    13661: {"name": "Middle Earth Universe",               "author": "J.R.R. Tolkien"},
    2294:  {"name": "Demon Princes",                       "author": "Jack Vance"},
}

_SERIES_TITLE_TYPES = (
    "NOVEL", "COLLECTION", "OMNIBUS", "ANTHOLOGY", "NONFICTION", "CHAPBOOK",
)


def get_series_letters(cursor) -> list:
    """
    Return the sorted list of first letters (A-Z) for which browsable series exist.
    'The ' prefix is stripped when determining the letter, matching the view logic.
    """
    cursor.execute("""
        SELECT DISTINCT
            UPPER(LEFT(
                CASE WHEN UPPER(s.series_title) LIKE 'THE %%'
                     THEN SUBSTRING(s.series_title, 5)
                     ELSE s.series_title END,
                1
            )) AS letter
        FROM series s
        JOIN titles t ON t.series_id = s.series_id AND t.title_parent = 0
        WHERE s.series_title NOT LIKE '%%&#%%'
          AND s.series_title REGEXP '^[A-Za-z]'
        HAVING letter REGEXP '^[A-Z]$'
        ORDER BY letter
    """)
    return [row["letter"] for row in cursor.fetchall()]


def get_series_by_letter(cursor, letter: str, limit: int = 300) -> tuple:
    """
    Return (rows, total) for series whose effective first letter matches `letter`
    ('The ' prefix stripped).  Returns at most `limit` rows.
    """
    # The CASE strips 'The ' so "The Dark Tower" sorts/filters under D.
    cursor.execute("""
        SELECT s.series_id, s.series_title, COUNT(t.title_id) AS title_count
        FROM series s
        JOIN titles t ON t.series_id = s.series_id AND t.title_parent = 0
        WHERE s.series_title NOT LIKE '%%&#%%'
        GROUP BY s.series_id, s.series_title
        HAVING title_count >= 2
          AND UPPER(LEFT(
                CASE WHEN UPPER(series_title) LIKE 'THE %%'
                     THEN SUBSTRING(series_title, 5)
                     ELSE series_title END,
                1)) = %s
        ORDER BY CASE WHEN UPPER(series_title) LIKE 'THE %%'
                      THEN SUBSTRING(series_title, 5)
                      ELSE series_title END
    """, (letter.upper(),))
    rows = cursor.fetchall()
    total = len(rows)
    return rows[:limit], total


def search_series(cursor, query: str) -> list:
    """Return series whose name contains the query string (case-insensitive)."""
    cursor.execute("""
        SELECT s.series_id, s.series_title, COUNT(t.title_id) AS title_count
        FROM series s
        JOIN titles t ON t.series_id = s.series_id AND t.title_parent = 0
        WHERE s.series_title NOT LIKE '%%&#%%'
          AND s.series_title LIKE %s
        GROUP BY s.series_id, s.series_title
        HAVING title_count >= 2
        ORDER BY s.series_title
        LIMIT 300
    """, (f"%{query}%",))
    return cursor.fetchall()


def get_series_by_author(cursor, author_id: int) -> tuple:
    """
    Return (author_name, series_list) for a given author.

    series_list entries have keys: series_id, series_title, title_count.
    Includes all series the author has at least one canonical title in,
    ordered alphabetically by series title.
    """
    cursor.execute(
        "SELECT author_canonical FROM authors WHERE author_id = %s",
        (author_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None, []
    author_name = row["author_canonical"]

    cursor.execute("""
        SELECT s.series_id, s.series_title,
               COUNT(DISTINCT t.title_id) AS title_count
        FROM series s
        JOIN titles t            ON t.series_id  = s.series_id
                                AND t.title_parent = 0
        JOIN canonical_author ca ON ca.title_id  = t.title_id
        WHERE ca.author_id = %s
          AND s.series_title NOT LIKE '%%&#%%'
        GROUP BY s.series_id, s.series_title
        ORDER BY s.series_title
    """, (author_id,))
    return author_name, cursor.fetchall()


def get_series_detail(cursor, series_id: int) -> dict | None:
    """
    Return series metadata, its canonical titles (novels/collections/etc.),
    and any child subseries.
    """
    # Series info + optional note + parent link
    cursor.execute("""
        SELECT s.series_id, s.series_title, s.series_parent,
               sp.series_title AS parent_title,
               n.note_note     AS series_note
        FROM series s
        LEFT JOIN series sp ON sp.series_id = s.series_parent
        LEFT JOIN notes n   ON n.note_id    = s.series_note_id
        WHERE s.series_id = %s
    """, (series_id,))
    row = cursor.fetchone()
    if not row:
        return None
    row["series_note"] = _rewrite_isfdb_links(row.get("series_note") or "")

    # Titles (canonical only) ordered by series number then publication date
    type_placeholders = ", ".join(["%s"] * len(_SERIES_TITLE_TYPES))
    cursor.execute(f"""
        SELECT
            t.title_id,
            t.title_title,
            t.title_ttype,
            t.title_seriesnum,
            t.title_seriesnum_2,
            YEAR(t.title_copyright) AS pub_year,
            GROUP_CONCAT(
                a.author_canonical ORDER BY ca.ca_id SEPARATOR ' & '
            ) AS authors,
            GROUP_CONCAT(
                a.author_id ORDER BY ca.ca_id SEPARATOR ','
            ) AS author_ids
        FROM titles t
        LEFT JOIN canonical_author ca ON ca.title_id = t.title_id
        LEFT JOIN authors a           ON a.author_id  = ca.author_id
        WHERE t.series_id  = %s
          AND t.title_parent = 0
          AND t.title_ttype IN ({type_placeholders})
        GROUP BY t.title_id, t.title_title, t.title_ttype,
                 t.title_seriesnum, t.title_seriesnum_2, t.title_copyright
        ORDER BY (t.title_seriesnum IS NULL), t.title_seriesnum,
                 t.title_seriesnum_2, t.title_copyright
    """, (series_id, *_SERIES_TITLE_TYPES))
    titles = cursor.fetchall()
    for t in titles:
        t["type_label"]  = TITLE_TYPE_LABELS.get(t["title_ttype"], t["title_ttype"] or "")
        t["author_list"] = _make_author_list(t.get("authors"), t.get("author_ids"))
        t["is_book"]     = t["title_ttype"] in _SERIES_TITLE_TYPES
        # Format series number label: "3", "3a", etc.
        num = t.get("title_seriesnum")
        num2 = (t.get("title_seriesnum_2") or "").strip()
        if num is not None:
            t["series_label"] = f"{num}{num2}"
        else:
            t["series_label"] = ""
    row["titles"] = titles

    # Subseries
    cursor.execute("""
        SELECT series_id, series_title
        FROM series
        WHERE series_parent = %s
          AND series_title NOT LIKE '%%&#%%'
        ORDER BY (series_parent_position IS NULL), series_parent_position, series_title
    """, (series_id,))
    row["subseries"] = cursor.fetchall()

    return row


# ---------------------------------------------------------------------------
# Awards
# ---------------------------------------------------------------------------

_MAJOR_AWARD_IDS = [13, 7, 23, 27, 28, 31, 32, 35, 61, 44]

_MAJOR_AWARD_NAMES = {
    13: "Arthur C. Clarke Award",
    7:  "British Fantasy Award",
    23: "Hugo Award",
    27: "John W. Campbell Award / Astounding Award",
    28: "Locus Poll Award",
    31: "Nebula Award",
    32: "Philip K. Dick Award",
    35: "Rhysling Award",
    61: "Seiun Award",
    44: "World Fantasy Award",
}


def get_all_award_types(cursor) -> list:
    """Return all English-named award types, sorted alphabetically."""
    cursor.execute("""
        SELECT award_type_id, award_type_name
        FROM award_types
        WHERE award_type_name NOT LIKE '%%&#%%'
        ORDER BY award_type_name
    """)
    return cursor.fetchall()


def search_award_types(cursor, name: str) -> list:
    """Return award types whose name contains the given string."""
    cursor.execute("""
        SELECT award_type_id, award_type_name
        FROM award_types
        WHERE award_type_name LIKE %s
          AND award_type_name NOT LIKE '%%&#%%'
        ORDER BY award_type_name
    """, (f"%{name}%",))
    return cursor.fetchall()


_AWARD_LEVEL_LABELS = {
    "1": "Winner",
    "2": "Runner-up",
    "3": "Finalist",
    "4": "Finalist",
    "5": "Finalist",
}


def get_award_detail(cursor, award_type_id: int) -> dict | None:
    """
    Return award type info plus all its entries grouped by year then category.
    Each entry includes title, author, level, and optional title_id link.
    """
    cursor.execute("""
        SELECT award_type_id, award_type_name, award_type_wikipedia,
               award_type_by, award_type_for
        FROM award_types
        WHERE award_type_id = %s
    """, (award_type_id,))
    award_type = cursor.fetchone()
    if not award_type:
        return None

    cursor.execute("""
        SELECT
            YEAR(a.award_year)  AS award_year,
            ac.award_cat_name   AS category,
            a.award_title,
            a.award_author,
            a.award_level,
            ta.title_id
        FROM awards a
        JOIN award_cats ac ON ac.award_cat_id = a.award_cat_id
        LEFT JOIN title_awards ta ON ta.award_id = a.award_id
        WHERE a.award_type_id = %s
        ORDER BY YEAR(a.award_year) DESC, ac.award_cat_order, a.award_level
    """, (award_type_id,))
    entries = cursor.fetchall()

    # Determine if each title_id is a book or story for linking
    title_ids = [e["title_id"] for e in entries if e.get("title_id")]
    book_ids = set()
    if title_ids:
        placeholders = ", ".join(["%s"] * len(title_ids))
        type_placeholders = ", ".join(["%s"] * len(BOOK_TYPES))
        cursor.execute(
            f"SELECT title_id FROM titles WHERE title_id IN ({placeholders})"
            f" AND title_ttype IN ({type_placeholders})",
            (*title_ids, *BOOK_TYPES)
        )
        book_ids = {r["title_id"] for r in cursor.fetchall()}

    # Group by year then category
    from collections import defaultdict
    by_year = defaultdict(lambda: defaultdict(list))
    for e in entries:
        yr  = e["award_year"] or 0
        cat = e["category"] or ""
        level_label = _AWARD_LEVEL_LABELS.get(str(e["award_level"] or ""), "Nominee")
        by_year[yr][cat].append({
            "title":   e["award_title"] or "",
            "author":  e["award_author"] or "",
            "level":   level_label,
            "title_id": e.get("title_id"),
            "is_book": e.get("title_id") in book_ids if e.get("title_id") else False,
        })

    years = sorted(by_year.keys(), reverse=True)
    award_type["years"] = [
        {
            "year": yr if yr else "Unknown",
            "categories": [
                {"name": cat, "entries": by_year[yr][cat]}
                for cat in by_year[yr]
            ],
        }
        for yr in years
    ]
    return award_type
