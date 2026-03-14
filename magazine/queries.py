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
