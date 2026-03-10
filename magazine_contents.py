#!/usr/bin/env python3
"""
Report: Magazine Issue Contents

Usage:
    python3 magazine_contents.py "Astounding Science-Fiction" 1939-07
    python3 magazine_contents.py "Astounding" 1939
    python3 magazine_contents.py "Galaxy Science Fiction" 1951-10

Arguments:
    magazine_name  : Magazine title substring (case-insensitive partial match)
    date           : Year (YYYY) to list all issues, or Year-Month (YYYY-MM) for one issue

Note: pub_year is stored as YYYY-MM-00 (day=0); DATE_FORMAT is used for filtering.
"""
import sys
from db import get_connection

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

# Types shown in the main section vs "other" section
NARRATIVE_TYPES = {
    "SHORTFICTION", "NOVEL", "SERIAL", "POEM",
    "ESSAY", "INTERVIEW", "REVIEW", "NONFICTION", "CHAPBOOK",
}


def find_issues(cursor, magazine_name: str, date_filter: str) -> list:
    """Return matching magazine issues, sorted by date."""
    if len(date_filter) == 4 and date_filter.isdigit():
        date_clause = "YEAR(p.pub_year) = %s"
        date_params = (int(date_filter),)
    elif len(date_filter) == 7 and date_filter[4] == "-":
        year, month = date_filter.split("-")
        date_clause = "YEAR(p.pub_year) = %s AND MONTH(p.pub_year) = %s"
        date_params = (int(year), int(month))
    else:
        sys.exit(f"Error: date must be YYYY or YYYY-MM, got {date_filter!r}")

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
        ORDER BY p.pub_year, p.pub_title
    """
    cursor.execute(query, (f"%{magazine_name}%", *date_params))
    return cursor.fetchall()


def get_contents(cursor, pub_id: int) -> list:
    """Return the table of contents for a publication, sorted by page number."""
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
    return cursor.fetchall()


def format_date(year, month) -> str:
    month_name = MONTH_NAMES[month] if month and 1 <= month <= 12 else ""
    if month_name:
        return f"{month_name} {year}"
    return str(year)


def print_issue(issue: dict, contents: list) -> None:
    title = issue["pub_title"]
    date  = format_date(issue["pub_year"], issue["pub_month"])

    print(f"\n{'=' * 72}")
    print(f"  {title}")
    print(f"{'=' * 72}")

    narrative = [r for r in contents if r["title_ttype"] in NARRATIVE_TYPES]
    other     = [r for r in contents if r["title_ttype"] not in NARRATIVE_TYPES]

    if not narrative and not other:
        print("  (no contents recorded)\n")
        return

    page_w   = 5
    type_w   = 13
    title_w  = 36
    author_w = 26

    header = (
        f"  {'Page':<{page_w}}  "
        f"{'Type':<{type_w}}  "
        f"{'Title':<{title_w}}  "
        f"Author(s)"
    )
    divider = "  " + "-" * (page_w + type_w + title_w + author_w + 6)

    def fmt_row(row):
        page   = str(row["pubc_page"] or "").strip()
        ttype  = TITLE_TYPE_LABELS.get(row["title_ttype"], row["title_ttype"] or "")
        title  = str(row["title_title"] or "").strip()
        author = str(row["authors"] or "").strip()

        if len(title) > title_w:
            title = title[:title_w - 1] + "…"
        if len(author) > author_w:
            author = author[:author_w - 1] + "…"
        if len(ttype) > type_w:
            ttype = ttype[:type_w - 1] + "…"

        print(
            f"  {page:<{page_w}}  "
            f"{ttype:<{type_w}}  "
            f"{title:<{title_w}}  "
            f"{author}"
        )

    print(header)
    print(divider)
    for row in narrative:
        fmt_row(row)

    if other:
        print(f"\n  -- Art / Editorial --")
        print(header)
        print(divider)
        for row in other:
            fmt_row(row)

    print()


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    magazine_name = sys.argv[1]
    date_filter   = sys.argv[2]

    conn   = get_connection()
    cursor = conn.cursor(dictionary=True)

    issues = find_issues(cursor, magazine_name, date_filter)
    if not issues:
        print(f"No issues found for '{magazine_name}' matching '{date_filter}'.")
        sys.exit(0)

    for issue in issues:
        contents = get_contents(cursor, issue["pub_id"])
        print_issue(issue, contents)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
