"""
Database operations for the personal collection feature.

The collection is identified by an anonymous UUID token stored in a browser
cookie.  Two tables live alongside the ISFDB data in the same MySQL database:

  collection_tokens  — one row per token (UUID + creation timestamp + label)
  collection_items   — one row per owned/wanted item

  item_type = 'book'     → item_id = titles.title_id
  item_type = 'magazine' → item_id = pubs.pub_id
"""

from .queries import BOOK_TYPES, TITLE_TYPE_LABELS, _make_author_list, format_date


def get_collection_status(cursor, token: str,
                           item_type: str, item_id: int) -> dict:
    """Return {'owned': bool, 'wanted': bool} for one item."""
    cursor.execute("""
        SELECT status
        FROM collection_items
        WHERE token = %s AND item_type = %s AND item_id = %s
    """, (token, item_type, item_id))
    statuses = {r["status"] for r in cursor.fetchall()}
    return {"owned": "owned" in statuses, "wanted": "wanted" in statuses}


def toggle_collection_item(cursor, token: str,
                            item_type: str, item_id: int,
                            status: str) -> bool:
    """
    Toggle one item/status combination.  Creates the token row if absent.
    Returns True if the item was added, False if it was removed.
    """
    # INSERT IGNORE raises a warning escalated to an error by mysql-connector-python.
    # Use SELECT-then-INSERT instead to avoid duplicate-key noise.
    cursor.execute(
        "SELECT 1 FROM collection_tokens WHERE token = %s",
        (token,),
    )
    if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO collection_tokens (token) VALUES (%s)",
            (token,),
        )
    cursor.execute("""
        SELECT id FROM collection_items
        WHERE token = %s AND item_type = %s AND item_id = %s AND status = %s
    """, (token, item_type, item_id, status))
    existing = cursor.fetchone()

    if existing:
        cursor.execute(
            "DELETE FROM collection_items WHERE id = %s",
            (existing["id"],),
        )
        return False   # removed
    else:
        cursor.execute("""
            INSERT INTO collection_items (token, item_type, item_id, status)
            VALUES (%s, %s, %s, %s)
        """, (token, item_type, item_id, status))
        return True    # added


def get_full_collection(cursor, token: str) -> dict:
    """
    Return all collection items for a token, enriched with book/magazine
    details, organised by status.

    Returns:
        {owned_books, wanted_books, owned_magazines, wanted_magazines, total}
    """
    cursor.execute("""
        SELECT item_type, item_id, status, added_at
        FROM collection_items
        WHERE token = %s
        ORDER BY item_type, status, added_at DESC
    """, (token,))
    items = cursor.fetchall()

    empty = {
        "owned_books": [], "wanted_books": [],
        "owned_magazines": [], "wanted_magazines": [],
        "total": 0,
    }
    if not items:
        return empty

    book_ids = [r["item_id"] for r in items if r["item_type"] == "book"]
    mag_ids  = [r["item_id"] for r in items if r["item_type"] == "magazine"]

    # ── Book details ──────────────────────────────────────────────────────────
    book_map = {}
    if book_ids:
        bp = ", ".join(["%s"] * len(book_ids))
        tp = ", ".join(["%s"] * len(BOOK_TYPES))
        cursor.execute(f"""
            SELECT
                t.title_id,
                t.title_title,
                t.title_ttype,
                MIN(YEAR(p.pub_year)) AS first_year,
                GROUP_CONCAT(
                    DISTINCT a.author_canonical
                    ORDER BY ca.ca_id SEPARATOR ' & '
                ) AS authors,
                GROUP_CONCAT(
                    DISTINCT a.author_id
                    ORDER BY ca.ca_id SEPARATOR ','
                ) AS author_ids
            FROM titles t
            LEFT JOIN canonical_author ca ON ca.title_id = t.title_id
            LEFT JOIN authors a           ON a.author_id  = ca.author_id
            LEFT JOIN pub_content pc      ON pc.title_id  = t.title_id
            LEFT JOIN pubs p              ON p.pub_id     = pc.pub_id
                                        AND YEAR(p.pub_year) > 0
            WHERE t.title_id IN ({bp})
              AND t.title_ttype IN ({tp})
            GROUP BY t.title_id, t.title_title, t.title_ttype
        """, (*book_ids, *BOOK_TYPES))
        for r in cursor.fetchall():
            r["type_label"]  = TITLE_TYPE_LABELS.get(r["title_ttype"], r["title_ttype"] or "")
            r["author_list"] = _make_author_list(r.get("authors"), r.get("author_ids"))
            book_map[r["title_id"]] = r

    # ── Magazine details ──────────────────────────────────────────────────────
    mag_map = {}
    if mag_ids:
        mp = ", ".join(["%s"] * len(mag_ids))
        cursor.execute(f"""
            SELECT pub_id, pub_title,
                   YEAR(pub_year)  AS pub_year,
                   MONTH(pub_year) AS pub_month
            FROM pubs
            WHERE pub_id IN ({mp})
              AND pub_ctype = 'MAGAZINE'
        """, tuple(mag_ids))
        for r in cursor.fetchall():
            r["formatted_date"] = format_date(r["pub_year"], r["pub_month"])
            mag_map[r["pub_id"]] = r

    # ── Organise by status ────────────────────────────────────────────────────
    result = {
        "owned_books": [], "wanted_books": [],
        "owned_magazines": [], "wanted_magazines": [],
    }
    for item in items:
        itype, iid, status = item["item_type"], item["item_id"], item["status"]
        if itype == "book":
            detail = book_map.get(iid)
            if detail:
                result["owned_books" if status == "owned" else "wanted_books"].append(detail)
        else:
            detail = mag_map.get(iid)
            if detail:
                result["owned_magazines" if status == "owned" else "wanted_magazines"].append(detail)

    result["total"] = len(items)
    return result
