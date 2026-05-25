import json
import random
import uuid as _uuid
from urllib.parse import quote_plus

from django.conf import settings as django_settings
from django.core.mail import send_mail
from django.core.signing import TimestampSigner, BadSignature, SignatureExpired
from django.db import connection
from django.shortcuts import render, redirect
from django.http import Http404, JsonResponse
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from django.views.decorators.http import require_POST

from .queries import (
    find_issues, get_issue_meta, get_contents, get_archive_links, get_adjacent_issues,
    get_author_fiction, get_author_detail, get_author_works, get_author_books,
    get_book_detail, get_book_editions, get_book_contents, get_book_reviews, get_story_detail, find_titles,
    get_magazine_issues_by_name, get_magazine_group_info,
    get_all_magazines, get_magazine_issues, search_magazines,
    find_authors, find_artists, get_author_count, get_author_art, author_has_series,
    get_random_author_id, get_random_issue_id, get_random_book_title_id,
    get_random_short_fiction_id,
    get_all_award_types, search_award_types, get_award_detail, get_author_awards,
    get_award_type_info, get_award_categories, get_award_entries_by_category,
    _MAJOR_AWARD_IDS, _MAJOR_AWARD_NAMES,
    get_series_letters, get_series_count, get_series_by_letter, search_series, search_pub_series,
    get_series_detail, get_series_by_author,
    get_pub_series_detail,
    _MAJOR_SERIES_IDS, _MAJOR_SERIES_INFO,
    find_publishers, get_publisher_count,
    get_publisher_detail, get_publisher_books_by_year,
    get_publisher_books_by_author, get_publisher_all_authors,
    format_date, NARRATIVE_TYPES,
)
from .collection_queries import (
    get_collection_status, toggle_collection_item, get_full_collection,
)


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


def _split_contents(contents):
    """Split a contents list into narrative and art/editorial."""
    narrative = [r for r in contents if r["is_narrative"]]
    other     = [r for r in contents if not r["is_narrative"]]
    return narrative, other


_COLLECTION_COOKIE     = "collection_token"
_COLLECTION_COOKIE_AGE = 5 * 365 * 24 * 60 * 60   # 5 years


def _get_collection_token(request):
    """Return the collection token string from the cookie, or None."""
    return request.COOKIES.get(_COLLECTION_COOKIE) or None


def _empty_col_status():
    return {"owned": False, "wanted": False}


def home(request):
    """Home page with site description."""
    return render(request, "magazine/home.html")


def combined_search(request):
    """Combined search hub — three forms pointing to their result pages."""
    return render(request, "magazine/combined_search.html")


def search(request):
    """Magazine issue search form + results."""
    magazine_name = request.GET.get("magazine", "").strip()
    date_filter   = request.GET.get("date", "").strip()

    context = {
        "magazine_name": magazine_name,
        "date_filter":   date_filter,
    }

    if not magazine_name or not date_filter:
        return render(request, "magazine/search.html", context)

    # Validate date format
    valid_date = (
        (len(date_filter) == 4 and date_filter.isdigit()) or
        (len(date_filter) == 7 and date_filter[4] == "-" and
         date_filter[:4].isdigit() and date_filter[5:].isdigit())
    )
    if not valid_date:
        context["error"] = f'Date must be YYYY or YYYY-MM \u2014 got \u201c{date_filter}\u201d'
        return render(request, "magazine/search.html", context)

    cursor = _dict_cursor()
    try:
        try:
            issues = find_issues(cursor, magazine_name, date_filter)
        except ValueError as exc:
            context["error"] = str(exc)
            return render(request, "magazine/search.html", context)
    finally:
        cursor.close()

    if not issues:
        context["error"] = (
            f'No issues found matching \u201c{magazine_name}\u201d for date \u201c{date_filter}\u201d.'
        )
        return render(request, "magazine/search.html", context)

    if len(issues) == 1:
        return redirect("issue_detail", pub_id=issues[0]["pub_id"])

    # Multiple issues — show a list with formatted dates
    for issue in issues:
        issue["formatted_date"] = format_date(issue["pub_year"], issue["pub_month"])

    context["issues"] = issues
    return render(request, "magazine/search.html", context)


@ensure_csrf_cookie
def issue_detail(request, pub_id):
    """Table of contents for one magazine issue."""
    cursor = _dict_cursor()
    try:
        issue = get_issue_meta(cursor, pub_id)
        if not issue:
            raise Http404(f"No magazine issue with pub_id={pub_id}")
        contents = get_contents(cursor, pub_id)
        archive_links = get_archive_links(cursor, pub_id)
        prev_issue, next_issue = get_adjacent_issues(cursor, pub_id)
        token      = _get_collection_token(request)
        col_status = (get_collection_status(cursor, token, "magazine", pub_id)
                      if token else _empty_col_status())
    finally:
        cursor.close()

    narrative, other = _split_contents(contents)
    issue["formatted_date"] = format_date(issue["pub_year"], issue["pub_month"])

    # Pass search params back so the "Back" link restores the previous search
    back_params = request.GET.urlencode()

    return render(request, "magazine/issue.html", {
        "issue":            issue,
        "narrative":        narrative,
        "other":            other,
        "back_params":      back_params,
        "archive_links":    archive_links,
        "prev_issue":       prev_issue,
        "next_issue":       next_issue,
        "find_copy_links":  _find_copy_links_issue(issue),
        "col_status":       col_status,
        "col_item_type":    "magazine",
        "col_item_id":      pub_id,
    })


def author_search(request):
    """Search for all fiction by an author in a magazine."""
    magazine_name = request.GET.get("magazine", "").strip()
    author_name   = request.GET.get("author", "").strip()

    context = {
        "magazine_name": magazine_name,
        "author_name":   author_name,
    }

    if not magazine_name or not author_name:
        return render(request, "magazine/author_search.html", context)

    cursor = _dict_cursor()
    try:
        rows = get_author_fiction(cursor, magazine_name, author_name)
    finally:
        cursor.close()

    if not rows:
        context["error"] = (
            f'No fiction found matching author \u201c{author_name}\u201d'
            f' in magazine \u201c{magazine_name}\u201d.'
        )
        return render(request, "magazine/author_search.html", context)

    context["rows"]        = rows
    context["total"]       = len(rows)
    context["issue_count"] = len({r["pub_id"] for r in rows})
    return render(request, "magazine/author_search.html", context)


def author_detail(request, author_id):
    """Biography and metadata for a single author."""
    cursor = _dict_cursor()
    try:
        author = get_author_detail(cursor, author_id)
        if not author:
            raise Http404(f"No author with id={author_id}")
        mag_works      = get_author_works(cursor, author_id)
        books          = get_author_books(cursor, author_id)
        _, series      = get_series_by_author(cursor, author_id)
        author_awards  = get_author_awards(cursor, author_id)
        cover_art      = get_author_art(cursor, author_id, "COVERART")
        interior_art   = get_author_art(cursor, author_id, "INTERIORART")
    finally:
        cursor.close()

    return render(request, "magazine/author_detail.html", {
        "author":         author,
        "mag_works":      mag_works,
        "books":          books,
        "series":         series,
        "author_awards":  author_awards,
        "cover_art":      cover_art,
        "interior_art":   interior_art,
    })


def author_books(request, author_id):
    """All English-language books by a specific author, in chronological order."""
    cursor = _dict_cursor()
    try:
        author = get_author_detail(cursor, author_id)
        if not author:
            raise Http404(f"No author with id={author_id}")
        rows = get_author_books(cursor, author_id)
    finally:
        cursor.close()

    return render(request, "magazine/author_books.html", {
        "author": author,
        "rows":   rows,
        "total":  len(rows),
    })


def author_works(request, author_id):
    """All magazine works by a specific author, in chronological order."""
    cursor = _dict_cursor()
    try:
        author = get_author_detail(cursor, author_id)
        if not author:
            raise Http404(f"No author with id={author_id}")
        rows = get_author_works(cursor, author_id)
    finally:
        cursor.close()

    return render(request, "magazine/author_works.html", {
        "author": author,
        "rows":   rows,
        "total":  len(rows),
    })


# Magazine groups: curated lists of exact title variants shown as an
# intermediate card page before the user picks a specific run.
_MAGAZINE_GROUPS = {
    "astounding": [
        "Astounding Stories of Super-Science",
        "Astounding Stories",
        "Astounding Science-Fiction",
        "Astounding Science Fiction",
        "Astounding/Analog Science Fact & Fiction",
    ],
    "fsf": [
        "The Magazine of Fantasy & Science Fiction",
        "The Magazine of Fantasy and Science Fiction",
    ],
    "galaxy": [
        "Galaxy",
        "Galaxy Magazine",
        "Galaxy Science Fiction",
        "Galaxy Science Fiction Magazine",
    ],
    "unknown": [
        "Unknown",
        "Unknown Worlds",
    ],
}

# 24 highlighted authors shown on the Authors page, in display order.
# IDs verified against the local ISFDB database.
_SELECTED_AUTHORS = [
    ("Poul Anderson",        3),
    ("Isaac Asimov",         5),
    ("Ray Bradbury",       194),
    ("Edgar Rice Burroughs", 143),
    ("Octavia E. Butler",  186),
    ("Arthur C. Clarke",    17),
    ("Samuel R. Delany",    22),
    ("Philip K. Dick",      23),
    ("Robert A. Heinlein",  29),
    ("Ursula K. Le Guin",   37),
    ("Larry Niven",         42),
    ("Andre Norton",       209),
    ("Edgar Allan Poe",    622),
    ("Joanna Russ",        222),
    ("Clifford D. Simak",   55),
    ("Edward E. Smith",     67),
    ("Cordwainer Smith",   101),
    ("Olaf Stapledon",      81),
    ("Theodore Sturgeon",   56),
    ("J. R. R. Tolkien",  302),
    ("A. E. van Vogt",      58),
    ("Jules Verne",        159),
    ("Kurt Vonnegut, Jr.", 62),
    ("H. G. Wells",         65),
]

_SELECTED_MAGAZINES = [
    {"name": "Amazing Stories",                           "url": "/magazines/browse/?name=Amazing+Stories"},
    {"name": "Astounding Science Fiction",                "url": "/magazines/group/astounding/"},
    {"name": "Analog Science Fiction",                    "q":   "analog"},
    {"name": "The Magazine of Fantasy & Science Fiction", "url": "/magazines/group/fsf/"},
    {"name": "Galaxy Science Fiction",                    "url": "/magazines/group/galaxy/"},
    {"name": "Planet Stories",                            "url": "/magazines/browse/?name=Planet+Stories"},
    {"name": "Startling Stories",                         "url": "/magazines/browse/?name=Startling+Stories"},
    {"name": "Thrilling Wonder Stories",                  "url": "/magazines/browse/?name=Thrilling+Wonder+Stories"},
    {"name": "Unknown Worlds",                            "url": "/magazines/group/unknown/"},
    {"name": "Weird Tales",                               "url": "/magazines/browse/?name=Weird+Tales"},
    {"name": "Worlds of If",                              "url": "/magazines/browse/?name=Worlds+of+If"},
    {"name": "Worlds of Tomorrow",                        "url": "/magazines/browse/?name=Worlds+of+Tomorrow"},
]


def magazine_list(request):
    """Card grid of magazines — searchable, or browsed by first letter."""
    query  = request.GET.get("q", "").strip()
    letter = request.GET.get("letter", "").upper()
    if len(letter) != 1 or not letter.isalpha():
        letter = ""

    cursor = _dict_cursor()
    try:
        if query:
            magazines = search_magazines(cursor, query)
            all_mags  = get_all_magazines(cursor)
        else:
            all_mags  = get_all_magazines(cursor)
            magazines = None
    finally:
        cursor.close()

    def _first_letter(name):
        n = name.upper()
        if n.startswith("THE "):
            n = n[4:]
        return n[0] if n and n[0].isalpha() else "#"

    letters_with_mags = sorted({_first_letter(m["mag_name"]) for m in all_mags if _first_letter(m["mag_name"]) != "#"})

    if magazines is None:
        magazines = [m for m in all_mags if _first_letter(m["mag_name"]) == letter] if letter else []

    return render(request, "magazine/magazine_list.html", {
        "magazines":          magazines,
        "letter":             letter if not query else None,
        "letters_with_mags":  letters_with_mags,
        "total_all":          f"{len(all_mags):,}",
        "query":              query,
        "selected_magazines": _SELECTED_MAGAZINES,
    })


def magazine_group(request, group_slug):
    """Intermediate page showing cards for a curated group of magazine title variants."""
    names = _MAGAZINE_GROUPS.get(group_slug)
    if not names:
        raise Http404(f"No magazine group '{group_slug}'")
    cursor = _dict_cursor()
    try:
        magazines = get_magazine_group_info(cursor, names)
    finally:
        cursor.close()
    return render(request, "magazine/magazine_group.html", {
        "magazines": magazines,
        "group_slug": group_slug,
    })


def magazine_issues_by_name(request):
    """Chronological list of all issues for one magazine, looked up by name."""
    mag_name = request.GET.get("name", "").strip()
    if not mag_name:
        return redirect("magazine_list")

    cursor = _dict_cursor()
    try:
        rows = get_magazine_issues_by_name(cursor, mag_name)
    finally:
        cursor.close()

    if not rows:
        raise Http404(f"No issues found for magazine {mag_name!r}")

    use_accordion = len(rows) > 50
    decades = []
    if use_accordion:
        from collections import defaultdict
        by_decade = defaultdict(list)
        for r in rows:
            decade = (r["pub_year"] // 10) * 10
            by_decade[decade].append(r)
        for decade in sorted(by_decade):
            decades.append({
                "label":  f"{decade}s",
                "decade": decade,
                "issues": by_decade[decade],
            })

    return render(request, "magazine/magazine_issues.html", {
        "mag_name":      mag_name,
        "mag_code":      None,
        "rows":          rows,
        "total":         len(rows),
        "use_accordion": use_accordion,
        "decades":       decades,
    })


def magazine_issues(request, mag_code):
    """Chronological list of all issues for one magazine."""
    cursor = _dict_cursor()
    try:
        mag_name, rows = get_magazine_issues(cursor, mag_code)
    finally:
        cursor.close()
    if mag_name is None:
        raise Http404(f"No magazine with code {mag_code!r}")

    # Group by decade for large lists
    use_accordion = len(rows) > 50
    decades = []
    if use_accordion:
        from collections import defaultdict
        by_decade = defaultdict(list)
        for r in rows:
            decade = (r["pub_year"] // 10) * 10
            by_decade[decade].append(r)
        for decade in sorted(by_decade):
            decades.append({
                "label":  f"{decade}s",
                "decade": decade,
                "issues": by_decade[decade],
            })

    return render(request, "magazine/magazine_issues.html", {
        "mag_name":      mag_name,
        "mag_code":      mag_code,
        "rows":          rows,
        "total":         len(rows),
        "use_accordion": use_accordion,
        "decades":       decades,
    })


def _find_copy_links_issue(issue):
    """Build bookseller search links for a magazine issue, preferring ISBN."""
    isbn  = (issue.get("pub_isbn") or "").strip()
    title = quote_plus(issue.get("pub_title") or "")
    if isbn:
        return [
            {"label": "AbeBooks", "url": f"https://www.abebooks.com/servlet/SearchResults?isbn={isbn}"},
            {"label": "Amazon",   "url": f"https://www.amazon.com/s?k={isbn}"},
            {"label": "eBay",     "url": f"https://www.ebay.com/sch/i.html?_nkw={isbn}"},
            {"label": "WorldCat", "url": f"https://www.worldcat.org/isbn/{isbn}"},
        ]
    else:
        return [
            {"label": "AbeBooks", "url": f"https://www.abebooks.com/servlet/SearchResults?tn={title}"},
            {"label": "Amazon",   "url": f"https://www.amazon.com/s?k={title}"},
            {"label": "eBay",     "url": f"https://www.ebay.com/sch/i.html?_nkw={title}"},
            {"label": "WorldCat", "url": f"https://www.worldcat.org/search?q={title}"},
        ]


def _find_copy_links(book):
    """Build bookseller/library search links for a book, preferring ISBN."""
    isbn    = (book.get("pub_isbn") or "").strip()
    title   = quote_plus(book.get("title_title") or "")
    authors = quote_plus(book.get("authors") or "")
    if isbn:
        return [
            {"label": "AbeBooks",     "url": f"https://www.abebooks.com/servlet/SearchResults?isbn={isbn}"},
            {"label": "Amazon",       "url": f"https://www.amazon.com/s?k={isbn}"},
            {"label": "ThriftBooks",  "url": f"https://www.thriftbooks.com/browse/?b.search={isbn}"},
            {"label": "WorldCat",     "url": f"https://www.worldcat.org/isbn/{isbn}"},
            {"label": "Open Library", "url": f"https://openlibrary.org/isbn/{isbn}"},
        ]
    else:
        q = f"{title}+{authors}"
        return [
            {"label": "AbeBooks",     "url": f"https://www.abebooks.com/servlet/SearchResults?an={authors}&tn={title}"},
            {"label": "Amazon",       "url": f"https://www.amazon.com/s?k={q}"},
            {"label": "ThriftBooks",  "url": f"https://www.thriftbooks.com/browse/?b.search={q}"},
            {"label": "WorldCat",     "url": f"https://www.worldcat.org/search?q={q}"},
            {"label": "Open Library", "url": f"https://openlibrary.org/search?q={q}"},
        ]


@ensure_csrf_cookie
def book_detail(request, title_id):
    """Details for a single book title, optionally for a specific pub_id edition."""
    pub_param = request.GET.get("pub", "").strip()
    pub_id    = int(pub_param) if pub_param.isdigit() else None
    cursor = _dict_cursor()
    try:
        book = get_book_detail(cursor, title_id, pub_id)
        if not book:
            raise Http404(f"No book found for title_id={title_id}")
        editions   = get_book_editions(cursor, title_id, book["pub_id"])
        contents   = get_book_contents(cursor, book["pub_id"])
        reviews    = get_book_reviews(cursor, title_id)
        token      = _get_collection_token(request)
        col_status = (get_collection_status(cursor, token, "book", title_id)
                      if token else _empty_col_status())
    finally:
        cursor.close()
    return render(request, "magazine/book_detail.html", {
        "book": book, "editions": editions, "contents": contents, "reviews": reviews,
        "find_copy_links": _find_copy_links(book),
        "col_status":      col_status,
        "col_item_type":   "book",
        "col_item_id":     title_id,
    })


def author_list(request):
    """Authors page: count, dual search forms, and selected author cards."""
    query       = request.GET.get("q", "").strip()
    search_type = request.GET.get("search_type", "full")
    if search_type not in ("full", "last"):
        search_type = "full"

    cursor = _dict_cursor()
    try:
        total_authors = get_author_count(cursor)
        authors = find_authors(cursor, query, search_type) if query else []
    finally:
        cursor.close()

    return render(request, "magazine/author_list.html", {
        "query":            query,
        "search_type":      search_type,
        "authors":          authors,
        "total":            len(authors),
        "total_with_works": sum(1 for a in authors if a.get("title_count")),
        "total_authors":    f"{total_authors:,}",
        "selected_authors": _SELECTED_AUTHORS,
    })


_SELECTED_ARTISTS = [
    ("Frank R. Paul",      731,   "1884–1963"),
    ("Alex Schomburg",     1306,  "1905–1998"),
    ("Hannes Bok",         609,   "1914–1964"),
    ("Richard Powers",     3508,  "1921–1996"),
    ("Frank Kelly Freas",  488,   "1922–2005"),
    ("Ed Emshwiller",      550,   "1925–1990"),
    ("Frank Frazetta",     1825,  "1928–2010"),
    ("Jack Gaughan",       1805,  "1930–1985"),
    ("John Berkey",        22975, "1932–2008"),
    ("John Schoenherr",    1393,  "1935–2010"),
    ("H. R. Giger",        21090, "1940–2014"),
    ("Michael Whelan",     1804,  "1950–"),
]


def artist_list(request):
    """Artists page: count, search form, and selected artist cards."""
    query       = request.GET.get("q", "").strip()
    search_type = request.GET.get("search_type", "full")
    scope       = request.GET.get("scope", "artists")
    if search_type not in ("full", "last"):
        search_type = "full"
    if scope not in ("artists", "all"):
        scope = "artists"

    cursor = _dict_cursor()
    try:
        artists = find_artists(cursor, query, search_type, scope) if query else []
    finally:
        cursor.close()

    return render(request, "magazine/artist_list.html", {
        "query":            query,
        "search_type":      search_type,
        "scope":            scope,
        "artists":          artists,
        "total":            len(artists),
        "selected_artists": _SELECTED_ARTISTS,
    })


_SELECTED_PUBLISHERS = [
    # (display name, publisher_id, pub_count)
    ("Tor",                   23,    "11,538"),
    ("Pabel-Moewig",          34608, "9,237"),
    ("Ace Books",             37,    "8,309"),
    ("Del Rey / Ballantine",  15,    "6,582"),
    ("Heyne",                 1127,  "5,978"),
    ("Gollancz",              13,    "5,502"),
    ("Tantor Audio",          58216, "5,495"),
    ("Project Gutenberg",     13741, "5,168"),
    ("Baen Books",            38,    "5,025"),
    ("DAW Books",             43,    "4,837"),
    ("Orbit",                 113,   "4,689"),
    ("Bastei Lübbe",          5719,  "4,629"),
]


def publisher_list(request):
    """Publishers page: count, search form, and selected publisher cards."""
    query = request.GET.get("q", "").strip()

    cursor = _dict_cursor()
    try:
        total_publishers = get_publisher_count(cursor)
        publishers = find_publishers(cursor, query) if query else []
    finally:
        cursor.close()

    return render(request, "magazine/publisher_list.html", {
        "query":               query,
        "publishers":          publishers,
        "total":               len(publishers),
        "total_publishers":    f"{total_publishers:,}",
        "selected_publishers": _SELECTED_PUBLISHERS,
    })


def publisher_detail(request, publisher_id):
    """Publisher detail page: metadata, author search, year grid + book list."""
    cursor = _dict_cursor()
    try:
        publisher = get_publisher_detail(cursor, publisher_id)
        if publisher is None:
            raise Http404("Publisher not found")

        year_param      = request.GET.get("year",       "").strip()
        author_param    = request.GET.get("author",     "").strip()
        show_all_authors = request.GET.get("all_authors", "").strip()

        year_books    = []
        author_books  = []
        all_authors   = []
        selected_year = None

        if year_param and year_param.isdigit():
            selected_year = int(year_param)
            year_books = get_publisher_books_by_year(cursor, publisher_id, selected_year)
        elif author_param:
            author_books = get_publisher_books_by_author(cursor, publisher_id, author_param)
        elif show_all_authors:
            all_authors = get_publisher_all_authors(cursor, publisher_id)
    finally:
        cursor.close()

    # Build decade rows: one row per full decade, placeholder entries for
    # years with no publications so every row is exactly 10 wide.
    active_map = {y["yr"]: y["title_cnt"] for y in publisher["active_years"]}
    if active_map:
        decade_start = (min(active_map) // 10) * 10
        decade_end   = (max(active_map) // 10) * 10
        year_rows = [
            [
                {"yr": yr, "title_cnt": active_map.get(yr, 0), "active": yr in active_map}
                for yr in range(decade, decade + 10)
            ]
            for decade in range(decade_start, decade_end + 1, 10)
        ]
    else:
        year_rows = []

    return render(request, "magazine/publisher_detail.html", {
        "publisher":       publisher,
        "year_param":      year_param,
        "author_param":    author_param,
        "show_all_authors": show_all_authors,
        "selected_year":   selected_year,
        "year_books":      year_books,
        "author_books":    author_books,
        "all_authors":     all_authors,
        "year_rows":       year_rows,
    })


def random_item(request, kind):
    """Pick a random author, magazine issue, or book and redirect to its page."""
    cursor = _dict_cursor()
    try:
        if kind == "author":
            item_id = get_random_author_id(cursor)
            if item_id:
                return redirect("author_detail", author_id=item_id)
        elif kind == "issue":
            item_id = get_random_issue_id(cursor)
            if item_id:
                return redirect("issue_detail", pub_id=item_id)
        elif kind == "book":
            # Retry up to 5 times in case the title has no qualifying pub.
            for _ in range(5):
                item_id = get_random_book_title_id(cursor)
                if item_id and get_book_detail(cursor, item_id):
                    return redirect("book_detail", title_id=item_id)
        elif kind == "short_fiction":
            item_id = get_random_short_fiction_id(cursor)
            if item_id:
                return redirect("story_detail", title_id=item_id)
    finally:
        cursor.close()
    raise Http404(f"Could not find a random {kind}")


def title_list(request):
    """Titles landing page with search form and selected title cards."""
    return render(request, "magazine/title_list.html")


_VALID_TITLE_TYPES = {
    "ANTHOLOGY", "CHAPBOOK", "COLLECTION", "COVERART", "EDITOR",
    "ESSAY", "INTERIORART", "INTERVIEW", "NONFICTION", "NOVEL",
    "OMNIBUS", "POEM", "REVIEW", "SERIAL", "SHORTFICTION",
}

def title_search(request):
    """Search for titles by name."""
    query        = request.GET.get("q", "").strip()
    content_type = request.GET.get("content_type", "all")
    title_type   = request.GET.get("title_type", "").strip().upper()
    length       = request.GET.get("length", "").strip()
    language     = request.GET.get("language", "17").strip()
    juvenile     = bool(request.GET.get("juvenile"))
    novelization = bool(request.GET.get("novelization"))
    non_genre    = bool(request.GET.get("non_genre"))
    graphic      = bool(request.GET.get("graphic"))

    if content_type not in ("all", "book", "fiction"):   content_type = "all"
    if title_type   not in _VALID_TITLE_TYPES:           title_type   = ""
    if length       not in ("novelette", "novella", "short story"): length = ""
    lang_id = int(language) if language.isdigit() else 17

    context = {
        "query": query, "content_type": content_type,
        "title_type": title_type, "length": length, "language": str(lang_id),
        "juvenile": juvenile, "novelization": novelization,
        "non_genre": non_genre, "graphic": graphic,
    }

    if query:
        cursor = _dict_cursor()
        try:
            titles = find_titles(cursor, query, "exact", content_type,
                                 title_type, length, lang_id,
                                 juvenile, novelization, non_genre, graphic)
        finally:
            cursor.close()
        context["titles"] = titles
        context["total"]  = len(titles)

    return render(request, "magazine/title_search.html", context)


def story_detail(request, title_id):
    """Detail page for a magazine fiction title."""
    cursor = _dict_cursor()
    try:
        story = get_story_detail(cursor, title_id)
    finally:
        cursor.close()
    if not story:
        raise Http404(f"No title found for title_id={title_id}")
    return render(request, "magazine/story_detail.html", {"story": story})


def award_list(request):
    """Browse all awards with search and A-Z navigation."""
    query = request.GET.get("q", "").strip()
    letter = request.GET.get("letter", "").strip().upper()

    cursor = _dict_cursor()
    try:
        all_awards = get_all_award_types(cursor)
    finally:
        cursor.close()

    # Build A-Z letter set from available awards
    letters = sorted({a["award_type_name"][0].upper() for a in all_awards
                      if a["award_type_name"] and a["award_type_name"][0].isalpha()})

    # Filter for display
    if query:
        displayed = [a for a in all_awards
                     if query.lower() in a["award_type_name"].lower()]
    elif letter:
        displayed = [a for a in all_awards
                     if a["award_type_name"].upper().startswith(letter)]
    else:
        displayed = []

    major_awards = [{"id": aid, "name": _MAJOR_AWARD_NAMES[aid]}
                    for aid in _MAJOR_AWARD_IDS]

    return render(request, "magazine/award_list.html", {
        "query":        query,
        "letter":       letter,
        "letters":      letters,
        "displayed":    displayed,
        "major_awards": major_awards,
        "total_awards": len(all_awards),
    })


def new_award_list(request):
    """New awards page — currently a copy of award_list."""
    query = request.GET.get("q", "").strip()
    letter = request.GET.get("letter", "").strip().upper()

    cursor = _dict_cursor()
    try:
        all_awards = get_all_award_types(cursor)
    finally:
        cursor.close()

    letters = sorted({a["award_type_name"][0].upper() for a in all_awards
                      if a["award_type_name"] and a["award_type_name"][0].isalpha()})

    if query:
        displayed = [a for a in all_awards
                     if query.lower() in a["award_type_name"].lower()]
    elif letter:
        displayed = [a for a in all_awards
                     if a["award_type_name"].upper().startswith(letter)]
    else:
        displayed = []

    major_awards = [{"id": aid, "name": _MAJOR_AWARD_NAMES[aid]}
                    for aid in _MAJOR_AWARD_IDS]

    return render(request, "magazine/new_award_list.html", {
        "query":        query,
        "letter":       letter,
        "letters":      letters,
        "displayed":    displayed,
        "major_awards": major_awards,
        "total_awards": len(all_awards),
    })


def new_award_detail(request, award_type_id):
    """New award detail page with category filter, nominee toggle, and sort order."""
    selected_cat     = request.GET.get("cat", "").strip()
    include_nominees = request.GET.get("nominees", "no") == "yes"
    sort_asc         = request.GET.get("sort", "desc") == "asc"

    cursor = _dict_cursor()
    try:
        award = get_award_type_info(cursor, award_type_id)
        if not award:
            raise Http404(f"No award with id={award_type_id}")
        categories  = get_award_categories(cursor, award_type_id)
        year_blocks = (
            get_award_entries_by_category(cursor, award_type_id, selected_cat, sort_asc)
            if selected_cat else []
        )
    finally:
        cursor.close()

    return render(request, "magazine/new_award_detail.html", {
        "award":            award,
        "categories":       categories,
        "selected_cat":     selected_cat,
        "include_nominees": include_nominees,
        "sort_asc":         sort_asc,
        "year_blocks":      year_blocks,
    })


def award_detail(request, award_type_id):
    """All entries for a single award type, grouped by year and category."""
    cursor = _dict_cursor()
    try:
        award = get_award_detail(cursor, award_type_id)
    finally:
        cursor.close()
    if not award:
        raise Http404(f"No award with id={award_type_id}")
    return render(request, "magazine/award_detail.html", {"award": award})


_SERIES_LIST_LIMIT = 300


def series_list(request):
    """Browse all series: search by name or author, or browse A-Z."""
    query       = request.GET.get("q", "").strip()
    search_type = request.GET.get("search_type", "series")
    letter      = request.GET.get("letter", "").strip().upper()
    if search_type not in ("series", "author"):
        search_type = "series"
    if len(letter) != 1 or not letter.isalpha():
        letter = ""

    cursor = _dict_cursor()
    try:
        letters      = get_series_letters(cursor)
        total_series = get_series_count(cursor)
        if query and search_type == "author":
            # Return matching authors for the user to choose from
            author_matches  = find_authors(cursor, query)
            displayed       = []
            pub_series_hits = []
            total_shown     = 0
            total_letter    = None
        elif query:
            author_matches  = []
            displayed       = search_series(cursor, query)
            pub_series_hits = search_pub_series(cursor, query)
            total_shown     = len(displayed)
            total_letter    = None
        elif letter:
            author_matches  = []
            displayed, total_letter = get_series_by_letter(cursor, letter, _SERIES_LIST_LIMIT)
            pub_series_hits = []
            total_shown     = len(displayed)
        else:
            author_matches  = []
            displayed       = []
            pub_series_hits = []
            total_shown     = 0
            total_letter    = None
    finally:
        cursor.close()

    major_series = [
        {"id": sid, **_MAJOR_SERIES_INFO[sid]}
        for sid in _MAJOR_SERIES_IDS
    ]

    return render(request, "magazine/series_list.html", {
        "query":          query,
        "search_type":    search_type,
        "letter":         letter,
        "letters":        letters,
        "author_matches": author_matches,
        "displayed":      displayed,
        "pub_series_hits": pub_series_hits,
        "total_shown":    total_shown,
        "total_letter":   total_letter,
        "limit":          _SERIES_LIST_LIMIT,
        "major_series":   major_series,
        "total_series":   f"{total_series:,}",
    })


def series_by_author(request, author_id):
    """All series containing at least one title by the given author."""
    cursor = _dict_cursor()
    try:
        author_name, series = get_series_by_author(cursor, author_id)
    finally:
        cursor.close()
    if author_name is None:
        raise Http404(f"No author with id={author_id}")
    return render(request, "magazine/series_by_author.html", {
        "author_id":   author_id,
        "author_name": author_name,
        "series":      series,
        "total":       len(series),
    })


def series_detail(request, series_id):
    """All titles in a single series."""
    cursor = _dict_cursor()
    try:
        series = get_series_detail(cursor, series_id)
    finally:
        cursor.close()
    if not series:
        raise Http404(f"No series with id={series_id}")
    return render(request, "magazine/series_detail.html", {"series": series})


def pub_series_detail(request, pub_series_id):
    """All titles in a single publication series."""
    cursor = _dict_cursor()
    try:
        pub_series = get_pub_series_detail(cursor, pub_series_id)
    finally:
        cursor.close()
    if not pub_series:
        raise Http404(f"No publication series with id={pub_series_id}")
    return render(request, "magazine/pub_series_detail.html", {"pub_series": pub_series})


def about(request):
    """About page with hardcoded database statistics (from Feb 2026 snapshot)."""
    return render(request, "magazine/about.html")


_signer = TimestampSigner()


@csrf_exempt
def contact(request):
    """Contact form — sends an email to the site owner on POST."""
    success = False
    errors  = {}
    form    = {}

    if request.method == "POST":
        form["message"]  = request.POST.get("message", "").strip()
        form["email"]    = request.POST.get("email", "").strip()
        user_answer      = request.POST.get("captcha_answer", "").strip()
        signed_answer    = request.POST.get("captcha_signed", "")

        # Validate required fields
        if not form["message"]:
            errors["message"] = "Please enter a comment or question."
        if not form["email"]:
            errors["email"] = "Please enter your email address."
        elif "@" not in form["email"] or "." not in form["email"].split("@")[-1]:
            errors["email"] = "Please enter a valid email address."

        # Verify arithmetic challenge (signed answer valid for 1 hour)
        captcha_ok = False
        try:
            expected = _signer.unsign(signed_answer, max_age=3600)
            captcha_ok = user_answer == expected
        except (BadSignature, SignatureExpired):
            pass
        if not captcha_ok:
            errors["captcha"] = "Incorrect answer — please try again."

        if not errors:
            body = (
                f"Message:\n{form['message']}\n\n"
                f"From: {form['email']}"
            )
            try:
                send_mail(
                    subject="ISFDB Explorer — contact form",
                    message=body,
                    from_email=django_settings.DEFAULT_FROM_EMAIL,
                    recipient_list=[django_settings.CONTACT_RECIPIENT],
                    fail_silently=False,
                )
                success = True
            except Exception as exc:
                errors["email"] = (
                    f"Sorry, the message could not be sent "
                    f"({type(exc).__name__}: {exc}). "
                    "Please try again later."
                )

    # Generate a fresh arithmetic challenge for GET or after failed POST
    if not success:
        a, b = random.randint(1, 9), random.randint(1, 9)
        captcha_question = f"What is {a} + {b}?"
        captcha_signed   = _signer.sign(str(a + b))
    else:
        captcha_question = captcha_signed = ""

    return render(request, "magazine/contact.html", {
        "success":          success,
        "errors":           errors,
        "form":             form,
        "captcha_question": captcha_question,
        "captcha_signed":   captcha_signed,
    })


@require_POST
def collection_toggle(request):
    """
    AJAX endpoint: toggle owned/wanted status for one item.

    POST body (JSON): {"item_type": "book"|"magazine", "item_id": int, "status": "owned"|"wanted"}
    Response  (JSON): {"added": bool, "owned": bool, "wanted": bool}

    Creates the collection_token cookie if this is the user's first toggle.
    """
    try:
        data      = json.loads(request.body)
        item_type = data["item_type"]
        item_id   = int(data["item_id"])
        status    = data["status"]
    except (KeyError, ValueError, TypeError):
        return JsonResponse({"error": "bad request"}, status=400)

    if item_type not in ("book", "magazine") or status not in ("owned", "wanted"):
        return JsonResponse({"error": "bad request"}, status=400)

    token     = _get_collection_token(request)
    new_token = not token
    if new_token:
        token = str(_uuid.uuid4())

    cursor = _dict_cursor()
    try:
        added      = toggle_collection_item(cursor, token, item_type, item_id, status)
        col_status = get_collection_status(cursor, token, item_type, item_id)
    finally:
        cursor.close()

    response = JsonResponse({
        "added":  added,
        "owned":  col_status["owned"],
        "wanted": col_status["wanted"],
    })
    if new_token:
        response.set_cookie(
            _COLLECTION_COOKIE, token,
            max_age=_COLLECTION_COOKIE_AGE,
            httponly=True,
            samesite="Lax",
        )
    return response


def my_collection(request):
    """The My Collection page — shows all owned and wanted items for this token."""
    token = _get_collection_token(request)

    if token:
        cursor = _dict_cursor()
        try:
            collection = get_full_collection(cursor, token)
        finally:
            cursor.close()
    else:
        collection = {
            "owned_books":     [],
            "wanted_books":    [],
            "owned_magazines": [],
            "wanted_magazines": [],
            "total":           0,
        }

    return render(request, "magazine/my_collection.html", {
        "token":      token or "",
        "collection": collection,
    })


@require_POST
def collection_set_token(request):
    """POST endpoint: replace the active collection token with a user-supplied UUID."""
    raw = request.POST.get("token", "").strip()
    try:
        _uuid.UUID(raw)          # validate format
        token = raw.lower()
    except ValueError:
        return redirect("my_collection")

    response = redirect("my_collection")
    response.set_cookie(
        _COLLECTION_COOKIE, token,
        max_age=_COLLECTION_COOKIE_AGE,
        httponly=True,
        samesite="Lax",
    )
    return response


def error_404(request, exception=None):
    return render(request, "404.html", status=404)


def error_500(request):
    return render(request, "500.html", status=500)

