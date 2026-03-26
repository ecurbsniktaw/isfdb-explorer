from django.db import connection
from django.shortcuts import render, redirect
from django.http import Http404

from .queries import (
    find_issues, get_issue_meta, get_contents, get_archive_links,
    get_author_fiction, get_author_detail, get_author_works, get_author_books,
    get_book_detail, get_book_editions,
    get_magazine_issues_by_name,
    get_all_magazines, get_magazine_issues, search_magazines,
    find_authors,
    get_random_author_id, get_random_issue_id, get_random_book_title_id,
    format_date, NARRATIVE_TYPES,
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


def issue_detail(request, pub_id):
    """Table of contents for one magazine issue."""
    cursor = _dict_cursor()
    try:
        issue = get_issue_meta(cursor, pub_id)
        if not issue:
            raise Http404(f"No magazine issue with pub_id={pub_id}")
        contents = get_contents(cursor, pub_id)
        archive_links = get_archive_links(cursor, pub_id)
    finally:
        cursor.close()

    narrative, other = _split_contents(contents)
    issue["formatted_date"] = format_date(issue["pub_year"], issue["pub_month"])

    # Pass search params back so the "Back" link restores the previous search
    back_params = request.GET.urlencode()

    return render(request, "magazine/issue.html", {
        "issue":        issue,
        "narrative":    narrative,
        "other":        other,
        "back_params":  back_params,
        "archive_links": archive_links,
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
    finally:
        cursor.close()

    if not author:
        raise Http404(f"No author with id={author_id}")

    return render(request, "magazine/author_detail.html", {"author": author})


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


def magazine_list(request):
    """Card grid of magazines — searchable, or browsed by first letter."""
    query  = request.GET.get("q", "").strip()
    letter = request.GET.get("letter", "A").upper()
    if len(letter) != 1 or not letter.isalpha():
        letter = "A"

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
        magazines = [m for m in all_mags if _first_letter(m["mag_name"]) == letter]

    return render(request, "magazine/magazine_list.html", {
        "magazines":         magazines,
        "letter":            letter if not query else None,
        "letters_with_mags": letters_with_mags,
        "total_all":         len(all_mags),
        "query":             query,
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


def book_detail(request, title_id):
    """First-edition details for a single book title."""
    cursor = _dict_cursor()
    try:
        book = get_book_detail(cursor, title_id)
        if not book:
            raise Http404(f"No book found for title_id={title_id}")
        editions = get_book_editions(cursor, title_id, book["pub_id"])
    finally:
        cursor.close()
    return render(request, "magazine/book_detail.html", {"book": book, "editions": editions})


def find_authors_view(request):
    """Search for authors by name and display a list of matches."""
    query = request.GET.get("q", "").strip()
    context = {"query": query}

    if query:
        cursor = _dict_cursor()
        try:
            authors = find_authors(cursor, query)
        finally:
            cursor.close()
        context["authors"] = authors
        context["total"] = len(authors)
        context["total_with_works"] = sum(1 for a in authors if a.get("title_count"))

    return render(request, "magazine/find_authors.html", context)


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
    finally:
        cursor.close()
    raise Http404(f"Could not find a random {kind}")
