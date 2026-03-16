from django.db import connection
from django.shortcuts import render, redirect
from django.http import Http404

from .queries import (
    find_issues, get_issue_meta, get_contents,
    get_author_fiction, get_author_detail, get_author_works, get_author_books,
    find_authors,
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


def search(request):
    """Home page: search form + results."""
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
    finally:
        cursor.close()

    narrative, other = _split_contents(contents)
    issue["formatted_date"] = format_date(issue["pub_year"], issue["pub_month"])

    # Pass search params back so the "Back" link restores the previous search
    back_params = request.GET.urlencode()

    return render(request, "magazine/issue.html", {
        "issue":      issue,
        "narrative":  narrative,
        "other":      other,
        "back_params": back_params,
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

    return render(request, "magazine/find_authors.html", context)
