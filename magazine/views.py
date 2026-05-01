import random

from django.conf import settings as django_settings
from django.core.mail import send_mail
from django.core.signing import TimestampSigner, BadSignature, SignatureExpired
from django.db import connection
from django.shortcuts import render, redirect
from django.http import Http404
from django.views.decorators.csrf import csrf_exempt

from .queries import (
    find_issues, get_issue_meta, get_contents, get_archive_links, get_adjacent_issues,
    get_author_fiction, get_author_detail, get_author_works, get_author_books,
    get_book_detail, get_book_editions, get_book_contents, get_book_reviews, get_story_detail, find_titles,
    get_magazine_issues_by_name, get_magazine_group_info,
    get_all_magazines, get_magazine_issues, search_magazines,
    find_authors, get_author_count, get_author_art, author_has_series,
    get_random_author_id, get_random_issue_id, get_random_book_title_id,
    get_all_award_types, search_award_types, get_award_detail,
    _MAJOR_AWARD_IDS, _MAJOR_AWARD_NAMES,
    get_series_letters, get_series_count, get_series_by_letter, search_series, search_pub_series,
    get_series_detail, get_series_by_author,
    get_pub_series_detail,
    _MAJOR_SERIES_IDS, _MAJOR_SERIES_INFO,
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
        prev_issue, next_issue = get_adjacent_issues(cursor, pub_id)
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
        "prev_issue":   prev_issue,
        "next_issue":   next_issue,
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
        mag_works    = get_author_works(cursor, author_id)
        cover_art    = get_author_art(cursor, author_id, "COVERART")
        interior_art = get_author_art(cursor, author_id, "INTERIORART")
        has_series   = author_has_series(cursor, author_id)
    finally:
        cursor.close()

    return render(request, "magazine/author_detail.html", {
        "author":       author,
        "mag_works":    mag_works,
        "cover_art":    cover_art,
        "interior_art": interior_art,
        "has_series":   has_series,
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


def book_detail(request, title_id):
    """First-edition details for a single book title."""
    cursor = _dict_cursor()
    try:
        book = get_book_detail(cursor, title_id)
        if not book:
            raise Http404(f"No book found for title_id={title_id}")
        editions  = get_book_editions(cursor, title_id, book["pub_id"])
        contents  = get_book_contents(cursor, book["pub_id"])
        reviews   = get_book_reviews(cursor, title_id)
    finally:
        cursor.close()
    return render(request, "magazine/book_detail.html", {
        "book": book, "editions": editions, "contents": contents, "reviews": reviews,
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


def title_search(request):
    """Search for titles by name."""
    query        = request.GET.get("q", "").strip()
    match_type   = request.GET.get("match_type", "exact")
    content_type = request.GET.get("content_type", "all")
    if match_type   not in ("exact", "partial"): match_type   = "exact"
    if content_type not in ("all", "book", "fiction"): content_type = "all"

    context = {"query": query, "match_type": match_type, "content_type": content_type}

    if query:
        cursor = _dict_cursor()
        try:
            titles = find_titles(cursor, query, match_type, content_type)
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
