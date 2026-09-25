import html as _html
from django import template

register = template.Library()


@register.filter
def unescape_html(value):
    """Decode HTML entities stored in the ISFDB database (e.g. &#2325; → क)."""
    if not value:
        return value
    return _html.unescape(value)
