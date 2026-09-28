import html as _html
import re
from django import template

register = template.Library()


@register.filter
def unescape_html(value):
    """Decode HTML entities stored in the ISFDB database (e.g. &#2325; → क)."""
    if not value:
        return value
    return _html.unescape(value)


_CATALOG_LABELS = {
    'lccn': 'LCCN',
    'oclc': 'OCLC',
    'asin': 'ASIN',
    'reginald1': 'Reginald (1st ed.)',
    'reginald3': 'Reginald (3rd ed.)',
    'locus1': 'Locus Index',
    'tuck': 'Tuck',
    'miller/contento': 'Miller/Contento',
    'contento': 'Contento',
    'sfe': 'SFE',
    'libris': 'Libris',
    'dnb': 'DNB',
    'bl': 'BL',
    'ppn': 'PPN',
    'goodreads': 'Goodreads',
    'fantlab-pub': 'FantLab',
    'fantlab-title': 'FantLab',
    'ltf-pub': 'LTF',
    'bleiler78': 'Bleiler (1978)',
    'clute/nicholls': 'Clute/Nicholls',
    'currey': 'Currey',
    'nilf': 'NILF',
}

_NO_PARAM = {
    'incomplete': (
        'The Contents section of this record is incomplete; '
        'additional eligible titles still need to be added.'
    ),
    "achevé d’imprimer": 'Date when the last copy of the print run was physically produced',
    "achevé d'imprimer":      'Date when the last copy of the print run was physically produced',
    'dépôt légal': (
        'Estimated date when this publication is supposed to be officially registered '
        'and copies sent to the Bibliothèque nationale de France. Reprints may list '
        'either both the original Dépôt Légal date and the new one or '
        'only the original Dépôt Légal date.'
    ),
    'multis': (
        'Note that this title belongs to multiple series. '
        'Because of software limitations, only one of them currently appears in the Series field.'
    ),
    'multipubs': (
        'Note that this publication belongs to multiple publication series. '
        'Because of software limitations, only one of them currently appears in the Publication Series field.'
    ),
    'break': '<br>',
}

_TEMPLATE_RE = re.compile(r'\{\{([^{}|]+)(?:\|([^{}]*))?\}\}')


def _replace_one(m):
    name = m.group(1).strip()
    param = (m.group(2) or '').strip()
    key = name.lower()

    if key in _NO_PARAM:
        return _NO_PARAM[key]

    if key == 'tr':
        return f'Translated by {param}' if param else 'Translated by'
    if key == 'narrator':
        return f'Narrated by {param}' if param else 'Narrated by'
    if key == 'watchprepub':
        return (
            'The following data is based on questionable pre-publication information '
            f'and may be incorrect: {param}'
        )
    if key == 'isbn':
        return f'Additional ISBN {param}'
    if key == 'a':
        return param  # author/artist link → plain name
    if key == 'pv':
        return f'per {param}' if param else 'per a primary verifier'
    if key in ('publisher',):
        return param
    if key in ('pubs', 'pubseries', 's'):
        return param

    if key in _CATALOG_LABELS:
        label = _CATALOG_LABELS[key]
        return f'{label}: {param}' if param else label

    # Unknown template — show bracketed so editors can spot them
    return f'[{name}{": " + param if param else ""}]'


def expand_isfdb_templates(text):
    """Expand ISFDB wiki template strings (e.g. {{Tr|name}}) in note text."""
    if not text:
        return text
    # Multiple passes to resolve nesting like {{Tr|{{A|name}}}}
    for _ in range(3):
        expanded = _TEMPLATE_RE.sub(_replace_one, text)
        if expanded == text:
            break
        text = expanded
    return text


@register.filter
def expand_templates(value):
    return expand_isfdb_templates(value)
