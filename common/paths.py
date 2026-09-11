"""Where a document is stored, in the landing zone and later the curated zone.

Landing keys mirror the source URL path. Curated keys are renamed to
identifier.ext as the assignment requires.

Mirroring the source in landing is deliberate. The site lists ADJ-00054476
twice under two different URLs:

    /en/cases/2025/adj-00054476.html
    /en/cases/2025/july/adj-00054476.html

Naming landing objects after the identifier would make the second overwrite
the first, destroying data in a zone that is supposed to be immutable. The
URL path is unique by construction, so mirroring it keeps both copies and
preserves an obvious link back to where each came from.
"""

import re
from urllib.parse import urlparse

#: Everything under /en/cases/ is a decision document; the prefix carries no
#: information worth repeating in every object key.
_CASES_PREFIX = re.compile(r"^/[a-z]{2}/cases/", re.IGNORECASE)

_SLUG_STRIP = re.compile(r"[^a-z0-9]+")


def slugify(value):
    """Make a value safe for a filesystem path or object key.

    'Workplace Relations Commission' -> 'workplace-relations-commission'
    """
    if not value:
        return "unknown"
    return _SLUG_STRIP.sub("-", value.lower()).strip("-") or "unknown"


def source_relative_path(doc_url):
    """The document's path with the /en/cases/ prefix removed.

    'https://.../en/cases/2025/july/adj-00054476.html'
        -> '2025/july/adj-00054476.html'
    """
    path = urlparse(doc_url).path
    trimmed = _CASES_PREFIX.sub("", path)
    return trimmed.strip("/") or "index.html"


def landing_key(partition_date, body, doc_url, content_hash):
    """Object key for a raw document in the landing zone.

    Partition first, then body, then the source path with a short content
    hash before the extension:

    '2025-07/workplace-relations-commission/2025/july/adj-00054476.9cd39353dbad4973.html'

    Leading with the partition means one run's output sits under a single
    prefix, which makes it cheap to list, re-run or delete a partition.

    Including the content hash is what lets the landing zone stay immutable
    while still preserving amendments. Re-scraping identical content produces
    an identical key, so nothing is rewritten; a genuinely amended document
    produces a different key, so the new version lands beside the old one
    instead of overwriting it.
    """
    relative = source_relative_path(doc_url)
    short_hash = (content_hash or "nohash")[:16]
    stem, dot, extension = relative.rpartition(".")
    versioned = f"{stem}.{short_hash}.{extension}" if dot else f"{relative}.{short_hash}"
    return f"{partition_date}/{slugify(body)}/{versioned}"


def curated_key(partition_date, identifier, extension):
    """Object key for a transformed document.

    The assignment requires curated files to be named identifier.ext.

    '2025-07/ADJ-00054476.html'
    """
    extension = (extension or "").lstrip(".")
    return f"{partition_date}/{identifier}.{extension}"
