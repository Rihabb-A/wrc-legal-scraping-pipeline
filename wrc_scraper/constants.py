"""Facts about the workplacerelations.ie website.

These are not configuration - they are properties of the source. If a value
here is wrong, the site changed. Tunable settings (delays, concurrency,
credentials) live in .env instead.

Every value was verified by hand against the live site; see docs/recon.md.
"""

import re
from datetime import date
from urllib.parse import urlencode

#: The Decisions and Determinations search page.
SEARCH_URL = "https://www.workplacerelations.ie/en/search/"

#: The four bodies from the Body filter, mapped to the ``value`` attribute of
#: their checkbox. Read directly from the page, not guessed.
#:
#: Careful: the *Case Type* checkboxes reuse the values 1, 2 and 3 for
#: Appeal / Complaint / Enforcement. These are a different filter.
BODIES = {
    "Employment Appeals Tribunal": "2",
    "Equality Tribunal": "1",
    "Labour Court": "3",
    "Workplace Relations Commission": "15376",
}

#: Results per page. Fixed by the site: pageSize, size, perPage, pagesize,
#: resultsPerPage and rows were all tested and are ignored.
PAGE_SIZE = 10

#: Dates are displayed as dd/mm/yyyy.
SITE_DATE_FORMAT = "%d/%m/%Y"

#: The site reports its own total, e.g. "Shows 1 to 10 of 32 results", but
#: renders it with newlines and doubled spaces, so collapse whitespace first.
RESULT_COUNT_RE = re.compile(r"Shows\s+\d+\s+to\s+\d+\s+of\s+(\d+)\s+results")

#: Dash-like characters that appear in identifiers where an ASCII hyphen is
#: meant, mapped to the hyphen.
#:
#: An evaluation run over 994 decisions found four identifiers using an EN
#: DASH: "IR-SC\u201300002726" rather than "IR-SC-00002726". They look
#: identical and are not: a lookup by identifier misses them, and the curated
#: filename ends up carrying a non-ASCII character. The site is inconsistent
#: with itself here - every other IR-SC reference uses a plain hyphen.
DASH_CHARACTERS = {
    "\u2010": "-",  # hyphen
    "\u2011": "-",  # non-breaking hyphen
    "\u2012": "-",  # figure dash
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u2015": "-",  # horizontal bar
    "\u2212": "-",  # minus sign
}

#: Shown when a search legitimately matches nothing, and also when pageNumber
#: runs past the last page. Quite different from div.searchhead being absent,
#: which means the request itself was malformed.
NO_RESULTS_TEXT = "There are no search results"


def build_search_url(body_id: str, start: date, end: date, page: int = 1) -> str:
    """Build a search URL for one body over one date range.

    Dates are sent as ISO ``yyyy-mm-dd``. The site also accepts dd/mm/yyyy,
    but silently returns nothing for the American mm/dd/yyyy - ISO removes
    the ambiguity entirely.
    """
    params = {
        "decisions": 1,
        "body": body_id,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "pageNumber": page,
    }
    return f"{SEARCH_URL}?{urlencode(params)}"


def resolve_bodies(spec: str | None = None) -> dict[str, str]:
    """Pick which bodies to scrape.

    Args:
        spec: Comma-separated body names, or None for all four.

    Returns:
        Mapping of body name to body id, in the order given.

    Raises:
        ValueError: If a name is not one of the four known bodies. Failing
            here is deliberate - a typo would otherwise scrape nothing and
            the run would look successful.
    """
    if spec is None or not spec.strip():
        return dict(BODIES)

    selected = {}
    for raw_name in spec.split(","):
        name = raw_name.strip()
        if not name:
            continue
        match = next((known for known in BODIES if known.lower() == name.lower()), None)
        if match is None:
            raise ValueError(
                f"Unknown body {name!r}. Expected one or more of: "
                + ", ".join(repr(b) for b in BODIES)
            )
        selected[match] = BODIES[match]

    if not selected:
        raise ValueError("No bodies selected")
    return selected
