"""Reduce a stored WRC page to just the legal decision.

A raw page is roughly 32 KB, of which the decision itself is a small part.
The rest is site furniture: cookie banner, language switcher, two logos, a
disclaimer, a "Return to Search" link, a footer, social links, an "Add To My
Documents" widget and seven script tags.

Reconnaissance established the structure (docs/recon.md section 4):

    body
    └── div#search
        ├── header                     <- chrome
        ├── div.container.searchbanner  <- chrome
        ├── div.container.mb-4
        │   └── div.container > div.row
        │       ├── div.col-sm-3        <- chrome: "Return to Search"
        │       └── div.col-sm-9
        │           ├── h1.page-title   <- KEEP: the identifier
        │           ├── div.content     <- KEEP: the decision
        │           └── script
        ├── footer                     <- chrome
        └── script x 7

Keeping rather than deleting
----------------------------
Two approaches were possible: delete the unwanted elements, or extract the
wanted ones. This module extracts.

Deleting means listing every piece of furniture, and anything the site adds
later leaks into the curated output unnoticed. Extracting means only
``h1.page-title`` and ``div.content`` can ever appear, so new furniture is
excluded automatically. It is also safer for the legal text: nothing inside
``div.content`` is touched, so tables, headings and emphasis all survive
intact, and there is no risk of a delete rule catching real content.
"""

import re

from bs4 import BeautifulSoup, NavigableString

#: Parser used for every document. lxml is fast and, importantly, repairs
#: broken markup the same way each time - a parser that guessed differently
#: between runs would change content_hash for no real reason.
PARSER = "lxml"

#: The decision body. Everything kept comes from inside this element.
CONTENT_SELECTOR = "div.content"

#: The identifier heading, e.g. "TED2513". Sits beside div.content rather
#: than inside it, so it is selected separately.
TITLE_SELECTOR = "h1.page-title"

#: Stripped from inside the kept content as a safety net. Reconnaissance
#: found none there, but a script surviving into curated output would be both
#: useless and a security smell.
DROP_INSIDE_CONTENT = ("script", "style", "noscript", "iframe")

#: Characters that are invisible on screen but break text matching.
#:
#: One Labour Court page carries nine zero-width spaces and four non-breaking
#: spaces inside the decision text - "DECISION<ZWSP> NO." looks identical to
#: "DECISION NO." but will not match a search for it. For a corpus whose whole
#: purpose is being searched, that is a defect worth removing.
#:
#: Only the curated copy is normalised. The landing copy keeps every byte the
#: server sent, so nothing here is irreversible.
INVISIBLE_CHARACTERS = {
    "\u200b": "",   # zero width space
    "\u200c": "",   # zero width non-joiner
    "\u200d": "",   # zero width joiner
    "\ufeff": "",   # byte order mark appearing mid-text
    "\u00ad": "",   # soft hyphen
    "\u00a0": " ",  # non-breaking space -> ordinary space
}

#: Runs of whitespace are collapsed to a single space.
#:
#: A browser already renders "DECISION  NO." as "DECISION NO.", so collapsing
#: changes nothing a reader sees while making the text match a plain search.
#: Removing a zero-width space often leaves a double space behind, so this
#: finishes the job the substitutions above start.
WHITESPACE_RUN = re.compile(r"\s+")

#: Elements where whitespace is significant and must not be collapsed. None
#: appear in the fixtures, but a future decision could include one.
PREFORMATTED = ("pre", "textarea")


class CleaningError(Exception):
    """Raised when a page does not contain a recognisable decision.

    Raised rather than returning empty output so the caller records a
    failure. Silently writing an empty curated file would be far worse: the
    record would look processed while the decision had been lost.
    """


def parse(raw):
    """Parse raw bytes into a soup.

    Bytes are passed in rather than text so BeautifulSoup can apply the
    page's own declared encoding. Decoding beforehand would mean guessing,
    and these pages contain Irish names such as FHÁTHARTA that a wrong guess
    would corrupt.
    """
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return BeautifulSoup(raw, PARSER)


def find_decision(soup):
    """Locate the decision body and its heading.

    Returns:
        (title_tag, content_tag). title_tag may be None; content_tag never is.

    Raises:
        CleaningError: If no decision content can be found.
    """
    content = soup.select_one(CONTENT_SELECTOR)
    if content is None:
        raise CleaningError(f"No {CONTENT_SELECTOR} element found")

    if not content.get_text(strip=True):
        raise CleaningError(f"{CONTENT_SELECTOR} is present but empty")

    return soup.select_one(TITLE_SELECTOR), content


def normalise_invisible(value):
    """Replace invisible characters in a piece of text."""
    for character, replacement in INVISIBLE_CHARACTERS.items(): 
        if character in value:
            value = value.replace(character, replacement)
    return value


def collapse_whitespace(value):
    """Collapse runs of whitespace to a single space."""
    return WHITESPACE_RUN.sub(" ", value) #Find every run of whitespace in value and replace it with one normal space.


def normalise_text_nodes(tag):
    """Normalise every text node under ``tag``, in place.

    Only text is touched. Tag names and attributes are left alone, so the
    document structure - tables, headings, emphasis - is unaffected.

    Returns:
        Number of text nodes that were changed.
    """
    changed = 0
    for node in list(tag.find_all(string=True)):
        normalised = normalise_invisible(str(node))
        # Whitespace carries meaning inside <pre> and <textarea>, so those are
        # left exactly as they are.
        if not node.find_parent(PREFORMATTED):
            normalised = collapse_whitespace(normalised)
        if normalised != node:
            node.replace_with(NavigableString(normalised))
            changed += 1
    return changed


def clean_html(raw, identifier=None):
    """Return a minimal HTML document containing only the decision.

    Args:
        raw: The stored page, as bytes.
        identifier: Used for the <title> of the cleaned document. Falls back
            to the page's own heading.

    Returns:
        UTF-8 encoded bytes of a small, valid HTML document.

    Raises:
        CleaningError: If the page contains no recognisable decision.
    """
    soup = parse(raw)
    title_tag, content = find_decision(soup)

    for tag in content.find_all(DROP_INSIDE_CONTENT):
        tag.decompose()

    # Curated text should be searchable, so invisible characters go.
    normalise_text_nodes(content)
    if title_tag is not None:
        normalise_text_nodes(title_tag)

    heading = identifier or (title_tag.get_text(strip=True) if title_tag else "")

    # Built from a template rather than by mutating the original document, so
    # nothing from the source page can survive by accident.
    cleaned = BeautifulSoup(
        "<!DOCTYPE html>"
        '<html lang="en"><head><meta charset="utf-8"><title></title></head>'
        "<body></body></html>",
        PARSER,
    )
    cleaned.title.string = heading
    body = cleaned.body

    if title_tag is not None:
        body.append(title_tag)
    body.append(content)

    # UTF-8 regardless of what the source declared, so every curated file is
    # encoded the same way.
    return cleaned.encode("utf-8")


def extract_text(raw):
    """Plain text of the decision, with blank lines collapsed.

    Not stored, but used by tests and quality checks to confirm that cleaning
    kept the legal text rather than only the markup around it.
    """
    _, content = find_decision(parse(raw))
    normalise_text_nodes(content)
    lines = [line.strip() for line in content.get_text("\n").splitlines()]
    return "\n".join(line for line in lines if line)


def cleaning_stats(raw, cleaned):
    """How much was removed, for logging and for spotting over-aggressive rules."""
    return {
        "raw_bytes": len(raw),
        "cleaned_bytes": len(cleaned),
        "reduction_pct": round(100 * (1 - len(cleaned) / len(raw)), 1) if raw else 0.0,
    }
