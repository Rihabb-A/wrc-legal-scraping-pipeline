"""Spider for Workplace Relations decisions.

Step 5 (current): extract every record in a search range, across all pages,
and reconcile the number found against the number scraped.
Later steps add partitions, all four bodies, and storage.
"""

import math
import re
from datetime import datetime

import scrapy
from w3lib.url import add_or_replace_parameter

# The search page reports its own total, e.g. "Shows 1 to 10 of 32 results".
# The site renders this with newlines and doubled spaces, so we collapse
# whitespace before matching.
RESULT_COUNT_RE = re.compile(r"Shows\s+\d+\s+to\s+\d+\s+of\s+(\d+)\s+results")

# The site displays dates as dd/mm/yyyy
SITE_DATE_FORMAT = "%d/%m/%Y"

# Results per page is fixed at 10. Recon tested pageSize, size, perPage,
# pagesize, resultsPerPage and rows - the site ignores all of them.
PAGE_SIZE = 10

# Shown when a search legitimately matches nothing, and also when pageNumber
# runs past the last page. Distinct from div.searchhead being absent entirely,
# which means the request itself was malformed (docs/recon.md §3).
NO_RESULTS_TEXT = "There are no search results"


class DecisionsSpider(scrapy.Spider):
    """Scrapes decisions from workplacerelations.ie."""

    name = "decisions"
    allowed_domains = ["workplacerelations.ie"]

    # One hard-coded URL for now: Labour Court, July 2025.
    # Recon verified this range returns exactly 32 results across 4 pages.
    start_urls = [
        "https://www.workplacerelations.ie/en/search/"
        "?decisions=1&body=3&from=2025-07-01&to=2025-07-31&pageNumber=1"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Reconciliation counters: the site tells us how many records exist,
        # so "found" is a known number rather than a guess. Anything missing
        # at the end of the run must be explained, not silently dropped.
        self.records_found = 0
        self.records_scraped = 0
        self.failed_pages = []

    def parse(self, response, is_first_page=True):
        """Handle a search results page.

        On the first page of a range we also schedule every remaining page.
        """
        # div.searchhead is the site's own record count. It is also our only
        # reliable success signal: this site returns HTTP 200 even for broken
        # requests, so the status code alone proves nothing (docs/recon.md §3).
        #Shows 1 to 10 of 32 results
        searchhead = response.css("div.searchhead::text").get()

        if searchhead is None:
            # Not "zero results" - the request was malformed or the response
            # is broken. Record it so the end-of-run summary cannot silently
            # under-report.
            self.logger.error("No div.searchhead: %s", response.url)
            self.failed_pages.append(
                {"url": response.url, "reason": "missing_searchhead"}
            )
            return

        searchhead = " ".join(searchhead.split())

        if NO_RESULTS_TEXT in searchhead:
            self.logger.info("No results for %s", response.url)
            return

        match = RESULT_COUNT_RE.search(searchhead)
        if match is None:
            self.logger.error("Unrecognised searchhead %r at %s", searchhead, response.url)
            self.failed_pages.append(
                {"url": response.url, "reason": "unparseable_searchhead"}
            )
            return

        total = int(match.group(1))
        cards = response.css("li.each-item")

        if not cards:
            self.logger.error("Reported %s results but no cards: %s", total, response.url)
            self.failed_pages.append({"url": response.url, "reason": "no_cards"})
            return

        for card in cards:
            self.records_scraped += 1
            yield self.parse_card(card, response)

        #Pages 2, 3, 4 should only extract their cards. 
        #They should not calculate pagination again.
        #Only page 1 does that.
        if not is_first_page:
            return

        # First page only: the total is now known, so every remaining page can
        # be requested immediately. Following "next page" links instead would
        # serialise the whole range - page N must be parsed before N+1 is even
        # known - and search pages take 2-30s each (docs/recon.md §7).
        self.records_found += total
        #calculate nb of page
        last_page = math.ceil(total / PAGE_SIZE) 
        self.logger.info(
            "Range has %s records across %s pages: %s", total, last_page, response.url
        )

        for page in range(2, last_page + 1):
            yield response.follow(
                add_or_replace_parameter(response.url, "pageNumber", str(page)),
                callback=self.parse,
                cb_kwargs={"is_first_page": False},
            )

    def closed(self, reason):
        """Log the end-of-run reconciliation summary."""
        self.logger.info(
            "RUN SUMMARY: found=%s scraped=%s missing=%s failed_pages=%s reason=%s",
            self.records_found,
            self.records_scraped,
            self.records_found - self.records_scraped,
            self.failed_pages,
            reason,
        )

    def parse_card(self, card, response):
        """Turn one ``li.each-item`` result into a metadata dict.

        Selecting the card first, then reading fields *within* it, keeps every
        field tied to the same decision. Selecting all identifiers and all dates
        as two separate lists would silently misalign if any card were missing a
        field.
        """
        identifier = self.clean_identifier(card.css("span.refNO::text").get())
        raw_date = self.text(card.css("span.date::text").get())

        return {
            "identifier": identifier,
            "title": self.text(card.css("h2.title a::text").get()),
            "description": self.text(card.css("p.description::text").get()),
            "published_date": self.parse_date(raw_date),
            # The href is relative ("/en/cases/..."), so urljoin turns it into
            # a full URL using the current page as the base.
            "doc_url": response.urljoin(card.css("div.link a::attr(href)").get()),
        }

    @staticmethod
    def text(value):
        """Collapse runs of whitespace and trim. Returns None if there was none."""
        if value is None:
            return None
        return " ".join(value.split()) or None

    @staticmethod
    def clean_identifier(value):
        """Strip *all* whitespace from an identifier.

        Some references are rendered with spaces around the hyphens, e.g.
        "IR - SC - 00001595". Removing whitespace yields "IR-SC-00001595",
        which is the form used in the document URL and in curated filenames.
        """
        if value is None:
            return None
        return "".join(value.split()) or None

    #31/07/2025 -> parse_date() -> 2025-07-31
    def parse_date(self, value):
        """Convert the site's dd/mm/yyyy into an ISO yyyy-mm-dd string.

        Stored as ISO so date ranges sort and compare correctly later; the
        displayed format would sort as text and give wrong results.
        """
        if not value:
            return None
        try:
            #convert first from string to date format then to 2025-07-31
            return datetime.strptime(value, SITE_DATE_FORMAT).date().isoformat()
        except ValueError:
            self.logger.warning("Unparseable published_date %r", value)
            return None
