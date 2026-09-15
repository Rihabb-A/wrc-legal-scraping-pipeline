"""Spider for Workplace Relations decisions.

Step 8 (current): follow every document link, work out whether the response
is HTML or a binary document, and hand the raw bytes to the landing
pipeline. Nothing is cleaned or rewritten at this stage.

Run it with:

    scrapy crawl decisions -a start_date=2025-07-01 -a end_date=2025-07-31

Later steps add document downloads and storage.
"""

import math
import time
from datetime import datetime

import scrapy
from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.spidermiddlewares.httperror import HttpError
from w3lib.url import add_or_replace_parameter

from common.filetypes import UNKNOWN, detect_file_type, is_binary
from common.hashing import listing_digest
from common.logging_config import EventLog
from common.partitioning import MONTHLY, generate_partitions
from storage.mongo import MongoStore
from wrc_scraper.constants import (
    DASH_CHARACTERS,
    NO_RESULTS_TEXT,
    PAGE_SIZE,
    RESULT_COUNT_RE,
    SITE_DATE_FORMAT,
    build_search_url,
    resolve_bodies,
)


class DecisionsSpider(scrapy.Spider):
    """Scrapes decisions from workplacerelations.ie."""

    name = "decisions"
    allowed_domains = ["workplacerelations.ie"]

    def __init__(
        self,
        start_date=None,
        end_date=None,
        partition_size=MONTHLY,
        bodies=None,
        refresh=False,
        *args,
        **kwargs,
    ):
        """Validate the run's inputs before a single request is made.

        Spider arguments arrive as strings from the command line, e.g.
        ``-a start_date=2025-07-01``. Everything is validated here so that a
        bad input fails immediately rather than halfway through a long crawl.
        """
        super().__init__(*args, **kwargs)

        if not start_date or not end_date:
            raise ValueError(
                "start_date and end_date are required, e.g. "
                "scrapy crawl decisions -a start_date=2025-07-01 -a end_date=2025-07-31"
            )

        # Both of these raise on bad input rather than quietly producing an
        # empty run that would look like a success.
        self.partitions = generate_partitions(start_date, end_date, partition_size)
        self.bodies = resolve_bodies(bodies)

        # Reconciliation is tracked per slice - one partition, one body - so a
        # shortfall can be traced to the exact search that lost records,
        # instead of only showing up as a wrong grand total.
        # -a refresh=true re-downloads everything, ignoring what is already
        # stored. Useful after changing how documents are parsed.
        self.refresh = str(refresh).strip().lower() in {"1", "true", "yes", "on"}
        self.slice_stats = {}
        self.failures = []
        # Documents skipped this run, so their "still listed" timestamp can
        # be refreshed in one bulk write at the end.
        self.skipped_urls = []
        # Both set by from_crawler; None when built directly in tests.
        self.store = None
        self.events = None
        self.started_at = time.monotonic()

        self.logger.info(
            "Run covers %s partitions x %s bodies = %s searches (%s..%s)",
            len(self.partitions),
            len(self.bodies),
            len(self.partitions) * len(self.bodies),
            self.partitions[0].start.isoformat(),
            self.partitions[-1].end.isoformat(),
        )
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Attach a MongoDB connection used to decide what can be skipped.

        Connecting here means an unreachable database stops the run before
        any page is fetched.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        settings = crawler.settings
        spider.events = EventLog(settings.get("EVENT_LOG_FILE", "structured_log.jsonl"))
        spider.events.emit(
            "run_started",
            start_date=spider.partitions[0].start.isoformat(),
            end_date=spider.partitions[-1].end.isoformat(),
            partitions=len(spider.partitions),
            bodies=list(spider.bodies),
            partition_size=spider.partitions[0].partition_date,
            refresh=spider.refresh,
        )
        spider.store = MongoStore(
            uri=settings.get("MONGO_URI"),
            database=settings.get("MONGO_DB"),
            landing_collection=settings.get("MONGO_LANDING_COLLECTION"),
            curated_collection=settings.get("MONGO_CURATED_COLLECTION"),
        ).connect()
        return spider

    async def start(self):
        """Issue the first search page for every partition/body combination.

        All combinations are scheduled up front rather than run one after
        another. They are independent, so this lets the downloader work on
        several at once - which is the point of partitioning.
        """
        for partition in self.partitions:
            for body_name, body_id in self.bodies.items():
                if self.events is not None:
                    self.events.emit(
                        "partition_started",
                        partition_date=partition.partition_date,
                        body=body_name,
                        start=partition.start.isoformat(),
                        end=partition.end.isoformat(),
                    )
                yield scrapy.Request(
                    build_search_url(body_id, partition.start, partition.end),
                    callback=self.parse,
                    cb_kwargs={
                        "partition_date": partition.partition_date,
                        "body": body_name,
                    },
                )

    def parse(self, response, partition_date, body, is_first_page=True):
        """Handle a search results page.

        On the first page of a slice we also schedule its remaining pages.
        """
        stats = self.slice_stats.setdefault(
            (partition_date, body), self.new_stats()
        )
        stats["pages"] += 1

        # div.searchhead is the site's own record count. It is also our only
        # reliable success signal: this site returns HTTP 200 even for broken
        # requests, so the status code alone proves nothing (docs/recon.md §3).
        searchhead = response.css("div.searchhead::text").get()

        if searchhead is None:
            # Not "zero results" - the response is broken or truncated. This
            # site answers 200 to everything, so Scrapy's retry middleware
            # never sees a failure here and the request must be retried by
            # hand. get_retry_request returns None once RETRY_TIMES is spent,
            # at which point the loss is recorded rather than hidden.
            retry = get_retry_request(
                response.request, spider=self, reason="missing_searchhead"
            )
            if retry is not None:
                yield retry
                return
            self.record_failure(response.url, partition_date, body, "missing_searchhead")
            return

        searchhead = " ".join(searchhead.split())

        if NO_RESULTS_TEXT in searchhead:
            self.logger.info("No results: partition=%s body=%s", partition_date, body)
            return

        match = RESULT_COUNT_RE.search(searchhead)
        if match is None:
            self.record_failure(
                response.url,
                partition_date,
                body,
                f"unparseable_searchhead:{searchhead!r}",
            )
            return

        total = int(match.group(1))
        cards = response.css("li.each-item")

        if not cards:
            # The page claims results but carries none: another broken 200.
            retry = get_retry_request(
                response.request, spider=self, reason="no_cards_but_reported"
            )
            if retry is not None:
                yield retry
                return
            self.record_failure(
                response.url, partition_date, body, f"no_cards_but_reported_{total}"
            )
            return

        records = [
            self.parse_card(card, response, partition_date, body) for card in cards
        ]
        stats["listed"] += len(records)

        # One query for the whole page rather than one per record: which of
        # these documents do we already hold exactly as the listing now
        # describes them? The server sends no ETag or Last-Modified and
        # ignores conditional requests, so the listing row is the only
        # change signal available without downloading the document itself.
        already_held = set()
        if not self.refresh and self.store is not None:
            already_held = self.store.known_listings(
                [r["doc_url"] for r in records if r["doc_url"]]
            )

        for record in records:
            if not record["doc_url"]:
                stats["failed"] += 1
                self.record_failure(
                    response.url, partition_date, body, "no_doc_url",
                    identifier=record["identifier"],
                )
                continue

            record["listing_hash"] = listing_digest(record)

            if (record["doc_url"], record["listing_hash"]) in already_held:
                # Unchanged since the last run: do not download it again.
                stats["unchanged"] += 1
                self.skipped_urls.append(record["doc_url"])
                continue

            yield response.follow(
                record["doc_url"],
                callback=self.parse_document,
                errback=self.handle_download_error,
                cb_kwargs={"record": record},
                # Two listings can point at the same document - the same case
                # can appear under more than one body. Filtering those out
                # would make a listed record vanish with no explanation, so
                # each listing is downloaded and accounted for on its own.
                dont_filter=True,
            )

        if not is_first_page:
            return

        # First page only: the total is now known, so every remaining page can
        # be requested immediately. Following "next page" links instead would
        # serialise the whole slice - page N must be parsed before N+1 is even
        # known - and search pages take 2-30s each (docs/recon.md §7).
        stats["found"] = total
        last_page = math.ceil(total / PAGE_SIZE)
        if self.events is not None:
            self.events.emit(
                "records_found",
                partition_date=partition_date,
                body=body,
                found=total,
                pages=last_page,
            )
        self.logger.info(
            "partition=%s body=%s found=%s pages=%s",
            partition_date,
            body,
            total,
            last_page,
        )

        for page in range(2, last_page + 1):
            yield response.follow(
                add_or_replace_parameter(response.url, "pageNumber", str(page)),
                callback=self.parse,
                cb_kwargs={
                    "partition_date": partition_date,
                    "body": body,
                    "is_first_page": False,
                },
            )

    @staticmethod
    def new_stats():
        """Counters for one partition/body slice.

        found      - what the site says exists
        listed     - result cards we parsed out of the listing
        downloaded - documents actually retrieved and stored
        unchanged  - already held, so deliberately not downloaded again
        failed     - documents we could not retrieve, each logged with a reason
        """
        return {
            "found": 0,
            "listed": 0,
            "downloaded": 0,
            "unchanged": 0,
            "failed": 0,
            "pages": 0,
        }

    def parse_document(self, response, record):
        """Handle the decision document itself.

        The bytes are passed through untouched. HTML is not cleaned here:
        the landing zone keeps exactly what the server sent, and cleaning
        happens later in the transformation stage so the raw copy is always
        available to re-process.
        """
        partition_date, body = record["partition_date"], record["body"]
        stats = self.slice_stats.setdefault((partition_date, body), self.new_stats())

        content_type = response.headers.get("Content-Type", b"").decode(
            "ascii", "replace"
        )
        # The header describes what the server actually sent, so it wins over
        # the URL extension.
        file_type = detect_file_type(content_type, response.url)

        if file_type == UNKNOWN:
            # Still stored - we simply cannot label it, and a silent guess
            # would be worse than an honest "unknown".
            self.logger.warning(
                "Unknown file type: identifier=%s content_type=%r url=%s",
                record["identifier"],
                content_type,
                response.url,
            )

        stats["downloaded"] += 1

        yield {
            **record,
            "file_type": file_type,
            "content_type": content_type,
            "is_binary": is_binary(file_type),
            "http_status": response.status,
            # Consumed and removed by LandingFilePipeline.
            "file_bytes": response.body,
        }

    def handle_download_error(self, failure):
        """Account for a document that could not be downloaded.

        Reached only after Scrapy has exhausted its retries, so this is a
        final failure rather than a transient one.
        """
        record = failure.request.cb_kwargs.get("record", {})
        partition_date = record.get("partition_date")
        body = record.get("body")
        stats = self.slice_stats.setdefault((partition_date, body), self.new_stats())
        stats["failed"] += 1

        if failure.check(HttpError):
            status = failure.value.response.status
            reason = f"http_{status}"
        else:
            # DNS failure, timeout, connection reset, and so on.
            status = None
            reason = type(failure.value).__name__

        self.record_failure(
            failure.request.url,
            partition_date,
            body,
            reason,
            identifier=record.get("identifier"),
            status_code=status,
        )

    def record_failure(
        self, url, partition_date, body, reason, identifier=None, status_code=None
    ):
        """Log a failure and keep it for the end-of-run summary."""
        self.logger.error(
            "FAILED partition=%s body=%s identifier=%s reason=%s status=%s url=%s",
            partition_date,
            body,
            identifier,
            reason,
            status_code,
            url,
        )
        failure = {
            "url": url,
            "partition_date": partition_date,
            "body": body,
            "identifier": identifier,
            "reason": reason,
            "status_code": status_code,
        }
        self.failures.append(failure)
        if self.events is not None:
            self.events.error("download_failed", **failure)

    def closed(self, reason):
        """Log the end-of-run reconciliation summary."""
        # Refresh the "still listed" timestamp for everything skipped, in
        # one write rather than one per document.
        if self.store is not None and self.skipped_urls:
            self.store.touch_listed(self.skipped_urls)

        totals = {
            key: sum(s[key] for s in self.slice_stats.values())
            for key in ("found", "listed", "downloaded", "unchanged", "failed")
        }

        # The run is only fully accounted for when every record the site
        # reported was downloaded, deliberately skipped as unchanged, or
        # explicitly failed with a reason.
        unaccounted = (
            totals["found"]
            - totals["downloaded"]
            - totals["unchanged"]
            - totals["failed"]
        )

        # Any slice that does not add up is named, so a shortfall points at
        # the exact search that caused it rather than only a wrong total.
        mismatched = {
            f"{partition_date}/{body}": stats
            for (partition_date, body), stats in sorted(self.slice_stats.items())
            if stats["found"]
            != stats["downloaded"] + stats["unchanged"] + stats["failed"]
        }

        self.logger.info(
            "RUN SUMMARY: found=%s listed=%s downloaded=%s unchanged=%s "
            "failed=%s unaccounted=%s slices=%s mismatched=%s reason=%s",
            totals["found"],
            totals["listed"],
            totals["downloaded"],
            totals["unchanged"],
            totals["failed"],
            unaccounted,
            len(self.slice_stats),
            mismatched or "none",
            reason,
        )

        if self.events is not None:
            # One event per partition/body, so a shortfall can be traced to
            # the exact search that caused it.
            for (partition_date, slice_body), slice_stats in sorted(
                self.slice_stats.items()
            ):
                self.events.emit(
                    "partition_completed",
                    partition_date=partition_date,
                    body=slice_body,
                    **slice_stats,
                )

            self.events.emit(
                "run_summary",
                duration_seconds=round(time.monotonic() - self.started_at, 2),
                finish_reason=reason,
                slices=len(self.slice_stats),
                failures=len(self.failures),
                mismatched_slices=list(mismatched),
                unaccounted=unaccounted,
                **totals,
            )
            self.events.close()

        if self.store is not None:
            self.store.close()

    def parse_card(self, card, response, partition_date, body):
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
            # Which search produced this record. Never derived from the URL
            # path: PWD2528 is published 31 July but lives under /august/.
            "body": body,
            "partition_date": partition_date,
        }

    @staticmethod
    def text(value):
        """Collapse runs of whitespace and trim. Returns None if there was none."""
        if value is None:
            return None
        return " ".join(value.split()) or None

    @staticmethod
    def clean_identifier(value):
        """Normalise an identifier to its canonical, ASCII form.

        Two inconsistencies in the source are corrected here:

        * spaces around the hyphens, e.g. "IR - SC - 00001595", which become
          "IR-SC-00001595" - the form used in the document URL and in curated
          filenames
        * Unicode dashes where a hyphen is meant, e.g. an EN DASH in
          "IR-SC\u201300002726", found in four of 994 decisions

        Both matter because the identifier is how a human looks a decision up
        and what the curated file is named. Two spellings of one reference
        would mean lookups quietly missing documents.
        """
        if value is None:
            return None
        for dash, replacement in DASH_CHARACTERS.items():
            if dash in value:
                value = value.replace(dash, replacement)
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
            #conv
            return datetime.strptime(value, SITE_DATE_FORMAT).date().isoformat()
        except ValueError:
            self.logger.warning("Unparseable published_date %r", value)
            return None
