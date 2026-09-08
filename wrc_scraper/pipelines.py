"""Item pipelines: what happens to a record after the spider produces it.

Storage lives here rather than in the spider so the spider stays responsible
for one thing only - reading the website. Swapping local disk for MinIO in a
later step is then a change to this file alone.
"""

from pathlib import Path

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from common.paths import landing_key


class LandingFilePipeline:
    """Write each downloaded document to the landing zone.

    Interim implementation: writes to the local filesystem. A later step
    replaces the write with an upload to MinIO, keeping the same object key
    so nothing else has to change.
    """

    def __init__(self, crawler, landing_dir):
        self.crawler = crawler
        self.landing_dir = Path(landing_dir)
        self.written = 0
        self.skipped_existing = 0
        self.write_errors = 0

    @classmethod
    def from_crawler(cls, crawler):
        """Read the output directory from settings rather than hardcoding it."""
        return cls(crawler, crawler.settings.get("LANDING_DIR", "data/landing"))

    @property
    def spider(self):
        return self.crawler.spider

    def process_item(self, item):
        adapter = ItemAdapter(item)
        identifier = adapter.get("identifier")

        # Bytes are carried on the item only between the download and this
        # pipeline. They are popped here so the item stays JSON-serialisable
        # for feed exports and, later, for MongoDB.
        body = adapter.pop("file_bytes", None)
        if body is None:
            self.fail(item, identifier, "no_file_bytes")

        key = landing_key(
            adapter["partition_date"], adapter["body"], adapter["doc_url"]
        )
        destination = self.landing_dir / key

        try:
            # The landing zone is immutable. The object key is derived from the
            # source URL, so an object already at this key came from the same
            # document and is left exactly as it was first stored. Detecting a
            # genuine content *change* is the hashing step's job, and it will
            # record a new version rather than overwrite this one.
            #
            # This is also what makes re-running a range idempotent: without
            # it, every run would rewrite every file, because these pages embed
            # a server timing comment that differs on each request
            # (docs/recon.md section 10).
            if destination.exists():
                self.skipped_existing += 1
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
                self.written += 1
        except OSError as exc:
            # A storage failure must be counted, never silently swallowed.
            # Letting the exception escape would drop the item while the run
            # summary still claimed every record was accounted for.
            self.write_errors += 1
            self.fail(item, identifier, f"write_failed:{type(exc).__name__}", str(exc))

        adapter["file_path"] = key
        adapter["file_size"] = len(body)
        return item

    def fail(self, item, identifier, reason, detail=None):
        """Record a storage failure against the spider, then drop the item."""
        adapter = ItemAdapter(item)
        spider = self.spider
        if spider is not None:
            stats = spider.slice_stats.setdefault(
                (adapter.get("partition_date"), adapter.get("body")),
                spider.new_stats(),
            )
            stats["failed"] += 1
            stats["downloaded"] -= 1  # it was downloaded but never stored
            spider.record_failure(
                adapter.get("doc_url"),
                adapter.get("partition_date"),
                adapter.get("body"),
                reason,
                identifier=identifier,
            )
        raise DropItem(f"{identifier}: {reason} {detail or ''}".strip())

    def close_spider(self):
        spider = self.spider
        message = (
            "LANDING FILES: written=%s skipped_existing=%s write_errors=%s dir=%s"
        )
        args = (self.written, self.skipped_existing, self.write_errors, self.landing_dir)
        if spider is not None:
            spider.logger.info(message, *args)
