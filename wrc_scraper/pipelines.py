"""Item pipelines: what happens to a record after the spider produces it.

Storage lives here rather than in the spider so the spider stays responsible
for one thing only - reading the website. Swapping local disk for MinIO in a
later step is then a change to this file alone.
"""

from pathlib import Path

from itemadapter import ItemAdapter

from common.paths import landing_key


class LandingFilePipeline:
    """Write each downloaded document to the landing zone.

    Interim implementation: writes to the local filesystem. A later step
    replaces the write with an upload to MinIO, keeping the same object key
    so nothing else has to change.
    """

    def __init__(self, landing_dir):
        self.landing_dir = Path(landing_dir)
        self.written = 0
        self.skipped_existing = 0

    @classmethod
    def from_crawler(cls, crawler):
        """Read the output directory from settings rather than hardcoding it."""
        return cls(landing_dir=crawler.settings.get("LANDING_DIR", "data/landing"))

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Bytes are carried on the item only between the download and this
        # pipeline. They are popped here so the item stays JSON-serialisable
        # for feed exports and, later, for MongoDB.
        body = adapter.pop("file_bytes", None)
        if body is None:
            spider.logger.error(
                "No file_bytes for %s; nothing written", adapter.get("identifier")
            )
            return item

        key = landing_key(
            adapter["partition_date"], adapter["body"], adapter["doc_url"]
        )
        destination = self.landing_dir / key
        destination.parent.mkdir(parents=True, exist_ok=True)

        # The landing zone is immutable: an object already at this key came
        # from the same source URL, so it is left untouched. Detecting genuine
        # content *changes* is the job of the hashing step.
        if destination.exists() and destination.stat().st_size == len(body):
            self.skipped_existing += 1
        else:
            destination.write_bytes(body)
            self.written += 1

        adapter["file_path"] = key
        adapter["file_size"] = len(body)
        return item

    def close_spider(self, spider):
        spider.logger.info(
            "LANDING FILES: written=%s skipped_existing=%s dir=%s",
            self.written,
            self.skipped_existing,
            self.landing_dir,
        )
