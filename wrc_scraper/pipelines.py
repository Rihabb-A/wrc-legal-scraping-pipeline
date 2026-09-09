"""Item pipelines: what happens to a record after the spider produces it.

Storage lives here rather than in the spider so the spider stays responsible
for one thing only - reading the website. Swapping local disk for MinIO in a
later step is then a change to this file alone.
"""

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from common.paths import landing_key
from storage.minio import MinioStore, S3Error
from storage.mongo import MongoStore, PyMongoError


class LandingObjectPipeline:
    """Upload each downloaded document to the landing bucket in MinIO.

    The object key is the same one computed before, so moving from local
    disk to object storage changed where bytes go, not how they are named.
    """

    def __init__(self, crawler, store):
        self.crawler = crawler
        self.store = store
        self.stored = 0
        self.skipped_existing = 0
        self.errors = 0

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        store = MinioStore(
            endpoint=settings.get("MINIO_ENDPOINT"),
            access_key=settings.get("MINIO_ACCESS_KEY"),
            secret_key=settings.get("MINIO_SECRET_KEY"),
            landing_bucket=settings.get("MINIO_LANDING_BUCKET"),
            curated_bucket=settings.get("MINIO_CURATED_BUCKET"),
            secure=settings.getbool("MINIO_SECURE"),
        )
        return cls(crawler, store)

    @property
    def spider(self):
        return self.crawler.spider

    def open_spider(self, spider):
        # Fail before the crawl rather than after it: an unreachable object
        # store discovered on the first upload wastes the whole run.
        self.store.connect()
        spider.logger.info(
            "MINIO: connected to %s bucket=%s",
            self.store.endpoint,
            self.store.landing_bucket,
        )

    def process_item(self, item):
        #item is the Scrapy item containing your data.
        #wraps the item so you can easily access and modify it like a dictionary
        adapter = ItemAdapter(item)
        identifier = adapter.get("identifier")

        # Bytes are carried on the item only between the download and this
        # pipeline. They are popped here so the item stays JSON-serialisable
        # for feed exports and for MongoDB.
        body = adapter.pop("file_bytes", None)
        if body is None:
            self.fail(item, identifier, "no_file_bytes")

        key = landing_key(
            adapter["partition_date"], adapter["body"], adapter["doc_url"]
        )

        try:
            outcome, stored_size = self.store.upload_if_absent(
                self.store.landing_bucket,
                key,
                body,
                content_type=adapter.get("content_type") or "application/octet-stream",
            )
        except S3Error as exc:
            # A storage failure must be counted, never silently swallowed.
            # Letting it escape would drop the record while the run summary
            # still claimed everything was accounted for.
            self.errors += 1
            self.fail(item, identifier, f"minio_failed:{exc.code}", str(exc))

        if outcome == "stored":
            self.stored += 1
        else:
            self.skipped_existing += 1

        adapter["bucket"] = self.store.landing_bucket
        adapter["file_path"] = key
        # The size of what is *stored*, not of what was just downloaded. On a
        # re-run those differ, and metadata that describes bytes nobody kept
        # is worse than useless.
        adapter["file_size"] = stored_size
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
        if spider is not None:
            spider.logger.info(
                "MINIO: stored=%s skipped_existing=%s errors=%s objects_in_bucket=%s",
                self.stored,
                self.skipped_existing,
                self.errors,
                self.store.count(self.store.landing_bucket),
            )
        self.store.close()

################### MONGO PIPELINEE #######################################################################
class MongoPipeline:
    """Store each record's metadata in MongoDB.

    Runs after LandingFilePipeline so that file_path and file_size are
    already set on the item: the metadata record describes where the file
    was actually stored.
    """

    def __init__(self, crawler, store):
        self.crawler = crawler
        self.store = store
        self.inserted = 0
        self.updated = 0
        self.errors = 0

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        store = MongoStore(
            uri=settings.get("MONGO_URI"),
            database=settings.get("MONGO_DB"),
            landing_collection=settings.get("MONGO_LANDING_COLLECTION"),
            curated_collection=settings.get("MONGO_CURATED_COLLECTION"),
        )
        return cls(crawler, store)

    @property
    def spider(self):
        return self.crawler.spider

    def open_spider(self, spider):
        # Connecting here means an unreachable database stops the run before
        # a single page is fetched, rather than after a long crawl.

        #connects to MongoDB immediately when the spider starts.
        self.store.connect()
        spider.logger.info(
            "MONGO: connected to %s db=%s collection=%s",
            self.store.uri,
            self.store.database_name,
            self.store.landing_collection_name,
        )

    def process_item(self, item):
        adapter = ItemAdapter(item)
        #turn it as dict
        record = adapter.asdict()

        try:
            outcome = self.store.upsert_landing(record)
        except PyMongoError as exc:
            # Same rule as file storage: a failure is counted and logged, so
            # the run summary can never claim a record was handled when it
            # was not.
            self.errors += 1
            spider = self.spider
            if spider is not None:
                stats = spider.slice_stats.setdefault(
                    (adapter.get("partition_date"), adapter.get("body")),
                    spider.new_stats(),
                )
                stats["failed"] += 1
                stats["downloaded"] -= 1
                spider.record_failure(
                    adapter.get("doc_url"),
                    adapter.get("partition_date"),
                    adapter.get("body"),
                    f"mongo_failed:{type(exc).__name__}",
                    identifier=adapter.get("identifier"),
                )
            raise DropItem(f"{adapter.get('identifier')}: mongo write failed: {exc}")

        if outcome == "inserted":
            self.inserted += 1
        else:
            self.updated += 1
        return item

    def close_spider(self):
        spider = self.spider
        if spider is not None:
            spider.logger.info(
                "MONGO: inserted=%s updated=%s errors=%s total_in_collection=%s",
                self.inserted,
                self.updated,
                self.errors,
                self.store.count(),
            )
        self.store.close()
