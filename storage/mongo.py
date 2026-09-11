"""MongoDB adapter for document metadata.

All Mongo access goes through this class rather than being scattered through
the spider and pipelines. That keeps the query and index details in one place
and makes the storage layer swappable and testable on its own.

MongoDB stores information *about* a document - identifier, dates, body,
where the file lives and what it hashes to. The file itself lives in object
storage, because a database row is the wrong place for a 30 KB blob.
"""

from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

#: Fields never overwritten on a re-run: they record the first time we saw
#: this document, which later runs must not rewrite.
_INSERT_ONLY = ("first_seen_at",)


class MongoStore:
    """Thin wrapper around one MongoDB database."""
    #stores all mango settings
    def __init__(
        self,
        uri,
        database,
        landing_collection="landing_documents",
        curated_collection="curated_documents",
        timeout_ms=5000,
    ):
        self.uri = uri
        self.database_name = database
        self.landing_collection_name = landing_collection
        self.curated_collection_name = curated_collection
        self.timeout_ms = timeout_ms
        self._client = None

    # -- lifecycle ------------------------------------------------------

    def connect(self):
        """Open the connection and fail fast if the server is unreachable.

        PyMongo connects lazily, so without an explicit ping a bad URI would
        only surface on the first insert - halfway through a crawl.
        """
        #create mango client
        self._client = MongoClient(self.uri, serverSelectionTimeoutMS=self.timeout_ms)
        #actually check that mongo is reachable
        self._client.admin.command("ping")
        #create the required index
        self.ensure_indexes()
        return self

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *exc_info):
        self.close()

    # -- accessors ------------------------------------------------------

    @property
    def db(self):
        if self._client is None:
            raise RuntimeError("MongoStore.connect() must be called first")
        return self._client[self.database_name]

    @property
    def landing(self):
        return self.db[self.landing_collection_name]

    @property
    def curated(self):
        return self.db[self.curated_collection_name]

    # -- schema ---------------------------------------------------------

    def ensure_indexes(self):
        """Create the indexes the pipeline relies on. Safe to call repeatedly.

        The unique index on (doc_url, content_hash) is the database's own
        guarantee against duplicate records: even a buggy pipeline cannot
        store the same version of the same document twice. Application-level
        checks can be bypassed; a unique index cannot.

        doc_url is part of the key rather than identifier, because identifier
        is not unique in the source - the site lists ADJ-00054476 twice under
        two different URLs (docs/recon.md section 5).

        content_hash is the other part so that an amended decision is kept as
        a *new* record beside the old one rather than overwriting it. Keying
        on doc_url alone would silently discard the previous version.
        """
        for collection in (self.landing, self.curated):
            collection.create_index(
                [("doc_url", ASCENDING), ("content_hash", ASCENDING)],
                unique=True,
                name="uniq_doc_version",
            )
            collection.create_index([("doc_url", ASCENDING)], name="by_doc_url")
            # Lookups by reference: how a human finds a decision.
            collection.create_index([("identifier", ASCENDING)], name="by_identifier")
            # The transformation step selects a date range, and re-runs and
            # backfills select a single partition.
            collection.create_index([("partition_date", ASCENDING)], name="by_partition")
            collection.create_index([("published_date", ASCENDING)], name="by_published_date")
            # Change detection compares the stored content_hash against a
            # freshly computed one.
            collection.create_index([("content_hash", ASCENDING)], name="by_content_hash")

    # -- writes ---------------------------------------------------------

    def upsert(self, collection, record):
        """Insert or update one metadata record, keyed on (doc_url, content_hash).

        Returns "inserted" or "updated". Upserting rather than inserting is
        what makes re-running a date range safe: the same document produces
        the same key, so a second run updates the existing record instead of
        creating a duplicate.

        Raises:
            PyMongoError: propagated so the caller can count the failure
                rather than lose the record silently.
        """
        now = datetime.now(timezone.utc)
        payload = {key: value for key, value in record.items() if key not in _INSERT_ONLY}
        payload["last_seen_at"] = now

        result = collection.update_one(
            {"doc_url": record["doc_url"], "content_hash": record.get("content_hash")},
            {"$set": payload, "$setOnInsert": {"first_seen_at": now}},
            upsert=True,
        )
        return "inserted" if result.upserted_id is not None else "updated"

    def upsert_landing(self, record):
        return self.upsert(self.landing, record)

    def upsert_curated(self, record):
        return self.upsert(self.curated, record)

    # -- reads ----------------------------------------------------------

    def known_listings(self, doc_urls, collection=None):
        """Which (doc_url, listing_hash) pairs are already stored.

        Called once per results page with that page's URLs, so deciding what
        to skip costs one query per ten records rather than one per record.

        A pair being present means we already hold the document as the
        listing currently describes it, so it does not need downloading again.
        """
        collection = collection if collection is not None else self.landing
        doc_urls = list(doc_urls)
        if not doc_urls:
            return set()
        cursor = collection.find(
            {"doc_url": {"$in": doc_urls}},
            {"doc_url": 1, "listing_hash": 1, "_id": 0},
        )
        return {
            (doc["doc_url"], doc.get("listing_hash"))
            for doc in cursor
            if doc.get("listing_hash")
        }

    def touch_listed(self, doc_urls, collection=None):
        """Record that these documents were still in the search index.

        Skipped documents are not re-downloaded, so last_seen_at - which
        means "last fetched" - must not move. This records that the decision
        was still listed, which is a different and useful fact.
        """
        collection = collection if collection is not None else self.landing
        doc_urls = list(doc_urls)
        if not doc_urls:
            return 0
        result = collection.update_many(
            {"doc_url": {"$in": doc_urls}},
            {"$set": {"last_listed_at": datetime.now(timezone.utc)}},
        )
        return result.modified_count

    def find_by_date_range(self, start_date, end_date, collection=None):
        """Metadata for every document published within an inclusive range.

        Used by the transformation stage, which is given a start and end date
        and must find the landing records to process.
        """
        collection = collection if collection is not None else self.landing
        return collection.find(
            {"published_date": {"$gte": start_date, "$lte": end_date}}
        ).sort("published_date", ASCENDING)

    def count(self, collection=None):
        collection = collection if collection is not None else self.landing
        return collection.count_documents({})


__all__ = ["MongoStore", "PyMongoError"]
