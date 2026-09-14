"""Landing Zone -> Curated Zone.

Given a start and end date, this reads landing metadata from MongoDB, fetches
each stored file from MinIO, transforms it, and writes the result to a
separate bucket and collection:

    python -m transform.transform --start-date 2025-08-01 --end-date 2025-08-31

HTML is reduced to the legal decision (see html_cleaner). PDF, DOC and DOCX
pass through byte for byte, as the assignment requires - there is nothing to
strip from them, and rewriting a binary risks corrupting it.

The landing zone is never modified. Everything here reads from it and writes
somewhere else, so transformation can be re-run with better rules at any time
without re-scraping the website.

Two hashes, again
-----------------
``file_hash``     SHA-256 of the curated bytes actually stored.
``content_hash``  SHA-256 of the decision *text*.

The second is deliberately text-based rather than markup-based, which differs
from the landing zone's definition. The site lists ADJ-00054476 twice under
two URLs whose cleaned markup differs by a single empty <h1> element while
the text is identical. Those are the same decision and must produce one
curated file, so "same content" at this layer means "same legal text".
"""

import argparse
import logging
import sys
import time

from common.config import env, env_bool
from common.hashing import HASH_ALGORITHM, sha256_bytes
from common.logging_config import EventLog
from common.partitioning import to_date
from common.paths import curated_key
from storage.minio import MinioStore, S3Error
from storage.mongo import MongoStore, PyMongoError
from transform.html_cleaner import CleaningError, clean_html, extract_text

logger = logging.getLogger("transform")

#: Metadata copied from the landing record onto the curated record. The
#: curated document describes the same decision, so these do not change.
CARRIED_FIELDS = (
    "identifier",
    "title",
    "description",
    "published_date",
    "body",
    "partition_date",
    "file_type",
)

#: How many landing records to look up in Mongo at a time.
BATCH_SIZE = 100


class TransformationError(Exception):
    """A document could not be transformed. Always recorded, never swallowed."""


def transform_document(raw, file_type, identifier):
    """Turn one stored document into its curated form.

    Returns:
        (curated_bytes, content_hash). content_hash is text-based for HTML
        and byte-based for binaries, which have no text to extract.

    Raises:
        TransformationError: If HTML cannot be reduced to a decision.
    """
    if file_type != "html":
        # PDF/DOC/DOCX are stored exactly as received. Their bytes are the
        # content, so the two hashes coincide.
        return raw, sha256_bytes(raw)

    try:
        curated = clean_html(raw, identifier=identifier)
        text = extract_text(raw)
    except CleaningError as exc:
        raise TransformationError(str(exc)) from exc

    return curated, sha256_bytes(text.encode("utf-8"))


class Transformation:
    """One transformation run over a date range."""

    def __init__(self, start_date, end_date, mongo, minio, events=None, refresh=False):
        self.start_date = to_date(start_date)
        self.end_date = to_date(end_date)
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date {self.start_date} is after end_date {self.end_date}"
            )

        self.mongo = mongo
        self.minio = minio
        self.events = events
        self.refresh = refresh

        self.counts = {
            "found": 0,        # landing records in range
            "transformed": 0,  # written to the curated zone
            "unchanged": 0,    # already curated from this exact source
            "duplicates": 0,   # a second source for a decision already curated
            "failed": 0,       # could not be transformed, each with a reason
        }
        self.failures = []

        # Curated keys written during this run, as {key: curated record}.
        # The per-batch Mongo lookup is a snapshot taken before the batch is
        # processed, so a document written earlier in the same batch would
        # otherwise be invisible - and a second source for the same decision
        # would silently overwrite the first.
        self.written_this_run = {}

    # -- helpers --------------------------------------------------------

    def emit(self, event, **fields):
        if self.events is not None:
            self.events.emit(event, **fields)

    def fail(self, record, reason, detail=None):
        self.counts["failed"] += 1
        failure = {
            "identifier": record.get("identifier"),
            "doc_url": record.get("doc_url"),
            "source_file_path": record.get("file_path"),
            "partition_date": record.get("partition_date"),
            "reason": reason,
            "detail": detail,
        }
        self.failures.append(failure)
        logger.error("FAILED %s: %s %s", failure["identifier"], reason, detail or "")
        if self.events is not None:
            self.events.error("transform_failed", **failure)

    @staticmethod
    def curated_extension(file_type):
        """Extension for the curated file. Cleaned HTML is still HTML."""
        return "html" if file_type in ("html", "unknown") else file_type

    # -- the run --------------------------------------------------------

    def run(self):
        started = time.monotonic()
        self.emit(
            "transform_started",
            start_date=self.start_date.isoformat(),
            end_date=self.end_date.isoformat(),
            refresh=self.refresh,
        )

        cursor = self.mongo.find_by_date_range(
            self.start_date.isoformat(), self.end_date.isoformat()
        )

        for batch in self.batched(cursor, BATCH_SIZE):
            self.process_batch(batch)

        summary = dict(self.counts)
        summary["unaccounted"] = self.counts["found"] - (
            self.counts["transformed"]
            + self.counts["unchanged"]
            + self.counts["duplicates"]
            + self.counts["failed"]
        )
        summary["duration_seconds"] = round(time.monotonic() - started, 2)

        logger.info(
            "TRANSFORM SUMMARY: found=%(found)s transformed=%(transformed)s "
            "unchanged=%(unchanged)s duplicates=%(duplicates)s failed=%(failed)s "
            "unaccounted=%(unaccounted)s",
            summary,
        )
        self.emit("transform_summary", failures=len(self.failures), **summary)
        return summary

    @staticmethod
    def batched(cursor, size):
        """Yield lists of at most ``size`` records from a cursor."""
        batch = []
        for record in cursor:
            batch.append(record)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def process_batch(self, records):
        """Transform one batch, looking up existing curated records in one query."""
        self.counts["found"] += len(records)

        planned = []
        for record in records:
            identifier = record.get("identifier")
            if not identifier:
                self.fail(record, "missing_identifier")
                continue
            key = curated_key(
                record.get("partition_date"),
                identifier,
                self.curated_extension(record.get("file_type")),
            )
            planned.append((record, key))

        existing = self.mongo.curated_by_paths(key for _, key in planned)

        for record, key in planned:
            self.process_one(record, key, existing.get(key))

    def process_one(self, record, key, existing):
        """Transform a single landing record into the curated zone."""
        # Anything written moments ago counts as existing too.
        existing = existing or self.written_this_run.get(key)
        # Nothing to do if this exact source was already curated to this key.
        # Checked before downloading, so a re-run costs one Mongo query per
        # batch rather than a fetch per document.
        if (
            not self.refresh
            and existing is not None
            and existing.get("source_content_hash") == record.get("content_hash")
        ):
            self.counts["unchanged"] += 1
            return

        try:
            raw = self.minio.download(
                record.get("bucket") or self.minio.landing_bucket, record["file_path"]
            )
        except (S3Error, KeyError) as exc:
            self.fail(record, "landing_download_failed", str(exc))
            return

        try:
            curated, content_hash = transform_document(
                raw, record.get("file_type"), record["identifier"]
            )
        except TransformationError as exc:
            self.fail(record, "cleaning_failed", str(exc))
            return

        # A second landing record wanting the same curated file. This happens
        # because identifier is not unique in the source: the site lists
        # ADJ-00054476 twice under two URLs.
        if existing is not None and existing.get("content_hash") != content_hash:
            if not self.refresh:
                # Same name, genuinely different text. Refuse to overwrite and
                # record it, rather than letting one decision silently replace
                # another.
                self.fail(
                    record,
                    "curated_conflict",
                    f"{key} already holds different text from "
                    f"{existing.get('source_doc_url')}",
                )
                return
        elif existing is not None:
            # Same text from a different source URL: the same decision listed
            # twice. One curated file is correct; record the extra source so
            # the lineage stays complete.
            self.counts["duplicates"] += 1
            self.mongo.curated.update_one(
                {"file_path": key},
                {"$addToSet": {"also_sourced_from": record.get("doc_url")}},
            )
            logger.info(
                "Duplicate source for %s: %s already curated from %s",
                record.get("identifier"),
                key,
                existing.get("source_doc_url"),
            )
            self.emit(
                "duplicate_source",
                identifier=record.get("identifier"),
                file_path=key,
                doc_url=record.get("doc_url"),
                already_sourced_from=existing.get("source_doc_url"),
            )
            return

        try:
            self.minio.upload(
                self.minio.curated_bucket,
                key,
                curated,
                content_type="text/html" if record.get("file_type") == "html" else
                "application/octet-stream",
            )
        except S3Error as exc:
            self.fail(record, "curated_upload_failed", str(exc))
            return

        curated_record = {field: record.get(field) for field in CARRIED_FIELDS}
        curated_record.update(
            {
                "bucket": self.minio.curated_bucket,
                "file_path": key,
                "file_size": len(curated),
                "hash_algorithm": HASH_ALGORITHM,
                "file_hash": sha256_bytes(curated),
                "content_hash": content_hash,
                # Lineage: which raw document produced this curated one.
                "source_doc_url": record.get("doc_url"),
                "source_bucket": record.get("bucket"),
                "source_file_path": record.get("file_path"),
                "source_file_hash": record.get("file_hash"),
                "source_content_hash": record.get("content_hash"),
            }
        )

        try:
            self.mongo.upsert_curated(curated_record)
        except PyMongoError as exc:
            self.fail(record, "curated_metadata_failed", str(exc))
            return

        self.written_this_run[key] = curated_record
        self.counts["transformed"] += 1


def build_stores():
    """Open MongoDB and MinIO from configuration."""
    mongo = MongoStore(
        uri=env("MONGO_URI", "mongodb://localhost:27017"),
        database=env("MONGO_DB", "wrc"),
        landing_collection=env("MONGO_LANDING_COLLECTION", "landing_documents"),
        curated_collection=env("MONGO_CURATED_COLLECTION", "curated_documents"),
    ).connect()

    minio = MinioStore(
        endpoint=env("MINIO_ENDPOINT", "localhost:9000"),
        access_key=env("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=env("MINIO_SECRET_KEY", "minioadmin"),
        landing_bucket=env("MINIO_LANDING_BUCKET", "wrc-landing"),
        curated_bucket=env("MINIO_CURATED_BUCKET", "wrc-curated"),
        secure=env_bool("MINIO_SECURE", False),
    ).connect()

    return mongo, minio


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Transform Landing Zone documents into the Curated Zone."
    )
    parser.add_argument("--start-date", required=True, help="First day, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Last day, YYYY-MM-DD")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-transform everything, even documents already curated.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    mongo, minio = build_stores()
    events = EventLog(env("LOG_FILE", "structured_log.jsonl"))

    try:
        summary = Transformation(
            args.start_date, args.end_date, mongo, minio, events, args.refresh
        ).run()
    finally:
        events.close()
        mongo.close()
        minio.close()

    # Non-zero exit on failures, so an orchestrator treats the step as failed
    # rather than letting the next one run on incomplete data.
    return 1 if summary["failed"] or summary["unaccounted"] else 0


if __name__ == "__main__":
    sys.exit(main())
