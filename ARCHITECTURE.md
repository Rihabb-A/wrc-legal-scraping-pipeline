# Architecture

```
INGESTION  (scrapy crawl decisions)        TRANSFORMATION  (transform.transform)
  Landing Zone - raw, immutable       -->    Curated Zone - cleaned, renamed
  MongoDB landing_documents                  MongoDB curated_documents
  MinIO   wrc-landing                        MinIO   wrc-curated
```

Dagster (`orchestration/definitions.py`) runs these as two ops,
`ingest_decisions >> transform_decisions`. Transformation never starts if
ingestion raised, crashed, or left records unaccounted for.

## 1. Why monthly partitions?

A partition is one search of one body over one slice of time
(`common/partitioning.py`). Monthly is small enough that a failure costs one
slice rather than the whole range, and large enough to avoid per-day
orchestration overhead across multi-year ranges. It also matches the site's
own date filter and gives `partition_date`, which is stored on every record
and prefixes every object key. Both ends are inclusive and the next partition
starts the following day, so there are no gaps and no overlaps. Every
partition/body combination is scheduled independently at spider start, so
slices run concurrently and one slow search does not block the others.
`PARTITION_SIZE` in `.env` (or `-a partition_size=`) switches to daily,
weekly or yearly without a code change.

## 2. Retries and rate limiting

The site answers HTTP 200 even for broken responses, so a status code proves
nothing. Two layers therefore exist: Scrapy's `RetryMiddleware` for transport
failures and 5xx/408/429, and a manual `get_retry_request` in `parse()` for
the 200-but-broken case — a missing `div.searchhead`, or a page claiming
results while carrying no cards. Once retries are exhausted the loss is
recorded as a failure rather than dropped. 404 is deliberately not retried:
a missing document stays missing.

Concurrency is 8, measured rather than guessed (≈7× serial throughput, no
429s), paired with AutoThrottle, a randomised delay and a 90s timeout, since
search pages were observed taking up to 31 seconds. All are `.env` settings.

## 3. Deduplication and idempotency

Two properties of the source rule out the obvious keys. **`identifier` is not
unique** — the site lists the same decision under two URLs. **Raw bytes are
not stable** — every response carries a server timing comment, so the same
page hashes differently on every fetch.

So each record carries two hashes (`common/hashing.py`): `file_hash` over the
exact stored bytes, and `content_hash` over the content with that volatile
markup removed. Ingestion's key is **`(doc_url, content_hash)`**, enforced by
a unique index: `doc_url` keeps genuinely distinct sources apart, while
`content_hash` recognises an unchanged document. A third, cheaper
`listing_digest` of the search-result row lets the spider skip a document
without downloading it at all — a repeat run of 994 documents dropped from
1,110 HTTP requests to 112.

Transformation re-derives `content_hash` from the cleaned **text**, because
the duplicate pair cleans to markup differing by one empty tag while the legal
text is identical — and both must yield a single curated file, with the second
URL recorded in `also_sourced_from`. Differing text refuses to overwrite and
is logged as a conflict. The Landing Zone is never modified: changed content
becomes a new object, never an in-place edit.

## 4. Scaling to 50+ sources

Source-specific today: the spider, the body IDs in `constants.py`, and the
cleaner's selectors — these become one spider and one cleaner per source.
Already shared and unchanged: `common/` (partitioning, hashing, logging),
`storage/` (Mongo, MinIO), the Landing/Curated split, the two-hash idempotency
model, and the Dagster dependency.

Records would gain a `source` field, making `(source, partition_date, body)`
the unit of work; the ops already accept arbitrary dates and bodies per run,
so iterating a source registry is additive. MinIO already speaks S3, so
production storage is a config change. MongoDB would need its indexes
revisited and eventually sharding. The structured logs already carry per-slice
reconciliation, so alerting on `unaccounted > 0` across many sources builds
directly on what exists.
