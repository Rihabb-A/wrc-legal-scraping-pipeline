# Architecture

## Pipeline

```
INGESTION                              TRANSFORMATION
  |                                       |
Scrapy searches workplacerelations.ie   Reads Landing Zone (Mongo + MinIO)
  |                                       |
Landing Zone (raw, immutable)     -->   Curated Zone (cleaned, renamed)
  MongoDB: landing_documents             MongoDB: curated_documents
  MinIO:   wrc-landing                   MinIO:   wrc-curated
```

Dagster (`orchestration/definitions.py`) runs these as two ops in one job,
`ingest_decisions >> transform_decisions`. Transformation never starts if
ingestion raised, crashed, or left records unaccounted for.

## 1. Why monthly partitions?

A partition is one search of one body over one slice of time
(`common/partitioning.py`). Monthly is the default because:

- It is small enough that a failure costs one slice, not the whole range - a
  bad month can be re-run in isolation.
- It is large enough to avoid per-day orchestration overhead for a range that
  can span years.
- It matches the site's own date filter and lines up with `partition_date`,
  which is stored on every record and used to lay out object-storage paths
  (`wrc-landing/<partition_date>/...`).

Every partition/body combination is scheduled independently at spider start,
so slices execute concurrently and one slow or failing search does not block
the others.

## 2. Retries and rate limiting

The site returns HTTP 200 even for broken responses (empty or truncated
search pages), so a 200 status proves nothing. Two layers of retry exist:

- **Scrapy's `RetryMiddleware`**, for transient transport failures (timeouts,
  connection resets, 5xx).
- **A manual retry via `get_retry_request`** in `parse()`, for the
  200-but-broken case: a missing `div.searchhead` or a page that claims
  results but carries no `li.each-item` cards. Once retries are exhausted the
  loss is recorded as a failure rather than silently dropped.

Concurrency and pacing come from Scrapy's `AUTOTHROTTLE_*` and
`DOWNLOAD_DELAY` settings, configurable via environment variables rather than
hardcoded, so the crawl can be slowed down or sped up without a code change.

## 3. Deduplication strategy

Two properties of the source ruled out the obvious keys:

- **`identifier` is not unique.** July 2025 lists `ADJ-00054476` twice, under
  two different URLs, with slightly different bytes.
- **Raw bytes are not stable.** The same page returns a different SHA-256 on
  every fetch, because of a server-side timing comment
  (`<!-- Elapsed time: ... -->`). Hashing raw bytes would mark every document
  changed on every run.

So every document carries **two hashes** (`common/hashing.py`):

| Field | Hashes | Answers |
|---|---|---|
| `file_hash` | exact stored bytes | is the stored object intact? |
| `content_hash` | HTML with comments stripped (binaries: raw bytes) | did the content actually change? |

Ingestion's idempotency key is **`(doc_url, content_hash)`** - `doc_url` is
unique by construction, so the two `ADJ-00054476` copies stay separate
Landing objects instead of one overwriting the other; `content_hash` ignores
the volatile comment, so an unchanged document compares equal on the next
run. Before even downloading a document, the spider also checks a cheap
`listing_digest` of the search-result row (`identifier`, `title`,
`description`, `published_date`, `doc_url`) to skip documents that have not
moved since the last run without fetching them at all.

The transformation stage re-derives its own `content_hash` from the cleaned
**text**, not the cleaned markup, because the two `ADJ-00054476` sources
clean to markup that differs by one empty tag while the legal text is
identical - and both must produce a single curated file.

`identifier` is still stored and indexed for humans to look decisions up by,
it is simply not the uniqueness key. The Landing Zone is never overwritten:
changed content produces a new stored object, not an in-place edit.

## 4. Scaling to 50+ sources

What is source-specific today (`wrc_scraper/spiders/decisions.py`, the body
IDs in `wrc_scraper/constants.py`, `transform/html_cleaner.py`'s selectors)
would become one spider and one cleaner per source, each following the same
shape. What is already shared and would not need to change:

- `common/partitioning.py`, `common/hashing.py`, `common/logging_config.py`
- `storage/mongo.py`, `storage/minio.py`
- the Landing/Curated split and the two-hash idempotency model
- `orchestration/definitions.py`'s ingest-then-transform dependency

Records would gain a `source` field alongside `partition_date`, so
`(source, partition_date, body)` becomes the unit of work instead of just
`(partition_date, body)` - the pattern in `orchestration/definitions.py`
already accepts arbitrary start/end dates and bodies per run, so extending it
to iterate over a source registry is additive, not a rewrite. At larger
volume, MinIO already speaks the S3 API, so moving to real S3 is a
configuration change (`MINIO_ENDPOINT`), not a code change; MongoDB would
need sharding or indexes revisited once collections grow past a single
node's comfort. Structured JSON logs already carry per-slice reconciliation,
which is what alerting on `unaccounted > 0` across many sources would build
on directly.
