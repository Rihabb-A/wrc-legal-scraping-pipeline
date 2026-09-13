"""Scrapy settings for the WRC pipeline.

Every tunable value is read from .env via common.config, so nothing here is
hardcoded and behaviour can be changed without editing Python.

The performance numbers are not guesses. They come from measuring the live
site during reconnaissance (docs/recon.md section 7):

    search results page   ~792 KB    2-31 s
    decision detail page  26-80 KB   0.2-1 s

    concurrency 1  ->  1.00x throughput
    concurrency 4  ->  3.79x
    concurrency 8  ->  7.01x       no 429s, no blocking, no slowdown

Docs:
    https://docs.scrapy.org/en/latest/topics/settings.html
    https://docs.scrapy.org/en/latest/topics/autothrottle.html
"""

from common.config import env, env_bool, env_float, env_int

BOT_NAME = "wrc_scraper"

SPIDER_MODULES = ["wrc_scraper.spiders"]
NEWSPIDER_MODULE = "wrc_scraper.spiders"

ADDONS = {}

# ---------------------------------------------------------------------------
# Politeness
# ---------------------------------------------------------------------------

# Identify the crawler honestly. A contactable user agent means the site owner
# can get in touch rather than silently blocking an anonymous bot.
USER_AGENT = env(
    "USER_AGENT",
    "wrc-legal-scraping-pipeline (+https://github.com/; contact: webmaster@workplacerelations.ie)",
)

# robots.txt disallows /en/Cases/ with a capital C, while every real link on
# the site is lowercase /en/cases/. Robots path matching is case-sensitive, so
# the documents are not actually disallowed - verified against Protego, the
# parser Scrapy itself uses (docs/recon.md section 6). Obeying costs nothing
# and is the honest default.
ROBOTSTXT_OBEY = env_bool("ROBOTSTXT_OBEY", True)

# The site sets an ASP.NET session cookie we have no use for. Reading results
# needs no session, so cookies are disabled: fewer moving parts, and no risk
# of requests being coupled through shared session state.
COOKIES_ENABLED = env_bool("COOKIES_ENABLED", False)

TELNETCONSOLE_ENABLED = False

# ---------------------------------------------------------------------------
# Concurrency and throttling
# ---------------------------------------------------------------------------

# 8 was measured, not assumed: it gives ~7x the throughput of serial fetching
# with no 429s and no per-request slowdown. Going higher was not tested
# against a public service that is not ours to load-test.
CONCURRENT_REQUESTS = env_int("CONCURRENT_REQUESTS", 8)

# Everything we fetch is one domain, so this must match CONCURRENT_REQUESTS or
# it becomes the real, lower limit.
CONCURRENT_REQUESTS_PER_DOMAIN = CONCURRENT_REQUESTS

# A small floor between requests. AutoThrottle raises it when the server slows
# down; this is simply the fastest we are ever willing to go.
DOWNLOAD_DELAY = env_float("DOWNLOAD_DELAY", 0.25)

# Vary the delay by 0.5x-1.5x so requests do not arrive in a machine-gun
# rhythm that is trivial to fingerprint and rate-limit.
RANDOMIZE_DOWNLOAD_DELAY = True

# AutoThrottle watches actual response latency and widens the delay when the
# server struggles, narrowing it when the server is comfortable. Measured
# latency on this site ranges from 0.2s to 31s, so a fixed delay would be
# either needlessly slow or inconsiderate depending on the hour.
AUTOTHROTTLE_ENABLED = env_bool("AUTOTHROTTLE_ENABLED", True)
AUTOTHROTTLE_START_DELAY = env_float("AUTOTHROTTLE_START_DELAY", 1.0)
# Above the worst latency observed, so a slow spell throttles rather than
# turning into a wave of timeouts.
AUTOTHROTTLE_MAX_DELAY = env_float("AUTOTHROTTLE_MAX_DELAY", 30.0)
AUTOTHROTTLE_TARGET_CONCURRENCY = env_float(
    "AUTOTHROTTLE_TARGET_CONCURRENCY", float(CONCURRENT_REQUESTS)
)
AUTOTHROTTLE_DEBUG = env_bool("AUTOTHROTTLE_DEBUG", False)

# ---------------------------------------------------------------------------
# Timeouts and retries
# ---------------------------------------------------------------------------

# Generous on purpose. Search pages were measured taking up to 31 seconds, so
# a typical 30s timeout would manufacture failures out of healthy responses.
DOWNLOAD_TIMEOUT = env_int("DOWNLOAD_TIMEOUT", 90)

RETRY_ENABLED = True
RETRY_TIMES = env_int("RETRY_TIMES", 3)

# Retry only what is plausibly transient. Scrapy's default list is kept
# deliberately: 5xx are server hiccups, 408 and 429 are explicit "try later"
# signals. 404 is absent on purpose - a missing document will still be missing
# on the fourth attempt, so retrying it only wastes the site's capacity.
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------

# Order matters: the file is stored first so the metadata record can say where
# it actually ended up, and with which hash.
ITEM_PIPELINES = {
    "wrc_scraper.pipelines.LandingObjectPipeline": 300,
    "wrc_scraper.pipelines.MongoPipeline": 400,
}

# ---------------------------------------------------------------------------
# Storage, all from .env
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = env("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = env_bool("MINIO_SECURE", False)
MINIO_LANDING_BUCKET = env("MINIO_LANDING_BUCKET", "wrc-landing")
MINIO_CURATED_BUCKET = env("MINIO_CURATED_BUCKET", "wrc-curated")

MONGO_URI = env("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = env("MONGO_DB", "wrc")
MONGO_LANDING_COLLECTION = env("MONGO_LANDING_COLLECTION", "landing_documents")
MONGO_CURATED_COLLECTION = env("MONGO_CURATED_COLLECTION", "curated_documents")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = env("LOG_LEVEL", "INFO")

# Machine-readable run log: one JSON object per line, written alongside the
# human-readable console output. Configured in common/logging_config.py.
EVENT_LOG_FILE = env("LOG_FILE", "structured_log.jsonl")

FEED_EXPORT_ENCODING = "utf-8"
