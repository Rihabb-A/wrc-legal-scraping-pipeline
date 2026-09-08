"""Split a date range into smaller, independently retryable partitions.

A partition is the unit of work for the whole pipeline: one search of one
body over one slice of time. Keeping them small means a failure costs one
slice rather than the entire range, and it produces enough independent
requests to keep the downloader busy.

Both ends of a partition are **inclusive**, which matches the Workplace
Relations date filter: recon verified that 2025-07-01..2025-07-15 (17
records) plus 2025-07-16..2025-07-31 (15 records) equals the 32 records of
the whole month - no gaps, no double counting.
"""

from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Iterable, NamedTuple, Union

DateLike = Union[date, datetime, str] #accepted date types

#partition size
DAILY = "daily"
WEEKLY = "weekly"
MONTHLY = "monthly"
YEARLY = "yearly"

#: Supported partition sizes. Configurable via PARTITION_SIZE in .env.
PARTITION_SIZES = (DAILY, WEEKLY, MONTHLY, YEARLY)


class Partition(NamedTuple):
    """One unit of work.

    Attributes:
        start: First day covered, inclusive.
        end: Last day covered, inclusive.
        partition_date: Stable label stored on every record produced from
            this slice, e.g. ``"2025-07"``. Used to group records, to name
            object-storage paths, and as the Dagster partition key.
    """
    #start, end and label
    start: date
    end: date
    partition_date: str

    #dict useful for logging
    def as_dict(self) -> dict:
        """ISO-string form, for logging and for storing on a record."""
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "partition_date": self.partition_date,
        }

#
def to_date(value: DateLike) -> date:
    """Coerce a date, datetime or ISO string into a plain ``date``.

    Accepting strings means the CLI, Dagster config and .env can all pass
    dates without each caller repeating the parsing.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ValueError(
                f"Expected a date as YYYY-MM-DD, got {value!r}"
            ) from exc
    raise TypeError(f"Cannot interpret {value!r} as a date")


def _period_end(day: date, size: str) -> date:
    """Last day of the period that ``day`` falls in."""
    if size == DAILY:
        return day
    if size == WEEKLY:
        # weekday() is 0 on Monday, so this lands on the following Sunday.
        return day + timedelta(days=6 - day.weekday())
    if size == MONTHLY:
        # monthrange returns (first weekday, number of days), and it already
        # knows that February 2024 has 29 days.
        return day.replace(day=monthrange(day.year, day.month)[1])
    if size == YEARLY:
        return date(day.year, 12, 31)
    raise ValueError(f"Unknown partition size {size!r}; expected one of {PARTITION_SIZES}")


def _label(day: date, size: str) -> str:
    """Human-readable, sortable label for the period ``day`` falls in."""
    if size == DAILY:
        return day.isoformat()
    if size == WEEKLY:
        # The ISO year is not always the calendar year: 2024-12-30 belongs to
        # week 1 of ISO year 2025. isocalendar() gets this right.
        iso_year, iso_week, _ = day.isocalendar()
        return f"{iso_year:04d}-W{iso_week:02d}"
    if size == MONTHLY:
        return f"{day.year:04d}-{day.month:02d}"
    if size == YEARLY:
        return f"{day.year:04d}"
    raise ValueError(f"Unknown partition size {size!r}; expected one of {PARTITION_SIZES}")


def generate_partitions(
    start_date: DateLike,
    end_date: DateLike,
    size: str = MONTHLY,
) -> list[Partition]:
    """Split ``start_date``..``end_date`` into consecutive partitions.

    The first and last partitions are clipped to the requested range, so a
    range starting mid-month yields a short first partition rather than
    silently scraping days nobody asked for.

    Args:
        start_date: First day to cover, inclusive.
        end_date: Last day to cover, inclusive.
        size: One of :data:`PARTITION_SIZES`.

    Returns:
        Partitions in chronological order. Together they cover the requested
        range exactly: no gaps, no overlaps, nothing outside it.

    Raises:
        ValueError: If ``size`` is unknown, a date cannot be parsed, or
            ``start_date`` is after ``end_date``.

    Example:
        >>> parts = generate_partitions("2024-01-15", "2024-03-01")
        >>> [p.partition_date for p in parts]
        ['2024-01', '2024-02', '2024-03']
        >>> parts[1].start.isoformat(), parts[1].end.isoformat()
        ('2024-02-01', '2024-02-29')
    """
    size = size.strip().lower()
    if size not in PARTITION_SIZES:
        raise ValueError(
            f"Unknown partition size {size!r}; expected one of {PARTITION_SIZES}"
        )

    start = to_date(start_date)
    end = to_date(end_date)

    # Fail loudly. A reversed range would otherwise produce zero partitions,
    # and the run would "succeed" having scraped nothing at all.
    if start > end:
        raise ValueError(f"start_date {start.isoformat()} is after end_date {end.isoformat()}")

    partitions: list[Partition] = []
    #Start at the requested date, find the end of the current period, create the partition, 
    #move to the next day, and repeat until the end date.
    cursor = start
    while cursor <= end:
        # Clip to the caller's range so we never scrape outside it.
        period_end = min(_period_end(cursor, size), end)
        partitions.append(Partition(cursor, period_end, _label(cursor, size))) #prevent scrapping outside periode range
        # The next partition starts the day after this one ends. Because both
        # ends are inclusive, this is what guarantees no gaps and no overlaps.
        cursor = period_end + timedelta(days=1)

    return partitions


def describe(partitions: Iterable[Partition]) -> str:
    """One-line summary, handy in logs."""
    partitions = list(partitions)
    if not partitions:
        return "0 partitions"
    return (
        f"{len(partitions)} partitions "
        f"{partitions[0].start.isoformat()}..{partitions[-1].end.isoformat()}"
    )
