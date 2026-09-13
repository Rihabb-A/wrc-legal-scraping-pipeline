"""Structured JSON logging, one object per line.

Scrapy's console output is written for a human watching a run. This is the
other half: a machine-readable record of what happened, so a run can be
audited afterwards without anyone having to read or grep prose.

Each line of the log file is a complete, self-contained JSON object:

    {"timestamp": "...", "run_id": "a3f9...", "event": "partition_started",
     "partition_date": "2025-08", "body": "Labour Court"}

That format - JSON Lines - is chosen because a run can be appended to safely,
truncated without corrupting earlier entries, and read back one line at a
time. A single top-level JSON array would need the whole file rewritten on
every event and would be unreadable until the run finished.

Every line carries the same ``run_id``, so the log can accumulate across many
runs and still be split back into individual ones.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

#: Name of the dedicated logger. Kept separate from Scrapy's own loggers so
#: that JSON events and human-readable console output never mix.
EVENT_LOGGER_NAME = "wrc.events"

#: Keys written first, in this order, before any event-specific fields.
_LEADING_KEYS = ("timestamp", "run_id", "level", "event")


class JsonLineFormatter(logging.Formatter):
    """Render a log record as one line of JSON."""

    def format(self, record):
        payload = {
            # UTC with an explicit offset: a run's logs may be read in a
            # different timezone from the machine that produced them.
            "timestamp": datetime.fromtimestamp(
                record.created, timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "event": getattr(record, "event", "message"),
        }

        run_id = getattr(record, "run_id", None)
        if run_id:
            payload["run_id"] = run_id

        # Event-specific fields, attached by EventLog.emit.
        payload.update(getattr(record, "fields", {}))

        message = record.getMessage()
        if message and not getattr(record, "event", None):
            payload["message"] = message

        ordered = {key: payload.pop(key) for key in _LEADING_KEYS if key in payload}
        ordered.update(payload)

        # default=str so a stray date or ObjectId cannot crash a run; the log
        # must never be the thing that breaks the pipeline. ensure_ascii=False
        # keeps Irish names such as FHÁTHARTA readable rather than escaped.
        return json.dumps(ordered, default=str, ensure_ascii=False)


class EventLog:
    """Writes structured events for one pipeline run.

    Args:
        path: Destination .jsonl file. Parent directories are created.
        run_id: Identifier shared by every line of this run. Generated if
            not supplied.
        also_console: Mirror events to the console. Off by default, since
            Scrapy already prints a readable narrative there.
    """

    def __init__(self, path, run_id=None, also_console=False):
        self.path = Path(path)
        self.run_id = run_id or uuid.uuid4().hex[:12]

        self.path.parent.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger(f"{EVENT_LOGGER_NAME}.{self.run_id}")
        self._logger.setLevel(logging.INFO)
        # Do not hand these records to the root logger: Scrapy would print the
        # JSON to the console as well, duplicating every line.
        self._logger.propagate = False

        # Append rather than truncate, so a log accumulates across runs and an
        # earlier run's evidence is never destroyed by a later one.
        self._handler = logging.FileHandler(self.path, mode="a", encoding="utf-8")
        self._handler.setFormatter(JsonLineFormatter())
        self._logger.addHandler(self._handler)

        if also_console:
            console = logging.StreamHandler()
            console.setFormatter(JsonLineFormatter())
            self._logger.addHandler(console)

    def emit(self, event, level=logging.INFO, **fields):
        """Write one event.

        Args:
            event: Short machine-readable name, e.g. "download_failed".
            level: Logging level; ERROR for failures so they can be filtered.
            **fields: Everything else to record about the event.
        """
        self._logger.log(
            level,
            event,
            extra={"event": event, "run_id": self.run_id, "fields": fields},
        )

    def error(self, event, **fields):
        """Emit an event at ERROR level."""
        self.emit(event, level=logging.ERROR, **fields)

    def close(self):
        self._handler.flush()
        self._logger.removeHandler(self._handler)
        self._handler.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


def read_events(path):
    """Read a .jsonl log back into a list of dicts.

    Used by tests and by anyone reconciling a run afterwards. Blank lines are
    skipped so a partially flushed file still parses.
    """
    events = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events
