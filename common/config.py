"""Read configuration from environment variables, with .env as the source.

The assignment requires that connection strings, storage paths, partition
sizes and scraping parameters are all configurable, with no hardcoded values.
Everything funnels through here so there is exactly one place that decides
what a setting is called and what its default is.

Precedence: a real environment variable wins over .env, which wins over the
default passed at the call site. That order lets Docker, CI or a shell export
override the file without editing it.
"""

import os

from dotenv import load_dotenv

# override=False: a variable already set in the real environment is left
# alone, so `MONGO_URI=... scrapy crawl ...` beats whatever .env says.
load_dotenv(override=False)

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def env(name, default=None):
    """String setting. Blank is treated as unset."""
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def env_int(name, default):
    """Integer setting. Falls back to the default if the value is not a number."""
    raw = env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def env_float(name, default):
    """Float setting, used for sub-second delays."""
    raw = env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def env_bool(name, default):
    """Boolean setting accepting 1/true/yes/on and 0/false/no/off."""
    raw = env(name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")
