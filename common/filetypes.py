"""Work out what kind of document we just downloaded.

The Content-Type header is the primary signal, because it describes what the
server actually sent. The URL extension is only a fallback: a URL ending in
.html proves nothing if the server returns a PDF, and recon found decision
URLs whose path is misleading in other ways too.
"""

from posixpath import splitext
from urllib.parse import urlparse

#: Media type -> the extension we store the file under.
CONTENT_TYPE_EXTENSIONS = {
    "text/html": "html",
    "application/xhtml+xml": "html",
    "application/pdf": "pdf",
    "application/msword": "doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/rtf": "rtf",
    "text/rtf": "rtf",
    "text/plain": "txt",
}

#: Extensions we accept from a URL when the Content-Type is unhelpful.
KNOWN_EXTENSIONS = frozenset({"html", "htm", "pdf", "doc", "docx", "rtf", "txt"})

#: File types that must be stored byte-for-byte and never parsed or rewritten.
BINARY_TYPES = frozenset({"pdf", "doc", "docx", "rtf"})

UNKNOWN = "unknown"


def normalise_content_type(content_type):
    """Strip parameters and casing: 'text/html; charset=utf-8' -> 'text/html'."""
    if not content_type:
        return ""
    if isinstance(content_type, bytes):
        content_type = content_type.decode("ascii", "replace")
    return content_type.split(";")[0].strip().lower()


def extension_from_url(url):
    """Lower-case file extension from a URL path, without the dot."""
    if not url:
        return ""
    path = urlparse(url).path
    return splitext(path)[1].lstrip(".").lower()


def detect_file_type(content_type=None, url=None):
    """Classify a downloaded document.

    Args:
        content_type: The response Content-Type header.
        url: The URL it came from, used only as a fallback.

    Returns:
        One of "html", "pdf", "doc", "docx", "rtf", "txt", or "unknown".
        Returning "unknown" rather than guessing means an unexpected format
        is logged and stored as-is instead of being silently mislabelled.
    """
    media_type = normalise_content_type(content_type)
    if media_type in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[media_type]

    extension = extension_from_url(url)
    if extension in KNOWN_EXTENSIONS:
        return "html" if extension == "htm" else extension

    return UNKNOWN


def is_binary(file_type):
    """True for formats that must be stored unchanged (PDF, DOC, DOCX, RTF)."""
    return file_type in BINARY_TYPES
