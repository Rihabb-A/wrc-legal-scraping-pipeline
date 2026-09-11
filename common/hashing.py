"""SHA-256 fingerprints for stored documents.

Two different questions need two different hashes:

``file_hash``
    SHA-256 of exactly the bytes held in object storage. It answers "is the
    stored object intact and unchanged?" and satisfies the requirement to
    record a file hash alongside each record.

``content_hash``
    SHA-256 of the document with volatile markup removed. It answers "did
    this document actually change since last time?"

The second exists because of a verified property of this site: the same
decision page returns different bytes on every request, differing only in a
server timing comment:

    <!-- Elapsed time: 0.0155965 -->

Hashing raw bytes would therefore report every document as changed on every
run, which would defeat deduplication entirely (docs/recon.md section 10).
"""

import hashlib
import re

from common.filetypes import is_binary

#: Named so it can be recorded next to the digest. A stored hash is only
#: meaningful if you know which algorithm produced it.
HASH_ALGORITHM = "sha256"

#: Matches any HTML comment, including ones spanning several lines.
#:
#: All comments are stripped, not just the two volatile ones, so that a new
#: server-generated comment cannot silently break change detection later. The
#: trade-off is accepted deliberately: comments never carry legal content, and
#: this hash is only ever used for comparison - the bytes we store are
#: untouched.
HTML_COMMENT_RE = re.compile(rb"<!--.*?-->", re.DOTALL)

#: Read files in 1 MiB pieces rather than loading them whole.
CHUNK_SIZE = 1024 * 1024


def sha256_bytes(data: bytes) -> str:
    """SHA-256 of a bytes object, as lowercase hex.

    >>> sha256_bytes(b"")[:16]
    'e3b0c44298fc1c14'
    """
    #makes sure you actually passed binary data.
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError(f"Expected bytes, got {type(data).__name__}")
    return hashlib.sha256(data).hexdigest()

#large files, processed in chunks
def sha256_stream(fileobj, chunk_size: int = CHUNK_SIZE) -> str:
    """SHA-256 of a readable binary stream, without loading it all into memory.

    Used for large objects read back out of storage; hashing is incremental,
    so memory use stays flat no matter how big the file is.
    """
    digest = hashlib.sha256()
    while True:
        chunk = fileobj.read(chunk_size)
        if not chunk:
            break
        #add pieces into running hash calculation
        digest.update(chunk)
    return digest.hexdigest()

#remove comments only used before calculating content hash
def normalise_html(data: bytes) -> bytes:
    """Remove HTML comments so the same page hashes the same every time.

    Only used to compute a comparison hash. The bytes written to the landing
    zone are never normalised - that copy stays exactly as the server sent it.
    """
    return HTML_COMMENT_RE.sub(b"", data)

#content hash
def content_digest(data: bytes, file_type: str) -> str:
    """Stable hash of a document's meaningful content.

    HTML is normalised first. Binary formats such as PDF and DOCX are hashed
    as-is: they contain no volatile markup, so for those ``content_hash`` and
    ``file_hash`` are identical by construction.
    """
    if is_binary(file_type):
        return sha256_bytes(data)
    return sha256_bytes(normalise_html(data))


def digests(data: bytes, file_type: str) -> dict:
    """Both hashes for one document, ready to merge into a metadata record."""
    return {
        "hash_algorithm": HASH_ALGORITHM,
        "file_hash": sha256_bytes(data),
        "content_hash": content_digest(data, file_type),
    }


#: Listing fields that would change if a decision were amended or re-issued.
#: Deliberately excludes anything volatile or derived.
LISTING_FIELDS = ("identifier", "title", "description", "published_date", "doc_url")


def listing_digest(record: dict) -> str:
    """Fingerprint of a search-result row, computed without downloading anything.

    The server sends no ETag or Last-Modified and ignores conditional
    requests, so there is no cheap way to ask "has this document changed?".
    The search listing is the next best signal: it is fetched anyway, and it
    carries the fields that move when a decision is amended or re-issued.

    An unchanged listing row means we already hold that version of the
    document and can skip re-downloading it. A changed row means fetch it and
    let content_hash decide whether the content really differs.
    """
    parts = [f"{field}={record.get(field) or ''}" for field in LISTING_FIELDS]
    return sha256_bytes(chr(10).join(parts).encode("utf-8"))
