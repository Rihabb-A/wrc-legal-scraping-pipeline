"""MinIO adapter for the document files themselves.

MongoDB holds metadata *about* a document; this holds the bytes. Object
storage is the right home for them: files are large, immutable once written,
and only ever fetched whole by key.

MinIO speaks the S3 API, so moving to real S3 later is a change of endpoint
and credentials, not a change of code.
"""
#Used to turn raw bytes into an in-memory file-like object.
import io

from minio import Minio 
from minio.error import S3Error


class MinioStore:
    """Thin wrapper around one MinIO server and its two buckets."""

    def __init__(
        self,
        endpoint,
        access_key,
        secret_key,
        landing_bucket="wrc-landing",
        curated_bucket="wrc-curated",
        secure=False,
    ):
        self.endpoint = endpoint
        self.landing_bucket = landing_bucket
        self.curated_bucket = curated_bucket
        self._client = Minio(
            endpoint, access_key=access_key, secret_key=secret_key, secure=secure
        )

    # -- lifecycle ------------------------------------------------------

    def connect(self):
        """Verify the server is reachable and both buckets exist.

        Called before the crawl starts so an unreachable or misconfigured
        object store fails immediately rather than after hundreds of pages.
        """
        for bucket in (self.landing_bucket, self.curated_bucket):
            if not self._client.bucket_exists(bucket):
                # docker-compose normally creates these, but creating them
                # here keeps the pipeline runnable against any S3 endpoint.
                self._client.make_bucket(bucket)
        return self

    def close(self):
        """Nothing to release: the MinIO client holds no persistent connection."""

    def __enter__(self):
        return self.connect()

    def __exit__(self, *exc_info):
        self.close()

    # -- reads ----------------------------------------------------------

    def stat(self, bucket, key):
        """Size in bytes of the stored object, or None if there is none.

        Returning the *stored* size matters: metadata must describe what is
        actually in the bucket, not what the last download happened to
        contain. Those differ on this site, because each response carries a
        server timing comment (docs/recon.md section 10).
        """
        try:
            return self._client.stat_object(bucket, key).size
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "NotFound"):
                return None
            raise

    def exists(self, bucket, key):
        """True if an object is already stored under this key.

        This is what keeps the landing zone immutable: an object already at
        a key came from the same source URL and is never overwritten.
        """
        return self.stat(bucket, key) is not None

    def download(self, bucket, key):
        """Fetch an object's bytes.

        The response must be closed and its connection released, otherwise
        the pool leaks - which is why this is wrapped rather than called
        directly at each site.
        """
        response = None
        try:
            response = self._client.get_object(bucket, key)
            return response.read()
        finally:
            if response is not None:
                response.close()
                response.release_conn()

    def list_keys(self, bucket, prefix=""):
        """Object keys under a prefix, e.g. everything in one partition."""
        return [
            obj.object_name
            for obj in self._client.list_objects(bucket, prefix=prefix, recursive=True)
        ]

    # -- writes ---------------------------------------------------------

    def upload(self, bucket, key, data, content_type="application/octet-stream"):
        """Store bytes under a key. Overwrites if the key already exists.

        Callers that must not overwrite should check :meth:`exists` first;
        :meth:`upload_if_absent` does both.
        """
        self._client.put_object(
            bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return key

    def upload_if_absent(self, bucket, key, data, content_type="application/octet-stream"):
        """Store bytes only if nothing is stored under this key yet.

        Returns ``(outcome, stored_size)`` where outcome is "stored" or
        "skipped_existing" and stored_size is the size of the object now in
        the bucket - which, when skipping, is the size of the copy already
        there rather than of the bytes just downloaded.

        Skipping rather than overwriting is what makes re-running a date
        range idempotent. Without it every run would rewrite every object,
        because these pages embed a server timing comment that differs on
        each request (docs/recon.md section 10).
        """
        existing_size = self.stat(bucket, key)
        if existing_size is not None:
            return "skipped_existing", existing_size
        self.upload(bucket, key, data, content_type)
        return "stored", len(data)

    def count(self, bucket, prefix=""):
        return len(self.list_keys(bucket, prefix))


__all__ = ["MinioStore", "S3Error"]
