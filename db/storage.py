"""Object storage client for artifact bodies (Fly Tigris in prod, minio in dev/test)."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)

JSON_CONTENT_TYPE = "application/json"
TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"
DEFAULT_PRESIGN_TTL = 300

_VALID_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class StorageError(RuntimeError):
    """Base class for storage failures."""


class StorageUnavailable(StorageError):
    """Storage backend is unreachable or misconfigured."""


class StorageKeyMissing(StorageError):
    """The requested key does not exist in the bucket."""


def _safe_name(name: str) -> str:
    """Reject artifact names with path separators or control characters."""
    if not _VALID_NAME_RE.match(name):
        raise ValueError(f"Unsafe artifact name for storage key: {name!r}")
    return name


def _key_prefix() -> str:
    """Optional prefix for every storage key. Used to scope PR-preview envs to
    a shared bucket (e.g. ``pr-123/``) so teardown can wipe one prefix cleanly.

    Normalized to an empty string or a single trailing slash.
    """
    prefix = os.environ.get("ARTIFACT_STORAGE_PREFIX", "").strip().strip("/")
    return f"{prefix}/" if prefix else ""


def artifact_key(job_id: UUID | str, name: str) -> str:
    """Deterministic S3 key for an artifact body."""
    return f"{_key_prefix()}artifacts/{job_id}/{_safe_name(name)}"


def source_file_key(job_id: UUID | str, path: str) -> str:
    """Deterministic S3 key for a source file (path is hashed to avoid unsafe chars)."""
    digest = hashlib.sha1(path.encode("utf-8")).hexdigest()
    return f"{_key_prefix()}source_files/{job_id}/{digest}"


def serialize_artifact(data: Any | None, text_data: str | None) -> tuple[bytes, str]:
    """Encode an artifact payload to (bytes, content_type)."""
    if data is not None:
        body = json.dumps(data, default=str).encode("utf-8")
        return body, JSON_CONTENT_TYPE
    if text_data is not None:
        return text_data.encode("utf-8"), TEXT_CONTENT_TYPE
    return b"", TEXT_CONTENT_TYPE


def deserialize_artifact(body: bytes, content_type: str | None) -> dict | list | str:
    """Decode bytes from storage back to a Python value."""
    if content_type and content_type.startswith("application/json"):
        return json.loads(body.decode("utf-8"))
    return body.decode("utf-8")


class StorageClient:
    """Thin wrapper over an S3-compatible backend (Tigris, minio, S3, R2)."""

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str = "auto",
    ) -> None:
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:
            raise StorageUnavailable(
                "boto3 is required for object storage; install it (uv sync) or unset ARTIFACT_STORAGE_*"
            ) from exc

        self.bucket = bucket
        self.endpoint = endpoint
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=2,
                read_timeout=5,
                retries={"max_attempts": 1},
            ),
        )

    def put(
        self,
        key: str,
        body: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> None:
        from botocore.exceptions import BotoCoreError, ClientError

        params: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
        }
        if metadata:
            params["Metadata"] = metadata
        try:
            self._client.put_object(**params)
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable(f"put_object failed for {key}: {exc}") from exc

    def get(self, key: str) -> bytes:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404"}:
                raise StorageKeyMissing(key) from exc
            raise StorageUnavailable(f"get_object failed for {key}: {exc}") from exc
        except BotoCoreError as exc:
            raise StorageUnavailable(f"get_object transport error for {key}: {exc}") from exc
        return response["Body"].read()

    def get_many(self, keys: list[str]) -> dict[str, bytes | None]:
        """Fetch multiple keys concurrently. Returns a dict keyed by every
        unique input key. The value is the bytes if the GET succeeded, or
        ``None`` if the object was missing (NoSuchKey/404) **or** the
        transport failed for that key. Per-key transport errors are logged
        and surfaced as ``None`` so a flaky bucket can't take down a whole
        API response — callers iterate and treat ``None`` as "skip".

        The boto3 S3 client is documented as thread-safe, so a small fixed
        pool gives effectively-parallel HTTP round-trips.
        """
        if not keys:
            return {}
        unique = list(dict.fromkeys(keys))

        def _fetch(k: str) -> tuple[str, bytes | None]:
            try:
                return k, self.get(k)
            except StorageKeyMissing:
                return k, None
            except StorageError as exc:
                logger.warning("get_many: transport error fetching %s: %s", k, exc)
                return k, None

        with ThreadPoolExecutor(max_workers=16) as ex:
            return dict(ex.map(_fetch, unique))

    def presign(self, key: str, expires_in: int = DEFAULT_PRESIGN_TTL) -> str:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            return self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_in,
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable(f"presign failed for {key}: {exc}") from exc

    def delete(self, key: str) -> None:
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self._client.delete_object(Bucket=self.bucket, Key=key)
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable(f"delete failed for {key}: {exc}") from exc

    def copy(self, src_key: str, dst_key: str) -> None:
        """Server-side copy within the same bucket (no egress)."""
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self._client.copy_object(
                Bucket=self.bucket,
                Key=dst_key,
                CopySource={"Bucket": self.bucket, "Key": src_key},
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable(f"copy {src_key} -> {dst_key} failed: {exc}") from exc

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist. Used by the test harness."""
        from botocore.exceptions import ClientError

        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self._client.create_bucket(Bucket=self.bucket)

    def health_check(self) -> None:
        """Verify the bucket is reachable. Raises StorageUnavailable on failure."""
        from botocore.exceptions import BotoCoreError, ClientError

        try:
            self._client.head_bucket(Bucket=self.bucket)
        except (BotoCoreError, ClientError) as exc:
            raise StorageUnavailable(f"head_bucket failed for {self.bucket}: {exc}") from exc


def _read_env() -> tuple[str | None, str | None, str | None, str | None]:
    return (
        os.environ.get("ARTIFACT_STORAGE_ENDPOINT"),
        os.environ.get("ARTIFACT_STORAGE_BUCKET"),
        os.environ.get("ARTIFACT_STORAGE_ACCESS_KEY"),
        os.environ.get("ARTIFACT_STORAGE_SECRET_KEY"),
    )


@functools.lru_cache(maxsize=1)
def get_storage_client() -> StorageClient | None:
    """Return a StorageClient if ARTIFACT_STORAGE_* env vars are set, else None.

    Returning None is the explicit "no object storage configured" signal —
    callers fall back to inline Postgres storage. This keeps local development
    and unit tests usable without a running minio container.
    """
    endpoint, bucket, access_key, secret_key = _read_env()
    if not (endpoint and bucket and access_key and secret_key):
        logger.info("ARTIFACT_STORAGE_* env vars not all set — artifact bodies will be stored inline in Postgres")
        return None
    return StorageClient(endpoint, bucket, access_key, secret_key)


def reset_client_cache() -> None:
    """Drop the cached client so a subsequent call re-reads env. For tests."""
    get_storage_client.cache_clear()
