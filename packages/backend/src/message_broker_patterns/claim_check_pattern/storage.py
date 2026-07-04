from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

logger = logging.getLogger(__name__)


class PayloadNotFoundError(KeyError):
    """Raised when a claim id has no payload in the store (never stored, or deleted)."""


class FilesystemPayloadStore:
    """A filesystem-backed store for large payloads, keyed by claim id.

    This stands in for something like an object store (S3) or a shared file
    server — the "external storage" of the Claim Check pattern. It is a small,
    explicit class rather than an abstract interface: this repo imports
    infrastructure directly instead of hiding it behind premature abstractions.

    Each payload is written to ``<root>/<claim_id>.bin``. ``put`` mints the
    claim id, so the store is the single authority for the storage token that
    the producer then wraps in a :class:`ClaimCheck`.
    """

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes) -> str:
        """Write payload bytes to storage; return the freshly minted claim id."""
        claim_id = uuid4().hex
        self._path(claim_id).write_bytes(data)
        logger.debug("Stored payload claim=%s size=%d", claim_id, len(data))
        return claim_id

    def get(self, claim_id: str) -> bytes:
        """Read the payload bytes for a claim id.

        Raises :class:`PayloadNotFoundError` when nothing is stored under the id.
        """
        path = self._path(claim_id)
        if not path.exists():
            raise PayloadNotFoundError(claim_id)
        return path.read_bytes()

    def delete(self, claim_id: str) -> None:
        """Remove a stored payload. Idempotent — deleting a missing id is a no-op."""
        self._path(claim_id).unlink(missing_ok=True)
        logger.debug("Deleted payload claim=%s", claim_id)

    def exists(self, claim_id: str) -> bool:
        """Return whether a payload is currently stored under this claim id."""
        return self._path(claim_id).exists()

    def path_for(self, claim_id: str) -> Path:
        """Return the on-disk path where a claim id's payload is (or would be) stored."""
        return self._path(claim_id)

    def _path(self, claim_id: str) -> Path:
        return self._root / f"{claim_id}.bin"
