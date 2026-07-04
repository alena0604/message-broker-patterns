from pathlib import Path

import pytest

from message_broker_patterns.claim_check_pattern.storage import (
    FilesystemPayloadStore,
    PayloadNotFoundError,
)


def test_put_creates_a_file_and_returns_a_claim_id(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)

    claim_id = store.put(b"large payload")

    assert claim_id
    assert store.exists(claim_id) is True


def test_store_creates_its_root_directory(tmp_path: Path) -> None:
    root = tmp_path / "nested" / "storage"

    FilesystemPayloadStore(root)

    assert root.is_dir()


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"small",
        b"x" * 10_000,
        bytes(range(256)),
        "unicode: ☃é".encode(),
    ],
)
def test_round_trip_preserves_bytes_exactly(tmp_path: Path, data: bytes) -> None:
    store = FilesystemPayloadStore(tmp_path)

    claim_id = store.put(data)

    assert store.get(claim_id) == data


def test_put_generates_unique_claim_ids(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)

    ids = {store.put(b"payload") for _ in range(50)}

    assert len(ids) == 50


def test_delete_removes_the_payload(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    claim_id = store.put(b"payload")

    store.delete(claim_id)

    assert store.exists(claim_id) is False


def test_delete_is_idempotent(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    claim_id = store.put(b"payload")

    store.delete(claim_id)
    store.delete(claim_id)  # second delete must not raise

    assert store.exists(claim_id) is False


def test_get_missing_payload_raises(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)

    with pytest.raises(PayloadNotFoundError):
        store.get("does-not-exist")


def test_get_after_delete_raises(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    claim_id = store.put(b"payload")
    store.delete(claim_id)

    with pytest.raises(PayloadNotFoundError):
        store.get(claim_id)


def test_path_for_points_at_the_stored_file(tmp_path: Path) -> None:
    store = FilesystemPayloadStore(tmp_path)
    claim_id = store.put(b"payload")

    path = store.path_for(claim_id)

    assert path == tmp_path / f"{claim_id}.bin"
    assert path.is_file()
