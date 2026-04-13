from __future__ import annotations

from pathlib import Path

from app.services.object_store import LocalObjectStore


def test_local_object_store_roundtrip(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path / "objects")
    stored = store.put_bytes("raw/fl/test.txt", b"hello world", content_type="text/plain")

    assert stored.key == "raw/fl/test.txt"
    assert stored.size_bytes == 11
    assert store.exists("raw/fl/test.txt")
    assert store.get_bytes("raw/fl/test.txt") == b"hello world"

    destination = tmp_path / "copy.txt"
    store.write_to_path("raw/fl/test.txt", destination)
    assert destination.read_bytes() == b"hello world"
