from message_broker_patterns.competing_consumers_pattern.models import Task


def test_to_fields_serializes_to_string_mapping() -> None:
    task = Task(task_id="t-1", payload="hello")

    fields = task.to_fields()

    assert fields == {"task_id": "t-1", "payload": "hello"}


def test_from_fields_reconstructs_task_from_bytes() -> None:
    raw = {b"task_id": b"t-2", b"payload": b"world"}

    task = Task.from_fields(raw)

    assert task == Task(task_id="t-2", payload="world")


def test_round_trip_preserves_task() -> None:
    original = Task(task_id="t-3", payload="round-trip")

    # to_fields yields str->str; Redis stores it as bytes->bytes on read-back.
    encoded = {k.encode(): v.encode() for k, v in original.to_fields().items()}

    assert Task.from_fields(encoded) == original
