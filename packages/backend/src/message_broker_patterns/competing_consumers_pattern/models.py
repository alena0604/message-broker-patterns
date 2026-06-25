from dataclasses import dataclass


@dataclass(frozen=True)
class Task:
    """A unit of work pushed onto the stream for competing consumers to process."""

    task_id: str
    payload: str

    def to_fields(self) -> dict[str, str]:
        """Serialize to the flat string->string mapping Redis Streams stores."""
        return {"task_id": self.task_id, "payload": self.payload}

    @classmethod
    def from_fields(cls, fields: dict[bytes, bytes]) -> "Task":
        """Reconstruct a Task from raw Redis Stream fields (bytes->bytes)."""
        return cls(
            task_id=fields[b"task_id"].decode(),
            payload=fields[b"payload"].decode(),
        )
