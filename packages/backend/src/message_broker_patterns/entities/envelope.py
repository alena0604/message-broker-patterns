from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

logger = logging.getLogger(__name__)

HEADER_FIELDS = frozenset({"message_id", "correlation_id", "causation_id", "occurred_at"})
"""Wire keys the envelope owns. A payload may not use them — see :class:`Envelope`."""


def new_message_id() -> str:
    """Mint a fresh message id.

    Indirected through a module-level function (rather than calling ``uuid4``
    inline) so a test can patch it and assert an exact id.
    """
    return uuid4().hex


def utc_now() -> datetime:
    """Current instant, timezone-aware UTC. Patchable for deterministic tests."""
    return datetime.now(UTC)


@dataclass(frozen=True)
class Envelope:
    """The shared metadata wrapper every message can travel in.

    A pattern's own dataclass (``Payment``, ``Task``, ``SupportTicket``, ...)
    keeps producing the flat ``str -> str`` mapping Redis Streams stores; the
    envelope adds the four tracing headers around it. ``to_fields`` merges the
    headers and the payload into one flat mapping, so a consumer that still
    calls ``Payment.from_fields(raw)`` keeps working against an enveloped entry
    — the envelope is additive on the wire, not a new format.

    Because headers and payload share one flat namespace, a payload key that
    collides with a header (:data:`HEADER_FIELDS`) is rejected at construction
    rather than silently shadowed.

    ``correlation_id`` defaults to ``message_id``: a message that starts a flow
    is its own correlation root. ``causation_id`` is empty for such a root and
    otherwise carries the ``message_id`` of the message that caused this one.
    ``occurred_at`` must be timezone-aware; a naive datetime is rejected and a
    non-UTC offset is normalized to UTC.
    """

    message_id: str = field(default_factory=lambda: new_message_id())
    correlation_id: str = ""
    causation_id: str = ""
    occurred_at: datetime = field(default_factory=lambda: utc_now())
    payload: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None:
            raise ValueError(
                f"occurred_at must be timezone-aware (UTC); got naive {self.occurred_at!r}"
            )
        collisions = sorted(HEADER_FIELDS.intersection(self.payload))
        if collisions:
            raise ValueError(f"payload keys collide with envelope headers: {', '.join(collisions)}")
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(UTC))
        object.__setattr__(self, "payload", dict(self.payload))
        if not self.correlation_id:
            object.__setattr__(self, "correlation_id", self.message_id)

    def to_fields(self) -> dict[str, str]:
        """Serialize to the flat string->string mapping Redis Streams stores."""
        return {
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "occurred_at": self.occurred_at.isoformat(),
            **self.payload,
        }

    @classmethod
    def from_fields(cls, fields: Mapping[bytes | str, bytes | str]) -> Envelope:
        """Reconstruct an Envelope from raw Redis Stream fields (bytes->bytes).

        Also accepts an already-decoded ``str -> str`` mapping, so the output of
        :meth:`to_fields` round-trips without an encode step.
        """
        decoded = {_decode(k): _decode(v) for k, v in fields.items()}
        occurred_at = datetime.fromisoformat(decoded["occurred_at"])
        return cls(
            message_id=decoded["message_id"],
            correlation_id=decoded["correlation_id"],
            causation_id=decoded["causation_id"],
            occurred_at=occurred_at,
            payload={k: v for k, v in decoded.items() if k not in HEADER_FIELDS},
        )


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value
