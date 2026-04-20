import json
from dataclasses import dataclass, field
from typing import Any

from config.enums import MessageType


# Wire envelope: one JSON object per message, newline-terminated for TCP framing.
@dataclass
class Message:
    type: MessageType
    sender: int
    ts: int
    payload: dict[str, Any] = field(default_factory=dict)

    def to_bytes(self) -> bytes:
        obj = {
            "type": self.type.value,
            "sender": self.sender,
            "ts": self.ts,
            "payload": self.payload,
        }
        return (json.dumps(obj) + "\n").encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        obj = json.loads(data.decode("utf-8"))
        return cls(
            type=MessageType(obj["type"]),
            sender=obj["sender"],
            ts=obj["ts"],
            payload=obj.get("payload") or {},
        )
