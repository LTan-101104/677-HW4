import json
from dataclasses import dataclass
from pathlib import Path


# frozen data class holding peer's info
@dataclass(frozen=True)
class PeerInfo:
    peer_id: int
    host: str
    port: int


# holding info and utilities for buidling peers
class PeerRegistry:
    def __init__(self, peers: dict[int, PeerInfo]) -> None:
        self._peers = peers

    @classmethod
    def build(
        cls, n: int, host: str = "127.0.0.1", base_port: int = 5000
    ) -> "PeerRegistry":
        peers = {
            pid: PeerInfo(peer_id=pid, host=host, port=base_port + pid)
            for pid in range(n)
        }
        return cls(peers)

    @classmethod
    def load(cls, path: Path) -> "PeerRegistry":
        data = json.loads(path.read_text())
        peers = {
            int(pid): PeerInfo(peer_id=int(pid), host=info["host"], port=info["port"])
            for pid, info in data.items()
        }
        return cls(peers)

    def save(self, path: Path) -> None:
        data = {
            str(p.peer_id): {"host": p.host, "port": p.port}
            for p in self._peers.values()
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))

    def get(self, peer_id: int) -> PeerInfo:
        return self._peers[peer_id]

    # sorted lists required for bully in leader election
    def all_ids(self) -> list[int]:
        return sorted(self._peers.keys())

    def others(self, self_id: int) -> list[PeerInfo]:
        return [p for pid, p in self._peers.items() if pid != self_id]

    def __len__(self) -> int:
        return len(self._peers)
