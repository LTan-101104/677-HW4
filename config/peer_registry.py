import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PeerInfo:
    """Immutable record identifying a single peer on the network."""

    peer_id: int
    host: str
    port: int


class PeerRegistry:
    """Directory of all peers in the system, keyed by peer_id."""

    def __init__(self, peers: dict[int, PeerInfo]) -> None:
        """Wraps a pre-built peer map; prefer `build` or `load` to construct."""
        self._peers = peers

    @classmethod
    def build(
        cls, n: int, host: str = "127.0.0.1", base_port: int = 5000
    ) -> "PeerRegistry":
        """Generates a fresh registry of n peers on sequential ports.

        Peer IDs run 0..n-1; each gets a port of `base_port + peer_id`.
        Used by the parent program at startup before spawning peers.
        """
        peers = {
            pid: PeerInfo(peer_id=pid, host=host, port=base_port + pid)
            for pid in range(n)
        }
        return cls(peers)

    @classmethod
    def load(cls, path: Path) -> "PeerRegistry":
        """Reconstructs a registry from the JSON file written by `save`."""
        data = json.loads(path.read_text())
        peers = {
            int(pid): PeerInfo(peer_id=int(pid), host=info["host"], port=info["port"])
            for pid, info in data.items()
        }
        return cls(peers)

    def save(self, path: Path) -> None:
        """Persists the registry to disk so all peers can read the same map.

        Creates the parent directory if it does not already exist.
        Won't be useful if running on a single process, but in multi-process scenario this can enable different process sharing info about peer
        """
        data = {
            str(p.peer_id): {"host": p.host, "port": p.port}
            for p in self._peers.values()
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))

    def get(self, peer_id: int) -> PeerInfo:
        """Looks up one peer's connection info by ID."""
        return self._peers[peer_id]

    def all_ids(self) -> list[int]:
        """Returns every peer_id in ascending order.

        The sort is relied on by Bully election (iterating "higher-ID peers").
        """
        return sorted(self._peers.keys())

    def others(self, self_id: int) -> list[PeerInfo]:
        """Returns every peer except the caller, for multicast targeting."""
        return [p for pid, p in self._peers.items() if pid != self_id]

    def __len__(self) -> int:
        """Total number of peers in the registry."""
        return len(self._peers)
