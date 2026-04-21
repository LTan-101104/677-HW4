import argparse
import time
from pathlib import Path

from config.enums import MessageType
from config.peer_registry import PeerRegistry
from peer.messages import Message
from peer.peer import Peer


PEERS_JSON = Path("config/peers.json")


def make_ping_handler(peer: Peer):
    def handler(msg: Message) -> None:
        print(
            f"[peer={peer.peer_id}] ping from {msg.sender} "
            f"ts={msg.ts} clock={peer.clock.value}"
        )
    return handler


def smoke_test(n: int) -> None:
    registry = PeerRegistry.build(n)
    registry.save(PEERS_JSON)
    print(f"[main] built registry of {n} peers -> {PEERS_JSON}")

    peers = [Peer(pid, registry) for pid in range(n)]
    for p in peers:
        p.register_handler(MessageType.ELECTION, make_ping_handler(p))
        p.start()

    time.sleep(0.5)  # let accept loops come up

    print("[main] all peers started; broadcasting pings")
    for p in peers:
        for other in registry.others(p.peer_id):
            p.unicast(other.peer_id, MessageType.ELECTION, {"ping": True})

    time.sleep(1.0)  # let messages flow

    for p in peers:
        p.stop()
    print("[main] all peers stopped")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=6, help="number of peers")
    args = parser.parse_args()
    smoke_test(args.n)


if __name__ == "__main__":
    main()
