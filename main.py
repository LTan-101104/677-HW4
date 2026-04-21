import argparse
import time
from pathlib import Path

from config.enums import MessageType, Role
from config.peer_registry import PeerRegistry
from peer.messages import Message
from peer.peer import Peer
from peer.roles import BuyerBehavior, SellerBehavior, assign_roles


PEERS_JSON = Path("config/peers.json")


def _make_trader_log_handler(peer: Peer, label: str):
    """Returns a handler that just logs inbound messages at the 'trader'.

    Used for the Phase 3 smoke test so we can observe BUY / SELL_DEPOSIT
    traffic arriving — actual trader logic comes in Phase 5.
    """
    def handler(msg: Message) -> None:
        print(
            f"[trader={peer.peer_id}] {label} from {msg.sender} "
            f"ts={msg.ts} payload={msg.payload}"
        )
    return handler


def smoke_test(n: int, duration: float) -> None:
    registry = PeerRegistry.build(n)
    registry.save(PEERS_JSON)
    roles = assign_roles(n)
    trader_id = n - 1  # placeholder until election exists

    print(f"[main] built registry of {n} peers -> {PEERS_JSON}")
    for pid, role in enumerate(roles):
        marker = " <- trader" if pid == trader_id else ""
        print(f"[main] peer {pid}: {role.value}{marker}")

    peers = [Peer(pid, registry) for pid in range(n)]
    behaviors: list = []

    for pid, (peer, role) in enumerate(zip(peers, roles)):
        peer.coordinator_id = trader_id
        if pid == trader_id:
            peer.register_handler(
                MessageType.BUY, _make_trader_log_handler(peer, "BUY")
            )
            peer.register_handler(
                MessageType.SELL_DEPOSIT,
                _make_trader_log_handler(peer, "SELL_DEPOSIT"),
            )
        peer.start()

        if pid == trader_id:
            continue  # trader does not buy or sell while coordinating
        if role in (Role.BUYER, Role.BOTH):
            b = BuyerBehavior(peer, min_interval=0.3, max_interval=0.8)
            b.start()
            behaviors.append(b)
        if role in (Role.SELLER, Role.BOTH):
            s = SellerBehavior(peer, min_interval=0.3, max_interval=0.8)
            s.start()
            behaviors.append(s)

    print(f"[main] running for {duration}s")
    time.sleep(duration)

    for b in behaviors:
        b.stop()
    for p in peers:
        p.stop()
    print("[main] all peers stopped")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=6, help="number of peers")
    parser.add_argument("--duration", type=float, default=3.0, help="test duration seconds")
    args = parser.parse_args()
    smoke_test(args.n, args.duration)


if __name__ == "__main__":
    main()
