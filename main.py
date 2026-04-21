import argparse
import time
from pathlib import Path

from config.enums import MessageType, Role
from config.peer_registry import PeerRegistry
from peer.election import ElectionManager
from peer.messages import Message
from peer.peer import Peer
from peer.roles import BuyerBehavior, SellerBehavior, assign_roles


PEERS_JSON = Path("config/peers.json")


def _make_trader_log_handler(peer: Peer, label: str):
    """Returns a handler that just logs inbound messages at the trader.

    Used for the Phase 4 smoke test so we can observe BUY / SELL_DEPOSIT
    traffic arriving at the elected coordinator — actual trader logic
    comes in Phase 5.
    """
    def handler(msg: Message) -> None:
        if peer.peer_id != peer.coordinator_id:
            return  # only the current trader should log
        print(
            f"[trader={peer.peer_id}] {label} from {msg.sender} "
            f"ts={msg.ts} payload={msg.payload}"
        )
    return handler


def run(n: int, duration: float) -> None:
    registry = PeerRegistry.build(n)
    registry.save(PEERS_JSON)
    roles = assign_roles(n)

    print(f"[main] built registry of {n} peers -> {PEERS_JSON}")
    for pid, role in enumerate(roles):
        print(f"[main] peer {pid}: {role.value}")

    peers = [Peer(pid, registry) for pid in range(n)]
    elections = [ElectionManager(p) for p in peers]

    # Wire election handlers + a placeholder BUY/SELL logger on every peer
    # (the logger self-filters on coordinator_id so only the winner speaks).
    for peer, election in zip(peers, elections):
        election.wire_handlers()
        peer.register_handler(MessageType.BUY, _make_trader_log_handler(peer, "BUY"))
        peer.register_handler(
            MessageType.SELL_DEPOSIT, _make_trader_log_handler(peer, "SELL_DEPOSIT")
        )
        peer.start()

    time.sleep(0.3)  # let accept loops come up before anyone sends ELECTION

    print("[main] triggering elections")
    for election in elections:
        election.start_election()

    time.sleep(1.0)  # give Bully time to converge
    winner = peers[0].coordinator_id
    print(f"[main] coordinator converged to peer {winner}")

    behaviors: list = []
    for peer, role in zip(peers, roles):
        if peer.peer_id == winner:
            continue  # trader does not buy or sell
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
    parser.add_argument("--duration", type=float, default=3.0, help="runtime seconds")
    args = parser.parse_args()
    run(args.n, args.duration)


if __name__ == "__main__":
    main()
