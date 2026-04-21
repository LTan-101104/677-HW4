import argparse
import time
from pathlib import Path

from config.enums import MessageType, Role
from config.peer_registry import PeerRegistry
from peer.election import ElectionManager
from peer.messages import Message
from peer.peer import Peer
from peer.roles import BuyerBehavior, SellerBehavior, assign_roles
from peer.trader import TraderBehavior


PEERS_JSON = Path("config/peers.json")


def _make_buyer_log_handler(peer: Peer):
    """Returns a handler that logs BUY_RESP messages the buyer receives."""

    def handler(msg: Message) -> None:
        print(
            f"[peer={peer.peer_id} buyer] BUY_RESP {msg.payload.get('status')} "
            f"{msg.payload.get('item')} x{msg.payload.get('qty')} ts={msg.ts}"
        )

    return handler


def _make_seller_log_handler(peer: Peer):
    """Returns a handler that logs SOLD_NOTIFY messages the seller receives."""

    def handler(msg: Message) -> None:
        print(
            f"[peer={peer.peer_id} seller] SOLD_NOTIFY {msg.payload.get('item')} "
            f"x{msg.payload.get('qty')} ts={msg.ts}"
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

    # Wire election + buyer/seller response loggers on every peer. The trader
    # itself gets its handlers installed after election converges.
    for peer, election in zip(peers, elections):
        election.wire_handlers()
        peer.register_handler(MessageType.BUY_RESP, _make_buyer_log_handler(peer))
        peer.register_handler(MessageType.SOLD_NOTIFY, _make_seller_log_handler(peer))
        peer.start()

    time.sleep(0.3)  # let accept loops come up before anyone sends ELECTION

    print("[main] triggering elections")
    for election in elections:  # have to start all bc the starting can be a failed node
        election.start_election()

    time.sleep(1.0)  # give Bully time to converge
    winner = peers[0].coordinator_id
    assert winner is not None, "election failed to converge"
    print(f"[main] coordinator converged to peer {winner}")

    # Install trader behavior on the winner; its BUY / SELL_DEPOSIT handlers
    # now override any earlier placeholders registered for the same types.
    trader = TraderBehavior(peers[winner])
    trader.install()
    print(f"[main] trader installed on peer {winner}")

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
    print(f"[main] final balances: {trader.balances}")
    inv_summary = ", ".join(
        f"{item.value}: {sum(e[1] for e in entries)}"
        for item, entries in trader.inventory.items()
    )
    print(f"[main] final inventory: {{{inv_summary}}}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=6, help="number of peers")
    parser.add_argument("--duration", type=float, default=3.0, help="runtime seconds")
    args = parser.parse_args()
    run(args.n, args.duration)


if __name__ == "__main__":
    main()
