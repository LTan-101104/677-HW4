import argparse
import time
from pathlib import Path

from config.enums import BuyStatus, MessageType, Role
from config.peer_registry import PeerRegistry
from peer.election import ElectionManager
from peer.logger import log
from peer.messages import Message
from peer.peer import Peer
from peer.roles import BuyerBehavior, SellerBehavior, assign_roles
from peer.trader import TraderBehavior
from config.constant import FIRST_TRADER_RESIGNS, RESIGN_AFTER, RESIGN_YIELD

PEERS_JSON = Path("config/peers.json")


def _make_buyer_log_handler(peer: Peer):
    """Returns a handler that logs BUY_RESP messages the buyer receives.

    Emits `log.buy_result` for every response and the spec-mandated
    `log.bought` additionally on SUCCESS.
    """

    def handler(msg: Message) -> None:
        status = msg.payload.get("status")
        item = msg.payload.get("item")
        qty = msg.payload.get("qty")
        log.buy_result(peer.peer_id, status, item, qty, from_peer=msg.sender)
        if status == BuyStatus.SUCCESS.value:
            log.bought(peer.peer_id, item, qty, from_peer=msg.sender)

    return handler


def _make_seller_log_handler(peer: Peer):
    """Returns a handler that logs SOLD_NOTIFY messages the seller receives."""

    def handler(msg: Message) -> None:
        log.sold(peer.peer_id, msg.payload.get("item"), msg.payload.get("qty"))

    return handler


def run(n: int, duration: float) -> None:
    registry = PeerRegistry.build(n)
    registry.save(PEERS_JSON)
    roles = assign_roles(n)

    log.info(f"[main] built registry of {n} peers -> {PEERS_JSON}")
    for pid, role in enumerate(roles):
        log.info(f"[main] peer {pid}: {role.value}")

    peers = [Peer(pid, registry) for pid in range(n)]
    elections = [ElectionManager(p) for p in peers]

    # Track every TraderBehavior that has held the role this run, so we can
    # stop their workers cleanly at shutdown regardless of resignations.
    active_traders: list[TraderBehavior] = []

    def make_install_trader(peer: Peer, election: ElectionManager):
        """Returns the on_become_coordinator hook for `peer`.

        Only the first elected trader is set up to resign; subsequent ones
        run to completion (see FIRST_TRADER_RESIGNS comment for why).
        """

        def install_trader() -> None:
            buyer_ids = [
                pid
                for pid in range(n)
                if pid != peer.peer_id and roles[pid] in (Role.BUYER, Role.BOTH)
            ]
            will_resign = len(active_traders) == 0 and FIRST_TRADER_RESIGNS
            trader = TraderBehavior(
                peer,
                buyer_ids=buyer_ids,
                resign_after=RESIGN_AFTER if will_resign else None,
                on_resign=(
                    (lambda: election.yield_for(RESIGN_YIELD)) if will_resign else None
                ),
            )
            trader.load_state()  # picks up the previous trader's checkpoint
            trader.install()
            active_traders.append(trader)
            log.info(
                f"[main] trader installed on peer {peer.peer_id} "
                f"(buyer_ids={buyer_ids}, will_resign={will_resign})"
            )

        return install_trader

    def make_get_other_buyers(peer: Peer):
        """Live recomputation of `other_buyer_ids` for one BuyerBehavior.

        Excludes self and the current coordinator, so when the trader
        changes, multicast targets follow without any explicit re-wire.
        """

        def fn() -> list[int]:
            current_trader = peer.coordinator_id
            return [
                pid
                for pid in range(n)
                if roles[pid] in (Role.BUYER, Role.BOTH)
                and pid != peer.peer_id
                and pid != current_trader
            ]

        return fn

    # Wire election handlers (incl. the on_become_coordinator hook that
    # installs TraderBehavior on whoever wins) and start every peer.
    for peer, election in zip(peers, elections):
        election.on_become_coordinator = make_install_trader(peer, election)
        election.wire_handlers()
        peer.start()

    # Behaviors are created on every peer per role — including any peer that
    # later becomes trader. While trader, the behavior loops auto-pause via
    # the `coordinator_id == peer_id` check; on resignation they auto-resume.
    behaviors: list = []
    for peer, role in zip(peers, roles):
        if role in (Role.BUYER, Role.BOTH):
            peer.register_handler(MessageType.BUY_RESP, _make_buyer_log_handler(peer))
            b = BuyerBehavior(
                peer,
                other_buyer_ids=make_get_other_buyers(peer),
                min_interval=0.3,
                max_interval=0.8,
            )
            b.start()
            behaviors.append(b)
        if role in (Role.SELLER, Role.BOTH):
            peer.register_handler(
                MessageType.SOLD_NOTIFY, _make_seller_log_handler(peer)
            )
            s = SellerBehavior(peer, min_interval=0.3, max_interval=0.8)
            s.start()
            behaviors.append(s)

    time.sleep(0.3)  # let accept loops come up before anyone sends ELECTION

    log.info("[main] triggering elections")
    for election in elections:  # have to start all bc the starting can be a failed node
        election.start_election()

    time.sleep(1.0)  # give Bully time to converge
    winner = peers[0].coordinator_id
    assert winner is not None, "election failed to converge"
    log.info(f"[main] coordinator converged to peer {winner}")

    log.info(f"[main] running for {duration}s")
    time.sleep(duration)

    for b in behaviors:
        b.stop()
    for trader in active_traders:
        trader.stop()
    for p in peers:
        p.stop()
    log.info("[main] all peers stopped")

    # The newest trader holds the live state; older ones are stale snapshots.
    final_trader = active_traders[-1] if active_traders else None
    if final_trader is not None:
        log.info(f"[main] final coordinator was peer {final_trader.peer.peer_id}")
        log.info(f"[main] final balances: {final_trader.balances}")
        inv_summary = ", ".join(
            f"{item.value}: {sum(e[1] for e in entries)}"
            for item, entries in final_trader.inventory.items()
        )
        log.info(f"[main] final inventory: {{{inv_summary}}}")
    if len(active_traders) > 1:
        log.info(f"[main] {len(active_traders)} traders held the role this run")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=6, help="number of peers")
    parser.add_argument("--duration", type=float, default=3.0, help="runtime seconds")
    args = parser.parse_args()
    run(args.n, args.duration)


if __name__ == "__main__":
    main()
