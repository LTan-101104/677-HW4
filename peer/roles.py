import random
import threading
import time
from typing import Callable, Iterable, Optional

from config.constant import DEFAULT_STOCK
from config.enums import Item, MessageType, Role
from peer.messages import Message
from peer.peer import Peer


class _BehaviorLoop:
    """Base class: a pausable background thread that fires at random intervals."""

    def __init__(
        self,
        peer: Peer,
        name: str,
        min_interval: float = 0.5,
        max_interval: float = 2.0,
    ) -> None:
        self.peer = peer
        self.name = name
        self.min_interval = min_interval
        self.max_interval = max_interval

        # Pause mechanism: `_enabled.wait()` blocks the loop while cleared, so
        # suspending behavior (e.g., when elected trader) is a one-line call.
        self._enabled = threading.Event()
        self._enabled.set()  # pause flag
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Launches the behavior loop on its own daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name=self.name)
        self._thread.start()

    def stop(self) -> None:
        """Signals the loop to exit and unblocks it if currently paused."""
        self._running = False
        self._enabled.set()

    def pause(self) -> None:
        """Suspends the loop until `resume` is called (used when elected trader)."""
        self._enabled.clear()

    def resume(self) -> None:
        """Re-enables a paused loop."""
        self._enabled.set()

    def _loop(self) -> None:
        while self._running:
            self._enabled.wait()  # will block here if paused
            if not self._running:
                break
            time.sleep(random.uniform(self.min_interval, self.max_interval))
            if not self._running:
                break
            self._tick()  # sleep randomly and simulates the fire of event

    def _tick(self) -> None:
        """Override: the one action to perform each interval."""
        raise NotImplementedError


class BuyerBehavior(_BehaviorLoop):
    """Periodically multicasts random buy requests to trader + peer buyers.

    Phase 6: BUYs go to every live buyer so each one can ACK back to the
    trader. The trader uses those ACKs to deliver buys in Lamport
    `(ts, sender)` order, independent of TCP arrival.

    Phase 7: `other_buyer_ids` is a callable so the multicast targets stay
    current as the coordinator changes (a freshly resigned trader rejoins
    the buyer pool, the new trader leaves it).
    """

    def __init__(
        self,
        peer: Peer,
        other_buyer_ids: Callable[[], Iterable[int]] = lambda: (),
        max_qty: int = 3,
        **kwargs,
    ) -> None:
        super().__init__(peer, name=f"peer-{peer.peer_id}-buyer", **kwargs)
        self.max_qty = max_qty
        self.other_buyer_ids = other_buyer_ids
        # Receiver-side responsibility: when a peer buyer multicasts a BUY to
        # us, we ACK it to the current trader. Installed only on peers that
        # actually run BuyerBehavior, so seller-only peers don't ACK BUYs
        # they were never multicasted. On a peer that becomes trader,
        # TraderBehavior.install() overrides this — and restores it on resign.
        peer.register_handler(MessageType.BUY, self._forward_ack)

    def _tick(self) -> None:
        """Picks a random item + qty and multicasts one BUY.

        Targets: current trader + every other live buyer. Skips if no
        trader has been elected yet, or if this peer is itself the trader.
        Targets are de-duplicated in case the provider lists the coordinator.
        """
        if self.peer.coordinator_id in (None, self.peer.peer_id):
            return
        item = random.choice(list(Item))
        qty = random.randint(1, self.max_qty)
        targets = list({self.peer.coordinator_id, *self.other_buyer_ids()})
        self.peer.multicast(
            MessageType.BUY,
            {"item": item.value, "qty": qty},
            targets=targets,
        )

    def _forward_ack(self, msg: Message) -> None:
        """ACKs a received BUY back to the current trader.

        Harmless if no coordinator is known yet — we just drop the ACK; the
        trader couldn't have enqueued the BUY either.
        """
        if self.peer.coordinator_id is None:
            return
        self.peer.unicast(
            self.peer.coordinator_id,
            MessageType.ACK,
            {"ack_for_ts": msg.ts, "ack_for_sender": msg.sender},
        )


class SellerBehavior(_BehaviorLoop):
    """Drip-feeds one item type to the trader, then restocks to a new item.

    Each seller carries one item + a local stock counter. On every tick it
    deposits a batch with the trader and decrements that counter. When the
    counter hits zero, it picks a fresh random item and resets the counter
    to `initial_stock` — modeling a seller whose warehouse is finite but
    periodically restocked with whatever is in season.
    """

    def __init__(
        self,
        peer: Peer,
        initial_stock: int = DEFAULT_STOCK,
        max_batch: int = 1,
        **kwargs,
    ) -> None:
        super().__init__(peer, name=f"peer-{peer.peer_id}-seller", **kwargs)
        self.initial_stock = initial_stock
        self.max_batch = max_batch
        self.current_item: Item = random.choice(list(Item))
        self.current_stock: int = initial_stock

    def _tick(self) -> None:
        """Deposits a batch of the current item and restocks when empty.

        Skips silently if no trader has been elected yet, or if this peer
        is currently the trader. Price is not sent: it is fixed a priori
        and the trader looks it up from PRICES.
        """
        if self.peer.coordinator_id in (None, self.peer.peer_id):
            return

        if self.current_stock <= 0:
            self.current_item = random.choice(list(Item))
            self.current_stock = self.initial_stock
            print(
                f"[peer={self.peer.peer_id} seller] restocked "
                f"-> {self.current_item.value} x{self.current_stock}"
            )

        qty = min(
            random.randint(1, self.max_batch), self.current_stock
        )  # for handling when we want to transfer qty > 1
        self.current_stock -= qty
        self.peer.unicast(
            self.peer.coordinator_id,
            MessageType.SELL_DEPOSIT,
            {"item": self.current_item.value, "qty": qty},
        )


def assign_roles(n: int, rng: Optional[random.Random] = None) -> list[Role]:
    """Randomly assigns a role to each of the n peers.

    Guarantees at least one peer can buy (BUYER or BOTH) and at least one
    can sell (SELLER or BOTH), so the market is never degenerate. Pass a
    seeded `random.Random` for reproducible runs during debugging.
    """
    r = rng or random
    roles = [r.choice(list(Role)) for _ in range(n)]

    # If no buyer exists, every role must be SELLER — promote one to BOTH.
    if not any(role in (Role.BUYER, Role.BOTH) for role in roles):
        roles[r.randrange(n)] = Role.BUYER

    # Symmetric case: no seller means every role is BUYER.
    if not any(role in (Role.SELLER, Role.BOTH) for role in roles):
        roles[r.randrange(n)] = Role.SELLER

    return roles
