import random
import threading
import time
from typing import Optional

from config.enums import Item, MessageType, Role
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
        self._enabled.set()
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
            self._enabled.wait()
            if not self._running:
                break
            time.sleep(random.uniform(self.min_interval, self.max_interval))
            if not self._running:
                break
            self._tick()

    def _tick(self) -> None:
        """Override: the one action to perform each interval."""
        raise NotImplementedError


class BuyerBehavior(_BehaviorLoop):
    """Periodically issues random buy requests to the current trader."""

    def __init__(self, peer: Peer, max_qty: int = 3, **kwargs) -> None:
        super().__init__(peer, name=f"peer-{peer.peer_id}-buyer", **kwargs)
        self.max_qty = max_qty

    def _tick(self) -> None:
        """Picks a random item + qty and sends one BUY to the trader.

        Skips silently if no trader has been elected yet.
        """
        if self.peer.coordinator_id is None:
            return
        item = random.choice(list(Item))
        qty = random.randint(1, self.max_qty)
        self.peer.unicast(
            self.peer.coordinator_id,
            MessageType.BUY,
            {"item": item.value, "qty": qty},
        )


class SellerBehavior(_BehaviorLoop):
    """Periodically deposits fresh stock of a random item with the trader."""

    def __init__(self, peer: Peer, max_qty: int = 5, **kwargs) -> None:
        super().__init__(peer, name=f"peer-{peer.peer_id}-seller", **kwargs)
        self.max_qty = max_qty

    def _tick(self) -> None:
        """Picks a random item + qty and sends one SELL_DEPOSIT to the trader.

        Skips silently if no trader has been elected yet. Price is not sent:
        it is fixed a priori and the trader looks it up from PRICES.
        """
        if self.peer.coordinator_id is None:
            return
        item = random.choice(list(Item))
        qty = random.randint(1, self.max_qty)
        self.peer.unicast(
            self.peer.coordinator_id,
            MessageType.SELL_DEPOSIT,
            {"item": item.value, "qty": qty},
        )


def assign_roles(n: int, rng: Optional[random.Random] = None) -> list[Role]:
    """Randomly assigns a role to each of the n peers.

    Guarantees at least one peer can buy (BUYER or BOTH) and at least one
    can sell (SELLER or BOTH), so the market is never degenerate. Pass a
    seeded `random.Random` for reproducible runs during debugging.
    """
    r = rng or random
    roles = [r.choice(list(Role)) for _ in range(n)]

    has_buyer = any(role in (Role.BUYER, Role.BOTH) for role in roles)
    if not has_buyer:
        idx = r.randrange(n)
        roles[idx] = Role.BOTH if roles[idx] == Role.SELLER else Role.BUYER

    has_seller = any(role in (Role.SELLER, Role.BOTH) for role in roles)
    if not has_seller:
        idx = r.randrange(n)
        roles[idx] = Role.BOTH if roles[idx] == Role.BUYER else Role.SELLER

    return roles
