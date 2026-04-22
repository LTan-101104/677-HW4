import heapq
import json
import os
import random
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

from config.constant import PRICES, TRADER_COMMISSION
from config.enums import BuyStatus, Item, MessageType
from peer.messages import Message
from peer.peer import Peer


STATE_PATH = Path("state/trader_state.json")


@dataclass
class _PendingBuy:
    """One BUY in the priority queue waiting for ACKs.

    The slot is created lazily — on BUY arrival *or* on an out-of-order ACK
    arrival — and filled in as the remaining pieces come in. `payload` stays
    None until the BUY itself lands, which is how `_head_ready` knows not
    to deliver an ACK-only slot.
    """

    payload: Optional[dict] = None
    buyer_id: Optional[int] = None
    ack_set: set[int] = field(default_factory=set)
    in_heap: bool = False


class TraderBehavior:
    """Runs the coordinator's trading-post logic: inventory, sales, payouts.

    BUY messages enter a `(ts, sender)` priority queue and are only delivered
    once every live buyer has ACKed — giving all peers the same total order
    regardless of TCP arrival order. SELL_DEPOSIT is processed in arrival
    order (the spec only asks for ordering across buys).
    """

    def __init__(
        self,
        peer: Peer,
        buyer_ids: Iterable[int] = (),
        state_path: Path = STATE_PATH,
        resign_after: Optional[tuple[float, float]] = None,
        on_resign: Optional[Callable[[], None]] = None,
    ) -> None:
        self.peer = peer
        self.state_path = state_path

        # FIFO queue of (seller_id, qty) per item — oldest deposit paid first.
        self.inventory: dict[Item, list[list[int]]] = {item: [] for item in Item}
        # Running credit per seller, already net of trader commission.
        self.balances: dict[int, float] = {}

        # Guards inventory + balances. The delivery queue has its own condition.
        self._lock = threading.Lock()

        # Phase 6: timestamp-ordered delivery of BUYs.
        # `buyer_ids` is the set of peers we expect ACKs from. Treated as
        # static for the lifetime of *this* trader instance; on resignation,
        # the next trader is constructed with a freshly computed set.
        self.buyer_ids: set[int] = set(buyer_ids)
        self._pending: dict[tuple[int, int], _PendingBuy] = {}
        self._heap: list[tuple[int, int]] = []
        self._deliver_cond = threading.Condition()

        self._worker_running = False
        self._worker_thread: Optional[threading.Thread] = None

        # Phase 7: resignation. `resign_after=(min, max)` schedules a one-shot
        # resign at a random delay in that range; None disables it. `on_resign`
        # runs after the RESIGN broadcast and before handlers are restored —
        # main.py uses it to yield this peer's election manager.
        self.resign_after = resign_after
        self.on_resign = on_resign
        self._resign_timer: Optional[threading.Timer] = None
        self._resigned = False

        # Snapshot of handlers we override at install(); restored on resign.
        self._prev_handlers: dict[MessageType, Optional[Callable[[Message], None]]] = {}

    # Lifecycle

    def install(self) -> None:
        """Registers trader handlers and starts the ordered-delivery worker.

        Snapshots whatever was previously bound to BUY / ACK / SELL_DEPOSIT
        so `_resign` can put them back. On a fresh peer those slots are
        empty; on a peer that was acting as a buyer (BuyerBehavior already
        installed `_forward_ack`), the snapshot lets us hand the buyer role
        back cleanly when we step down.
        """
        for mt in (MessageType.SELL_DEPOSIT, MessageType.BUY, MessageType.ACK):
            self._prev_handlers[mt] = self.peer.get_handler(mt)
        self.peer.register_handler(MessageType.SELL_DEPOSIT, self._handle_sell_deposit)
        self.peer.register_handler(MessageType.BUY, self._handle_buy)
        self.peer.register_handler(MessageType.ACK, self._handle_ack)
        self._start_worker()
        self._schedule_resignation()

    def stop(self) -> None:
        """Signals the delivery worker to exit on its next wake-up."""
        with self._deliver_cond:
            self._worker_running = False
            self._deliver_cond.notify_all()
        if self._resign_timer is not None:
            self._resign_timer.cancel()

    # Phase 7: resignation

    def _schedule_resignation(self) -> None:
        """Arms the one-shot timer that fires `_resign` after a random delay."""
        if self.resign_after is None:
            return
        delay = random.uniform(*self.resign_after)
        self._resign_timer = threading.Timer(delay, self._resign)
        self._resign_timer.daemon = True
        self._resign_timer.start()

    def _resign(self) -> None:
        """Steps down: final checkpoint, RESIGN broadcast, restore handlers.

        In-flight BUYs sitting in the priority queue at this moment are
        dropped — they reached the old trader but never satisfied their ACK
        set, and the new trader has no record of them. The originating
        buyers will simply not receive a BUY_RESP for those requests; this
        is a known limitation (see Phase 7 in CLAUDE.md).
        """
        if self._resigned:
            return
        self._resigned = True
        print(f"[trader={self.peer.peer_id}] resigning")

        # Final checkpoint so the successor has the latest state on load.
        with self._lock:
            self._checkpoint_locked()

        # Stop accepting new work: drain worker, then put back original
        # handlers (BuyerBehavior's _forward_ack will reappear if it was
        # there pre-install; otherwise BUY/ACK/SELL_DEPOSIT stay unbound).
        self.stop()
        for mt, prev in self._prev_handlers.items():
            if prev is None:
                self.peer.unregister_handler(mt)
            else:
                self.peer.register_handler(mt, prev)

        # Clear our own pointer so role-behaviors don't race the new election.
        self.peer.coordinator_id = None

        # Yield BEFORE broadcasting: as soon as RESIGN goes out, other peers
        # start ELECTION rounds. We must already be yielded by the time any
        # ELECTION reaches us, otherwise we'd reply OK and win again.
        if self.on_resign is not None:
            try:
                self.on_resign()
            except Exception as e:
                print(f"[trader={self.peer.peer_id}] on_resign callback error: {e}")

        # Tell everyone we're stepping down. Other peers' RESIGN handlers
        # clear their coordinator_id and trigger a jittered new election.
        try:
            self.peer.multicast(MessageType.RESIGN)
        except OSError as e:
            print(f"[trader={self.peer.peer_id}] RESIGN broadcast error: {e}")

    def load_state(self) -> bool:
        """Restores inventory + balances from the checkpoint file, if present.

        Returns True on successful load, False when no checkpoint exists yet.
        Used by a new trader after takeover so outstanding stock and accrued
        seller balances survive the handover.
        """
        if not self.state_path.exists():
            return False
        with self.state_path.open("r") as f:
            data = json.load(f)
        with self._lock:
            self.inventory = {
                Item(k): [list(entry) for entry in v]
                for k, v in data.get("inventory", {}).items()
            }
            for item in Item:
                self.inventory.setdefault(item, [])
            self.balances = {
                int(k): float(v) for k, v in data.get("balances", {}).items()
            }
        return True

    # Incoming message handlers

    def _handle_sell_deposit(self, msg: Message) -> None:
        """Appends a seller's deposit to the item's FIFO queue."""
        item = Item(msg.payload["item"])
        qty = int(msg.payload["qty"])
        with self._lock:
            self.inventory[item].append([msg.sender, qty])
            self._checkpoint_locked()
        print(
            f"[trader={self.peer.peer_id}] deposit from seller {msg.sender}: "
            f"{item.value} x{qty}"
        )

    def _handle_buy(self, msg: Message) -> None:
        """Enqueues a BUY for ordered delivery.

        The sender is treated as having auto-ACKed: the BUY carries their
        own timestamp, so we already know they've observed it. Only the
        other live buyers still owe us ACKs.
        """
        key = (msg.ts, msg.sender)
        with self._deliver_cond:
            entry = self._pending.get(key)
            if entry is None:
                entry = _PendingBuy()
                self._pending[key] = entry
            if entry.payload is not None:
                return  # duplicate BUY — first one wins
            entry.payload = dict(msg.payload)
            entry.buyer_id = msg.sender
            entry.ack_set.add(msg.sender)
            if not entry.in_heap:
                heapq.heappush(self._heap, key)
                entry.in_heap = True
            self._deliver_cond.notify_all()

    def _handle_ack(self, msg: Message) -> None:
        """Records an ACK against a pending BUY.

        ACKs can arrive before the BUY itself (per-message TCP connections
        break strict FIFO across channels), so we lazily create the slot
        and merge once the BUY lands.
        """
        try:
            ack_for_ts = int(msg.payload["ack_for_ts"])
            ack_for_sender = int(msg.payload["ack_for_sender"])
        except (KeyError, TypeError, ValueError):
            return
        key = (ack_for_ts, ack_for_sender)
        with self._deliver_cond:
            entry = self._pending.get(key)
            if entry is None:
                entry = _PendingBuy()
                self._pending[key] = entry
            entry.ack_set.add(msg.sender)
            self._deliver_cond.notify_all()

    # Delivery worker

    def _start_worker(self) -> None:
        if self._worker_running:
            return
        self._worker_running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name=f"trader-{self.peer.peer_id}-deliver",
        )
        self._worker_thread.start()

    def _worker_loop(self) -> None:
        """Pops BUYs in (ts, sender) order as their ACK sets complete."""
        while True:
            with self._deliver_cond:
                while self._worker_running and not self._head_ready():
                    self._deliver_cond.wait(timeout=1.0)
                if not self._worker_running:
                    return
                key = heapq.heappop(self._heap)
                entry = self._pending.pop(key)
            # Release the queue lock before processing: inventory work holds
            # its own lock, and new BUY/ACK intake should keep flowing.
            assert entry.buyer_id is not None and entry.payload is not None
            self._process_buy(entry.buyer_id, entry.payload)

    def _head_ready(self) -> bool:
        """True when the head has its BUY payload AND all live buyers have ACKed."""
        if not self._heap:
            return False
        key = self._heap[0]
        entry = self._pending[key]
        if entry.payload is None:
            return False
        return self.buyer_ids <= entry.ack_set

    # Core buy fulfillment (formerly _handle_buy in Phase 5)

    def _process_buy(self, buyer_id: int, payload: dict) -> None:
        """Applies a BUY against FIFO inventory; replies to buyer + credited sellers.

        Fulfillment walks the FIFO queue for the item, drawing quantities
        from the oldest deposits first. If total stock is insufficient, the
        request is rejected with OUT_OF_STOCK and no state changes.
        """
        item = Item(payload["item"])
        qty = int(payload["qty"])

        with self._lock:
            total_available = sum(entry[1] for entry in self.inventory[item])

            if total_available < qty:
                self.peer.unicast(
                    buyer_id,
                    MessageType.BUY_RESP,
                    {
                        "item": item.value,
                        "qty": qty,
                        "status": BuyStatus.OUT_OF_STOCK.value,
                    },
                )
                print(
                    f"[trader={self.peer.peer_id}] BUY {item.value} x{qty} "
                    f"from {buyer_id} -> OUT_OF_STOCK"
                )
                return

            # Drain from the FIFO queue until the buyer's qty is satisfied.
            remaining = qty
            payouts: list[tuple[int, int]] = []  # (seller_id, qty_sold)
            price = PRICES[item]
            while remaining > 0:
                seller_id, have = self.inventory[item][0]
                take = min(have, remaining)
                remaining -= take
                have -= take
                if have == 0:
                    self.inventory[item].pop(0)
                else:
                    self.inventory[item][0][1] = have
                credit = price * take * (1 - TRADER_COMMISSION)
                self.balances[seller_id] = self.balances.get(seller_id, 0.0) + credit
                payouts.append((seller_id, take))

            self._checkpoint_locked()

        # Notifications go outside the lock — they make network calls and we
        # don't want a slow syscall to stall concurrent deposits/buys.
        self.peer.unicast(
            buyer_id,
            MessageType.BUY_RESP,
            {"item": item.value, "qty": qty, "status": BuyStatus.SUCCESS.value},
        )
        for seller_id, sold_qty in payouts:
            self.peer.unicast(
                seller_id,
                MessageType.SOLD_NOTIFY,
                {"item": item.value, "qty": sold_qty},
            )
        print(
            f"[trader={self.peer.peer_id}] BUY {item.value} x{qty} "
            f"from {buyer_id} -> SUCCESS (payouts={payouts})"
        )

    # Persistence

    def _checkpoint_locked(self) -> None:
        """Atomically flushes current state to the JSON file. Caller holds `_lock`.

        Writes to a sibling `.tmp` file first and then `os.replace`s it into
        place so a crash mid-write can never leave a half-written file.
        """
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        payload = {
            "inventory": {
                item.value: entries for item, entries in self.inventory.items()
            },
            "balances": {str(sid): bal for sid, bal in self.balances.items()},
        }
        with tmp.open("w") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, self.state_path)
