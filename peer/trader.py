import json
import os
import threading
from pathlib import Path

from config.constant import PRICES, TRADER_COMMISSION
from config.enums import BuyStatus, Item, MessageType
from peer.messages import Message
from peer.peer import Peer


STATE_PATH = Path("state/trader_state.json")


class TraderBehavior:
    """Runs the coordinator's trading-post logic: inventory, sales, payouts.

    The winner of the Bully election instantiates this and registers its
    handlers. All state mutations happen under `_lock` and are flushed to
    `state/trader_state.json` so a successor trader (Phase 7) can resume.

    Also keep track of money (of seller and itself)
    """

    def __init__(self, peer: Peer, state_path: Path = STATE_PATH) -> None:
        self.peer = peer
        self.state_path = state_path

        # FIFO queue of (seller_id, qty) per item — oldest deposit paid first.
        self.inventory: dict[Item, list[list[int]]] = {item: [] for item in Item}
        # Running credit per seller, already net of trader commission.
        self.balances: dict[int, float] = {}

        # Single coarse lock: cheap and eliminates every inventory/balance race.
        self._lock = threading.Lock()

    # Lifecycle

    def install(self) -> None:
        """Registers the trader handlers on the owning peer."""
        self.peer.register_handler(MessageType.SELL_DEPOSIT, self._handle_sell_deposit)
        self.peer.register_handler(MessageType.BUY, self._handle_buy)

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

    # Handlers

    def _handle_sell_deposit(self, msg: Message) -> None:
        """Accepts a seller's deposit and appends it to the item's FIFO queue."""
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
        """Tries to fulfill a buy request and responds to buyer + credited sellers.

        Fulfillment walks the FIFO queue for the item, drawing quantities
        from the oldest deposits first. If total stock is insufficient, the
        request is rejected with OUT_OF_STOCK and no state changes.
        """
        item = Item(msg.payload["item"])
        qty = int(msg.payload["qty"])
        buyer_id = msg.sender

        with self._lock:
            total_available = sum(entry[1] for entry in self.inventory[item])

            # handle out of stock
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
