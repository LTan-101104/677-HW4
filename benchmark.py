"""Phase 9 performance evaluation harness.

Fires `--requests` sequential BUYs from one designated measured buyer and
records per-request round-trip time to a CSV. Three scenarios:

    baseline     — 1 measured buyer, 2 sellers. Plenty of stock, no contention,
                   no resignation. Establishes the "happy-path" RTT baseline.
    contention   — measured buyer + K background buyers issuing random BUYs
                   (via the normal BuyerBehavior loop). Tests the fair-ordering
                   queue under load; RTT includes ACK-set completion waits.
    resignation  — 1 measured buyer. An external timer forces the live trader
                   to resign mid-run so the trace captures the handover spike.

CSV columns:
    seq, t_send_rel, t_recv_rel, rtt_ms, status, item, qty

Usage:
    python benchmark.py --scenario baseline --requests 1000
    python benchmark.py --scenario contention --requests 1000 --bg-buyers 3
    python benchmark.py --scenario resignation --requests 1000

Known limitation: BUY_RESP doesn't carry the original BUY's `(ts, sender)`,
so if a response arrives *after* its request's per-request timeout fires,
the harness can't reject it — the late response would be attributed to the
next outstanding request. In practice this does not happen: RESIGN drops
any BUYs queued on the resigning trader, and BUYs multicast during the
handover window target the dead trader, so no late BUY_RESP ever arrives.
"""
import argparse
import csv
import random
import statistics
import threading
import time
from pathlib import Path
from typing import Callable, Optional

from config.constant import RESIGN_YIELD
from config.enums import Item, MessageType, Role
from config.peer_registry import PeerRegistry
from peer.election import ElectionManager
from peer.logger import log
from peer.messages import Message
from peer.peer import Peer
from peer.roles import BuyerBehavior, SellerBehavior
from peer.trader import TraderBehavior


PEERS_JSON = Path("config/peers.json")
STATE_PATH = Path("state/trader_state.json")
RESULTS_DIR = Path("bench_results")

# Per-request deadline for the measured buyer. 3s is loose enough to absorb
# a mid-run handover (RESIGN -> new election -> install ~ 1s) without
# treating steady-state successes as timeouts.
PER_REQUEST_TIMEOUT = 3.0

# Time after election convergence before we start measuring — lets sellers
# seed the inventory so early requests aren't uniformly OUT_OF_STOCK.
WARMUP_SECONDS = 1.5

# How long we wait for Bully to pick a coordinator after starting elections.
ELECTION_CONVERGE_DELAY = 1.0

# Tight seller cadence — keep the FIFO queue topped up under a fast buyer.
FAST_SELLER_INTERVAL = (0.01, 0.025)

# Resignation scenario: how long after warmup we force the first trader down.
# At ~1ms/request, 1000 requests finish in ~1s, so 0.3s lands roughly 1/3
# of the way through a 1000-request run.
RESIGN_DELAY_AFTER_WARMUP = 0.3


class BenchmarkBuyer:
    """Synchronous one-shot buyer — fires a single BUY, blocks on BUY_RESP.

    No random tick loop (unlike `BuyerBehavior`), so the measured RTT
    reflects exactly one request's path through the system. Still installs
    the BUY ACK-forwarder so this peer remains a good citizen in the
    multicast Lamport protocol when contention scenarios multicast to it.
    """

    def __init__(
        self,
        peer: Peer,
        other_buyer_ids: Callable[[], list[int]] = lambda: [],
    ) -> None:
        self.peer = peer
        self.other_buyer_ids = other_buyer_ids
        self._response_event = threading.Event()
        self._last_status: Optional[str] = None
        self._last_item: Optional[str] = None
        self._last_qty: Optional[int] = None
        peer.register_handler(MessageType.BUY, self._forward_ack)
        peer.register_handler(MessageType.BUY_RESP, self._on_buy_resp)

    def _forward_ack(self, msg: Message) -> None:
        if self.peer.coordinator_id is None:
            return
        self.peer.unicast(
            self.peer.coordinator_id,
            MessageType.ACK,
            {"ack_for_ts": msg.ts, "ack_for_sender": msg.sender},
        )

    def _on_buy_resp(self, msg: Message) -> None:
        self._last_status = msg.payload.get("status")
        self._last_item = msg.payload.get("item")
        self._last_qty = msg.payload.get("qty")
        self._response_event.set()

    def fire(
        self, item: Item, qty: int, timeout: float
    ) -> tuple[Optional[float], str, str, int]:
        """Fires one BUY; returns (rtt_seconds or None, status, item, qty).

        `None` RTT means the request timed out or no trader was known.
        """
        self._response_event.clear()
        if self.peer.coordinator_id is None:
            return (None, "NO_TRADER", item.value, qty)
        targets = list({self.peer.coordinator_id, *self.other_buyer_ids()})
        t_send = time.perf_counter()
        self.peer.multicast(
            MessageType.BUY,
            {"item": item.value, "qty": qty},
            targets=targets,
        )
        if self._response_event.wait(timeout):
            rtt = time.perf_counter() - t_send
            return (rtt, self._last_status or "NONE", item.value, qty)
        return (None, "TIMEOUT", item.value, qty)


def _build(
    n: int,
    roles: list[Role],
    *,
    first_trader_auto_resigns: bool = False,
    resign_after: Optional[tuple[float, float]] = None,
) -> tuple[list[Peer], list[ElectionManager], list[TraderBehavior]]:
    """Spawns `n` peers, wires election + install-trader hook, starts each peer.

    `first_trader_auto_resigns` arms an internal resign timer on the first
    elected trader only; set False when the caller wants to trigger resign
    externally (the resignation scenario does this for deterministic timing).
    """
    registry = PeerRegistry.build(n)
    registry.save(PEERS_JSON)
    peers = [Peer(pid, registry) for pid in range(n)]
    elections = [ElectionManager(p) for p in peers]
    active_traders: list[TraderBehavior] = []

    def make_install(peer: Peer, election: ElectionManager):
        def install() -> None:
            buyer_ids = [
                pid
                for pid in range(n)
                if pid != peer.peer_id and roles[pid] in (Role.BUYER, Role.BOTH)
            ]
            will_auto_resign = (
                len(active_traders) == 0 and first_trader_auto_resigns
            )
            trader = TraderBehavior(
                peer,
                buyer_ids=buyer_ids,
                resign_after=resign_after if will_auto_resign else None,
                # Always provide on_resign so external _resign() calls also
                # yield this peer's election manager — otherwise Bully just
                # re-elects it.
                on_resign=lambda: election.yield_for(RESIGN_YIELD),
            )
            trader.load_state()
            trader.install()
            active_traders.append(trader)

        return install

    for peer, election in zip(peers, elections):
        election.on_become_coordinator = make_install(peer, election)
        election.wire_handlers()
        peer.start()

    return peers, elections, active_traders


def _converge_election(elections: list[ElectionManager]) -> None:
    """Kicks elections on every peer and waits for Bully to converge."""
    for election in elections:
        election.start_election()
    time.sleep(ELECTION_CONVERGE_DELAY)


def _start_sellers(
    peers: list[Peer], roles: list[Role], max_batch: int = 3
) -> list[SellerBehavior]:
    """Starts a tight-cadence SellerBehavior on every seller peer.

    Also installs a no-op SOLD_NOTIFY handler so the peer's fallback
    "no handler" log line doesn't fire on every credit — the benchmark
    doesn't care about the per-sale seller log.
    """
    sellers: list[SellerBehavior] = []
    for pid in range(len(peers)):
        if roles[pid] not in (Role.SELLER, Role.BOTH):
            continue
        peers[pid].register_handler(MessageType.SOLD_NOTIFY, lambda _msg: None)
        s = SellerBehavior(
            peers[pid],
            min_interval=FAST_SELLER_INTERVAL[0],
            max_interval=FAST_SELLER_INTERVAL[1],
            max_batch=max_batch,
        )
        s.start()
        sellers.append(s)
    return sellers


def _preseed_inventory(
    peers: list[Peer],
    roles: list[Role],
    coordinator_id: int,
    per_item_per_seller: int = 2000,
) -> None:
    """Fires real SELL_DEPOSITs from each seller so the trader starts with stock.

    Needed because a fast measured buyer can outpace SellerBehavior's tick
    cadence — without pre-seeding, the baseline CSV is dominated by
    OUT_OF_STOCK rows rather than the SUCCESS round-trip we're trying to
    characterize. Uses the normal SELL_DEPOSIT path so the trader's FIFO
    and checkpoint stay consistent.
    """
    for pid in range(len(peers)):
        if roles[pid] not in (Role.SELLER, Role.BOTH):
            continue
        for item in Item:
            peers[pid].unicast(
                coordinator_id,
                MessageType.SELL_DEPOSIT,
                {"item": item.value, "qty": per_item_per_seller},
            )
    time.sleep(0.3)  # let deposits land + checkpoint


def _shutdown(behaviors: list, traders: list[TraderBehavior], peers: list[Peer]) -> None:
    for b in behaviors:
        b.stop()
    for t in traders:
        t.stop()
    for p in peers:
        p.stop()


# Scenarios

def run_baseline(requests: int) -> list[tuple]:
    """Stable trader, 2 sellers, 1 measured buyer — happy-path RTT."""
    n = 3
    roles = [Role.BUYER, Role.SELLER, Role.SELLER]
    measured_pid = 0

    peers, elections, active_traders = _build(n, roles)
    measured = BenchmarkBuyer(peers[measured_pid])
    sellers = _start_sellers(peers, roles)

    _converge_election(elections)
    coordinator_id = peers[measured_pid].coordinator_id
    assert coordinator_id is not None, "baseline: election failed to converge"
    _preseed_inventory(peers, roles, coordinator_id)
    time.sleep(WARMUP_SECONDS)

    rows = _run_requests(measured, requests)

    _shutdown(sellers, active_traders, peers)
    return rows


def run_contention(requests: int, bg_buyers: int) -> list[tuple]:
    """Measured buyer + K background buyers racing shared inventory."""
    n = 1 + bg_buyers + 2
    roles: list[Role] = (
        [Role.BUYER] * (1 + bg_buyers) + [Role.SELLER, Role.SELLER]
    )
    measured_pid = 0

    peers, elections, active_traders = _build(n, roles)

    # Measured buyer's multicast targets = trader + every other buyer peer.
    other_buyers_static = [pid for pid in range(1, 1 + bg_buyers)]
    measured = BenchmarkBuyer(
        peers[measured_pid],
        other_buyer_ids=lambda: other_buyers_static,
    )

    # Background buyers: normal BuyerBehavior, tight cadence to create load.
    bg_behaviors: list[BuyerBehavior] = []
    for pid in range(1, 1 + bg_buyers):
        this_pid = pid

        def other_fn(this_pid=this_pid) -> list[int]:
            trader = peers[this_pid].coordinator_id
            return [
                b
                for b in range(1 + bg_buyers)
                if b != this_pid and b != trader
            ]

        b = BuyerBehavior(
            peers[pid],
            other_buyer_ids=other_fn,
            min_interval=0.05,
            max_interval=0.15,
        )
        b.start()
        bg_behaviors.append(b)

    sellers = _start_sellers(peers, roles, max_batch=2)

    _converge_election(elections)
    coordinator_id = peers[measured_pid].coordinator_id
    assert coordinator_id is not None, "contention: election failed to converge"
    _preseed_inventory(peers, roles, coordinator_id)
    time.sleep(WARMUP_SECONDS)

    rows = _run_requests(measured, requests)

    _shutdown(bg_behaviors + sellers, active_traders, peers)
    return rows


def run_resignation(requests: int) -> list[tuple]:
    """Trader resigns mid-run; trace captures the handover latency spike."""
    n = 4
    roles = [Role.BUYER, Role.SELLER, Role.SELLER, Role.SELLER]
    measured_pid = 0

    # first_trader_auto_resigns=False — we fire the resign ourselves from
    # the benchmark side so the timing is deterministic relative to the
    # measurement loop rather than to trader install.
    peers, elections, active_traders = _build(n, roles)

    measured = BenchmarkBuyer(peers[measured_pid])
    sellers = _start_sellers(peers, roles)

    _converge_election(elections)
    coordinator_id = peers[measured_pid].coordinator_id
    assert coordinator_id is not None, "resignation: election failed to converge"
    _preseed_inventory(peers, roles, coordinator_id)
    time.sleep(WARMUP_SECONDS)

    # Force the live trader to resign at a known offset into measurement.
    def do_resign() -> None:
        if not active_traders:
            return
        first_trader = active_traders[0]
        if not first_trader._resigned:
            first_trader._resign()

    resign_timer = threading.Timer(RESIGN_DELAY_AFTER_WARMUP, do_resign)
    resign_timer.daemon = True
    resign_timer.start()

    rows = _run_requests(measured, requests)

    resign_timer.cancel()
    _shutdown(sellers, active_traders, peers)
    return rows


# Request loop

def _run_requests(
    measured: BenchmarkBuyer, requests: int
) -> list[tuple]:
    """Fires `requests` BUYs sequentially; returns CSV rows."""
    rows: list[tuple] = []
    t0 = time.perf_counter()
    for seq in range(requests):
        item = random.choice(list(Item))
        qty = random.randint(1, 3)
        t_send_rel = time.perf_counter() - t0
        rtt, status, item_val, qty_val = measured.fire(
            item, qty, PER_REQUEST_TIMEOUT
        )
        if rtt is None:
            rows.append(
                (seq, f"{t_send_rel:.6f}", "", "", status, item_val, qty_val)
            )
        else:
            t_recv_rel = t_send_rel + rtt
            rows.append(
                (
                    seq,
                    f"{t_send_rel:.6f}",
                    f"{t_recv_rel:.6f}",
                    f"{rtt * 1000:.3f}",
                    status,
                    item_val,
                    qty_val,
                )
            )
    return rows


# CSV + summary

def _write_csv(path: Path, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["seq", "t_send_rel", "t_recv_rel", "rtt_ms", "status", "item", "qty"]
        )
        writer.writerows(rows)


def _summarize(rows: list[tuple]) -> str:
    success_rtts = [float(r[3]) for r in rows if r[4] == "SUCCESS"]
    oos = sum(1 for r in rows if r[4] == "OUT_OF_STOCK")
    timeouts = sum(1 for r in rows if r[4] == "TIMEOUT")
    no_trader = sum(1 for r in rows if r[4] == "NO_TRADER")
    lines = [
        f"  n={len(rows)} SUCCESS={len(success_rtts)} "
        f"OUT_OF_STOCK={oos} TIMEOUT={timeouts} NO_TRADER={no_trader}"
    ]
    if success_rtts:
        s = sorted(success_rtts)
        mean = statistics.mean(s)
        median = statistics.median(s)
        p95 = s[int(len(s) * 0.95)]
        p99 = s[int(len(s) * 0.99)]
        lines.append(
            f"  SUCCESS RTT (ms): mean={mean:.2f} median={median:.2f} "
            f"p95={p95:.2f} p99={p99:.2f} min={s[0]:.2f} max={s[-1]:.2f}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=["baseline", "contention", "resignation"],
        required=True,
    )
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument(
        "--bg-buyers",
        type=int,
        default=2,
        help="contention scenario only: number of background buyers",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="CSV output path (default: bench_results/<scenario>.csv)",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="delete state/trader_state.json before running",
    )
    args = parser.parse_args()

    if args.reset_state and STATE_PATH.exists():
        STATE_PATH.unlink()

    print(
        f"[bench] scenario={args.scenario} requests={args.requests}"
        + (f" bg_buyers={args.bg_buyers}" if args.scenario == "contention" else "")
    )

    # Route verbose per-request log lines to a file so stdout is just the
    # summary — easier to read when running all three scenarios in sequence.
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RESULTS_DIR / f"{args.scenario}.log"
    log.set_output_file(log_path)
    try:
        if args.scenario == "baseline":
            rows = run_baseline(args.requests)
        elif args.scenario == "contention":
            rows = run_contention(args.requests, args.bg_buyers)
        else:
            rows = run_resignation(args.requests)
    finally:
        log.close()

    out = args.out or (RESULTS_DIR / f"{args.scenario}.csv")
    _write_csv(out, rows)
    print(f"[bench] wrote {len(rows)} rows -> {out}")
    print(f"[bench] full log at {log_path}")
    print(_summarize(rows))


if __name__ == "__main__":
    main()
