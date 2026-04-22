# HW4 — Asterix and the Trading Post, Part 2

A distributed peer-to-peer trading post. `N` peers run as concurrent processes
over local TCP. Each peer is assigned a role (`buyer`, `seller`, or `both`),
and one peer at a time acts as the **trader** (trading post coordinator),
elected by the **Bully** algorithm. Buyers multicast `BUY` requests;
the trader orders them fairly using **multicast Lamport clocks with ACKs**,
then fulfills from a FIFO inventory, credits the originating sellers (less
a 10% commission), and persists its state to JSON on every change so a
successor trader can resume after a handover.

## What's implemented

- **Bully leader election** (`peer/election.py`) — highest PID wins, with
  OK / COORDINATOR timeouts and a `yield_for` cooldown used by a freshly
  resigned trader so the next round picks a different peer.
- **Lamport clocks** (`peer/clock.py`) — thread-safe, `tick` and
  `tick_receive` applied on every send/recv.
- **Total-ordered multicast** — BUYs are multicast to the trader + every
  other live buyer. Each receiver ACKs to the trader, who delivers from a
  `(ts, sender)` min-heap only once every live buyer has ACKed the head.
- **Trader logic** (`peer/trader.py`) — FIFO per-item inventory, per-seller
  running balances, atomic JSON checkpoint (`state/trader_state.json`) on
  every mutation, and `load_state()` for successor resume.
- **Orderly resignation + handover** — first elected trader resigns after
  a random delay (RESIGN broadcast → new election → successor loads
  checkpoint → announces as coordinator). Subsequent traders run to
  completion to avoid Bully bouncing the role between the same two peers.
- **Timestamped logging** (`peer/logger.py`) — every line prefixed with
  `DD.MM.YYYY HH:MM:SS.MS`; spec-required loglines exposed as `log.coordinator`,
  `log.bought`, `log.buy_result`, plus `sold`, `deposit`, `payout`, `info`.

## Requirements

- Python 3.9+ (uses PEP 585 builtin generics like `list[int]`, `dict[...]`).
- Core system: standard library only — no third-party dependencies.
- Performance study (optional): `matplotlib>=3.8` for `plot_benchmark.py`.

## How to run

From this directory (`677-HW4/`):

```
python main.py --n 6 --duration 8
```

### Arguments

| flag         | default | meaning                                  |
|--------------|---------|------------------------------------------|
| `--n`        | 6       | number of peers to spawn                 |
| `--duration` | 3.0     | how long (seconds) to run the workload   |

### Capturing a run to a file

```
python main.py --n 6 --duration 8 > output.txt 2>&1
```

### Configuration knobs (in `main.py`)

- `FIRST_TRADER_RESIGNS = True` — set `False` to have the first elected
  trader run to completion (no handover demo).
- `RESIGN_AFTER = (2.0, 4.0)` — random delay range (seconds) before the
  first trader resigns.
- `RESIGN_YIELD = 3.0` — seconds the resigned trader sits out of the next
  election.

## Performance study

A separate harness fires 1000 sequential BUYs from one designated measured
buyer and records per-request round-trip time to CSV. Three scenarios:

| scenario      | what it measures                                           |
|---------------|------------------------------------------------------------|
| `baseline`    | steady-state RTT — 1 buyer, 2 sellers, plenty of stock     |
| `contention`  | ACK-set wait cost — measured buyer + `K` background buyers |
| `resignation` | handover latency — trader forced to resign mid-run         |

Run each scenario (CSVs land in `bench_results/`, full log in
`bench_results/<scenario>.log`, summary stats printed to stdout):

```
python benchmark.py --scenario baseline    --requests 1000 --reset-state
python benchmark.py --scenario contention  --requests 1000 --bg-buyers 2 --reset-state
python benchmark.py --scenario resignation --requests 1000 --reset-state
```

Then render the plots (PNGs land in `plots/`):

```
python plot_benchmark.py
```

This produces:

- `plots/latency_timeline.png` — per-request RTT over time, three stacked
  panels (TIMEOUTs marked as red triangles).
- `plots/latency_distribution.png` — SUCCESS-RTT CDF overlay + boxplot
  across all three scenarios, both on log scale.

## File layout

```
677-HW4/
├── main.py                  # Orchestrator: build registry, wire peers, run workload
├── benchmark.py             # Perf harness: 1000 sequential BUYs across 3 scenarios
├── plot_benchmark.py        # Reads bench_results/*.csv, writes plots/*.png
├── config/
│   ├── constant.py          # PRICES, TRADER_COMMISSION, DEFAULT_STOCK, resign knobs
│   ├── enums.py             # Role, MessageType, BuyStatus, Item
│   ├── peer_registry.py     # peer_id <-> host/port map (JSON save/load)
│   └── peers.json           # generated at runtime
├── peer/
│   ├── clock.py             # LamportClock (tick, tick_receive)
│   ├── messages.py          # Message envelope (JSON, newline-framed)
│   ├── peer.py              # TCP server + client, handler registry, Lamport-bump on recv
│   ├── roles.py             # BuyerBehavior, SellerBehavior, assign_roles
│   ├── election.py          # ElectionManager (Bully, RESIGN handler, yield_for)
│   ├── trader.py            # TraderBehavior (ordered delivery, FIFO, checkpoint)
│   └── logger.py            # thread-safe timestamped Logger + `log` singleton
├── state/
│   └── trader_state.json    # atomic checkpoint written by the trader
├── bench_results/           # CSV + per-scenario log (created by benchmark.py)
└── plots/                   # PNG figures (created by plot_benchmark.py)
```

