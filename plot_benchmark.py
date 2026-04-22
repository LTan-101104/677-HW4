"""Phase 9 plotting: generates matplotlib figures from the benchmark CSVs.

Reads `bench_results/{baseline,contention,resignation}.csv` (produced by
`benchmark.py`) and writes two PNGs to `plots/` for the design PDF:

  plots/latency_timeline.png      — per-request RTT over time, 3 stacked
                                    panels (one per scenario). The
                                    resignation panel shows the handover
                                    spike clearly; TIMEOUTs marked as red
                                    triangles above the data.
  plots/latency_distribution.png  — SUCCESS-RTT CDF overlay (left) +
                                    box-plot comparison (right) across all
                                    three scenarios.

Requires matplotlib. Usage:

  python plot_benchmark.py                        # default dirs
  python plot_benchmark.py --input-dir bench_results --output-dir plots
"""
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


RESULTS_DIR = Path("bench_results")
PLOTS_DIR = Path("plots")
SCENARIOS = ["baseline", "contention", "resignation"]


def _load_csv(path: Path) -> list[dict]:
    """Reads one benchmark CSV; coerces seq/rtt_ms, leaves rtt None for timeouts."""
    rows: list[dict] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
            rtt_raw = r["rtt_ms"]
            rows.append(
                {
                    "seq": int(r["seq"]),
                    "rtt_ms": float(rtt_raw) if rtt_raw else None,
                    "status": r["status"],
                }
            )
    return rows


def _plot_timelines(
    scenario_rows: dict[str, list[dict]], out_path: Path
) -> None:
    """One subplot per scenario: seq on x-axis, RTT (ms) on y-axis.

    SUCCESS and OUT_OF_STOCK share the y-axis (both have real RTTs).
    TIMEOUTs have no RTT, so we mark them as red triangles at the top of
    each panel so they're still visible in the trace.
    """
    fig, axes = plt.subplots(len(SCENARIOS), 1, figsize=(10, 9), sharex=True)
    if len(SCENARIOS) == 1:
        axes = [axes]

    for ax, scen in zip(axes, SCENARIOS):
        rows = scenario_rows[scen]
        success = [(r["seq"], r["rtt_ms"]) for r in rows if r["status"] == "SUCCESS"]
        oos = [(r["seq"], r["rtt_ms"]) for r in rows if r["status"] == "OUT_OF_STOCK"]
        timeout = [r["seq"] for r in rows if r["status"] == "TIMEOUT"]

        if success:
            xs, ys = zip(*success)
            ax.plot(xs, ys, ".", markersize=2, alpha=0.6, label="SUCCESS")
        if oos:
            xs, ys = zip(*oos)
            ax.plot(
                xs, ys, "x", markersize=4, color="orange",
                alpha=0.5, label="OUT_OF_STOCK",
            )
        if timeout:
            # Set y-lim first so "top of plot" is meaningful.
            ymax = ax.get_ylim()[1]
            ax.plot(
                timeout, [ymax] * len(timeout), "r^",
                markersize=9, label=f"TIMEOUT (n={len(timeout)})",
            )

        ax.set_title(f"{scen}")
        ax.set_ylabel("RTT (ms)")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right", fontsize=8)

    axes[-1].set_xlabel("Request # (sequential)")
    fig.suptitle("Per-request RTT over time", fontsize=13)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def _plot_distribution(
    scenario_rows: dict[str, list[dict]], out_path: Path
) -> None:
    """Two panels: SUCCESS-RTT CDF overlay + per-scenario boxplot."""
    fig, (ax_cdf, ax_box) = plt.subplots(1, 2, figsize=(12, 5))

    for scen in SCENARIOS:
        rows = scenario_rows[scen]
        rtt_values = sorted(
            r["rtt_ms"] for r in rows if r["status"] == "SUCCESS"
        )
        if not rtt_values:
            continue
        cdf = [i / len(rtt_values) for i in range(1, len(rtt_values) + 1)]
        ax_cdf.plot(rtt_values, cdf, label=scen, linewidth=1.5)
    # Log x so the steady-state bodies (~1 ms) remain legible alongside
    # the handover tail (~80 ms on resignation).
    ax_cdf.set_xscale("log")
    ax_cdf.set_xlabel("RTT (ms, log scale)")
    ax_cdf.set_ylabel("Cumulative fraction of SUCCESS requests")
    ax_cdf.set_title("SUCCESS RTT — CDF")
    ax_cdf.grid(True, which="both", alpha=0.3)
    ax_cdf.legend()

    box_data = [
        [r["rtt_ms"] for r in scenario_rows[s] if r["status"] == "SUCCESS"]
        for s in SCENARIOS
    ]
    ax_box.boxplot(box_data, labels=SCENARIOS, showfliers=True)
    ax_box.set_yscale("log")
    ax_box.set_ylabel("RTT (ms, log scale)")
    ax_box.set_title("SUCCESS RTT — distribution")
    ax_box.grid(True, which="both", alpha=0.3, axis="y")

    fig.suptitle("SUCCESS RTT across scenarios (n=1000 each)", fontsize=13)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--output-dir", type=Path, default=PLOTS_DIR)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    scenario_rows: dict[str, list[dict]] = {}
    missing: list[str] = []
    for scen in SCENARIOS:
        path = args.input_dir / f"{scen}.csv"
        if not path.exists():
            missing.append(scen)
            continue
        scenario_rows[scen] = _load_csv(path)

    if missing:
        print(
            f"[plot] missing scenarios: {missing}. "
            f"Run `python benchmark.py --scenario <name>` first."
        )
        return

    timeline_path = args.output_dir / "latency_timeline.png"
    dist_path = args.output_dir / "latency_distribution.png"
    _plot_timelines(scenario_rows, timeline_path)
    _plot_distribution(scenario_rows, dist_path)
    print(f"[plot] wrote {timeline_path}")
    print(f"[plot] wrote {dist_path}")


if __name__ == "__main__":
    main()
