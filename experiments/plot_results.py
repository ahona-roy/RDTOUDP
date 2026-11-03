#!/usr/bin/env python3
"""Turn results/experiments.csv into graphs and a summary table.

    python experiments/plot_results.py
    python experiments/plot_results.py --input results/experiments.csv --outdir results/plots

Reads only rows with status == ok, averages them over the seeds, and draws the mean
with error bars (+/- 1 standard deviation). A graph whose data is missing is skipped
with a message, so you can run this on partial results at any time.

Outputs (in --outdir, default results/plots):
    goodput_vs_loss.png         retx_vs_loss.png        goodput_vs_window.png
    retx_vs_reorder.png         rto_sensitivity.png     goodput_vs_delay.png
    goodput_vs_jitter.png       summary.csv
"""
import argparse
import csv
import os
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")  # draw to files; no window needed
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STYLE = {  # protocol -> (label, colour, marker)
    "saw": ("Stop-and-Wait", "#7f7f7f", "s"),
    "gbn": ("Go-Back-N", "#d95f02", "o"),
    "sr":  ("Selective Repeat", "#1f77b4", "^"),
}
NUMERIC = ["loss", "corrupt", "dup", "reorder", "delay", "jitter", "window", "rto_s", "seed",
           "file_bytes", "goodput_mbps", "transfer_time_s", "packets_sent", "retransmissions",
           "retx_per_packet", "efficiency", "acks_received", "corrupted_dropped", "avg_rtt_ms"]


def load(path):
    rows, skipped = [], 0
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("status") != "ok":
                skipped += 1
                continue
            try:
                for k in NUMERIC:
                    r[k] = float(r[k])
            except (KeyError, ValueError):
                skipped += 1
                continue
            rows.append(r)
    return rows, skipped


def group(rows, experiment, xkey, ykey, extra=None):
    """-> {protocol: [(x, mean, std, n), ...]} sorted by x."""
    cells = defaultdict(list)
    for r in rows:
        if r["experiment"] != experiment or (extra and not extra(r)):
            continue
        cells[(r["protocol"], r[xkey])].append(r[ykey])
    out = defaultdict(list)
    for (proto, x), ys in cells.items():
        out[proto].append((x, float(np.mean(ys)), float(np.std(ys, ddof=1)) if len(ys) > 1 else 0.0, len(ys)))
    for proto in out:
        out[proto].sort()
    return out


def draw(ax, data, scale=1.0):
    for proto in ("saw", "gbn", "sr"):
        pts = data.get(proto)
        if not pts:
            continue
        label, colour, marker = STYLE[proto]
        x = [p[0] * scale for p in pts]
        ax.errorbar(x, [p[1] for p in pts], yerr=[p[2] for p in pts], label=label, color=colour,
                    marker=marker, capsize=3, linewidth=1.8, markersize=6)
    ax.grid(True, alpha=0.3)
    ax.legend()


def save(fig, outdir, name):
    path = os.path.join(outdir, name)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("  wrote", path)


def line_graph(rows, outdir, name, experiment, xkey, ykey, xlabel, ylabel, title,
               xscale=1.0, extra=None, tweak=None):
    data = group(rows, experiment, xkey, ykey, extra)
    if not data:
        print(f"  skipped {name}: no '{experiment}' data yet")
        return None
    fig, ax = plt.subplots(figsize=(7, 4.5))
    draw(ax, data, xscale)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if tweak:
        tweak(ax, rows)
    save(fig, outdir, name)
    return data


def window_reference(ax, rows):
    """Dashed line: Stop-and-Wait at the same loss/delay as the window sweep (it has no window)."""
    ref = [r["goodput_mbps"] for r in rows if r["experiment"] == "loss" and r["protocol"] == "saw"
           and abs(r["loss"] - 0.10) < 1e-9 and abs(r["delay"] - 10) < 1e-9 and r["jitter"] == 0]
    if ref:
        ax.axhline(float(np.mean(ref)), color=STYLE["saw"][1], linestyle="--",
                   label="Stop-and-Wait (no window)")
        ax.legend()


def rto_graph(rows, outdir):
    fixed = group(rows, "rto", "rto_s", "goodput_mbps", lambda r: r["rto_mode"] == "fixed")
    adaptive = defaultdict(list)
    for r in rows:
        if r["experiment"] == "rto" and r["rto_mode"] == "adaptive":
            adaptive[r["protocol"]].append(r["goodput_mbps"])
    if not fixed:
        print("  skipped rto_sensitivity.png: no 'rto' data yet")
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    draw(ax, fixed)
    for proto, ys in adaptive.items():
        label, colour, _ = STYLE[proto]
        m, s = float(np.mean(ys)), (float(np.std(ys, ddof=1)) if len(ys) > 1 else 0.0)
        ax.axhline(m, color=colour, linestyle="--", linewidth=1.5, label=f"{label}, adaptive RTO")
        ax.axhspan(m - s, m + s, color=colour, alpha=0.12)
    ax.set_xscale("log")
    ax.set_xlabel("Fixed RTO (seconds, log scale)")
    ax.set_ylabel("Goodput (Mbps)")
    ax.set_title("RTO sensitivity: fixed timeouts vs adaptive RTO")
    ax.legend()
    if not adaptive:
        print("  note: no adaptive-RTO runs yet; the graph shows fixed timeouts only")
    save(fig, outdir, "rto_sensitivity.png")


def summary(rows, outdir):
    cells = defaultdict(list)
    keys = ("experiment", "protocol", "loss", "reorder", "delay", "jitter", "window", "rto_mode", "rto_s")
    for r in rows:
        cells[tuple(r[k] for k in keys)].append(r)
    path = os.path.join(outdir, "summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(list(keys) + ["n_seeds", "goodput_mean", "goodput_std", "retx_per_packet_mean",
                                 "retx_per_packet_std", "avg_rtt_ms_mean", "transfer_time_mean_s"])
        for k in sorted(cells, key=str):
            g = cells[k]
            def ms(name):
                v = [r[name] for r in g]
                return float(np.mean(v)), (float(np.std(v, ddof=1)) if len(v) > 1 else 0.0)
            gm, gs = ms("goodput_mbps")
            rm, rs = ms("retx_per_packet")
            w.writerow(list(k) + [len(g), round(gm, 4), round(gs, 4), round(rm, 4), round(rs, 4),
                                  round(ms("avg_rtt_ms")[0], 3), round(ms("transfer_time_s")[0], 3)])
    print("  wrote", path)


def claim_check(rows):
    """Plain-text look at the claim: at each loss rate >= 5%, who has the best goodput / fewest retx?"""
    gp = group(rows, "loss", "loss", "goodput_mbps")
    rx = group(rows, "loss", "loss", "retx_per_packet")
    if "sr" not in gp or len(gp) < 2:
        print("\nClaim check: needs loss-sweep data for Selective Repeat and at least one other protocol.")
        return
    print("\nClaim check (Selective Repeat vs the others, loss >= 5%; means over seeds)")
    print(f"  {'loss':>5}  {'best goodput':<18}{'fewest retx/packet':<22}")
    wins_g = wins_r = total = 0
    losses = sorted({p[0] for pts in gp.values() for p in pts if p[0] >= 0.05})
    for loss in losses:
        g = {p: dict((x, m) for x, m, _, _ in pts).get(loss) for p, pts in gp.items()}
        r = {p: dict((x, m) for x, m, _, _ in pts).get(loss) for p, pts in rx.items()}
        g = {p: v for p, v in g.items() if v is not None}
        r = {p: v for p, v in r.items() if v is not None}
        if "sr" not in g:
            continue
        bg, br = max(g, key=g.get), min(r, key=r.get)
        total += 1
        wins_g += bg == "sr"
        wins_r += br == "sr"
        print(f"  {loss * 100:4.0f}%  {STYLE[bg][0]:<18}{STYLE[br][0]:<22}")
    if total:
        print(f"  Selective Repeat had the best goodput at {wins_g}/{total} loss rates and the fewest "
              f"retransmissions per packet at {wins_r}/{total}.")
        print("  (Check the error bars before concluding: differences smaller than the spread are not evidence.)")


def main():
    p = argparse.ArgumentParser(description="Plot experiment results")
    p.add_argument("--input", default=os.path.join(ROOT, "results", "experiments.csv"))
    p.add_argument("--outdir", default=os.path.join(ROOT, "results", "plots"))
    a = p.parse_args()
    if not os.path.exists(a.input):
        print(f"ERROR: {a.input} not found. Run experiments/run_experiments.py first.")
        return 3
    rows, skipped = load(a.input)
    print(f"Loaded {len(rows)} successful runs from {a.input} ({skipped} rows ignored: not ok)")
    if not rows:
        return 3
    os.makedirs(a.outdir, exist_ok=True)

    line_graph(rows, a.outdir, "goodput_vs_loss.png", "loss", "loss", "goodput_mbps",
               "Packet loss rate (%)", "Goodput (Mbps)", "Goodput vs packet loss", xscale=100)
    line_graph(rows, a.outdir, "retx_vs_loss.png", "loss", "loss", "retx_per_packet",
               "Packet loss rate (%)", "Retransmissions per delivered packet",
               "Retransmissions vs packet loss", xscale=100)
    line_graph(rows, a.outdir, "goodput_vs_window.png", "window", "window", "goodput_mbps",
               "Window size (packets)", "Goodput (Mbps)", "Goodput vs window size (10% loss)",
               tweak=lambda ax, rws: (ax.set_xscale("log", base=2), ax.set_xticks(
                   sorted({r["window"] for r in rws if r["experiment"] == "window"})),
                   ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter()),
                   window_reference(ax, rws)))
    line_graph(rows, a.outdir, "retx_vs_reorder.png", "reorder", "reorder", "retx_per_packet",
               "Reordering probability (%)", "Retransmissions per delivered packet",
               "Retransmissions vs reordering (5% loss)", xscale=100)
    rto_graph(rows, a.outdir)
    line_graph(rows, a.outdir, "goodput_vs_delay.png", "delay", "delay", "goodput_mbps",
               "One-way delay (ms)", "Goodput (Mbps)", "Goodput vs network delay (10% loss)")
    line_graph(rows, a.outdir, "goodput_vs_jitter.png", "jitter", "jitter", "goodput_mbps",
               "Jitter (+/- ms)", "Goodput (Mbps)", "Goodput vs jitter (20 ms delay, 10% loss)")
    summary(rows, a.outdir)
    claim_check(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())