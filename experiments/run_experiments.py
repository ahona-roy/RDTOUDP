#!/usr/bin/env python3
"""Run the experiment sweeps and save one CSV row per run.

For every setting and seed it starts   server  +  emulator  +  client,
waits for the transfer, and records goodput, retransmissions, RTT and whether the
file arrived intact (SHA-256).

    python experiments/run_experiments.py --dry-run          # show the plan, run nothing
    python experiments/run_experiments.py --quick            # small, fast smoke test
    python experiments/run_experiments.py                    # all sweeps, 3 seeds, 1 MB
    python experiments/run_experiments.py --experiments loss window --seeds 5

Results go to results/experiments.csv. If you stop (Ctrl+C) or something crashes,
run the same command again: runs that already succeeded are skipped (resume).
"""
import argparse
import csv
import hashlib
import importlib
import os
import random
import socket
import subprocess
import sys
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.registry import PROTOCOLS  # noqa: E402

EMULATOR = os.path.join(ROOT, "emulator", "channel.py")
SERVER = os.path.join(ROOT, "app", "server.py")
CLIENT = os.path.join(ROOT, "app", "client.py")
STARTUP_WAIT = 0.6          # seconds for emulator + server to start listening

# Baseline network used by every sweep unless the sweep varies that value.
# Jitter is 0 on purpose: the emulator gives every packet its own random delay, so jitter makes
# packets overtake each other (accidental reordering) and would hide the effect of the
# loss/reorder sweeps. Jitter has its own sweep below.
BASE = dict(loss=0.10, corrupt=0.0, dup=0.0, reorder=0.0, delay=10.0, jitter=0.0, window=8)

# What each experiment varies. Edit the value lists here to change the plan.
SWEEPS = {
    # Goodput vs loss rate
    "loss":    dict(protocols=["saw", "gbn", "sr"], vary="loss",
                    values=[0.0, 0.05, 0.10, 0.20, 0.30]),
    # Goodput vs window size (Stop-and-Wait has no window, so it is left out)
    "window":  dict(protocols=["gbn", "sr"], vary="window", values=[2, 4, 8, 16, 32]),
    # Retransmissions vs reordering probability
    "reorder": dict(protocols=["saw", "gbn", "sr"], vary="reorder",
                    values=[0.0, 0.05, 0.10, 0.20, 0.30], over=dict(loss=0.05)),
    # Effect of network delay
    "delay":   dict(protocols=["saw", "gbn", "sr"], vary="delay", values=[0, 10, 20, 50, 100]),
    # Effect of jitter (which also causes reordering in this emulator)
    "jitter":  dict(protocols=["saw", "gbn", "sr"], vary="jitter", values=[0.0, 1.0, 2.0, 5.0, 10.0],
                    over=dict(delay=20.0)),
    # RTO sensitivity: several fixed timeouts vs the adaptive one
    "rto":     dict(protocols=["gbn", "sr"], vary="rto",
                    values=[0.02, 0.05, 0.10, 0.20, 0.50, 1.00, "adaptive"]),
}
QUICK_VALUES = {
    "loss": [0.0, 0.10, 0.30], "window": [2, 8, 32], "reorder": [0.0, 0.10, 0.30],
    "delay": [0, 20, 50], "jitter": [0.0, 2.0, 10.0], "rto": [0.05, 0.20, "adaptive"],
}

FIELDS = ["experiment", "protocol", "loss", "corrupt", "dup", "reorder", "delay", "jitter",
          "window", "rto_mode", "rto_s", "seed", "file_bytes", "status",
          "goodput_mbps", "transfer_time_s", "packets_sent", "retransmissions",
          "retx_per_packet", "efficiency", "acks_received", "corrupted_dropped",
          "avg_rtt_ms", "wall_time_s", "timestamp"]
KEY_FIELDS = ["experiment", "protocol", "loss", "corrupt", "dup", "reorder", "delay",
              "jitter", "window", "rto_mode", "rto_s", "seed", "file_bytes"]


# ---------------------------------------------------------------- helpers
def parse_size(text):
    t = str(text).strip().upper()
    for unit, mult in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10), ("B", 1)):
        if t.endswith(unit):
            return int(float(t[:-len(unit)]) * mult)
    return int(t)


def human(n):
    return f"{n / (1 << 20):g}MB" if n >= (1 << 20) else f"{n / 1024:g}KB"


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def protocol_ready(name):
    module, sender, receiver = PROTOCOLS[name]
    try:
        mod = importlib.import_module(module)
    except ImportError:
        return False
    return hasattr(mod, sender) and hasattr(mod, receiver)


def adaptive_ready():
    try:
        return hasattr(importlib.import_module("protocols.rto"), "RTOEstimator")
    except ImportError:
        return False


def auto_rto(delay_ms, jitter_ms):
    """A sensible fixed RTO for the emulated delay: about 3 x the round-trip time."""
    rtt = 2 * (delay_ms + jitter_ms) / 1000.0
    return max(0.05, round(3 * rtt + 0.02, 3))


def make_testfile(path, size):
    if os.path.exists(path) and os.path.getsize(path) == size:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    rng = random.Random(1)
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            n = min(remaining, 1 << 20)
            f.write(rng.getrandbits(8 * n).to_bytes(n, "little"))
            remaining -= n


def norm(v):
    try:
        return f"{float(v):.6g}"
    except (TypeError, ValueError):
        return str(v)


def key_of(row):
    return tuple(norm(row[k]) for k in KEY_FIELDS)


# ---------------------------------------------------------------- plan
def build_runs(args, protocols_ok, adaptive_ok):
    seeds = list(range(1, args.seeds + 1))
    runs, notes = [], []
    for exp in args.experiments:
        sw = SWEEPS[exp]
        values = QUICK_VALUES[exp] if args.quick else sw["values"]
        for proto in sw["protocols"]:
            if proto not in args.protocols:
                continue
            if proto not in protocols_ok:
                notes.append(f"{exp}: skipping '{proto}' (not written yet)")
                continue
            for v in values:
                p = dict(BASE, **sw.get("over", {}))
                rto_mode, rto_s = "fixed", None
                if sw["vary"] == "rto":
                    if v == "adaptive":
                        if not adaptive_ok:
                            notes.append(f"{exp}: skipping adaptive RTO (protocols/rto.py missing)")
                            continue
                        rto_mode, rto_s = "adaptive", auto_rto(p["delay"], p["jitter"])
                    else:
                        rto_s = float(v)
                else:
                    p[sw["vary"]] = v
                    rto_s = args.rto if args.rto else auto_rto(p["delay"], p["jitter"])
                size = args.saw_size if proto == "saw" else args.size
                for seed in seeds:
                    runs.append(dict(experiment=exp, protocol=proto, **p, rto_mode=rto_mode,
                                     rto_s=rto_s, seed=seed, file_bytes=size))
    return runs, sorted(set(notes))


# ---------------------------------------------------------------- one run
def run_one(spec, testfile, expected_hash, workdir, run_timeout):
    server_port, emu_port = free_udp_port(), free_udp_port()
    received = os.path.join(workdir, "received.bin")
    stats_csv = os.path.join(workdir, "client_stats.csv")
    for path in (received, stats_csv):
        if os.path.exists(path):
            os.remove(path)

    emu_cmd = [sys.executable, EMULATOR, "--listen-port", str(emu_port),
               "--server-port", str(server_port), "--seed", str(spec["seed"]),
               "--loss", str(spec["loss"]), "--corrupt", str(spec["corrupt"]),
               "--dup", str(spec["dup"]), "--reorder", str(spec["reorder"]),
               "--delay", str(spec["delay"]), "--jitter", str(spec["jitter"])]
    server_cmd = [sys.executable, SERVER, "--bind", "127.0.0.1", "--port", str(server_port),
                  "--protocol", spec["protocol"], "--out", received, "--linger", "0.5",
                  "--idle-timeout", "60", "--fin-timeout", "60"]
    client_cmd = [sys.executable, CLIENT, "--host", "127.0.0.1", "--port", str(emu_port),
                  "--protocol", spec["protocol"], "--file", testfile,
                  "--window", str(spec["window"]), "--rto", str(spec["rto_s"]),
                  "--stats-out", stats_csv]
    if spec["rto_mode"] == "adaptive":
        client_cmd.append("--adaptive")

    start = time.monotonic()
    status, code = "error", None
    emu = server = None
    try:
        emu = subprocess.Popen(emu_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        server = subprocess.Popen(server_cmd, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True)
        time.sleep(STARTUP_WAIT)
        try:
            client = subprocess.run(client_cmd, capture_output=True, text=True,
                                    timeout=run_timeout)
            code = client.returncode
        except subprocess.TimeoutExpired:
            status = "timeout"
        if code is not None:
            status = {0: "ok", 1: "integrity_failed", 2: "unverified", 3: "failed"}.get(code, "error")
        try:
            server.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            pass
    finally:
        for proc in (server, emu):
            if proc is not None:
                if proc.poll() is None:
                    proc.kill()
                try:
                    proc.communicate(timeout=5)
                except Exception:
                    pass

    # Independent check by the runner itself: the received file must match the original.
    if status == "ok":
        if not os.path.exists(received) or sha256_file(received) != expected_hash:
            status = "integrity_failed"

    row = dict(spec, status=status, wall_time_s=round(time.monotonic() - start, 2),
               timestamp=datetime.now().isoformat(timespec="seconds"))
    if os.path.exists(stats_csv):
        with open(stats_csv, newline="") as f:
            stats = next(csv.DictReader(f), None)
        if stats:
            sent, retx = int(stats["packets_sent"]), int(stats["retransmissions"])
            row.update(goodput_mbps=stats["goodput_mbps"], transfer_time_s=stats["transfer_time_s"],
                       packets_sent=sent, retransmissions=retx,
                       retx_per_packet=stats["retx_per_packet"],
                       efficiency=round((sent - retx) / sent, 4) if sent else "",
                       acks_received=stats["acks_received"],
                       corrupted_dropped=stats["corrupted_dropped"], avg_rtt_ms=stats["avg_rtt_ms"])
    return row


# ---------------------------------------------------------------- main
def parse_args():
    p = argparse.ArgumentParser(description="Run RDT-over-UDP experiments")
    p.add_argument("--experiments", nargs="+", choices=list(SWEEPS), default=list(SWEEPS))
    p.add_argument("--protocols", nargs="+", choices=list(PROTOCOLS), default=list(PROTOCOLS))
    p.add_argument("--seeds", type=int, default=3, help="repeat every setting with seeds 1..N")
    p.add_argument("--size", default="1MB", help="file size for GBN / SR (e.g. 500KB, 1MB, 100MB)")
    p.add_argument("--saw-size", default="200KB",
                   help="smaller file for Stop-and-Wait (it is very slow); goodput stays comparable")
    p.add_argument("--rto", type=float, default=None,
                   help="force one fixed RTO (seconds) everywhere; default: chosen from the delay")
    p.add_argument("--run-timeout", type=float, default=300, help="give up on one run after N seconds")
    p.add_argument("--out", default=os.path.join(ROOT, "results", "experiments.csv"))
    p.add_argument("--workdir", default=os.path.join(ROOT, "results", "_work"))
    p.add_argument("--quick", action="store_true", help="tiny smoke test: few values, 2 seeds, small files")
    p.add_argument("--fresh", action="store_true", help="ignore existing results (do not resume)")
    p.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    a = p.parse_args()
    if a.quick:
        a.seeds = min(a.seeds, 2)
        if a.size == "1MB":
            a.size = "100KB"
        if a.saw_size == "200KB":
            a.saw_size = "50KB"
    a.size, a.saw_size = parse_size(a.size), parse_size(a.saw_size)
    return a


def main():
    args = parse_args()
    if not os.path.exists(EMULATOR):
        print("ERROR: emulator/channel.py not found")
        return 3
    protocols_ok = {n for n in PROTOCOLS if protocol_ready(n)}
    runs, notes = build_runs(args, protocols_ok, adaptive_ready())

    done = set()
    if os.path.exists(args.out) and not args.fresh:
        with open(args.out, newline="") as f:
            for r in csv.DictReader(f):
                if r.get("status") == "ok":
                    try:
                        done.add(key_of(r))
                    except KeyError:
                        pass
    todo = [r for r in runs if key_of(r) not in done]

    print(f"Protocols ready: {sorted(protocols_ok)} | adaptive RTO: {adaptive_ready()}")
    for n in notes:
        print("  note:", n)
    print(f"Planned runs: {len(runs)} | already done: {len(runs) - len(todo)} | to run: {len(todo)}")
    for exp in args.experiments:
        for proto in args.protocols:
            n = sum(1 for r in todo if r["experiment"] == exp and r["protocol"] == proto)
            if n:
                print(f"  {exp:8s} {proto:4s} {n:4d} runs")
    print(f"File sizes: {human(args.size)} (GBN/SR), {human(args.saw_size)} (Stop-and-Wait)")
    if args.dry_run or not todo:
        return 0

    os.makedirs(args.workdir, exist_ok=True)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    files = {}
    for size in {r["file_bytes"] for r in todo}:
        path = os.path.join(args.workdir, f"testfile_{size}.bin")
        make_testfile(path, size)
        files[size] = (path, sha256_file(path))

    new_file = not os.path.exists(args.out) or os.path.getsize(args.out) == 0
    t_start = time.monotonic()
    failures = 0
    try:
        with open(args.out, "a", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=FIELDS, extrasaction="ignore")
            if new_file:
                writer.writeheader()
            for i, spec in enumerate(todo, 1):
                path, digest = files[spec["file_bytes"]]
                row = run_one(spec, path, digest, args.workdir, args.run_timeout)
                writer.writerow(row)
                out.flush()
                if row["status"] != "ok":
                    failures += 1
                elapsed = time.monotonic() - t_start
                eta = elapsed / i * (len(todo) - i)
                desc = (f"loss={spec['loss']} reorder={spec['reorder']} delay={spec['delay']} "
                        f"w={spec['window']} rto={spec['rto_mode'] if spec['rto_mode'] == 'adaptive' else spec['rto_s']} "
                        f"seed={spec['seed']}")
                result = (f"{row.get('goodput_mbps', '?')} Mbps retx={row.get('retransmissions', '?')}"
                          if row["status"] == "ok" else row["status"].upper())
                print(f"[{i}/{len(todo)}] {spec['experiment']:7s} {spec['protocol']:3s} {desc} -> "
                      f"{result} ({row['wall_time_s']}s, ETA {eta / 60:.1f} min)", flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted. Run the same command again to resume.")
        return 130
    print(f"\nFinished. {len(todo) - failures} ok, {failures} not ok. Results: {args.out}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())