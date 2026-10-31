"""Channel emulator: a UDP middleman that damages packets on purpose.

    Client --> Emulator (listen port) --> Server
    Client <-- Emulator <-- Server

Run example:
    python emulator/channel.py --listen-port 8000 --server-port 9000 \
        --loss 0.1 --delay 20 --jitter 5 --seed 42
"""
import argparse
import heapq
import random
import select
import socket
import sys
import time


def parse_args():
    p = argparse.ArgumentParser(description="UDP channel emulator")
    p.add_argument("--listen-host", default="127.0.0.1")
    p.add_argument("--listen-port", type=int, default=8000)
    p.add_argument("--server-host", default="127.0.0.1")
    p.add_argument("--server-port", type=int, default=9000)
    p.add_argument("--loss", type=float, default=0.0, help="drop probability 0..1")
    p.add_argument("--corrupt", type=float, default=0.0, help="flip-a-byte probability")
    p.add_argument("--dup", type=float, default=0.0, help="duplicate probability")
    p.add_argument("--reorder", type=float, default=0.0, help="hold-back probability")
    p.add_argument("--delay", type=float, default=0.0, help="base delay in ms")
    p.add_argument("--jitter", type=float, default=0.0, help="+/- jitter in ms")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main():
    a = parse_args()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((a.listen_host, a.listen_port))
    server_addr = (socket.gethostbyname(a.server_host), a.server_port)
    client_addr = None

    # One random generator per direction, so runs are reproducible.
    rng = {"fwd": random.Random(a.seed), "rev": random.Random(a.seed + 1)}
    counts = {"seen": 0, "dropped": 0, "corrupted": 0, "duplicated": 0, "reordered": 0}

    heap = []   # entries: (send_time, counter, data, destination)
    counter = 0  # tie-breaker so heap never compares bytes

    def schedule(data, dest, extra=0.0, r=None):
        nonlocal counter
        d = a.delay
        if a.jitter:
            d += r.uniform(-a.jitter, a.jitter)
        d = max(0.0, d) / 1000.0 + extra
        heapq.heappush(heap, (time.monotonic() + d, counter, data, dest))
        counter += 1

    print(f"[emulator] listening on {a.listen_host}:{a.listen_port} -> "
          f"server {server_addr}, loss={a.loss} corrupt={a.corrupt} dup={a.dup} "
          f"reorder={a.reorder} delay={a.delay}ms jitter={a.jitter}ms seed={a.seed}",
          flush=True)

    try:
        while True:
            # Wait for a packet, but only until the next delayed packet is due.
            timeout = 0.5
            if heap:
                timeout = max(0.0, min(timeout, heap[0][0] - time.monotonic()))
            ready, _, _ = select.select([sock], [], [], timeout)

            if ready:
                try:
                    data, src = sock.recvfrom(65535)
                except ConnectionResetError:
                    continue  # Windows: a previous send hit a closed port
                counts["seen"] += 1

                if src == server_addr:
                    direction, dest = "rev", client_addr
                else:
                    client_addr = src
                    direction, dest = "fwd", server_addr
                r = rng[direction]

                if dest is None:
                    pass  # server spoke before any client: nothing to do
                elif r.random() < a.loss:
                    counts["dropped"] += 1
                    if a.verbose:
                        print(f"[emulator] {direction} DROP", flush=True)
                else:
                    if a.corrupt and r.random() < a.corrupt and data:
                        b = bytearray(data)
                        b[r.randrange(len(b))] ^= 0xFF
                        data = bytes(b)
                        counts["corrupted"] += 1
                    extra = 0.0
                    if a.reorder and r.random() < a.reorder:
                        # hold this packet back so later ones overtake it
                        extra = (2 * a.delay + 20) / 1000.0
                        counts["reordered"] += 1
                    schedule(data, dest, extra, r)
                    if a.dup and r.random() < a.dup:
                        counts["duplicated"] += 1
                        schedule(data, dest, 0.0, r)

            # Send everything whose time has come.
            now = time.monotonic()
            while heap and heap[0][0] <= now:
                _, _, data, dest = heapq.heappop(heap)
                try:
                    sock.sendto(data, dest)
                except OSError:
                    pass
    except KeyboardInterrupt:
        print(f"\n[emulator] stopped. {counts}", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()