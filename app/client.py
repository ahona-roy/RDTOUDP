#!/usr/bin/env python3
"""Reliable-file-transfer client (sender side).

    python app/client.py --host 127.0.0.1 --port 9000 --protocol gbn \
                         --file big.bin --window 8 --rto 0.5 [--adaptive]

Point --host/--port at the channel emulator to test under bad network conditions.
Exit codes: 0 = transferred and verified, 1 = hash mismatch, 2 = FIN-ACK missing
(integrity unconfirmed), 3 = connection/transfer failure.
"""
import argparse
import json
import logging
import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.registry import PROTOCOLS, load                                   # noqa: E402
from common.config import (DEFAULT_WINDOW, FIN, FIN_ACK, INITIAL_RTO,      # noqa: E402
                           MAX_RETRIES, SYN, SYN_ACK)
from common.integrity import sha256_file                                   # noqa: E402
from common.net import recv_valid, send_raw                                # noqa: E402
from common.packet import Packet                                           # noqa: E402
from common.stats import Stats, append_csv                                 # noqa: E402

log = logging.getLogger("rdt.client")


def parse_args():
    p = argparse.ArgumentParser(description="RDT-over-UDP client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--protocol", choices=PROTOCOLS.keys(), required=True)
    p.add_argument("--file", required=True, help="file to send")
    p.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                   help="window size (GBN/SR; ignored by stop-and-wait)")
    p.add_argument("--rto", type=float, default=INITIAL_RTO,
                   help="fixed RTO in seconds (also the handshake timeout)")
    p.add_argument("--adaptive", action="store_true",
                   help="use adaptive RTO (Jacobson/Karels + Karn) from protocols/rto.py")
    p.add_argument("--stats-out", help="append a CSV row of sender stats here")
    p.add_argument("--tag", default="", help="free-text label stored in the CSV row")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def request_response(sock, addr, request, want_type, timeout):
    """Send `request` until a packet of `want_type` arrives. Returns it, or None."""
    for attempt in range(MAX_RETRIES):
        send_raw(sock, request, addr)
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            pkt, _ = recv_valid(sock, remaining)
            if pkt is None:
                break
            if pkt.ptype == want_type:
                return pkt
        log.debug("no reply to %r (attempt %d)", request, attempt + 1)
    return None


def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(message)s")

    if not os.path.isfile(args.file):
        print(f"ERROR: file not found: {args.file}")
        return 3
    size = os.path.getsize(args.file)
    local_hash = sha256_file(args.file)

    sender_cls = load(args.protocol, "sender")
    estimator = None
    if args.adaptive:
        try:
            from protocols.rto import RTOEstimator
            estimator = RTOEstimator()
        except ImportError:
            print("ERROR: --adaptive needs protocols/rto.py (Member 3) which is not there yet")
            return 3

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    dest = (args.host, args.port)

    # ---- handshake -------------------------------------------------------
    meta = {"name": os.path.basename(args.file), "size": size,
            "protocol": args.protocol, "window": args.window}
    syn = Packet(SYN, payload=json.dumps(meta).encode())
    if request_response(sock, dest, syn, SYN_ACK, args.rto) is None:
        print(f"ERROR: no SYN-ACK from {dest} after {MAX_RETRIES} tries")
        return 3
    log.info("connected to %s (%d bytes, protocol=%s, window=%d)",
             dest, size, args.protocol, args.window)

    # ---- data transfer ---------------------------------------------------
    stats = Stats()
    sender = sender_cls(sock, dest, args.window, stats,
                        rto_estimator=estimator, fixed_rto=args.rto)
    stats.start()
    try:
        sender.send_file(args.file)
    except ConnectionError as e:
        print(f"ERROR: transfer failed: {e}")
        return 3
    stats.stop()
    stats.bytes_delivered = size

    # ---- teardown + integrity -------------------------------------------
    fin = Packet(FIN, payload=local_hash.encode())
    reply = request_response(sock, dest, fin, FIN_ACK, args.rto)
    if reply is None:
        verdict, code = "UNVERIFIED (no FIN-ACK)", 2
    elif reply.payload == b"OK":
        verdict, code = "OK", 0
    else:
        verdict, code = "FAILED", 1

    # ---- report ----------------------------------------------------------
    s = stats.summary()
    print(f"SHA-256 (sent):     {local_hash}")
    print(f"INTEGRITY: {verdict}")
    print(f"[client] {args.protocol.upper()} | {size} bytes in {s['transfer_time_s']} s "
          f"= {s['goodput_mbps']} Mbps | sent {s['packets_sent']} pkts, "
          f"{s['retransmissions']} retx ({s['retx_per_packet']}/pkt) | "
          f"avg RTT {s['avg_rtt_ms']} ms")
    if args.stats_out:
        row = {"role": "client", "protocol": args.protocol, "tag": args.tag,
               "window": args.window, "rto": "adaptive" if estimator else args.rto,
               "integrity_ok": code == 0, **s}
        append_csv(args.stats_out, row)
    sock.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
