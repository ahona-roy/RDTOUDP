#!/usr/bin/env python3
"""Reliable-file-transfer server (receiver side).

    python app/server.py --port 9000 --protocol sr --out received.bin

Flow: wait for SYN -> SYN-ACK -> receive file via chosen ARQ protocol ->
wait for FIN (carries sender's SHA-256) -> compare hashes -> FIN-ACK (OK/FAIL).
"""
import argparse
import json
import logging
import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.registry import PROTOCOLS, load                       # noqa: E402
from common.config import DATA, FIN, FIN_ACK, SYN, SYN_ACK    # noqa: E402
from common.integrity import sha256_file                       # noqa: E402
from common.net import recv_valid, send_raw                    # noqa: E402
from common.packet import Packet                               # noqa: E402
from common.stats import Stats, append_csv                     # noqa: E402

log = logging.getLogger("rdt.server")


def parse_args():
    p = argparse.ArgumentParser(description="RDT-over-UDP server")
    p.add_argument("--bind", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--protocol", choices=PROTOCOLS.keys(), required=True)
    p.add_argument("--out", default="received.bin", help="where to save the file")
    p.add_argument("--idle-timeout", type=float, default=120.0,
                   help="seconds to wait for a client SYN")
    p.add_argument("--fin-timeout", type=float, default=30.0,
                   help="seconds to wait for FIN after data is complete")
    p.add_argument("--linger", type=float, default=2.0,
                   help="seconds to keep answering FIN after FIN-ACK (covers FIN-ACK loss)")
    p.add_argument("--stats-out", help="append a CSV row of receiver stats here")
    p.add_argument("--tag", default="", help="free-text label stored in the CSV row")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def wait_for_syn(sock, timeout):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None, None
        pkt, addr = recv_valid(sock, remaining)
        if pkt is not None and pkt.ptype == SYN:
            return pkt, addr


def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(message)s")

    receiver_cls = load(args.protocol, "receiver")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    sock.bind((args.bind, args.port))
    log.info("listening on %s:%d (protocol=%s)", args.bind, args.port, args.protocol)

    # ---- handshake -------------------------------------------------------
    syn, client = wait_for_syn(sock, args.idle_timeout)
    if syn is None:
        print("ERROR: no client connected (timed out waiting for SYN)")
        return 3
    try:
        meta = json.loads(syn.payload.decode())
        size = int(meta["size"])
    except (ValueError, KeyError):
        print("ERROR: malformed SYN payload")
        return 3
    if meta.get("protocol") != args.protocol:
        print(f"ERROR: client uses '{meta.get('protocol')}' but server runs '{args.protocol}'")
        return 3
    log.info("SYN from %s: file=%s size=%d window=%s",
             client, meta.get("name"), size, meta.get("window"))
    send_raw(sock, Packet(SYN_ACK), client)

    # ---- data transfer ---------------------------------------------------
    stats = Stats()
    receiver = receiver_cls(sock, stats)
    receiver.expected_size = size
    stats.start()
    try:
        receiver.receive_file(args.out)
    except TimeoutError as e:
        print(f"ERROR: transfer aborted: {e}")
        return 3
    stats.stop()
    stats.bytes_delivered = size
    log.info("data complete, waiting for FIN")

    # ---- teardown + integrity check -------------------------------------
    local_hash = sha256_file(args.out)
    deadline = time.monotonic() + args.fin_timeout
    fin, fin_addr = None, None
    while time.monotonic() < deadline:
        pkt, addr = receiver.recv_packet(1.0)
        if pkt is None:
            continue
        if pkt.ptype == DATA:
            receiver.handle_late_data(pkt, addr)      # e.g. re-ACK a lost final ACK
        elif pkt.ptype == FIN:
            fin, fin_addr = pkt, addr
            break

    ok = None
    if fin is None:
        print("WARNING: FIN never arrived; integrity could not be verified")
    else:
        ok = fin.payload.decode(errors="replace") == local_hash
        verdict = b"OK" if ok else b"FAIL"
        send_raw(sock, Packet(FIN_ACK, payload=verdict), fin_addr)
        end = time.monotonic() + args.linger
        while time.monotonic() < end:                 # re-answer repeated FINs
            pkt, addr = receiver.recv_packet(0.2)
            if pkt is not None and pkt.ptype == FIN:
                send_raw(sock, Packet(FIN_ACK, payload=verdict), addr)
            elif pkt is not None and pkt.ptype == DATA:
                receiver.handle_late_data(pkt, addr)

    # ---- report ----------------------------------------------------------
    s = stats.summary()
    print(f"SHA-256 (received): {local_hash}")
    print(f"INTEGRITY: {'OK' if ok else 'FAILED' if ok is False else 'UNVERIFIED'}")
    print(f"[server] {s['bytes_delivered']} bytes in {s['transfer_time_s']} s "
          f"= {s['goodput_mbps']} Mbps | corrupted dropped: {s['corrupted_dropped']}")
    if args.stats_out:
        row = {"role": "server", "protocol": args.protocol, "tag": args.tag,
               "integrity_ok": ok, **s}
        append_csv(args.stats_out, row)
    sock.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
