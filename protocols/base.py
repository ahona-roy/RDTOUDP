"""Base classes every ARQ protocol inherits from.

Contract for teammates
----------------------
Sender   : implement send_file(filepath). Use self.send_packet / self.recv_packet /
           self.read_chunks / self.current_rto(). Count retransmissions in
           self.stats.retransmissions. Raise ConnectionError after MAX_RETRIES.
Receiver : implement receive_file(out_path). self.expected_size is set by the server
           before the call; return once that many payload bytes were written in order.
           Reply to peers with self.send_packet(pkt, addr).
           Override handle_late_data() to re-ACK data that arrives after receive_file
           returned (e.g. when the final ACK was lost).
RTO      : rto_estimator (optional) must expose .rto (float), .update(rtt), .backoff().
"""
import time

from common.config import INITIAL_RTO, PACKET_SIZE, SYN, SYN_ACK
from common.net import recv_valid, send_raw
from common.packet import Packet


class BaseSender:
    def __init__(self, sock, dest_addr, window_size, stats,
                 rto_estimator=None, fixed_rto=INITIAL_RTO):
        self.sock = sock
        self.dest = dest_addr
        self.window = window_size
        self.stats = stats
        self.rto_estimator = rto_estimator
        self.fixed_rto = fixed_rto

    def current_rto(self) -> float:
        return self.rto_estimator.rto if self.rto_estimator else self.fixed_rto

    def send_packet(self, pkt):
        send_raw(self.sock, pkt, self.dest)
        self.stats.packets_sent += 1

    def recv_packet(self, timeout):
        """Returns (packet, addr) or (None, None) on timeout. Corrupt packets are skipped."""
        return recv_valid(self.sock, timeout, self.stats)

    @staticmethod
    def read_chunks(filepath, size=PACKET_SIZE):
        with open(filepath, "rb") as f:
            while True:
                chunk = f.read(size)
                if not chunk:
                    break
                yield chunk

    def send_file(self, filepath):
        raise NotImplementedError


class BaseReceiver:
    def __init__(self, sock, stats):
        self.sock = sock
        self.stats = stats
        self.expected_size = None   # set by server.py before receive_file()

    def send_packet(self, pkt, addr):
        send_raw(self.sock, pkt, addr)
        self.stats.packets_sent += 1

    def recv_packet(self, timeout):
        """Like the sender's, but transparently answers duplicate SYNs (lost SYN-ACK)."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None, None
            pkt, addr = recv_valid(self.sock, remaining, self.stats)
            if pkt is None:
                return None, None
            if pkt.ptype == SYN:
                send_raw(self.sock, Packet(SYN_ACK), addr)
                continue
            return pkt, addr

    def handle_late_data(self, pkt, addr):
        """Called for DATA arriving after receive_file() finished. Default: ignore."""

    def receive_file(self, out_path):
        raise NotImplementedError
