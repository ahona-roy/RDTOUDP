"""Selective Repeat over UDP.
 
Sender: per-packet deadlines kept in a min-heap, single-threaded loop.
Receiver: buffers out-of-order packets, ACKs each one individually
(ACK packet with ack == seq of the data packet), writes in order.
 
ASSUMPTIONS about Member 1's code (adjust the few marked lines if different):
  - common.packet exposes Packet, DATA, ACK
  - Packet(type=, seq=, ack=, payload=b"").pack() -> bytes
  - Packet.unpack(raw) -> Packet (with .type .seq .ack .payload) or None
  - sequence numbers start at 0 for the first data packet
"""
 
import heapq
import os
import socket
import time
 
from common.packet import Packet, DATA, ACK
from common.config import PACKET_SIZE, MAX_RETRIES, INITIAL_RTO
 
try:
    from protocols.base import BaseSender, BaseReceiver
except ImportError:  # base.py not merged yet
    class BaseSender:
        pass
 
    class BaseReceiver:
        pass
 
try:
    from protocols.rto import RTOEstimator
except ImportError:
    RTOEstimator = None
 
IDLE_TIMEOUT = 30.0  # receiver gives up if nothing valid arrives for this long
 
 
class SelectiveRepeatSender(BaseSender):
    def __init__(self, sock, dest_addr, window_size, stats, rto=None):
        self.sock = sock
        self.dest_addr = dest_addr
        self.window = window_size
        self.stats = stats
        # rto: an RTOEstimator (adaptive or fixed). Default: fixed INITIAL_RTO.
        if rto is None:
            if RTOEstimator is None:
                raise RuntimeError("protocols/rto.py (RTOEstimator) is missing")
            rto = RTOEstimator(initial_rto=INITIAL_RTO, fixed=True)
        self.rto = rto
 
    def _transmit(self, entry):
        self.sock.sendto(entry["raw"], self.dest_addr)
        self.stats.packets_sent += 1
        now = time.monotonic()
        entry["sent"] = now
        entry["deadline"] = now + self.rto.rto
        heapq.heappush(self.heap, (entry["deadline"], entry["seq"]))
 
    def send_file(self, filepath):
        total = -(-os.path.getsize(filepath) // PACKET_SIZE)  # ceil, 0 if empty
        self.heap = []
        inflight = {}        # seq -> entry for sent but un-ACKed packets
        acked = set()        # ACKed seqs >= base (out-of-order ACKs)
        base = next_seq = 0
 
        with open(filepath, "rb") as f:
            while base < total:
                # 1. fill the window
                while next_seq < total and next_seq < base + self.window:
                    chunk = f.read(PACKET_SIZE)
                    raw = Packet(type=DATA, seq=next_seq, ack=0,
                                 payload=chunk).pack()
                    entry = {"seq": next_seq, "raw": raw, "retries": 0,
                             "retx": False}
                    inflight[next_seq] = entry
                    self._transmit(entry)
                    next_seq += 1
 
                # 2. handle expired deadlines (one backoff per timeout event)
                now = time.monotonic()
                timed_out = False
                while self.heap and self.heap[0][0] <= now:
                    deadline, seq = heapq.heappop(self.heap)
                    entry = inflight.get(seq)
                    if entry is None or entry["deadline"] != deadline:
                        continue  # stale heap item (already ACKed/rescheduled)
                    entry["retries"] += 1
                    if entry["retries"] > MAX_RETRIES:
                        raise ConnectionError(
                            f"seq {seq}: no ACK after {MAX_RETRIES} retries")
                    entry["retx"] = True  # Karn: no RTT sample from this one
                    self.stats.retransmissions += 1
                    timed_out = True
                    self._transmit(entry)
                if timed_out:
                    self.rto.backoff()
 
                # 3. wait for an ACK until the earliest valid deadline
                while self.heap and (self.heap[0][1] not in inflight or
                                     inflight[self.heap[0][1]]["deadline"]
                                     != self.heap[0][0]):
                    heapq.heappop(self.heap)  # drop stale items
                wait = (self.heap[0][0] - time.monotonic()) if self.heap else 0.05
                self.sock.settimeout(max(0.001, wait))
                try:
                    raw, _ = self.sock.recvfrom(65535)
                except socket.timeout:
                    continue
                pkt = Packet.unpack(raw)
                if pkt is None or pkt.type != ACK:
                    continue
                self.stats.acks_received += 1
                entry = inflight.pop(pkt.ack, None)
                if entry is None:
                    continue  # duplicate ACK
                if not entry["retx"]:  # Karn's algorithm
                    rtt = time.monotonic() - entry["sent"]
                    self.stats.rtt_samples.append(rtt)
                    self.rto.update(rtt)
                acked.add(pkt.ack)
                while base in acked:
                    acked.discard(base)
                    base += 1
 
 
class SelectiveRepeatReceiver(BaseReceiver):
    # Seconds to keep re-ACKing retransmitted data after the last byte is
    # written. 0 = return immediately (as specified). If the sender hangs at
    # high loss because the final ACK was lost, set to ~2 * RTO.
    linger = 0.0
 
    def __init__(self, sock, stats, expected_size=None):
        self.sock = sock
        self.stats = stats
        self.expected_size = expected_size  # set by the app before receive_file
 
    def _ack(self, seq, addr):
        self.sock.sendto(Packet(type=ACK, seq=0, ack=seq).pack(), addr)
 
    def receive_file(self, out_path):
        if self.expected_size is None:
            raise ValueError("set receiver.expected_size before receive_file()")
        rcv_base = 0
        written = 0
        buffer = {}
        last_valid = time.monotonic()
 
        with open(out_path, "wb") as out:
            while written < self.expected_size:
                self.sock.settimeout(1.0)
                try:
                    raw, addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    if time.monotonic() - last_valid > IDLE_TIMEOUT:
                        raise TimeoutError("receiver idle: sender went silent")
                    continue
                pkt = Packet.unpack(raw)
                if pkt is None or pkt.type != DATA:
                    continue  # corrupted or not data: ignore
                last_valid = time.monotonic()
                self._ack(pkt.seq, addr)  # ACK new AND duplicate packets
                if pkt.seq >= rcv_base and pkt.seq not in buffer:
                    buffer[pkt.seq] = pkt.payload
                while rcv_base in buffer:
                    data = buffer.pop(rcv_base)
                    out.write(data)
                    written += len(data)
                    rcv_base += 1
 
            # optional: keep re-ACKing so a lost final ACK doesn't stall sender
            end = time.monotonic() + self.linger
            while time.monotonic() < end:
                self.sock.settimeout(max(0.001, end - time.monotonic()))
                try:
                    raw, addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    break
                pkt = Packet.unpack(raw)
                if pkt is not None and pkt.type == DATA:
                    self._ack(pkt.seq, addr)
 