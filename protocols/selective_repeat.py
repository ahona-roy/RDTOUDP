"""Selective Repeat ARQ (Member 3).
 
Sender  : per-packet deadlines in a min-heap, one single-threaded loop; a timeout
          resends only that packet. Karn's algorithm: no RTT sample from a
          retransmitted packet.
Receiver: ACKs every DATA packet individually (ACK.ack == data seq), buffers
          out-of-order packets, writes in order, returns once expected_size bytes
          have been written. handle_late_data() re-ACKs retransmissions that
          arrive after completion (lost final ACK).
"""
import heapq
import logging
import time
 
from common.config import ACK, DATA, MAX_RETRIES, RECV_IDLE_TIMEOUT
from common.packet import Packet
from protocols.base import BaseReceiver, BaseSender
 
log = logging.getLogger("rdt.sr")
 
 
class SelectiveRepeatSender(BaseSender):
    def _transmit(self, entry, heap):
        self.send_packet(entry["pkt"])
        entry["sent"] = time.monotonic()
        entry["deadline"] = entry["sent"] + self.current_rto()
        heapq.heappush(heap, (entry["deadline"], entry["pkt"].seq))
 
    def send_file(self, filepath):
        chunks = self.read_chunks(filepath)
        exhausted = False
        inflight = {}      # seq -> entry, sent but not yet ACKed
        acked = set()      # ACKed seqs at or above base (out-of-order ACKs)
        heap = []          # (deadline, seq); stale items skipped lazily
        base = next_seq = 0
 
        while True:
            # 1. fill the window
            while not exhausted and next_seq < base + self.window:
                try:
                    chunk = next(chunks)
                except StopIteration:
                    exhausted = True
                    break
                entry = {"pkt": Packet(DATA, seq=next_seq, payload=chunk),
                         "retries": 0, "retx": False}
                inflight[next_seq] = entry
                self._transmit(entry, heap)
                next_seq += 1
 
            if exhausted and not inflight:
                return
 
            # 2. retransmit packets whose deadline has passed (only those)
            now = time.monotonic()
            timed_out = False
            while heap and heap[0][0] <= now:
                deadline, seq = heapq.heappop(heap)
                entry = inflight.get(seq)
                if entry is None or entry["deadline"] != deadline:
                    continue                       # stale heap item
                entry["retries"] += 1
                if entry["retries"] > MAX_RETRIES:
                    raise ConnectionError(f"seq {seq}: no ACK after {MAX_RETRIES} retries")
                log.debug("timeout seq=%d (retry %d)", seq, entry["retries"])
                entry["retx"] = True               # Karn: never sample this packet
                self.stats.retransmissions += 1
                timed_out = True
                self._transmit(entry, heap)
            if timed_out and self.rto_estimator:
                self.rto_estimator.backoff()       # once per timeout event
 
            # 3. wait for an ACK until the earliest live deadline
            while heap and (heap[0][1] not in inflight or
                            inflight[heap[0][1]]["deadline"] != heap[0][0]):
                heapq.heappop(heap)
            wait = heap[0][0] - time.monotonic() if heap else 0.05
            ack, _ = self.recv_packet(max(0.001, wait))
            if ack is None or ack.ptype != ACK:
                continue
            entry = inflight.pop(ack.ack, None)
            if entry is None:
                continue                           # duplicate / stale ACK
            self.stats.acks_received += 1
            if not entry["retx"]:                  # Karn's algorithm
                rtt = time.monotonic() - entry["sent"]
                self.stats.rtt_samples.append(rtt)
                if self.rto_estimator:
                    self.rto_estimator.update(rtt)
            acked.add(ack.ack)
            while base in acked:
                acked.discard(base)
                base += 1
 
 
class SelectiveRepeatReceiver(BaseReceiver):
    def __init__(self, sock, stats):
        super().__init__(sock, stats)
        self.rcv_base = 0      # next seq to write
        self.buffer = {}       # out-of-order payloads
 
    def receive_file(self, out_path):
        written = 0
        last_activity = time.monotonic()
        with open(out_path, "wb") as f:
            while written < self.expected_size:
                pkt, addr = self.recv_packet(1.0)
                if pkt is None:
                    if time.monotonic() - last_activity > RECV_IDLE_TIMEOUT:
                        raise TimeoutError("receiver idle too long")
                    continue
                last_activity = time.monotonic()
                if pkt.ptype != DATA:
                    continue
                self.send_packet(Packet(ACK, ack=pkt.seq), addr)   # new or duplicate
                if pkt.seq >= self.rcv_base and pkt.seq not in self.buffer:
                    self.buffer[pkt.seq] = pkt.payload
                while self.rcv_base in self.buffer:
                    data = self.buffer.pop(self.rcv_base)
                    f.write(data)
                    written += len(data)
                    self.rcv_base += 1
 
    def handle_late_data(self, pkt, addr):
        if pkt.seq < self.rcv_base:
            self.send_packet(Packet(ACK, ack=pkt.seq), addr)