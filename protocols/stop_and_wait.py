"""Minimal reference Stop-and-Wait (stand-in so the app runs end to end).

Member 2 owns this file and may replace it; keep the class names and the
BaseSender / BaseReceiver interface unchanged.
"""
import logging
import time

from common.config import ACK, DATA, MAX_RETRIES, RECV_IDLE_TIMEOUT
from common.packet import Packet
from protocols.base import BaseReceiver, BaseSender

log = logging.getLogger("rdt.saw")


class StopAndWaitSender(BaseSender):
    def send_file(self, filepath):
        for seq, chunk in enumerate(self.read_chunks(filepath)):
            pkt = Packet(DATA, seq=seq, payload=chunk)
            retransmitted = False
            for attempt in range(MAX_RETRIES + 1):
                self.send_packet(pkt)
                sent_at = time.monotonic()
                deadline = sent_at + self.current_rto()
                acked = False
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    ack, _ = self.recv_packet(remaining)
                    if ack is None:
                        break
                    if ack.ptype == ACK and ack.ack == seq:
                        acked = True
                        self.stats.acks_received += 1
                        if not retransmitted:              # Karn's algorithm
                            rtt = time.monotonic() - sent_at
                            self.stats.rtt_samples.append(rtt)
                            if self.rto_estimator:
                                self.rto_estimator.update(rtt)
                        break
                if acked:
                    break
                log.debug("timeout seq=%d (attempt %d)", seq, attempt + 1)
                self.stats.retransmissions += 1
                retransmitted = True
                if self.rto_estimator:
                    self.rto_estimator.backoff()
            else:
                raise ConnectionError(f"seq {seq}: no ACK after {MAX_RETRIES} retries")


class StopAndWaitReceiver(BaseReceiver):
    def __init__(self, sock, stats):
        super().__init__(sock, stats)
        self.next_seq = 0

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
                if pkt.seq == self.next_seq:
                    f.write(pkt.payload)
                    written += len(pkt.payload)
                    self.next_seq += 1
                    self.send_packet(Packet(ACK, ack=pkt.seq), addr)
                elif pkt.seq < self.next_seq:               # duplicate: re-ACK only
                    self.send_packet(Packet(ACK, ack=pkt.seq), addr)

    def handle_late_data(self, pkt, addr):
        if pkt.seq < self.next_seq:
            self.send_packet(Packet(ACK, ack=pkt.seq), addr)
