"""Go-Back-N ARQ protocol implementation."""

import logging
import time

from common.config import ACK, DATA, MAX_RETRIES, RECV_IDLE_TIMEOUT
from common.packet import Packet
from protocols.base import BaseReceiver, BaseSender

log = logging.getLogger("rdt.gbn")


class GoBackNSender(BaseSender):
    def send_file(self, filepath):
        chunks = iter(self.read_chunks(filepath))

        base = 0
        next_seq = 0
        outstanding = {}
        retries = {}
        timer_start = None
        eof = False

        while True:
            while not eof and next_seq < base + max(1, self.window):
                try:
                    chunk = next(chunks)
                except StopIteration:
                    eof = True
                    break

                pkt = Packet(DATA, seq=next_seq, payload=chunk)
                outstanding[next_seq] = pkt
                retries[next_seq] = 0

                self.send_packet(pkt)

                if base == next_seq:
                    timer_start = time.monotonic()

                next_seq += 1

            if eof and not outstanding:
                return

            if not outstanding:
                continue

            elapsed = time.monotonic() - timer_start
            remaining = self.current_rto() - elapsed

            if remaining <= 0:
                ack = None
            else:
                ack, _ = self.recv_packet(remaining)

            if ack is not None and ack.ptype == ACK:
                ack_num = ack.ack

                if base <= ack_num < next_seq:
                    for seq in range(base, ack_num + 1):
                        outstanding.pop(seq, None)
                        retries.pop(seq, None)

                    base = ack_num + 1

                    if outstanding:
                        timer_start = time.monotonic()
                    else:
                        timer_start = None

                    self.stats.acks_received += 1
                    continue

            if ack is None or remaining <= 0:
                log.debug(
                    "Timeout: retransmitting window from seq=%d",
                    base
                )

                for seq in sorted(outstanding):
                    if retries[seq] >= MAX_RETRIES:
                        raise ConnectionError(
                            f"seq {seq}: no ACK after {MAX_RETRIES} retries"
                        )

                for seq in sorted(outstanding):
                    self.send_packet(outstanding[seq])
                    retries[seq] += 1
                    self.stats.retransmissions += 1

                if self.rto_estimator:
                    self.rto_estimator.backoff()

                timer_start = time.monotonic()


class GoBackNReceiver(BaseReceiver):
    def __init__(self, sock, stats):
        super().__init__(sock, stats)
        self.expected_seq = 0

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

                if pkt.seq == self.expected_seq:
                    remaining = self.expected_size - written
                    payload = pkt.payload[:remaining]

                    f.write(payload)
                    written += len(payload)
                    self.expected_seq += 1

                if self.expected_seq > 0:
                    self.send_packet(
                        Packet(ACK, ack=self.expected_seq - 1),
                        addr
                    )

    def handle_late_data(self, pkt, addr):
        if pkt.ptype == DATA and self.expected_seq > 0:
            self.send_packet(
                Packet(ACK, ack=self.expected_seq - 1),
                addr
            )