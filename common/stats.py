import csv
import os
import time


class Stats:
    def __init__(self):
        self.packets_sent = 0
        self.retransmissions = 0
        self.acks_received = 0
        self.corrupted_dropped = 0
        self.rtt_samples = []
        self.bytes_delivered = 0
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.monotonic()

    def stop(self):
        self.end_time = time.monotonic()

    def duration(self):
        if self.start_time is None or self.end_time is None:
            return 0.0
        return self.end_time - self.start_time

    def goodput_mbps(self):
        t = self.duration()
        return (self.bytes_delivered * 8) / t / 1e6 if t > 0 else 0.0

    def avg_rtt_ms(self):
        return 1000 * sum(self.rtt_samples) / len(self.rtt_samples) if self.rtt_samples else 0.0

    def summary(self) -> dict:
        unique = max(self.packets_sent - self.retransmissions, 1)
        return {
            "bytes_delivered": self.bytes_delivered,
            "transfer_time_s": round(self.duration(), 4),
            "goodput_mbps": round(self.goodput_mbps(), 4),
            "packets_sent": self.packets_sent,
            "retransmissions": self.retransmissions,
            "retx_per_packet": round(self.retransmissions / unique, 4),
            "acks_received": self.acks_received,
            "corrupted_dropped": self.corrupted_dropped,
            "avg_rtt_ms": round(self.avg_rtt_ms(), 3),
        }


def append_csv(path, row: dict):
    """Append one result row to a CSV file (writes the header if the file is new)."""
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)
