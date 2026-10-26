"""Selective Repeat + RTOEstimator tests. Run: python -m pytest tests/test_selective_repeat.py -v
(or: python tests/test_selective_repeat.py)"""
import hashlib
import os
import random
import socket
import sys
import tempfile
import threading
 
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
 
from common.config import DATA, PACKET_SIZE
from common.packet import Packet
from common.stats import Stats
from protocols.rto import RTOEstimator
from protocols.selective_repeat import SelectiveRepeatReceiver, SelectiveRepeatSender
 
 
class LossySocket:
    """Wraps a UDP socket; sendto() silently drops when drop_fn(raw) is True."""
    def __init__(self, sock, drop_fn):
        self._s, self._drop = sock, drop_fn
 
    def sendto(self, raw, addr):
        if not self._drop(raw):
            self._s.sendto(raw, addr)
 
    def __getattr__(self, name):
        return getattr(self._s, name)
 
 
def _sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()
 
 
def transfer(size, drop_data, drop_ack, window=8, adaptive=False):
    tmp = tempfile.mkdtemp()
    src, dst = os.path.join(tmp, "in.bin"), os.path.join(tmp, "out.bin")
    with open(src, "wb") as f:
        f.write(os.urandom(size))
    rs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rs.bind(("127.0.0.1", 0))
    ss.bind(("127.0.0.1", 0))
 
    s_stats, r_stats = Stats(), Stats()
    est = RTOEstimator(initial_rto=0.1) if adaptive else None
    sender = SelectiveRepeatSender(LossySocket(ss, drop_data), rs.getsockname(),
                                   window, s_stats, rto_estimator=est, fixed_rto=0.1)
    receiver = SelectiveRepeatReceiver(LossySocket(rs, drop_ack), r_stats)
    receiver.expected_size = size
    sender_done = threading.Event()
 
    def serve():
        # mimics app/server.py: receive the file, then keep answering late data
        receiver.receive_file(dst)
        while not sender_done.is_set():
            pkt, addr = receiver.recv_packet(0.05)
            if pkt is not None and pkt.ptype == DATA:
                receiver.handle_late_data(pkt, addr)
 
    t = threading.Thread(target=serve)
    t.start()
    try:
        sender.send_file(src)
    finally:
        sender_done.set()
        t.join(timeout=30)
    ok = (not t.is_alive()) and _sha(src) == _sha(dst)
    return ok, s_stats
 
 
def test_drop_packet_5_resends_only_5():
    dropped = []
 
    def drop_data(raw):
        p = Packet.unpack(raw)
        if p and p.ptype == DATA and p.seq == 5 and not dropped:
            dropped.append(1)
            return True
        return False
 
    ok, stats = transfer(20 * PACKET_SIZE, drop_data, lambda r: False)
    assert ok
    assert stats.retransmissions == 1
 
 
def test_random_loss_both_directions_fixed_rto():
    rng = random.Random(42)
    ok, stats = transfer(100 * PACKET_SIZE + 123, lambda r: rng.random() < 0.2,
                         lambda r: rng.random() < 0.2)
    assert ok and stats.retransmissions > 0
 
 
def test_random_loss_adaptive_rto():
    rng = random.Random(7)
    ok, stats = transfer(100 * PACKET_SIZE + 5, lambda r: rng.random() < 0.15,
                         lambda r: rng.random() < 0.15, adaptive=True)
    assert ok and len(stats.rtt_samples) > 0
 
 
def test_empty_file():
    ok, _ = transfer(0, lambda r: False, lambda r: False)
    assert ok
 
 
def test_one_byte_file():
    ok, _ = transfer(1, lambda r: False, lambda r: False)
    assert ok
 
 
def test_rto_estimator():
    e = RTOEstimator(initial_rto=0.5)
    e.update(0.1)
    assert abs(e.rto - 0.3) < 1e-9          # srtt=.1, rttvar=.05 -> .3
    e.backoff()
    assert abs(e.rto - 0.6) < 1e-9
    for _ in range(20):
        e.backoff()
    assert e.rto == 5.0                      # clamped at 5 s
    f = RTOEstimator(initial_rto=0.5, fixed=True)
    f.update(0.01)
    f.backoff()
    assert f.rto == 0.5
 
 
if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok  ", name)
    print("ALL PASSED")
 