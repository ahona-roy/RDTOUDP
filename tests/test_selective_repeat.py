import hashlib
import os
import random
import socket
import tempfile
import threading
import types
 
from common.packet import Packet, DATA
from common.config import PACKET_SIZE
from protocols.rto import RTOEstimator
from protocols.selective_repeat import SelectiveRepeatSender, SelectiveRepeatReceiver
 
 
def new_stats():
    return types.SimpleNamespace(packets_sent=0, retransmissions=0,
                                 acks_received=0, rtt_samples=[],
                                 start_time=None, end_time=None)
 
 
class LossySocket:
    """Wraps a UDP socket; sendto() drops according to drop_fn(raw)."""
    def __init__(self, sock, drop_fn):
        self._s, self._drop = sock, drop_fn
 
    def sendto(self, raw, addr):
        if not self._drop(raw):
            self._s.sendto(raw, addr)
 
    def __getattr__(self, name):
        return getattr(self._s, name)
 
 
def transfer(size, drop_data, drop_ack, window=8):
    tmp = tempfile.mkdtemp()
    src, dst = os.path.join(tmp, "in.bin"), os.path.join(tmp, "out.bin")
    with open(src, "wb") as f:
        f.write(os.urandom(size))
 
    rs, ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM), \
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rs.bind(("127.0.0.1", 0))
    ss.bind(("127.0.0.1", 0))
 
    stats = new_stats()
    rto = RTOEstimator(initial_rto=0.05, fixed=True)
    sender = SelectiveRepeatSender(LossySocket(ss, drop_data), rs.getsockname(),
                                   window, stats, rto=rto)
    receiver = SelectiveRepeatReceiver(LossySocket(rs, drop_ack), new_stats(),
                                       expected_size=size)
    t = threading.Thread(target=receiver.receive_file, args=(dst,))
    t.start()
    sender.send_file(src)
    t.join(timeout=20)
    sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()
    return sha(src) == sha(dst), stats
 
 
def test_drop_packet_5_resends_only_5():
    dropped = []
 
    def drop_data(raw):
        pkt = Packet.unpack(raw)
        if pkt.type == DATA and pkt.seq == 5 and not dropped:
            dropped.append(1)
            return True
        return False
 
    ok, stats = transfer(20 * PACKET_SIZE, drop_data, lambda r: False)
    assert ok
    assert stats.retransmissions == 1
 
 
def test_random_loss_both_directions():
    rng = random.Random(42)
    ok, stats = transfer(100 * PACKET_SIZE + 123,
                         lambda r: rng.random() < 0.2,
                         lambda r: rng.random() < 0.2)
    assert ok
    assert stats.retransmissions > 0
 
 
def test_empty_file():
    ok, _ = transfer(0, lambda r: False, lambda r: False)
    assert ok
 
 
def test_rto_estimator():
    e = RTOEstimator(initial_rto=0.5)
    e.update(0.1)
    assert abs(e.rto - 0.3) < 1e-9          # srtt=.1, rttvar=.05 -> .3
    e.backoff()
    assert abs(e.rto - 0.6) < 1e-9
    for _ in range(20):
        e.backoff()
    assert e.rto == 5.0                      # clamped
    f = RTOEstimator(initial_rto=0.5, fixed=True)
    f.update(0.01)
    f.backoff()
    assert f.rto == 0.5