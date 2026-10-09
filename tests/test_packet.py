import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import ACK, DATA, PACKET_SIZE
from common.packet import Packet


class TestPacket(unittest.TestCase):

    def test_roundtrip(self):
        """Pack then unpack gives back identical fields."""
        p = Packet(DATA, seq=7, ack=3, payload=b"hello world")
        q = Packet.unpack(p.pack())
        self.assertIsNotNone(q)
        self.assertEqual((q.ptype, q.seq, q.ack, q.payload), (DATA, 7, 3, b"hello world"))

    def test_corruption_detected(self):
        """Flipping any single byte must make unpack return None."""
        raw = bytearray(Packet(DATA, seq=1, payload=os.urandom(200)).pack())
        for i in range(len(raw)):          # try every byte position
            bad = bytearray(raw)
            bad[i] ^= 0xFF
            self.assertIsNone(Packet.unpack(bytes(bad)), f"byte {i} not detected")

    def test_random_byte_flip(self):
        random.seed(1)
        raw = bytearray(Packet(DATA, seq=5, payload=b"x" * 500).pack())
        for _ in range(100):
            bad = bytearray(raw)
            bad[random.randrange(len(bad))] ^= random.randint(1, 255)
            self.assertIsNone(Packet.unpack(bytes(bad)))

    def test_truncated(self):
        """A packet cut short (header or payload) returns None."""
        raw = Packet(DATA, seq=2, payload=b"abcdef").pack()
        for n in (0, 1, 10, 14, len(raw) - 1):
            self.assertIsNone(Packet.unpack(raw[:n]))

    def test_empty_payload(self):
        q = Packet.unpack(Packet(ACK, ack=9).pack())
        self.assertIsNotNone(q)
        self.assertEqual((q.ptype, q.ack, q.payload), (ACK, 9, b""))

    def test_max_payload(self):
        data = os.urandom(PACKET_SIZE)
        q = Packet.unpack(Packet(DATA, seq=123456, payload=data).pack())
        self.assertIsNotNone(q)
        self.assertEqual(q.payload, data)
        self.assertEqual(q.seq, 123456)


if __name__ == "__main__":
    unittest.main(verbosity=2)