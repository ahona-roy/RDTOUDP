"""Packet format (network byte order), 15-byte header:

    type(1) | seq(4) | ack(4) | length(2) | checksum(4) | payload(0..1024)

The checksum is CRC32 over type+seq+ack+length+payload.
"""
import struct

from common.checksum import compute_checksum

HEADER_FMT = "!BIIHI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 15
_PRE_CSUM_FMT = "!BIIH"


class Packet:
    def __init__(self, ptype, seq=0, ack=0, payload=b""):
        self.ptype = ptype
        self.seq = seq
        self.ack = ack
        self.payload = payload

    def pack(self) -> bytes:
        head = struct.pack(_PRE_CSUM_FMT, self.ptype, self.seq, self.ack, len(self.payload))
        csum = compute_checksum(head + self.payload)
        return head + struct.pack("!I", csum) + self.payload

    @staticmethod
    def unpack(raw: bytes):
        """Return a Packet, or None if the bytes are malformed or corrupted."""
        if len(raw) < HEADER_SIZE:
            return None
        ptype, seq, ack, length, csum = struct.unpack(HEADER_FMT, raw[:HEADER_SIZE])
        payload = raw[HEADER_SIZE:]
        if len(payload) != length:
            return None
        if compute_checksum(raw[:HEADER_SIZE - 4] + payload) != csum:
            return None
        return Packet(ptype, seq, ack, payload)

    def __repr__(self):
        from common.config import TYPE_NAMES
        return f"<{TYPE_NAMES.get(self.ptype, self.ptype)} seq={self.seq} ack={self.ack} len={len(self.payload)}>"
