"""Small socket helpers shared by the app and the protocols."""
import socket
import time

from common.packet import Packet


def send_raw(sock, pkt, addr):
    sock.sendto(pkt.pack(), addr)


def recv_valid(sock, timeout, stats=None):
    """Wait up to `timeout` seconds for a *valid* packet.

    Returns (packet, addr), or (None, None) on timeout. Corrupted packets are
    silently discarded (and counted in stats.corrupted_dropped if given).
    """
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None, None
        sock.settimeout(remaining)
        try:
            raw, addr = sock.recvfrom(65535)
        except socket.timeout:
            return None, None
        except ConnectionResetError:  # Windows: ICMP "port unreachable" on UDP
            continue
        pkt = Packet.unpack(raw)
        if pkt is None:
            if stats is not None:
                stats.corrupted_dropped += 1
            continue
        return pkt, addr
