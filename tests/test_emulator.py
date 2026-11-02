"""Unit tests for the channel emulator (emulator/channel.py), treated as a black box.

    python tests/test_emulator.py

Each test starts the emulator as a separate process with some flags, sends numbered
packets through it from a fake client to a fake server, and checks what comes out.
Flags used (as documented for channel.py):
    --listen-port --server-port --loss --delay --jitter --corrupt --dup --reorder --seed
"""
import os
import socket
import struct
import subprocess
import sys
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EMULATOR = os.path.join(ROOT, "emulator", "channel.py")
WARMUP = 0.8          # seconds to let the emulator start up
QUIET = 0.6           # stop collecting after this long with no new packet


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def make_packet(i):
    """A recognisable test packet: 4-byte number + 60 filler bytes."""
    return struct.pack("!I", i) + bytes([i % 251]) * 60


class Emulator:
    """Context manager: runs channel.py with the given options."""

    def __init__(self, **opts):
        self.listen_port = free_udp_port()
        self.server_port = free_udp_port()
        self.opts = opts

    def __enter__(self):
        cmd = [sys.executable, EMULATOR,
               "--listen-port", str(self.listen_port),
               "--server-port", str(self.server_port)]
        for key, value in self.opts.items():
            cmd += [f"--{key}", str(value)]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(WARMUP)
        if self.proc.poll() is not None:
            out = self.proc.communicate()[0]
            raise RuntimeError(f"emulator exited immediately:\n{out}")
        return self

    def __exit__(self, *exc):
        self.proc.terminate()
        try:
            self.proc.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def collect(sock, quiet=QUIET):
    """Read datagrams until nothing arrives for `quiet` seconds."""
    out = []
    sock.settimeout(quiet)
    while True:
        try:
            data, addr = sock.recvfrom(65535)
        except socket.timeout:
            return out
        except ConnectionResetError:
            continue
        out.append(data)


def send_through(n, **opts):
    """Send n numbered packets client -> emulator -> fake server; return what arrived."""
    with Emulator(**opts) as emu:
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind(("127.0.0.1", emu.server_port))
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            for i in range(n):
                client.sendto(make_packet(i), ("127.0.0.1", emu.listen_port))
                time.sleep(0.001)
            return collect(server)
        finally:
            server.close()
            client.close()


def index_of(packet):
    return struct.unpack("!I", packet[:4])[0]


class EmulatorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(EMULATOR):
            raise unittest.SkipTest("emulator/channel.py not found")

    def test_no_impairment_delivers_everything_in_order(self):
        got = send_through(200, loss=0, seed=1)
        self.assertEqual(got, [make_packet(i) for i in range(200)])

    def test_total_loss_drops_everything(self):
        self.assertEqual(send_through(100, loss=1.0, seed=1), [])

    def test_loss_rate_is_roughly_correct(self):
        n = 400
        got = send_through(n, loss=0.3, seed=1)
        # expect about 70% to arrive (280); allow a wide band (60%..80%)
        self.assertGreater(len(got), 0.6 * n, f"too few arrived: {len(got)}")
        self.assertLess(len(got), 0.8 * n, f"too many arrived: {len(got)}")

    def test_same_seed_gives_identical_results(self):
        """Without reordering, the output must be byte-for-byte identical."""
        opts = dict(loss=0.2, corrupt=0.1, dup=0.1, seed=7)
        first = send_through(200, **opts)
        second = send_through(200, **opts)
        self.assertEqual(first, second, "same seed must reproduce the same behaviour")

    def test_same_seed_same_packets_even_with_reordering(self):
        """Reordering holds packets back by a real-time delay, so the arrival ORDER of held
        packets can shift slightly between runs. The set of packets (what was dropped,
        corrupted, duplicated) must still be identical."""
        opts = dict(loss=0.2, corrupt=0.1, dup=0.1, reorder=0.1, seed=7)
        first = send_through(200, **opts)
        second = send_through(200, **opts)
        self.assertEqual(sorted(first), sorted(second))

    def test_different_seeds_give_different_results(self):
        a = send_through(200, loss=0.3, seed=1)
        b = send_through(200, loss=0.3, seed=2)
        self.assertNotEqual(a, b)

    def test_corruption_changes_packets(self):
        n = 100
        originals = {make_packet(i) for i in range(n)}
        got = send_through(n, corrupt=1.0, seed=1)
        self.assertEqual(len(got), n, "corruption must not remove packets")
        changed = [p for p in got if p not in originals]
        self.assertEqual(len(changed), n, "every packet should have been altered")

    def test_duplication_sends_packets_twice(self):
        n = 100
        got = send_through(n, dup=1.0, seed=1)
        self.assertEqual(len(got), 2 * n)
        self.assertEqual({index_of(p) for p in got}, set(range(n)))

    def test_reordering_changes_arrival_order(self):
        n = 200
        got = send_through(n, reorder=0.3, seed=1)
        indices = [index_of(p) for p in got]
        self.assertGreaterEqual(len(indices), n - 5, "reordering should not lose packets")
        self.assertNotEqual(indices, sorted(indices), "order should have changed")
        self.assertEqual(len(set(indices)), len(indices), "no duplicates expected")

    def test_delay_postpones_delivery(self):
        with Emulator(delay=100, jitter=0, seed=1) as emu:
            server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server.bind(("127.0.0.1", emu.server_port))
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                start = time.monotonic()
                client.sendto(make_packet(1), ("127.0.0.1", emu.listen_port))
                server.settimeout(2)
                server.recvfrom(65535)
                elapsed = time.monotonic() - start
            finally:
                server.close()
                client.close()
        self.assertGreaterEqual(elapsed, 0.08, f"arrived too fast: {elapsed:.3f}s")
        self.assertLess(elapsed, 0.6, f"arrived too slowly: {elapsed:.3f}s")

    def test_reverse_direction_works(self):
        """Server -> client replies must pass back through the emulator."""
        with Emulator(loss=0, seed=1) as emu:
            server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server.bind(("127.0.0.1", emu.server_port))
            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                client.sendto(b"ping", ("127.0.0.1", emu.listen_port))
                server.settimeout(2)
                data, addr = server.recvfrom(65535)
                self.assertEqual(data, b"ping")
                server.sendto(b"pong", addr)          # reply to whoever sent it
                client.settimeout(2)
                reply, _ = client.recvfrom(65535)
                self.assertEqual(reply, b"pong")
            finally:
                server.close()
                client.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)