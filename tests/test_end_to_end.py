"""End-to-end tests: run the real server and client as separate processes and
check the received file is identical. Works on Windows, Mac and Linux.

    python tests/test_end_to_end.py

A test is automatically SKIPPED while a protocol's file does not exist yet, so
this file can be committed now and starts covering GBN / SR as soon as they land.

Scenarios per protocol:
  clean_*   direct client -> server, no network problems
  lossy     through a built-in proxy: 10% loss, 2% corruption, 3% duplication
"""
import hashlib
import importlib
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.registry import PROTOCOLS  # noqa: E402  (name -> (module, SenderClass, ReceiverClass))

PROTOCOL_MODULES = {name: spec[0] for name, spec in PROTOCOLS.items()}


def protocol_ready(protocol):
    """True only when the module exists AND defines both required classes.
    An empty placeholder file counts as 'not written yet'."""
    module, sender_cls, receiver_cls = PROTOCOLS[protocol]
    try:
        mod = importlib.import_module(module)
    except ImportError:
        return False
    return hasattr(mod, sender_cls) and hasattr(mod, receiver_cls)


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


class LossyProxy(threading.Thread):
    """Tiny stand-in for the channel emulator: sits between client and server."""

    def __init__(self, listen_port, server_port, loss, corrupt, dup, seed=42):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", listen_port))
        self.sock.settimeout(0.2)
        self.server = ("127.0.0.1", server_port)
        self.client = None
        self.loss, self.corrupt, self.dup = loss, corrupt, dup
        self.rng = random.Random(seed)
        self.stop_flag = threading.Event()

    def run(self):
        while not self.stop_flag.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue
            if addr == self.server:
                dest = self.client
            else:
                self.client, dest = addr, self.server
            if dest is None or self.rng.random() < self.loss:
                continue
            if self.rng.random() < self.corrupt:
                b = bytearray(data)
                b[self.rng.randrange(len(b))] ^= 0xFF
                data = bytes(b)
            self.sock.sendto(data, dest)
            if self.rng.random() < self.dup:
                self.sock.sendto(data, dest)

    def stop(self):
        self.stop_flag.set()
        self.join(timeout=2)
        self.sock.close()


def run_transfer(protocol, size, loss=0.0, corrupt=0.0, dup=0.0, window=8, rto=0.05):
    """Returns (client_proc, server_proc, original_path, received_path, tmpdir)."""
    tmp = tempfile.TemporaryDirectory()
    src = os.path.join(tmp.name, "src.bin")
    dst = os.path.join(tmp.name, "dst.bin")
    rng = random.Random(1234)
    with open(src, "wb") as f:
        f.write(rng.getrandbits(8 * size).to_bytes(size, "little") if size else b"")

    server_port = free_udp_port()
    proxy = None
    client_port = server_port
    if loss or corrupt or dup:
        client_port = free_udp_port()
        proxy = LossyProxy(client_port, server_port, loss, corrupt, dup)
        proxy.start()

    server = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "app", "server.py"),
         "--bind", "127.0.0.1", "--port", str(server_port), "--protocol", protocol,
         "--out", dst, "--linger", "0.5", "--idle-timeout", "20", "--fin-timeout", "30"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        client = subprocess.run(
            [sys.executable, os.path.join(ROOT, "app", "client.py"),
             "--host", "127.0.0.1", "--port", str(client_port), "--protocol", protocol,
             "--file", src, "--window", str(window), "--rto", str(rto)],
            capture_output=True, text=True, timeout=120)
        server_out, _ = server.communicate(timeout=30)
    finally:
        if server.poll() is None:
            server.kill()
        if proxy:
            proxy.stop()
    server.output = server_out
    return client, server, src, dst, tmp


class EndToEnd(unittest.TestCase):
    def check(self, protocol, size, **net):
        if not protocol_ready(protocol):
            module, sender_cls, receiver_cls = PROTOCOLS[protocol]
            self.skipTest(f"{module} has no {sender_cls}/{receiver_cls} yet")
        client, server, src, dst, tmp = run_transfer(protocol, size, **net)
        try:
            detail = f"\n--- client ---\n{client.stdout}{client.stderr}\n--- server ---\n{server.output}"
            self.assertEqual(client.returncode, 0, "client failed" + detail)
            self.assertIn("INTEGRITY: OK", client.stdout + server.output, detail)
            self.assertTrue(os.path.exists(dst), "no output file" + detail)
            self.assertEqual(sha256(src), sha256(dst), "file contents differ" + detail)
        finally:
            tmp.cleanup()


SCENARIOS = {
    "clean_empty": dict(size=0),
    "clean_1_byte": dict(size=1),
    "clean_exact_multiple": dict(size=4096),
    "clean_100kb": dict(size=100_000),
    "lossy": dict(size=60_000, loss=0.10, corrupt=0.02, dup=0.03),
}


def _make(protocol, kwargs):
    def test(self):
        self.check(protocol, **kwargs)
    return test


for _proto in PROTOCOL_MODULES:
    for _name, _kw in SCENARIOS.items():
        setattr(EndToEnd, f"test_{_proto}_{_name}", _make(_proto, _kw))


if __name__ == "__main__":
    unittest.main(verbosity=2)