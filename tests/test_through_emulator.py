"""End-to-end transfers THROUGH the real channel emulator.

    python tests/test_through_emulator.py

Layout:  client  ->  emulator (listen port)  ->  server
A protocol is skipped automatically until its classes exist, so this file covers
Go-Back-N and Selective Repeat as soon as they are pushed.
"""
import os
import random
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_end_to_end as e2e  # reuses its helpers; importing the module keeps its tests out of this run

ROOT = e2e.ROOT
EMULATOR = os.path.join(ROOT, "emulator", "channel.py")

# name -> (emulator flags, client RTO in seconds)
SCENARIOS = {
    "loss10":  (dict(loss=0.10), 0.05),
    "loss20":  (dict(loss=0.20), 0.05),
    "reorder": (dict(reorder=0.10), 0.05),
    "delay":   (dict(delay=20, jitter=5), 0.2),
    "mixed":   (dict(loss=0.10, corrupt=0.02, dup=0.03, reorder=0.05, delay=10, jitter=3), 0.2),
}
SIZE = 50_000


def run_via_emulator(protocol, emu_opts, rto, size=SIZE, window=8, seed=42):
    tmp = tempfile.TemporaryDirectory()
    src = os.path.join(tmp.name, "src.bin")
    dst = os.path.join(tmp.name, "dst.bin")
    with open(src, "wb") as f:
        f.write(random.Random(99).getrandbits(8 * size).to_bytes(size, "little"))

    server_port, emu_port = e2e.free_udp_port(), e2e.free_udp_port()
    emu_cmd = [sys.executable, EMULATOR, "--listen-port", str(emu_port),
               "--server-port", str(server_port), "--seed", str(seed)]
    for key, value in emu_opts.items():
        emu_cmd += [f"--{key}", str(value)]

    emu = subprocess.Popen(emu_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    server = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "app", "server.py"),
         "--bind", "127.0.0.1", "--port", str(server_port), "--protocol", protocol,
         "--out", dst, "--linger", "0.5", "--idle-timeout", "30", "--fin-timeout", "60"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        time.sleep(0.8)  # let emulator and server start
        client = subprocess.run(
            [sys.executable, os.path.join(ROOT, "app", "client.py"),
             "--host", "127.0.0.1", "--port", str(emu_port), "--protocol", protocol,
             "--file", src, "--window", str(window), "--rto", str(rto)],
            capture_output=True, text=True, timeout=180)
        server_out, _ = server.communicate(timeout=60)
    finally:
        for proc in (server, emu):
            if proc.poll() is None:
                proc.kill()
            proc.wait()
    return client, server_out, src, dst, tmp


class ThroughEmulator(unittest.TestCase):
    def check(self, protocol, scenario):
        if not os.path.exists(EMULATOR):
            self.skipTest("emulator/channel.py not found")
        if not e2e.protocol_ready(protocol):
            module, s, r = e2e.PROTOCOLS[protocol]
            self.skipTest(f"{module} has no {s}/{r} yet")
        emu_opts, rto = SCENARIOS[scenario]
        client, server_out, src, dst, tmp = run_via_emulator(protocol, emu_opts, rto)
        try:
            detail = f"\n--- client ---\n{client.stdout}{client.stderr}\n--- server ---\n{server_out}"
            self.assertEqual(client.returncode, 0, "client failed" + detail)
            self.assertIn("INTEGRITY: OK", client.stdout + server_out, detail)
            self.assertEqual(e2e.sha256(src), e2e.sha256(dst), "file contents differ" + detail)
        finally:
            tmp.cleanup()


def _make(protocol, scenario):
    def test(self):
        self.check(protocol, scenario)
    return test


for _proto in e2e.PROTOCOLS:
    for _name in SCENARIOS:
        setattr(ThroughEmulator, f"test_{_proto}_{_name}", _make(_proto, _name))


if __name__ == "__main__":
    unittest.main(verbosity=2)