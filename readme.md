# RDTOUDP: Reliable Data Transfer over UDP

CS-30003, Coding Assignment 1 (Project P3, Path A)
Team: Ahona Roy (2405409), Aishee Maity (2405410), Debasmita Sarkar (2405425), Srotoswini Roy (2405466)

UDP delivers packets with no guarantees: they can be lost, corrupted, duplicated, delayed or reordered. This project builds a reliable file-transfer application on top of UDP using three ARQ (Automatic Repeat reQuest) protocols, and compares their performance under controlled network problems.

**Claim under test:** Selective Repeat achieves higher goodput and fewer retransmissions than Go-Back-N and Stop-and-Wait under moderate to high packet loss.

## Project status

| Component | Owner | Status |
|---|---|---|
| Packet format, checksum, stats, base classes | Member 1 | Done |
| UDP client and server, handshake, SHA-256 check | Member 1 | Done |
| Stop-and-Wait | Member 2 | Basic version working (placeholder, to be finalised) |
| Go-Back-N | Member 2 | In progress |
| Selective Repeat | Member 3 | In progress |
| Adaptive RTO (Jacobson/Karels + Karn) | Member 3 | In progress |
| Channel emulator | Member 4 | In progress |
| Experiments and graphs | Member 4 | Not started |

Update this table as work lands.

## Requirements

- Python 3.8 or newer (tested on Python 3.14)
- No packages are needed to run transfers or tests (standard library only)
- `matplotlib` and `numpy` are needed later for plotting graphs:

```
pip install -r requirements.txt
```

Works on Windows, macOS and Linux. On Windows use `python` (or `py`) in PowerShell.

## Folder structure

```
RDTOUDP/
├── app/
│   ├── server.py            receiver: waits for a client, saves the file, checks integrity
│   ├── client.py            sender: sends a file with the chosen protocol
│   └── registry.py          maps --protocol saw|gbn|sr to classes
├── common/
│   ├── config.py            shared constants (packet size, retries, packet types)
│   ├── packet.py            packet format: pack() / unpack() with checksum
│   ├── checksum.py          CRC32
│   ├── net.py               socket helpers (corrupt packets are discarded and counted)
│   ├── stats.py             counters, goodput, CSV export
│   └── integrity.py         SHA-256 of a file
├── protocols/
│   ├── base.py              BaseSender / BaseReceiver that every protocol inherits
│   ├── stop_and_wait.py
│   ├── go_back_n.py
│   ├── selective_repeat.py
│   └── rto.py               adaptive RTO estimator
├── emulator/
│   └── channel.py           network emulator (loss, corruption, duplication, reordering, delay)
├── experiments/
│   ├── run_experiments.py
│   └── plot_results.py
├── tools/
│   └── make_testfile.py     create reproducible random test files
├── tests/
│   ├── test_packet.py       packet unit tests
│   └── test_end_to_end.py   full client/server transfer tests
├── results/                 CSV output from experiments
├── docs/
├── requirements.txt
└── README.md
```

## Quick start

**1. Create a test file** (replaces the Linux `head` command, works everywhere):

```
python tools/make_testfile.py --size 1MB --out test.bin
```

**2. Start the server** in one terminal:

```
python app/server.py --port 9000 --protocol saw --out received.bin
```

**3. Run the client** in a second terminal:

```
python app/client.py --port 9000 --protocol saw --file test.bin --rto 0.2
```

Both sides print `INTEGRITY: OK` when the received file's SHA-256 matches the original. Replace `saw` with `gbn` or `sr` to use the other protocols once they are available.

To test under bad network conditions, start the channel emulator between them and point the client's `--port` at the emulator instead of the server.

## Command-line options

### Server (`app/server.py`)

| Option | Default | Meaning |
|---|---|---|
| `--protocol` | required | `saw`, `gbn` or `sr` (must match the client) |
| `--port` | 9000 | UDP port to listen on |
| `--bind` | 0.0.0.0 | address to bind |
| `--out` | received.bin | where to save the received file |
| `--idle-timeout` | 120 | seconds to wait for a client to connect |
| `--fin-timeout` | 30 | seconds to wait for the final FIN after all data arrived |
| `--linger` | 2 | seconds to keep answering FIN after FIN-ACK (covers a lost FIN-ACK) |
| `--stats-out` | none | append a CSV row of statistics to this file |
| `--tag` | empty | free-text label stored in the CSV row |
| `-v` | off | verbose logging |

### Client (`app/client.py`)

| Option | Default | Meaning |
|---|---|---|
| `--protocol` | required | `saw`, `gbn` or `sr` |
| `--file` | required | file to send |
| `--host` | 127.0.0.1 | server (or emulator) address |
| `--port` | 9000 | server (or emulator) port |
| `--window` | 8 | window size (used by Go-Back-N and Selective Repeat) |
| `--rto` | 0.5 | fixed retransmission timeout in seconds, also the handshake timeout |
| `--adaptive` | off | use adaptive RTO from `protocols/rto.py` |
| `--stats-out` | none | append a CSV row of statistics to this file |
| `--tag` | empty | free-text label stored in the CSV row |
| `-v` | off | verbose logging |

**Client exit codes:** 0 = transferred and verified, 1 = hash mismatch, 2 = FIN-ACK never arrived (integrity unconfirmed), 3 = connection or transfer failure.

## How it works

### Packet format

Every packet has a 15-byte header (network byte order), followed by up to 1024 bytes of data:

| Field | Size | Meaning |
|---|---|---|
| type | 1 byte | DATA, ACK, SACK, SYN, SYN-ACK, FIN, FIN-ACK |
| seq | 4 bytes | packet number |
| ack | 4 bytes | acknowledgment number |
| length | 2 bytes | number of data bytes |
| checksum | 4 bytes | CRC32 over all other fields and the data |
| payload | 0 to 1024 bytes | file data |

`Packet.unpack()` returns `None` for any packet that is truncated or fails the checksum, so corrupted packets are simply discarded and the ARQ protocol recovers by retransmission. With a 4-byte sequence number a 100 MB file never wraps around, so wraparound handling is not needed.

### Connection lifecycle

```
Client                                   Server
  | -- SYN (file name, size, protocol) -->  |
  | <------------------------- SYN-ACK ---  |
  | ============ DATA / ACK exchange =====  |   (Stop-and-Wait, GBN or SR)
  | -- FIN (SHA-256 of original file) --->  |   server hashes received file, compares
  | <---------------- FIN-ACK (OK/FAIL) --  |
```

SYN and FIN are retried on timeout, and the server keeps answering for a short time so a lost FIN-ACK does not leave the client stuck.

### Statistics

Both programs print a summary. With `--stats-out results/runs.csv` each run appends one row with: bytes delivered, transfer time, goodput (Mbps), packets sent, retransmissions, retransmissions per packet, ACKs received, corrupted packets dropped, and average RTT. The experiment scripts read these files.

## Adding or changing a protocol (for teammates)

Inherit from the classes in `protocols/base.py` and keep these exact class names, because `app/registry.py` looks them up:

| File | Classes |
|---|---|
| `protocols/stop_and_wait.py` | `StopAndWaitSender`, `StopAndWaitReceiver` |
| `protocols/go_back_n.py` | `GoBackNSender`, `GoBackNReceiver` |
| `protocols/selective_repeat.py` | `SelectiveRepeatSender`, `SelectiveRepeatReceiver` |

**Sender** implements `send_file(filepath)`. Available helpers: `self.send_packet(pkt)`, `self.recv_packet(timeout)`, `self.read_chunks(filepath)`, `self.current_rto()`. Count every retransmission in `self.stats.retransmissions`, and raise `ConnectionError` after `MAX_RETRIES` failed attempts.

**Receiver** implements `receive_file(out_path)`. `self.expected_size` is set before the call; return once that many bytes have been written in order. Reply with `self.send_packet(pkt, addr)`. Override `handle_late_data(pkt, addr)` to re-ACK data that arrives after you finished, so a lost final ACK does not stall the sender.

**RTO estimator** (`protocols/rto.py`) must define `RTOEstimator` with an `.rto` value and `.update(rtt)` and `.backoff()` methods. Only take RTT samples from packets that were not retransmitted (Karn's algorithm).

Use a single-threaded loop with socket timeouts rather than one thread per timer. It is simpler and more accurate in Python.

## Testing

```
python tests/test_packet.py          # packet format and corruption detection
python tests/test_end_to_end.py      # real client + server transfers per protocol
```

`test_end_to_end.py` runs each protocol through empty, 1-byte, exact-multiple, 100 KB and lossy-channel transfers and compares file hashes. A protocol is skipped automatically until its sender and receiver classes exist, so it starts covering Go-Back-N and Selective Repeat as soon as they are pushed.

**Run both test files before opening a pull request.**

## Channel emulator and experiments

The emulator sits between client and server and applies, per packet and in both directions, with a fixed seed so runs are repeatable:

- packet loss, corruption, duplication, reordering
- delay with jitter

Planned usage (flags may change slightly when the emulator is finished):

```
python emulator/channel.py --listen 8000 --target 9000 --loss 0.1 --corrupt 0.01 --dup 0.01 --reorder 0.05 --delay 20 --jitter 5 --seed 42
python app/client.py --port 8000 --protocol sr --file test.bin
```

Experiments (loss rate, window size, reordering, delay, RTO) are run with `experiments/run_experiments.py`, each setting repeated with several seeds, and graphed with `experiments/plot_results.py`.

**Metric definitions used in the report:**
- **Goodput** = file bytes delivered / total transfer time (headers and retransmissions excluded)
- **Retransmissions** are also reported per delivered packet, since Stop-and-Wait only ever resends one packet at a time
- **Moderate to high loss** = 5% packet loss and above

## Team workflow

- Nobody pushes directly to `main`. Work on your own branch, for example `member2/go-back-n`.
- Open a pull request and get one teammate to review before merging.
- Run `git pull origin main` at the start of every work session.
- Only edit your own files. If you need a change in someone else's file, ask them.
- Commit small and often.
- Use a fixed seed when debugging so a failing run can be reproduced exactly.

```
git checkout -b member2/go-back-n
git add .
git commit -m "Implement Go-Back-N sender and receiver"
git push origin member2/go-back-n
```

| Member | Responsibility |
|---|---|
| 1 | UDP client/server, packet format, checksum, handshake, integrity check |
| 2 | Stop-and-Wait and Go-Back-N |
| 3 | Selective Repeat, timers, adaptive RTO |
| 4 | Channel emulator, experiments, plotting, documentation, integration |

## Troubleshooting

- **`head` is not recognized (Windows):** use `python tools/make_testfile.py` instead.
- **`python` is not recognized:** try `py`.
- **Client says no SYN-ACK:** start the server first, and check that the port and `--protocol` match on both sides.
- **"Protocol ... is not available yet":** that protocol's file has not been pushed yet; pull the latest `main`.
- **`--adaptive` fails:** `protocols/rto.py` is not written yet.
- **Windows firewall prompt:** allow Python on private networks, or test on `127.0.0.1`.