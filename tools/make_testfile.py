#!/usr/bin/env python3
"""Create a reproducible random test file (works on Windows, Mac and Linux).

    python tools/make_testfile.py --size 1MB --out test1mb.bin
    python tools/make_testfile.py --size 100MB --out test100mb.bin --seed 7

Same --size and --seed always give the identical file, so the whole team can
compare results on the same data.
"""
import argparse
import random
import re
import sys

UNITS = {"": 1, "B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}


def parse_size(text: str) -> int:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMG]?B?)\s*", text.upper())
    if not m:
        raise argparse.ArgumentTypeError(f"bad size '{text}' (try 500, 100KB, 1MB, 100MB)")
    number, unit = m.groups()
    if unit in ("K", "M", "G"):
        unit += "B"
    return int(float(number) * UNITS[unit])


def main():
    p = argparse.ArgumentParser(description="Generate a reproducible random test file")
    p.add_argument("--size", type=parse_size, default="1MB", help="e.g. 500, 100KB, 1MB, 100MB")
    p.add_argument("--out", default="test.bin")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args()
    size = args.size if isinstance(args.size, int) else parse_size(args.size)

    rng = random.Random(args.seed)
    remaining = size
    with open(args.out, "wb") as f:
        while remaining > 0:
            n = min(remaining, 1 << 20)
            f.write(rng.getrandbits(8 * n).to_bytes(n, "little"))
            remaining -= n
    print(f"wrote {args.out}: {size} bytes (seed={args.seed})")


if __name__ == "__main__":
    sys.exit(main())