import zlib

def compute_checksum(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF