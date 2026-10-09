"""Shared constants. Everyone imports from here; do not hard-code these elsewhere."""

PACKET_SIZE = 1024        # max payload bytes per DATA packet
DEFAULT_WINDOW = 8
INITIAL_RTO = 0.5         # seconds
MAX_RETRIES = 50          # per packet / per handshake step
RECV_IDLE_TIMEOUT = 30.0  # receiver gives up after this much silence (seconds)

# Packet types
DATA, ACK, SACK, SYN, SYN_ACK, FIN, FIN_ACK = range(1, 8)
TYPE_NAMES = {1: "DATA", 2: "ACK", 3: "SACK", 4: "SYN", 5: "SYN-ACK", 6: "FIN", 7: "FIN-ACK"}
