"""Retransmission timeout estimation (RFC 6298 style Jacobson/Karels).
 
Karn's algorithm is enforced by the *sender*: it must only call update()
with RTT samples from packets that were never retransmitted.
"""
 
try:
    from common.config import INITIAL_RTO
except ImportError:  # allows standalone testing
    INITIAL_RTO = 0.5
 
MIN_RTO = 0.05
MAX_RTO = 5.0
 
 
class RTOEstimator:
    def __init__(self, initial_rto=INITIAL_RTO, min_rto=MIN_RTO,
                 max_rto=MAX_RTO, fixed=False):
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.fixed = fixed          # fixed mode = Risk 1 fallback
        self.srtt = None
        self.rttvar = None
        self.rto = self._clamp(initial_rto)
 
    def _clamp(self, value):
        return max(self.min_rto, min(self.max_rto, value))
 
    def update(self, rtt):
        """Feed one RTT sample (seconds) from a non-retransmitted packet."""
        if self.fixed:
            return
        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2
        else:
            # RFC 6298 order: RTTVAR uses the *old* SRTT.
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
            self.srtt = 0.875 * self.srtt + 0.125 * rtt
        self.rto = self._clamp(self.srtt + 4 * self.rttvar)
 
    def backoff(self):
        """Double the RTO after a timeout (no-op in fixed mode)."""
        if self.fixed:
            return
        self.rto = self._clamp(self.rto * 2)
 