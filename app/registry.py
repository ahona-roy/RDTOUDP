"""Maps --protocol names to classes. Imported lazily so one missing protocol
(not yet written by a teammate) does not break the others."""
import importlib

PROTOCOLS = {
    "saw": ("protocols.stop_and_wait", "StopAndWaitSender", "StopAndWaitReceiver"),
    "gbn": ("protocols.go_back_n", "GoBackNSender", "GoBackNReceiver"),
    "sr":  ("protocols.selective_repeat", "SelectiveRepeatSender", "SelectiveRepeatReceiver"),
}


def load(name, role):
    """role is 'sender' or 'receiver'."""
    module, sender_cls, receiver_cls = PROTOCOLS[name]
    try:
        mod = importlib.import_module(module)
    except ImportError as e:
        raise SystemExit(f"Protocol '{name}' is not available yet ({module}): {e}")
    return getattr(mod, sender_cls if role == "sender" else receiver_cls)
