import threading


# a Lamport clock per process
class LamportClock:
    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    # Tick for any event except receiving
    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def tick_receive(self, incoming_ts: int) -> int:
        with self._lock:
            self._value = max(self._value, incoming_ts) + 1
            return self._value
