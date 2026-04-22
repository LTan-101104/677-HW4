import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, TextIO


class Logger:
    """Thread-safe, timestamped logger producing the spec-mandated format.

    Every line is prefixed with `DD.MM.YYYY HH:MM:SS.MS` (2-digit hundredths
    of a second to match the PDF's `32.10` example). Output goes to stdout by
    default so a run can be captured with shell redirection (`> output.txt`),
    per the assignment instructions; callers can also point it at a file.
    """

    def __init__(self, stream: Optional[TextIO] = None) -> None:
        self._stream: TextIO = stream if stream is not None else sys.stdout
        self._lock = threading.Lock()
        self._owns_stream = False  # True only when we opened a file ourselves

    # Configuration

    def set_output_file(self, path: Path, append: bool = False) -> None:
        """Redirects subsequent log lines to a file, closing any previous file.

        Useful when `main.py` wants both stdout and a persistent `.txt`; for
        plain stdout capture, shell redirection is simpler and recommended
        by the spec.
        """
        with self._lock:
            if self._owns_stream:
                self._stream.close()
            mode = "a" if append else "w"
            self._stream = open(path, mode)
            self._owns_stream = True

    def close(self) -> None:
        """Closes an owned file stream; no-op when writing to stdout."""
        with self._lock:
            if self._owns_stream:
                self._stream.close()
                self._stream = sys.stdout
                self._owns_stream = False

    # Core

    def _stamp(self) -> str:
        """Returns the current wall-clock time as `DD.MM.YYYY HH:MM:SS.MS`."""
        now = datetime.now()
        centis = now.microsecond // 10000  # hundredths of a second (0–99)
        return f"{now.strftime('%d.%m.%Y %H:%M:%S')}.{centis:02d}"

    def _emit(self, line: str) -> None:
        """Writes one timestamped line atomically (thread-safe via `_lock`)."""
        with self._lock:
            self._stream.write(f"{self._stamp()} {line}\n")
            self._stream.flush()

    # Spec-required loglines

    def coordinator(self, peer_id: int) -> None:
        """Announces a newly elected coordinator (verbatim spec format)."""
        self._emit(
            f"Dear buyers and sellers, My ID is {peer_id}, and I am the new coordinator"
        )

    def bought(
        self, buyer_id: int, item: str | None, qty: int | None, from_peer: int | None
    ) -> None:
        """Logs a buyer's successful receipt of goods (`bought X from peerID`).

        `from_peer` is the coordinator (trading post) that fulfilled the order,
        since that is the peer the buyer transacted with.
        """
        self._emit(f"[peer={buyer_id}] bought {item} x{qty} from peer {from_peer}")

    def buy_result(
        self,
        buyer_id: int,
        status: str | None,
        item: str | None,
        qty: int | None,
        from_peer: int | None,
    ) -> None:
        """Logs the outcome of a buy query — the `nicely formatted` result.

        Used for both SUCCESS and OUT_OF_STOCK so every issued BUY produces
        exactly one result line at the buyer, with the local receive time.
        """
        self._emit(
            f"[peer={buyer_id}] BUY result: {status} for {item} x{qty} "
            f"(trader={from_peer})"
        )

    # Non-spec conveniences

    def sold(self, seller_id: int, item: str | None, qty: int | None) -> None:
        """Logs a seller's receipt of a SOLD_NOTIFY at the seller side."""
        self._emit(f"[peer={seller_id} seller] sold {item} x{qty}")

    def payout(
        self, trader_id: int, seller_id: int, item: str, qty: int, amount: float
    ) -> None:
        """Logs the trader-side credit paid to a seller after a sale.

        `amount` is the post-commission payout (the trader retains
        `TRADER_COMMISSION` of gross). Call this right after the trader
        unicasts SOLD_NOTIFY so the logline sequence mirrors the network
        order: credit issued -> seller notified.
        """
        self._emit(
            f"[trader={trader_id}] paid seller {seller_id}: "
            f"{item} x{qty} -> ${amount:.2f}"
        )

    def deposit(self, trader_id: int, seller_id: int, item: str, qty: int) -> None:
        """Logs a SELL_DEPOSIT arriving at the trader."""
        self._emit(
            f"[trader={trader_id}] deposit from seller {seller_id}: {item} x{qty}"
        )

    def info(self, message: str) -> None:
        """Generic timestamped line for startup / lifecycle messages."""
        self._emit(message)


# Module-level singleton — import-and-use from anywhere in the codebase.
log = Logger()
