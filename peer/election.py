import threading

from config.enums import MessageType
from peer.messages import Message
from peer.peer import Peer


# Bully-algorithm timeouts. Kept short because all peers are local.
OK_TIMEOUT = 0.5          # how long we wait for any OK after sending ELECTION
COORDINATOR_TIMEOUT = 1.5  # how long we wait for COORDINATOR after receiving OK


class ElectionManager:
    """Runs the Bully leader-election protocol on behalf of one peer.

    Wires up handlers for ELECTION / OK / COORDINATOR and exposes
    `start_election()` so the owning peer (or any other module) can kick off
    a fresh round — e.g., at startup or after a RESIGN.
    """

    def __init__(self, peer: Peer) -> None:
        self.peer = peer

        # Signalled by handle_ok() when any higher peer acks our ELECTION.
        self._ok_event = threading.Event()
        # Signalled by handle_coordinator() when a COORDINATOR arrives.
        self._coordinator_event = threading.Event()

        # Guards `_electing` so two concurrent triggers (e.g., startup and
        # RESIGN) don't both launch election threads.
        self._lock = threading.Lock()
        self._electing = False

    def wire_handlers(self) -> None:
        """Registers the election message handlers on the underlying peer."""
        self.peer.register_handler(MessageType.ELECTION, self._handle_election)
        self.peer.register_handler(MessageType.OK, self._handle_ok)
        self.peer.register_handler(MessageType.COORDINATOR, self._handle_coordinator)

    # Entry points

    def start_election(self) -> None:
        """Kicks off a new election round on a background thread.

        Idempotent: if an election is already in progress, the call is a
        no-op so concurrent triggers collapse into a single run.
        """
        with self._lock:
            if self._electing:
                return
            self._electing = True
        threading.Thread(
            target=self._run_election,
            daemon=True,
            name=f"peer-{self.peer.peer_id}-election",
        ).start()

    # Election worker

    def _run_election(self) -> None:
        """Body of one election round — sends ELECTION, waits, decides."""
        try:
            higher_ids = [
                pid for pid in self.peer.registry.all_ids()
                if pid > self.peer.peer_id
            ]

            # No higher peers: we are the highest live ID -> we win outright.
            if not higher_ids:
                self._become_coordinator()
                return

            self._ok_event.clear()
            for pid in higher_ids:
                self.peer.unicast(pid, MessageType.ELECTION)

            # If no OK arrives in time, we're the highest reachable peer.
            if not self._ok_event.wait(timeout=OK_TIMEOUT):
                self._become_coordinator()
                return

            # Someone higher answered -> wait for their COORDINATOR announce.
            self._coordinator_event.clear()
            if self._coordinator_event.wait(timeout=COORDINATOR_TIMEOUT):
                return  # announcement received; coordinator_id already set

            # Higher peer never announced -> assume it died, retry.
            with self._lock:
                self._electing = False
            self.start_election()
        finally:
            with self._lock:
                self._electing = False

    def _become_coordinator(self) -> None:
        """Elects self as trader and announces it to every other peer.

        Idempotent: if we're already the coordinator, the re-announce is
        suppressed so repeated Bully rounds don't spam duplicate loglines.
        """
        if self.peer.coordinator_id == self.peer.peer_id:
            return
        self.peer.coordinator_id = self.peer.peer_id
        self.peer.multicast(MessageType.COORDINATOR)
        # Spec-required announcement logline (date formatting comes in Phase 8).
        print(
            f"Dear buyers and sellers, My ID is {self.peer.peer_id}, "
            f"and I am the new coordinator"
        )

    # Handlers

    def _handle_election(self, msg: Message) -> None:
        """Received ELECTION from another peer.

        Per Bully, a higher peer replies OK to suppress the lower candidate
        and then starts its own election to surface the true highest peer.
        """
        if msg.sender < self.peer.peer_id:
            self.peer.unicast(msg.sender, MessageType.OK)
            self.start_election()

    def _handle_ok(self, msg: Message) -> None:
        """Received OK — a higher peer is alive, so we won't win this round."""
        self._ok_event.set()

    def _handle_coordinator(self, msg: Message) -> None:
        """Received COORDINATOR — record the new trader and wake any waiter."""
        self.peer.coordinator_id = msg.sender
        self._coordinator_event.set()
