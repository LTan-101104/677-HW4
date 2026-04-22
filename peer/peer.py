import socket
import threading
from typing import Callable, Optional

from config.enums import MessageType
from config.peer_registry import PeerRegistry
from peer.clock import LamportClock
from peer.messages import Message


HandlerFn = Callable[[Message], None]


class Peer:
    """One node in the trading post: TCP server + client + Lamport clock.

    Higher-level roles (buyer, seller, trader, election) plug in by calling
    `register_handler` for the message types they care about.
    """

    def __init__(self, peer_id: int, registry: PeerRegistry) -> None:
        """Creates a peer but does not open any sockets; call `start` to run."""
        self.peer_id = peer_id
        self.registry = registry
        self.info = registry.get(peer_id)
        self.clock = LamportClock()

        # Current trader; None until Bully election completes. Behavior loops
        # check this before attempting to send BUY / SELL_DEPOSIT.
        self.coordinator_id: Optional[int] = None

        self._handlers: dict[MessageType, HandlerFn] = {}
        self._server_sock: Optional[socket.socket] = None
        self._running = False
        self._accept_thread: Optional[threading.Thread] = None

    # Handler registration

    def register_handler(self, msg_type: MessageType, fn: HandlerFn) -> None:
        """Installs a callback invoked whenever a message of this type arrives.

        Later registrations for the same type overwrite earlier ones; only one
        handler per MessageType is supported.
        """
        self._handlers[msg_type] = fn

    def get_handler(self, msg_type: MessageType) -> Optional[HandlerFn]:
        """Returns the currently registered handler for `msg_type`, or None.

        Used by trader resignation to snapshot pre-existing handlers so they
        can be reinstated when the trader steps down.
        """
        return self._handlers.get(msg_type)

    def unregister_handler(self, msg_type: MessageType) -> None:
        """Removes the handler for `msg_type` if one is registered."""
        self._handlers.pop(msg_type, None)

    # Lifecycle

    def start(self) -> None:
        """Binds the server socket and launches the accept loop on a thread.

        Idempotent: calling `start` on an already-running peer is a no-op.
        """
        if self._running:
            return
        self._running = True
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.info.host, self.info.port))
        self._server_sock.listen()
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name=f"peer-{self.peer_id}-accept", daemon=True
        )
        self._accept_thread.start()

    def stop(self) -> None:
        """Closes the server socket so the accept loop exits.

        Existing per-connection handler threads are daemons and die with the
        process; we do not join them explicitly.
        """
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass

    # Sending

    def new_message(
        self, msg_type: MessageType, payload: Optional[dict] = None
    ) -> Message:
        """Stamps a fresh Message with the next Lamport tick.

        Ticks the clock exactly once so multicast callers can reuse the same
        envelope across every recipient, keeping timestamps consistent.
        """
        ts = self.clock.tick()
        return Message(
            type=msg_type,
            sender=self.peer_id,
            ts=ts,
            payload=payload or {},
        )

    def unicast(
        self, to_peer_id: int, msg_type: MessageType, payload: Optional[dict] = None
    ) -> Message:
        """Sends a single new message to one target peer and returns it."""
        msg = self.new_message(msg_type, payload)
        self._send_raw(to_peer_id, msg)
        return msg

    def multicast(
        self,
        msg_type: MessageType,
        payload: Optional[dict] = None,
        targets: Optional[list[int]] = None,
        include_self: bool = False,
    ) -> Message:
        """Sends one logical message (one timestamp) to many recipients.

        Defaults to every other peer in the registry; pass `targets` to
        restrict the set, or `include_self=True` to loop the message back
        through our own server socket (useful for symmetric ordering rules).
        """
        msg = self.new_message(msg_type, payload)
        if targets is None:
            targets = [p.peer_id for p in self.registry.others(self.peer_id)]
            if include_self:
                targets.append(self.peer_id)
        for pid in targets:
            self._send_raw(pid, msg)
        return msg

    def _send_raw(self, to_peer_id: int, message: Message) -> None:
        """Opens a one-shot TCP connection and writes the serialized message.

        Failures are logged but not raised: liveness tracking and retries
        belong to higher-level logic (election timers, ACK timeouts).
        """
        target = self.registry.get(to_peer_id)
        try:
            with socket.create_connection((target.host, target.port), timeout=2.0) as s:
                s.sendall(message.to_bytes())
        except OSError as e:
            print(f"[peer={self.peer_id}] send to {to_peer_id} failed: {e}")

    # Receive loop

    def _accept_loop(self) -> None:
        """Accepts incoming connections and spawns a handler thread for each."""
        assert self._server_sock is not None
        while self._running:
            try:
                conn, _addr = self._server_sock.accept()
            except OSError:
                break  # socket closed by stop()
            t = threading.Thread(
                target=self._handle_connection,
                args=(conn,),
                name=f"peer-{self.peer_id}-conn",
                daemon=True,
            )
            t.start()

    def _handle_connection(self, conn: socket.socket) -> None:
        """Reads newline-framed JSON messages off one connection until it closes.

        Updates the Lamport clock on every valid message before dispatch.
        Malformed lines are logged and skipped rather than tearing down the
        connection.
        """
        try:
            with (
                conn,
                conn.makefile("rb") as rfile,
            ):  # rfile is buffered reader over 1 connection of socket, right now a message is sent per connection so 1 wrapper per message
                for line in rfile:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = Message.from_bytes(line)
                    except (ValueError, KeyError) as e:
                        print(f"[peer={self.peer_id}] malformed message: {e}")
                        continue
                    self.clock.tick_receive(msg.ts)
                    self._dispatch(msg)
        except OSError:
            return

    def _dispatch(self, msg: Message) -> None:
        """Routes one message to its registered handler, or logs it if none."""
        handler = self._handlers.get(msg.type)
        if handler is None:
            print(
                f"[peer={self.peer_id}] recv {msg.type.value} "
                f"from {msg.sender} ts={msg.ts} (no handler)"
            )
            return
        handler(msg)
