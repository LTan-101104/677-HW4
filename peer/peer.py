import socket
import threading
from typing import Callable, Optional

from config.enums import MessageType
from config.peer_registry import PeerRegistry
from peer.clock import LamportClock
from peer.messages import Message


HandlerFn = Callable[[Message], None]


class Peer:
    def __init__(self, peer_id: int, registry: PeerRegistry) -> None:
        self.peer_id = peer_id
        self.registry = registry
        self.info = registry.get(peer_id)
        self.clock = LamportClock()

        self._handlers: dict[MessageType, HandlerFn] = {}
        self._server_sock: Optional[socket.socket] = None
        self._running = False
        self._accept_thread: Optional[threading.Thread] = None

    # Handler registration

    def register_handler(self, msg_type: MessageType, fn: HandlerFn) -> None:
        self._handlers[msg_type] = fn

    # Lifecycle

    def start(self) -> None:
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
        # One tick per logical outgoing event; the same Message can then be
        # sent to many targets (multicast) while preserving a single timestamp.
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
        msg = self.new_message(msg_type, payload)
        if targets is None:
            targets = [p.peer_id for p in self.registry.others(self.peer_id)]
            if include_self:
                targets.append(self.peer_id)
        for pid in targets:
            self._send_raw(pid, msg)
        return msg

    def _send_raw(self, to_peer_id: int, message: Message) -> None:
        target = self.registry.get(to_peer_id)
        try:
            with socket.create_connection((target.host, target.port), timeout=2.0) as s:
                s.sendall(message.to_bytes())
        except OSError as e:
            # Target may be down or unreachable. Swallow and continue; higher
            # layers (liveness tracking, retries) own recovery policy.
            print(f"[peer={self.peer_id}] send to {to_peer_id} failed: {e}")

    # Receive loop

    def _accept_loop(self) -> None:
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
        try:
            with conn, conn.makefile("rb") as rfile:
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
        handler = self._handlers.get(msg.type)
        if handler is None:
            print(
                f"[peer={self.peer_id}] recv {msg.type.value} "
                f"from {msg.sender} ts={msg.ts} (no handler)"
            )
            return
        handler(msg)
