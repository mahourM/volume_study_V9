from __future__ import annotations

import errno
import socket
import unittest

from core.startup_guard import (
    ServerBinding,
    is_address_in_use_error,
    unavailable_server_bindings,
)


class StartupGuardTests(unittest.TestCase):
    def test_unavailable_server_bindings_detects_listening_port(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = int(listener.getsockname()[1])
        try:
            unavailable = unavailable_server_bindings(
                (ServerBinding("test", "127.0.0.1", port),)
            )
        finally:
            listener.close()

        self.assertEqual(
            unavailable,
            (ServerBinding("test", "127.0.0.1", port),)
        )

    def test_unavailable_server_bindings_accepts_free_port(self) -> None:
        reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
        reservation.close()

        self.assertEqual(
            unavailable_server_bindings(
                (ServerBinding("test", "127.0.0.1", port),)
            ),
            (),
        )

    def test_address_in_use_recognizes_cross_platform_errno(self) -> None:
        self.assertTrue(is_address_in_use_error(OSError(errno.EADDRINUSE, "in use")))
        self.assertFalse(is_address_in_use_error(OSError(errno.EACCES, "denied")))
