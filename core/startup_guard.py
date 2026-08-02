from __future__ import annotations

import errno
import socket
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ServerBinding:
    name: str
    host: str
    port: int

    def display(self) -> str:
        return f"{self.name}={self.host}:{self.port}"


def unavailable_server_bindings(
    bindings: Iterable[ServerBinding],
) -> tuple[ServerBinding, ...]:
    unavailable = []
    for binding in bindings:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            exclusive_address_use = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
            if exclusive_address_use is not None:
                probe.setsockopt(socket.SOL_SOCKET, exclusive_address_use, 1)
            probe.bind((binding.host, binding.port))
        except OSError as exc:
            if is_address_in_use_error(exc):
                unavailable.append(binding)
                continue
            raise
        finally:
            probe.close()
    return tuple(unavailable)


def is_address_in_use_error(exc: BaseException) -> bool:
    return (
        isinstance(exc, OSError)
        and (
            exc.errno == errno.EADDRINUSE
            or getattr(exc, "winerror", None) == 10048
        )
    )
