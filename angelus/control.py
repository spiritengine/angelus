"""Daemon control socket.

The daemon is the only process that writes sqlite (WAL, single-writer). The
CLI is a separate process, so every operator action that would write must be
asked of the daemon instead. This module is that channel: a unix-domain socket
the daemon serves from inside its event loop, with the CLI as a client.

Trust boundary: the socket lives in state/ alongside angelus.pid and
angelus.sqlite3. That directory is daemon-owned; its filesystem permissions
are the only access control. There is deliberately no auth, handshake, TLS,
or protocol versioning here -- adding one would be scope this layer does not
need on a single host.

Protocol: newline-delimited JSON, one request per connection.

  request:  {"op": "<name>", "args": {<object>}}\n   (args optional)
  response: {"ok": true, "result": <any>}\n
            {"ok": false, "error": "<message>"}\n

The daemon closes the connection after the single response. Reads are bounded
(MAX_REQUEST_BYTES) so a misbehaving client cannot OOM the daemon, and one bad
client (oversized or malformed) is answered and dropped without taking the
server -- or the daemon -- down.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

MAX_REQUEST_BYTES = 64 * 1024

Handler = Callable[[dict[str, Any]], Awaitable[Any]]


class ControlServer:
    """A unix-domain control socket bound inside the daemon's event loop."""

    def __init__(self, socket_path: Path, dispatch: dict[str, Handler]) -> None:
        self.socket_path = socket_path
        self.dispatch = dispatch
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        # An unclean prior exit leaves a stale socket file; bind would then
        # fail with "address already in use". Mirror the pid_file handling in
        # daemon.run(): be explicit and defensive, remove it first.
        try:
            self.socket_path.unlink(missing_ok=True)
        except OSError:
            LOGGER.warning(
                "failed to remove stale control socket %s",
                self.socket_path,
                exc_info=True,
            )
        self._server = await asyncio.start_unix_server(
            self._handle,
            path=str(self.socket_path),
            limit=MAX_REQUEST_BYTES,
        )

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            try:
                raw = await reader.readuntil(b"\n")
            except asyncio.LimitOverrunError:
                await self._respond(
                    writer, {"ok": False, "error": "request too large"}
                )
                return
            except (asyncio.IncompleteReadError, ConnectionError):
                # Client closed without sending a full request line. Nothing
                # to answer; just drop the connection in finally.
                return

            response = await self._dispatch(raw)
            await self._respond(writer, response)
        except Exception:
            # One bad client must not take the server down.
            LOGGER.exception("control connection handler crashed")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass

    async def _dispatch(self, raw: bytes) -> dict[str, Any]:
        try:
            request = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            return {"ok": False, "error": f"malformed request: {exc}"}
        if not isinstance(request, dict):
            return {"ok": False, "error": "malformed request: expected a JSON object"}

        op = request.get("op")
        if not isinstance(op, str):
            return {"ok": False, "error": "missing or invalid op"}
        args = request.get("args") or {}
        if not isinstance(args, dict):
            return {"ok": False, "error": "args must be an object"}

        handler = self.dispatch.get(op)
        if handler is None:
            return {"ok": False, "error": f"unknown op: {op}"}
        try:
            result = await handler(args)
        except Exception as exc:
            LOGGER.exception("control op %s failed", op)
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "result": result}

    async def _respond(
        self, writer: asyncio.StreamWriter, payload: dict[str, Any]
    ) -> None:
        try:
            writer.write((json.dumps(payload) + "\n").encode("utf-8"))
            await writer.drain()
        except (OSError, ConnectionError):
            pass
