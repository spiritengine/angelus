"""Daemon control socket.

The daemon is the only process that writes sqlite (WAL, single-writer). The
CLI is a separate process, so every operator action that would write must be
asked of the daemon instead. This module is that channel: a unix-domain socket
the daemon serves from inside its event loop, with the CLI as a client.

Trust boundary: the socket lives in state/ alongside angelus.pid and
angelus.sqlite3. The socket is chmod'd to owner-only (0600) at bind time and
state/ is 0700, so only the daemon's own uid can connect. Those filesystem
permissions ARE the authz model -- there is deliberately no in-protocol auth,
handshake, TLS, or capability/token system, and adding one would be scope this
single-host layer does not need. Every op routed through this socket relies on
that boundary, including the mutating write ops (mute / incident-close /
replay / reprocess) added in slice 5b-2: there is no read/write distinction in
the dispatch map because owner-only filesystem perms already gate the lot.

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

# A well-behaved client sends its single request line immediately on connect.
# 10s is generous for that; past it the client is stalled or malicious and we
# refuse it rather than letting the handler block forever (which, on Python
# 3.12+, would in turn wedge daemon shutdown via Server.wait_closed()).
READ_TIMEOUT = 10.0

# Owner read/write only. This is the access-control boundary (see module
# docstring); the socket must never be group/other reachable.
SOCKET_MODE = 0o600

# Hard bound on Server.wait_closed() during shutdown. Tracked handlers are
# cancelled before we await it, so it normally returns at once. The bound
# exists for the accept race: a connection accepted by the kernel before
# asyncio spawns (and we track) its handler is untracked, so cancellation
# cannot reach it and on Python 3.12.0 wait_closed() would then block until
# that client leaves. The daemon is exiting and run() unlinks the socket
# regardless, so capping here turns "wedged forever" into a brief delay --
# the denial-of-shutdown property holds even for that race.
WAIT_CLOSED_TIMEOUT = 5.0

Handler = Callable[[dict[str, Any]], Awaitable[Any]]


class ControlServer:
    """A unix-domain control socket bound inside the daemon's event loop."""

    def __init__(
        self,
        socket_path: Path,
        dispatch: dict[str, Handler],
        *,
        read_timeout: float = READ_TIMEOUT,
    ) -> None:
        self.socket_path = socket_path
        self.dispatch = dispatch
        self._read_timeout = read_timeout
        self._server: asyncio.AbstractServer | None = None
        # Connection handler tasks are spawned by asyncio.start_unix_server,
        # not by us, so they are not in the daemon's task lists. Track them
        # here so stop() can cancel any still in-flight -- without this a
        # single stalled client wedges shutdown (issue-20260519-p3td).
        self._handlers: set[asyncio.Task[None]] = set()

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
        # asyncio does not chmod the socket; it lands at the process umask,
        # which under the common 022 is world-connectable. Lock it to the
        # owner immediately -- there is an unavoidable sub-millisecond window
        # between bind and chmod, minimized by doing this first thing after
        # the server returns. A failure here means we cannot enforce the
        # trust boundary, so let it propagate and refuse to run rather than
        # serve an unprotected socket.
        self.socket_path.chmod(SOCKET_MODE)

    async def stop(self) -> None:
        if self._server is None:
            return
        # Order matters: stop accepting, cancel in-flight handlers, THEN
        # wait_closed. On Python 3.12+ wait_closed() blocks until every
        # connection is done, so cancelling stalled handlers first is what
        # makes it return promptly instead of hanging the daemon's shutdown.
        self._server.close()
        handlers = list(self._handlers)
        for task in handlers:
            task.cancel()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
        try:
            await asyncio.wait_for(
                self._server.wait_closed(), WAIT_CLOSED_TIMEOUT
            )
        except (asyncio.TimeoutError, TimeoutError):
            # Accept race only (see WAIT_CLOSED_TIMEOUT). Proceed with
            # shutdown anyway; the process is going down and run() unlinks
            # the socket. The alternative is hanging forever, which is the
            # exact denial-of-shutdown this guards against.
            LOGGER.warning(
                "control server wait_closed timed out after %.0fs; "
                "proceeding with shutdown",
                WAIT_CLOSED_TIMEOUT,
            )
        self._server = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handlers.add(task)
        try:
            try:
                raw = await asyncio.wait_for(
                    reader.readuntil(b"\n"), self._read_timeout
                )
            except (asyncio.TimeoutError, TimeoutError):
                # Stalled (or malicious) client never sent its request line.
                # Answer if we still can, then drop it; the server lives on.
                if not writer.is_closing():
                    await self._respond(
                        writer, {"ok": False, "error": "request timed out"}
                    )
                return
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
        except asyncio.CancelledError:
            # stop() cancelled us mid-flight during daemon shutdown. Let it
            # propagate so wait_closed() sees this connection finish.
            raise
        except Exception:
            # One bad client must not take the server down.
            LOGGER.exception("control connection handler crashed")
        finally:
            if task is not None:
                self._handlers.discard(task)
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
            except asyncio.CancelledError:
                # Already being cancelled; the close above is best-effort.
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
        # "args" absent or explicitly null means "no args". Anything else
        # must be an object -- do not use `or {}`, which would silently
        # accept falsy non-objects ([], 0, "") as empty args.
        args = request.get("args")
        if args is None:
            args = {}
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
