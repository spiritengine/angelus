"""Control-socket timeout resolution + slow-vs-down classification.

Two behaviours guard against the same production false-negative: a loaded but
healthy daemon whose health op takes several seconds was being reported as
"not reachable" because the 5.0s socket deadline expired and _request returned
None -- the same signal as a refused/absent socket.

  1. The effective control timeout defaults to 30.0s and is overridable via
     ANGELUS_CONTROL_TIMEOUT (a malformed/empty/non-positive value falls back
     to the default without raising).
  2. A socket that CONNECTS but does not answer within the timeout renders the
     daemon as alive-but-slow, NOT "not reachable"/"not running" -- while still
     falling back to the read-only sqlite reader and exiting 0. A refused
     socket still reads as down.

The slow-handler tests shorten the deadline via ANGELUS_CONTROL_TIMEOUT so they
do not actually wait 30s; the stub unix server (bind/listen/accept then stall)
mirrors how tests/test_slice5b1_control_socket.py drives the CLI against a real
socket.
"""

from __future__ import annotations

import os
import socket as socketlib
import threading

from click.testing import CliRunner

from angelus.cli import _control_timeout, main
from angelus.storage import Catalog, init_db


# --- timeout resolution ---------------------------------------------------


def test_control_timeout_defaults_to_30(monkeypatch) -> None:
    # The default must be 30.0, not the old hard-coded 5.0. Reverting the bump
    # breaks this assertion.
    monkeypatch.delenv("ANGELUS_CONTROL_TIMEOUT", raising=False)
    assert _control_timeout() == 30.0


def test_control_timeout_env_override(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_CONTROL_TIMEOUT", "12.5")
    assert _control_timeout() == 12.5


def test_control_timeout_malformed_falls_back(monkeypatch) -> None:
    # A garbage value must not raise -- it degrades to the default.
    monkeypatch.setenv("ANGELUS_CONTROL_TIMEOUT", "not-a-float")
    assert _control_timeout() == 30.0


def test_control_timeout_empty_falls_back(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_CONTROL_TIMEOUT", "")
    assert _control_timeout() == 30.0


def test_control_timeout_nonpositive_falls_back(monkeypatch) -> None:
    # 0 / negative are parseable but unusable as a deadline; fall back rather
    # than disable the timeout.
    monkeypatch.setenv("ANGELUS_CONTROL_TIMEOUT", "0")
    assert _control_timeout() == 30.0
    monkeypatch.setenv("ANGELUS_CONTROL_TIMEOUT", "-3")
    assert _control_timeout() == 30.0


# --- slow-but-alive vs down classification --------------------------------


def _seed_one_observation(tmp_path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    catalog.write_observation(
        "scheduled/watch", {"url": "x"}, {"source": "scheduled/watch"}
    )
    connection.close()


def test_cli_reports_alive_but_slow_when_socket_connects_but_stalls(
    tmp_path, monkeypatch
) -> None:
    # Shorten the deadline so the test does not wait the 30s default. This also
    # proves the socket is configured with the resolved env timeout: it gives
    # up after ~0.3s, not 30s.
    monkeypatch.setenv("ANGELUS_CONTROL_TIMEOUT", "0.3")
    _seed_one_observation(tmp_path)

    sock_path = tmp_path / "state" / "angelus.sock"
    listener = socketlib.socket(socketlib.AF_UNIX, socketlib.SOCK_STREAM)
    listener.bind(str(sock_path))
    listener.listen(1)
    listener.settimeout(5.0)
    stop = threading.Event()

    def serve_stalled() -> None:
        # Accept the connection but never answer within the timeout: the daemon
        # is alive (holding the socket) yet too slow to respond.
        conn, _ = listener.accept()
        try:
            conn.recv(4096)
            stop.wait(5.0)  # outlast the 0.3s client deadline
        finally:
            conn.close()

    server_thread = threading.Thread(target=serve_stalled, daemon=True)
    server_thread.start()
    try:
        result = CliRunner().invoke(main, ["health", "--root", str(tmp_path)])
    finally:
        stop.set()
        server_thread.join(timeout=5.0)
        listener.close()

    assert result.exit_code == 0, result.output
    assert result.exception is None, result.exception
    # Alive-but-slow label, with the actually-used deadline rendered.
    assert (
        "daemon: alive but control socket did not respond within 0.3s"
        in result.output
    )
    # NOT misreported as down.
    assert "not reachable" not in result.output
    assert "daemon: not running" not in result.output
    # Still falls back to the read-only sqlite reader.
    assert "observations pending triage: 1" in result.output


def test_cli_reports_not_reachable_when_socket_refused(tmp_path) -> None:
    # A live pid with a socket path that refuses connection is the classic
    # down/unreachable case and must NOT be relabelled alive-but-slow: only a
    # post-connect timeout earns the slow label.
    _seed_one_observation(tmp_path)
    state = tmp_path / "state"
    # pid file pointing at this live test process -> _pid_status sees it alive.
    (state / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    # A regular file at the socket path: exists() is true, but connect() fails.
    (state / "angelus.sock").write_text("not a socket", encoding="utf-8")

    result = CliRunner().invoke(main, ["health", "--root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "daemon: not reachable" in result.output
    assert "alive but control socket" not in result.output
    assert "observations pending triage: 1" in result.output
