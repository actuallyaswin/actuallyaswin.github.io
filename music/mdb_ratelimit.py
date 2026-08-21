"""mdb_ratelimit — cross-process rate limiting for external API calls.

The problem this solves: mdb_apis.py has an in-process RateLimiter per
service, but that only paces requests made by ONE Python process. When
several independent processes (a main session plus background subagents)
each import mdb_apis and hit the same service concurrently, every process
has its own clock, so N processes multiply the effective request rate by N
— defeating the whole point of rate limiting. This is what caused Spotify
to get 429'd hard enough to stay broken for over an hour, and MusicBrainz to
503 repeatedly, during a session running multiple concurrent agents.

CrossProcessRateLimiter fixes this by tracking "last request time for
service X" in a shared file under /tmp, guarded by an flock so processes
serialize around it. flock is held on an open file descriptor, which the
kernel releases automatically the moment that descriptor closes — including
on a crash or SIGKILL — so a dead process can never leave the lock stuck
and deadlock everyone else waiting on it.

Fails open by design: any error (unwritable /tmp, corrupt state file,
locking failure, whatever) means wait() logs a warning and returns
immediately rather than raising. A bug in this module must never be able to
break a request that would otherwise have succeeded.
"""

import fcntl
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger(__name__)

_STATE_DIR = Path('/tmp/mdb_ratelimit')

# Safety valve: never sleep longer than this in a single wait() call, no
# matter what the computed remaining-interval looks like. Guards against a
# corrupt or stale timestamp in the state file (clock skew, a stray huge
# number written by a buggy process) turning into a multi-minute hang.
_MAX_SLEEP = 60.0


class CrossProcessRateLimiter:
    """Paces requests to `service` to at least `min_interval` seconds apart,
    coordinated across every process on this machine that uses this class
    for the same `service` name — not just the calling process's own
    history.

    Correctness notes:
    - The exclusive lock is an flock() on an open fd, not a "does a file
      exist" check. flock is atomic and is released by the kernel the
      instant the fd closes, so a process that crashes or is killed while
      holding it can never wedge other processes.
    - The sleep happens WHILE the lock is held. That is intentional: the
      whole point is that no other process may check in and decide it's
      also free to proceed during the window this process is waiting out.
      Any process blocked on the lock simply queues up; with N processes
      contending for the same service, the last one to get through waits at
      most roughly (N-1) * min_interval, which is bounded and not
      pathological for the sub-few-second intervals used here.
    - wait() never raises. Any failure (unwritable /tmp, lock error, corrupt
      state file) is caught, logged, and treated as "no rate limiting for
      this call" — fail open, never fail closed into breaking the caller.
    """

    def __init__(self, service: str, min_interval: float):
        self.service = service
        self.min_interval = float(min_interval)
        self._state_file = _STATE_DIR / f'{service}.json'
        self._lock_file = _STATE_DIR / f'{service}.lock'
        self._broken = False
        try:
            _STATE_DIR.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.warning(
                'mdb_ratelimit: cannot create %s (%s) — cross-process rate '
                'limiting disabled for %r', _STATE_DIR, e, service,
            )
            self._broken = True

    def wait(self) -> None:
        """Block until at least min_interval seconds have passed since the
        last recorded request to this service from ANY process, then record
        this request's timestamp. Fails open on any unexpected error."""
        if self._broken:
            return
        try:
            self._wait_impl()
        except Exception as e:
            log.warning('mdb_ratelimit: %s wait() failed open (%s)', self.service, e)

    def _wait_impl(self) -> None:
        fd = os.open(str(self._lock_file), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            # Blocks until acquired. Auto-released by the kernel if this
            # process dies while holding it, so this can never deadlock.
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                last = self._read_last()
                now = time.monotonic()
                if last is None:
                    remaining = 0.0
                else:
                    remaining = self.min_interval - (now - last)
                if remaining > 0:
                    time.sleep(min(remaining, _MAX_SLEEP))
                self._write_last(time.monotonic())
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _read_last(self) -> 'float | None':
        try:
            with open(self._state_file) as f:
                data = json.load(f)
            return float(data['last'])
        except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError, ValueError, OSError):
            return None

    def _write_last(self, ts: float) -> None:
        tmp = self._state_file.with_suffix('.tmp')
        try:
            with open(tmp, 'w') as f:
                json.dump({'last': ts}, f)
            os.replace(tmp, self._state_file)
        except OSError as e:
            log.warning('mdb_ratelimit: could not persist state for %r (%s)', self.service, e)
