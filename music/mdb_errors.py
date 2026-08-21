"""mdb_errors — typed exceptions for web-source fetch failures.

Before this module, every source class (MusicBrainz, Spotify, iTunes,
Beatport, Bandcamp, Deezer) raised a bare `ValueError` (malformed input —
that split already existed and stays as-is) or `RuntimeError` (everything
else), distinguishable only by reading the message text. That's exactly the
gap that caused this session's real import bugs to go undetected: "fetched
fine but the data is unusable" and "genuinely doesn't exist" and "we're
being rate-limited" all looked identical to any caller doing `except
Exception`. A caller — human or agent — had to grep message strings to
tell them apart.

All three are RuntimeError subclasses, so every existing `except
RuntimeError` / `except Exception` call site keeps working unchanged with
zero behavior change. The point is purely to let NEW code branch on *why*
a source failed instead of parsing prose.
"""


class SourceError(RuntimeError):
    """Base class for all web-source fetch failures. Catch this to mean
    "any of the three below" without caring which."""

    def __init__(self, message: str, *, source: str | None = None):
        super().__init__(message)
        self.source = source


class SourceNotFound(SourceError):
    """The id/URL doesn't correspond to anything on this source (HTTP 404
    or equivalent "no results"). Permanent — the identifier is wrong, not
    a source outage. Retrying won't help; a different id/source might."""


class SourceDataUnavailable(SourceError):
    """The source responded successfully (200 OK) but structurally lacks
    the data requested — e.g. Apple's iTunes Lookup returning a collection
    with trackCount=19 and zero individual track items (confirmed this
    session, not transient), or a Bandcamp/Beatport page missing its
    expected embedded JSON blob. Not retryable against this source; try a
    different one."""


class SourceRateLimited(SourceError):
    """Retries against this source were exhausted while still
    rate-limited or erroring (HTTP 429, or a 5xx/connection failure that
    didn't clear within the retry budget). Transient — safe to retry
    later, possibly with a longer backoff than was already tried."""


class NoSourcesAvailable(RuntimeError):
    """None of the sources `import_album_unified` tried (explicitly given
    or discovered via GTIN broadcast) could provide usable metadata — the
    terminal failure when every individual per-source fetch raised. Not a
    SourceError subclass: this is an aggregate failure across all sources,
    not one source's failure."""
