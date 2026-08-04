"""Tests for the pure functions that silently corrupt data when wrong.

These five areas were picked because they are (a) pure, so they need no
fixtures, and (b) the places where a subtle regression is invisible:

  * ``_date_prec`` / ``_should_update_date`` — decide which of five sources wins
    for every release date.
  * ``_base_title`` — decides which releases get grouped and merged.
  * ``_source_id`` — the listen dedup key; the only thing standing between a
    re-import and 139k double-counted scrobbles.
  * ``normalize_text`` / ``ascii_key`` — the matching substrate for every
    artist/track/album lookup.
  * ID extraction/validation — the entry point for every import.

Every expectation below was captured from the current implementation, so these
are regression tests: they pin down today's behaviour rather than asserting an
idealised spec.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import mdb_strings as m  # noqa: E402
import sync  # noqa: E402


# ── _date_prec ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize('value,expected', [
    ('2020',       1),
    ('2020-05',    2),
    ('2020-05-17', 3),
    # Spotify emits Jan 1 as a year-only placeholder, so it must not be trusted
    # as a full date — otherwise a real date loses to a fake one.
    ('2020-01-01', 1),
    ('',           0),
    (None,         0),
])
def test_date_prec(value, expected):
    assert m._date_prec(value) == expected


def test_date_prec_jan_first_does_not_beat_real_month():
    """The placeholder must lose to any genuinely more precise value."""
    assert m._date_prec('2020-01-01') < m._date_prec('2020-03')


# ── _should_update_date ───────────────────────────────────────────────────────

@pytest.mark.parametrize('existing,ex_source,new,new_source,expected', [
    # Higher precision wins regardless of source priority.
    ('2020',       'spotify',     '2020-05-17', 'musicbrainz', True),
    # Lower precision loses even from a higher-priority source.
    ('2020-05-17', 'wikipedia',   '2020',       'spotify',     False),
    # The Jan 1 placeholder is precision 1, so a real month beats it.
    ('2020-01-01', 'spotify',     '2020-03-01', 'musicbrainz', True),
    # Equal precision: higher source priority wins (manual > musicbrainz).
    ('2020-05-17', 'musicbrainz', '2020-05-17', 'manual',      True),
    # Anything beats nothing.
    (None,         None,          '2020',       'spotify',     True),
])
def test_should_update_date(existing, ex_source, new, new_source, expected):
    assert m._should_update_date(existing, ex_source, new, new_source) is expected


def test_manual_is_never_overwritten_at_equal_precision():
    """`enrich dates` saves as 'manual'; re-imports must not clobber it."""
    assert m._should_update_date(
        '2020-05-17', 'manual', '2020-06-01', 'musicbrainz'
    ) is False


# ── _base_title ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize('title,expected', [
    ('Album (Deluxe Edition)',      'Album'),
    ('Album - Deluxe Edition',      'Album'),
    ('Album (Original Soundtrack)', 'Album'),
    ('Album (Remaster)',            'Album'),
    # Titles with no qualifier must pass through untouched.
    ('Album',                       'Album'),
    ('Kid A',                       'Kid A'),
])
def test_base_title(title, expected):
    assert m._base_title(title) == expected


def test_base_title_groups_variants_together():
    """The grouping invariant: editions of one album collapse to one key."""
    variants = [
        'In Rainbows',
        'In Rainbows (Deluxe Edition)',
        'In Rainbows - Deluxe Edition',
        'In Rainbows (Remaster)',
    ]
    keys = {m._base_title(v).lower() for v in variants}
    assert keys == {'in rainbows'}


def test_base_title_does_not_merge_distinct_albums():
    """Guard against an over-greedy regex fusing different records."""
    assert m._base_title('Ok Computer') != m._base_title('Amnesiac')


# ── _source_id (listen dedup key) ─────────────────────────────────────────────

VALID_MBID = 'c0b3b6f0-0000-4000-8000-000000000000'
NIL_MBID = '00000000-0000-0000-0000-000000000000'


def test_source_id_prefers_valid_mbid():
    assert sync._source_id(VALID_MBID, 'Björk', 'Jóga') == VALID_MBID


@pytest.mark.parametrize('mbid', [None, '', NIL_MBID, 'not-a-uuid'])
def test_source_id_falls_back_to_normalised_pair(mbid):
    """Invalid MBIDs must fall back, not leak through as a key."""
    assert sync._source_id(mbid, 'Björk', 'Jóga') == 'bjork|||joga'


def test_source_id_is_stable_across_accents_and_case():
    """The dedup key must be identical for equivalent spellings, or a
    re-import inserts duplicates for every affected listen."""
    assert (sync._source_id(None, 'Björk', 'Jóga')
            == sync._source_id(None, 'bjork', 'joga')
            == sync._source_id(None, 'BJÖRK', '  Jóga  '))


def test_source_id_distinguishes_different_tracks():
    assert sync._source_id(None, 'Björk', 'Jóga') != sync._source_id(None, 'Björk', 'Hyperballad')


# ── normalize_text / ascii_key ────────────────────────────────────────────────

@pytest.mark.parametrize('value,expected', [
    ('Björk',                'bjork'),
    ('Sigur Rós',            'sigur ros'),
    ('  Multiple   Spaces ', 'multiple spaces'),
    ('AC/DC',                'ac/dc'),
])
def test_normalize_text(value, expected):
    assert m.normalize_text(value) == expected


@pytest.mark.parametrize('value,expected', [
    ('Björk',     'bjork'),
    ('Sigur Rós', 'sigur ros'),
    # ascii_key differs from normalize_text by replacing punctuation with space.
    ('AC/DC',     'ac dc'),
])
def test_ascii_key(value, expected):
    assert m.ascii_key(value) == expected


def test_ascii_key_strips_punctuation_normalize_text_keeps_it():
    assert m.normalize_text('AC/DC') != m.ascii_key('AC/DC')


def test_normalization_is_idempotent():
    for fn in (m.normalize_text, m.ascii_key):
        once = fn('  Björk / Sigur Rós  ')
        assert fn(once) == once, fn.__name__


# ── ID validation and extraction ──────────────────────────────────────────────

@pytest.mark.parametrize('value,expected', [
    (VALID_MBID, True),
    # The all-zero UUID is well-formed but is a null sentinel, not an ID.
    (NIL_MBID,   False),
    ('nope',     False),
    (None,       False),
    ('',         False),
])
def test_is_valid_mbid(value, expected):
    assert m.is_valid_mbid(value) is expected


@pytest.mark.parametrize('value,expected', [
    (f'https://musicbrainz.org/release/{VALID_MBID}', VALID_MBID),
    (VALID_MBID,                                      VALID_MBID),
    ('garbage',                                       None),
])
def test_extract_mbid(value, expected):
    assert m.extract_mbid(value) == expected


@pytest.mark.parametrize('value,expected', [
    ('https://open.spotify.com/album/1DFixLWuPkv3KT3TnV35m3', '1DFixLWuPkv3KT3TnV35m3'),
    ('1DFixLWuPkv3KT3TnV35m3',                                '1DFixLWuPkv3KT3TnV35m3'),
    ('nope!!',                                                None),
])
def test_extract_spotify_id(value, expected):
    assert m.extract_spotify_id(value) == expected


# ── detect_variant_type ───────────────────────────────────────────────────────

@pytest.mark.parametrize('title,expected', [
    ('Album (Deluxe Edition)', 'deluxe'),
    ('Album (Remaster)',       'remaster'),
    ('Album (Instrumental)',   'instrumental'),
    ('Album',                  None),
])
def test_detect_variant_type(title, expected):
    assert m.detect_variant_type(title) == expected


def test_detected_variant_types_are_in_the_declared_set():
    """A detector returning a value outside VARIANT_TYPES would write an
    unrecognised variant_type straight into the database."""
    titles = [
        'Album (Deluxe Edition)', 'Album (Remaster)', 'Album (Instrumental)',
        'Album (Anniversary Edition)', 'Album (Mono)', 'Album (Box Set)',
    ]
    for t in titles:
        found = m.detect_variant_type(t)
        if found is not None:
            assert found in m.VARIANT_TYPES, f'{t!r} -> {found!r}'
