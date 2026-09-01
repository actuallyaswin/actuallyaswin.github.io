"""mdb_concert_stats — precomputes every concert/live-attendance section
views/concert-stats.js renders, into the same `stats_cache` table
mdb.py's other `_stats_*` functions populate.

Kept in its own module (rather than folded into mdb.py alongside the
listening-history `_stats_*` functions) so the concert-specific schema
(venues/concert_events/concert_performances/concert_performance_guests) and
its query logic stay reviewable as one self-contained unit. Wired in from
mdb.py's cmd_stats_refresh with a single added call — see that function.
"""
import time

_CONCERT_DRILL_LIMIT = 4


def _concert_drill_artists(conn, extra_where: str, params=()) -> list:
    """Same output shape as mdb.py's _drill_artists() — [id, name, image_url,
    n, slug] — but 'n' counts distinct concert_events (nights seen), not
    listens, and the join runs through concert_performances instead of
    listens/tracks. Shape-compatible on purpose: views/concert-stats.js
    reuses stats.js's existing _drillPanel()/_rowWithDrill() unchanged.

    Ranked billing-first, then nights seen, then Spotify popularity -- not
    nights-seen alone. A one-off support slot and the headliner of that same
    show both land at n=1, and without a billing tiebreaker the drill list
    would order them arbitrarily (e.g. an opener like Lawrence outranking
    the Jonas Brothers at the one show they both played). best_billing takes
    the artist's most-senior billing among the rows this filter matched."""
    rows = conn.execute(f'''
        SELECT a.id, a.name, COALESCE(a.image_thumb_url, a.image_url) as image_url,
               COUNT(DISTINCT ce.id) as n, a.slug,
               MIN(CASE cp.billing
                   WHEN 'headliner' THEN 0 WHEN 'co-headliner' THEN 1
                   WHEN 'festival' THEN 2 WHEN 'support' THEN 3 ELSE 4 END) as best_billing
        FROM artists a
        JOIN concert_performances cp ON cp.artist_id = a.id
        JOIN concert_events ce ON ce.id = cp.event_id
        JOIN venues v ON v.id = ce.venue_id
        WHERE a.hidden = 0 {extra_where}
        GROUP BY a.id
        ORDER BY best_billing ASC, n DESC, a.spotify_popularity DESC NULLS LAST
        LIMIT {_CONCERT_DRILL_LIMIT}
    ''', params).fetchall()
    return [[r['id'], r['name'], r['image_url'], r['n'], r['slug']] for r in rows]


def _concerts_headline(conn, cache, vlog):
    t0 = time.perf_counter()
    row = conn.execute('''
        SELECT COUNT(DISTINCT ce.id) as concerts,
               COUNT(DISTINCT CASE WHEN NOT ce.is_festival THEN ce.id END) as non_festival_concerts,
               COUNT(DISTINCT ce.venue_id) as venues,
               COUNT(DISTINCT v.city) as cities,
               COUNT(DISTINCT v.country) as countries,
               COUNT(DISTINCT CASE WHEN ce.is_festival THEN ce.festival_name END) as festivals,
               MIN(ce.event_date) as first_show,
               MAX(ce.event_date) as latest_show
        FROM concert_events ce JOIN venues v ON v.id = ce.venue_id
    ''').fetchone()
    artists_seen = conn.execute('''
        SELECT COUNT(DISTINCT artist_id) FROM (
            SELECT artist_id FROM concert_performances
            UNION
            SELECT artist_id FROM concert_performance_guests
        )
    ''').fetchone()[0]
    cache['concertsHeadline'] = {
        'concerts': row['concerts'] or 0,
        'nonFestivalConcerts': row['non_festival_concerts'] or 0,
        'venues': row['venues'] or 0,
        'artists': artists_seen or 0,
        'cities': row['cities'] or 0,
        'countries': row['countries'] or 0,
        'festivals': row['festivals'] or 0,
        'firstShow': row['first_show'],
        'latestShow': row['latest_show'],
    }
    vlog('concertsHeadline', [cache['concertsHeadline']], t0)


def _concerts_venues(conn, cache, vlog):
    """Top 10 venues by distinct nights visited (not performances -- a
    4-act festival day shouldn't out-rank a venue you've been to 4 separate
    times on 4 separate nights). City/state is baked into the label itself
    (rather than a separate field) so this reuses utils.js's generic
    breakdown-row renderer unmodified."""
    t0 = time.perf_counter()
    rows = conn.execute('''
        SELECT v.id, v.name, v.city, v.state, COUNT(DISTINCT ce.id) as n
        FROM venues v JOIN concert_events ce ON ce.venue_id = v.id
        GROUP BY v.id ORDER BY n DESC LIMIT 10
    ''').fetchall()
    cache['concertsVenues'] = [
        {
            'label': f"{r['name']} ({', '.join(filter(None, [r['city'], r['state']]))})"
                      if (r['city'] or r['state']) else r['name'],
            'n': r['n'],
            'drill': _concert_drill_artists(conn, 'AND ce.venue_id = ?', (r['id'],)),
        }
        for r in rows
    ]
    vlog('concertsVenues', cache['concertsVenues'], t0)


def _concerts_top_artists(conn, cache, vlog):
    """Top 10 artists by stat_seen_live_count (already maintained elsewhere
    as the union of headline/support/festival performances AND guest
    cameos -- see the stat_seen_live_count refresh in mdb_ops). Links
    straight to the artist page; no drill-down (there's nothing further to
    drill into -- this row already IS the artist)."""
    t0 = time.perf_counter()
    rows = conn.execute('''
        SELECT id, name, slug, COALESCE(image_thumb_url, image_url) as img, stat_seen_live_count as n
        FROM artists WHERE hidden = 0 AND stat_seen_live_count > 0
        ORDER BY stat_seen_live_count DESC, name LIMIT 10
    ''').fetchall()
    cache['concertsTopArtists'] = [
        {'id': r['id'], 'name': r['name'], 'slug': r['slug'], 'img': r['img'], 'n': r['n']}
        for r in rows
    ]
    vlog('concertsTopArtists', cache['concertsTopArtists'], t0)


def _concerts_per_year(conn, cache, vlog):
    """Zero-fills years with no shows (rather than omitting them) so the bar
    chart's x-axis reflects real elapsed time -- a 2019->2021 gap with no
    2020 bar would otherwise look identical to an ordinary adjacent-year
    step."""
    t0 = time.perf_counter()
    rows = conn.execute('''
        SELECT CAST(strftime('%Y', event_date) AS INTEGER) as yr, COUNT(DISTINCT id) as n
        FROM concert_events GROUP BY yr ORDER BY yr
    ''').fetchall()
    counts = {r['yr']: r['n'] for r in rows}
    if counts:
        full_range = range(min(counts), max(counts) + 1)
        cache['concertsPerYear'] = [{'year': yr, 'n': counts.get(yr, 0)} for yr in full_range]
    else:
        cache['concertsPerYear'] = []
    vlog('concertsPerYear', cache['concertsPerYear'], t0)


def _artist_card(r) -> dict:
    return {'id': r['id'], 'name': r['name'], 'slug': r['slug'], 'img': r['img'], 'plays': r['plays']}


def _concerts_spotlights(conn, cache, vlog):
    """Two juxtaposition spotlights, same spirit as _stats_popularity's
    most-mainstream/most-obscure pair:
      - never_seen: your most-played artist you have NOT seen live.
      - obscure_seen: among artists you HAVE seen live, the one you play
        the least (can legitimately be 0 plays -- someone you caught live
        but apparently never spun on record)."""
    t0 = time.perf_counter()

    never_seen = conn.execute('''
        SELECT a.id, a.name, a.slug, COALESCE(a.image_thumb_url, a.image_url) as img, COUNT(l.id) as plays
        FROM artists a
        JOIN track_artists ta ON ta.artist_id = a.id AND ta.role = 'main'
        JOIN tracks t ON t.id = ta.track_id AND t.hidden = 0
        JOIN listens l ON l.track_id = t.id
        WHERE a.hidden = 0 AND a.name != 'Various Artists'
          AND (a.stat_seen_live_count IS NULL OR a.stat_seen_live_count = 0)
        GROUP BY a.id ORDER BY plays DESC LIMIT 1
    ''').fetchone()

    # Small set (only artists actually seen live) -- a per-row correlated
    # subquery for play count is fine at this scale, no separate CTE needed.
    obscure_seen = conn.execute('''
        SELECT a.id, a.name, a.slug, COALESCE(a.image_thumb_url, a.image_url) as img,
               (SELECT COUNT(*) FROM listens l
                JOIN tracks t ON t.id = l.track_id AND t.hidden = 0
                JOIN track_artists ta ON ta.track_id = t.id AND ta.role = 'main' AND ta.artist_id = a.id
               ) as plays
        FROM artists a
        WHERE a.hidden = 0 AND a.stat_seen_live_count > 0
        ORDER BY plays ASC, a.stat_seen_live_count DESC LIMIT 1
    ''').fetchone()

    cache['concertsSpotlights'] = {
        'neverSeen': _artist_card(never_seen) if never_seen else None,
        'obscureSeen': _artist_card(obscure_seen) if obscure_seen else None,
    }
    vlog('concertsSpotlights', [cache['concertsSpotlights']], t0)


_COVERAGE_TOP_N = 30


def _concerts_coverage(conn, cache, vlog):
    """Of your top N most-played artists, how many have you actually seen
    live? Plus the unseen ones (highest play count first) as a small
    "artists to catch live" nudge list."""
    t0 = time.perf_counter()
    rows = conn.execute('''
        SELECT a.id, a.name, a.slug, COALESCE(a.image_thumb_url, a.image_url) as img,
               a.stat_seen_live_count as seen, COUNT(l.id) as plays
        FROM artists a
        JOIN track_artists ta ON ta.artist_id = a.id AND ta.role = 'main'
        JOIN tracks t ON t.id = ta.track_id AND t.hidden = 0
        JOIN listens l ON l.track_id = t.id
        WHERE a.hidden = 0 AND a.name != 'Various Artists'
        GROUP BY a.id ORDER BY plays DESC LIMIT ?
    ''', (_COVERAGE_TOP_N,)).fetchall()

    seen_count = sum(1 for r in rows if r['seen'])
    unseen = [_artist_card(r) for r in rows if not r['seen']][:8]

    cache['concertsCoverage'] = {
        'topN': len(rows),
        'seenCount': seen_count,
        'unseen': unseen,
    }
    vlog('concertsCoverage', [cache['concertsCoverage']], t0)


def compute_concert_stats(conn, cache: dict, vlog) -> None:
    """Populates every concerts* key in `cache`. Called once from mdb.py's
    cmd_stats_refresh, alongside the listening-history _stats_* calls --
    same `cache` dict, same eventual _stats_write_cache() flush."""
    _concerts_headline(conn, cache, vlog)
    _concerts_venues(conn, cache, vlog)
    _concerts_top_artists(conn, cache, vlog)
    _concerts_per_year(conn, cache, vlog)
    _concerts_spotlights(conn, cache, vlog)
    _concerts_coverage(conn, cache, vlog)
