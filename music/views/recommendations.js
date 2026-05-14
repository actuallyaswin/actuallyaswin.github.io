const ViewRecommendations = (() => {
    let _db   = null;
    let _seed = 0;

    // ── Seed-based picker ──────────────────────────────────────────────────────
    function _pick(pool, n, shelfIdx = 0, exclude = new Set()) {
        if (!pool || !pool.length) return [];
        const eligible = pool.filter(r => !exclude.has(r[0]));
        if (!eligible.length) return [];
        const sorted = [...eligible].sort((a, b) => (a[0] < b[0] ? -1 : 1));
        const start  = (_seed + shelfIdx * 7919) % sorted.length;
        const out    = [];
        for (let i = 0; i < Math.min(n, sorted.length); i++)
            out.push(sorted[(start + i) % sorted.length]);
        return out;
    }

    // ── Card HTML ──────────────────────────────────────────────────────────────
    // Uses <div role="link"> so a nested <a> for the Spotify icon is valid HTML.
    function _card(id, title, artist, art, year, spotifyId) {
        const img = art
            ? `<div class="disc-card-img" style="background-image:url('${art}')"></div>`
            : `<div class="disc-card-img" style="background:var(--bg-tertiary)"></div>`;
        const sub = [artist, year].filter(Boolean).join(' · ');
        const streaming = SHOW_STREAMING_LINKS && spotifyId
            ? `<a class="disc-card-streaming" href="https://open.spotify.com/album/${spotifyId}"
                  target="_blank" rel="noopener" title="Open on Spotify">
                  <span class="disc-card-streaming-icon"></span>
               </a>`
            : '';
        return `<div class="disc-card" role="link" tabindex="0"
                     onclick="navigate({view:'release',id:'${id}'})"
                     onkeydown="if(event.key==='Enter'||event.key===' ')navigate({view:'release',id:'${id}'})">
            ${img}
            <div class="disc-card-meta">
                <div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(title || '')}</div>
                    <div class="disc-card-sub">${escapeHtml(sub)}</div>
                </div>
                ${streaming}
            </div></div>`;
    }

    // ── Shelf HTML — hidden if fewer than 2 results ────────────────────────────
    function _shelf(title, desc, rows) {
        if (rows.length < 2) return '';
        return `<section class="rec-shelf">
            <div class="rec-shelf-header">
                <h2>${escapeHtml(title)}</h2>
                <p class="rec-desc">${escapeHtml(desc)}</p>
            </div>
            <div class="disc-grid">${rows.map(r => _card(r[0], r[1], r[3], r[2], r[4], r[5])).join('')}</div>
        </section>`;
    }

    // ── SQL helper ─────────────────────────────────────────────────────────────
    function _q(sql) {
        const res = _db.exec(sql)[0];
        return res ? res.values : [];
    }

    const CANON = `t.hidden = 0 AND t.variant_section IS NULL AND r.hidden = 0`;
    const ART   = `COALESCE(r.album_art_thumb_url, r.album_art_url)`;
    const COLS  = `r.id, r.title, ${ART}, a.name, r.release_year, r.spotify_id`;

    // ── Shelf loaders ──────────────────────────────────────────────────────────

    function _favoritesThisYear(now) {
        const since = now - 90 * 86400;
        return _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE l.timestamp >= ${since} AND ${CANON}
            GROUP BY r.id ORDER BY plays DESC LIMIT 30
        `);
    }

    function _favoritesPerYear(currentYear, exclude) {
        // One top pick per completed year, deduped across years
        const years = [1,2,3,4,5].map(i => currentYear - i);
        const picks = [];
        const seen  = new Set(exclude);
        years.forEach((year, idx) => {
            // Fetch r.id, title, art, artist, listen_year (not release_year)
            const pool = _q(`
                SELECT r.id, r.title, ${ART}, a.name, ${year} listen_year, COUNT(l.id) plays
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN releases r ON r.id = t.release_id
                LEFT JOIN artists a ON a.id = r.primary_artist_id
                WHERE l.year = ${year} AND ${CANON}
                GROUP BY r.id ORDER BY plays DESC LIMIT 30
            `);
            const chosen = _pick(pool, 1, idx + 100, seen);
            if (chosen.length) {
                picks.push(chosen[0]);
                seen.add(chosen[0][0]);
            }
        });
        return picks;
    }

    function _throwbacks(currentYear) {
        return _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.release_year <= ${currentYear - 5} AND ${CANON}
            GROUP BY r.id ORDER BY plays DESC LIMIT 30
        `);
    }

    function _thisMonthPastYears(currentMonth, currentYear) {
        return _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE strftime('%m', datetime(l.timestamp,'unixepoch')) = '${currentMonth}'
              AND l.year < ${currentYear}
              AND ${CANON}
            GROUP BY r.id ORDER BY plays DESC LIMIT 30
        `);
    }

    function _rising(now) {
        const last30  = now - 30 * 86400;
        const prev30  = now - 60 * 86400;
        return _q(`
            SELECT ${COLS},
                   SUM(CASE WHEN l.timestamp >= ${last30} THEN 1 ELSE 0 END) r30,
                   SUM(CASE WHEN l.timestamp >= ${prev30} AND l.timestamp < ${last30} THEN 1 ELSE 0 END) p30
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE l.timestamp >= ${prev30} AND ${CANON}
            GROUP BY r.id
            HAVING r30 > p30 AND r30 > 0
            ORDER BY r30 - p30 DESC LIMIT 20
        `);
    }

    function _fadingFavorites(now) {
        const cutoff = now - 365 * 86400;
        return _q(`
            WITH top50 AS (
                SELECT r.id, COUNT(l.id) plays, MAX(l.timestamp) last_ts
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN releases r ON r.id = t.release_id
                WHERE ${CANON}
                GROUP BY r.id ORDER BY plays DESC LIMIT 50
            )
            SELECT ${COLS}, top50.plays
            FROM top50
            JOIN releases r ON r.id = top50.id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE top50.last_ts < ${cutoff} LIMIT 30
        `);
    }

    function _oneTrackAway() {
        return _q(`
            WITH ts AS (
                SELECT t.release_id,
                       COUNT(CASE WHEN t.duration_ms IS NULL OR t.duration_ms >= 30000 THEN 1 END) total_e,
                       COUNT(CASE WHEN (t.duration_ms IS NULL OR t.duration_ms >= 30000) AND l.id IS NOT NULL THEN 1 END) heard_e
                FROM tracks t
                LEFT JOIN listens l ON l.track_id = t.id
                WHERE t.hidden = 0 AND t.variant_section IS NULL
                GROUP BY t.release_id
            )
            SELECT ${COLS}
            FROM ts
            JOIN releases r ON r.id = ts.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.hidden = 0
              AND ts.total_e >= 4
              AND ts.heard_e = ts.total_e - 1
              AND ts.heard_e > 0
            LIMIT 20
        `);
    }

    function _deepCutNeeded() {
        return _q(`
            WITH tp AS (
                SELECT t.release_id, t.id tid, COUNT(l.id) plays
                FROM tracks t
                LEFT JOIN listens l ON l.track_id = t.id
                WHERE t.hidden = 0 AND t.variant_section IS NULL
                GROUP BY t.id
            ),
            rs AS (
                SELECT release_id, MAX(plays) mx, AVG(plays) av, COUNT(*) tc, SUM(plays) tot
                FROM tp GROUP BY release_id
                HAVING tc >= 4 AND tot > 0
            )
            SELECT ${COLS}
            FROM rs
            JOIN releases r ON r.id = rs.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.hidden = 0 AND rs.mx > 5 * rs.av
            ORDER BY rs.mx / rs.av DESC LIMIT 20
        `);
    }

    function _onlyHeardOnce(now) {
        const cutoff = now - 365 * 86400;
        const rows = _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE ${CANON}
            GROUP BY r.id
            HAVING plays BETWEEN 1 AND 5 AND MIN(l.timestamp) < ${cutoff}
            ORDER BY plays ASC LIMIT 60
        `);
        // Cap at 1 per artist to avoid same-artist clusters
        const seenArtists = new Set();
        return rows.filter(r => {
            const artist = r[3] || '';
            if (seenArtists.has(artist)) return false;
            seenArtists.add(artist);
            return true;
        }).slice(0, 30);
    }

    function _moreFromThisArtist(now) {
        const since = now - 14 * 86400;
        const artistRow = _q(`
            SELECT r.primary_artist_id, a.name, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE l.timestamp >= ${since} AND r.hidden = 0 AND t.hidden = 0
            GROUP BY r.primary_artist_id ORDER BY plays DESC LIMIT 1
        `);
        if (!artistRow.length) return { rows: [], name: '' };
        const aid  = artistRow[0][0].replace(/'/g, "''");
        const name = artistRow[0][1] || '';
        const rows = _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM releases r
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            LEFT JOIN tracks t ON t.release_id = r.id AND t.hidden = 0 AND t.variant_section IS NULL
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE r.primary_artist_id = '${aid}' AND r.hidden = 0
            GROUP BY r.id ORDER BY plays ASC LIMIT 20
        `);
        return { rows, name };
    }

    function _anniversary(currentMonth, now) {
        const cutoff = now - 365 * 86400;
        return _q(`
            SELECT ${COLS},
                   MIN(l.timestamp) first_ts,
                   ${new Date().getFullYear()} - CAST(strftime('%Y', datetime(MIN(l.timestamp),'unixepoch')) AS INTEGER) yrs
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE ${CANON}
            GROUP BY r.id
            HAVING strftime('%m', datetime(MIN(l.timestamp),'unixepoch')) = '${currentMonth}'
              AND MIN(l.timestamp) < ${cutoff}
            ORDER BY
                CASE WHEN yrs % 10 = 0 THEN 0 WHEN yrs % 5 = 0 THEN 1
                     WHEN yrs % 2 = 0 THEN 2 ELSE 3 END,
                yrs
            LIMIT 20
        `);
    }

    function _soundtrackSpotlight(now) {
        const cutoff = now - 90 * 86400;
        return _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.type_secondary = 'soundtrack' AND ${CANON}
            GROUP BY r.id
            HAVING MAX(l.timestamp) < ${cutoff}
            ORDER BY plays DESC LIMIT 20
        `);
    }

    function _shortFormFavourites() {
        return _q(`
            SELECT ${COLS}, COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.type IN ('ep','single') AND ${CANON}
            GROUP BY r.id ORDER BY plays DESC LIMIT 30
        `);
    }

    // ── Main loader ────────────────────────────────────────────────────────────

    function _load() {
        const el  = document.getElementById('recShelves');
        const now = Math.floor(Date.now() / 1000);
        const dt  = new Date();
        const cy  = dt.getFullYear();
        const cm  = String(dt.getMonth() + 1).padStart(2, '0');

        // Global seen set — shelves later in the list won't repeat albums
        // shown earlier. Order matters: higher-priority shelves claim first.
        const seen = new Set();

        function picked(pool, n, idx) {
            const rows = _pick(pool, n, idx, seen);
            rows.forEach(r => seen.add(r[0]));
            return rows;
        }


        const favYear   = picked(_favoritesThisYear(now), SHELF_DESKTOP, 0);

        // _favoritesPerYear manages its own internal dedup + respects global seen
        const perYear   = _favoritesPerYear(cy, seen);
        perYear.forEach(r => seen.add(r[0]));

        const throwback  = picked(_throwbacks(cy), SHELF_DESKTOP, 2);
        const thisMonth  = picked(_thisMonthPastYears(cm, cy), SHELF_DESKTOP, 3);
        const rising     = picked(_rising(now), SHELF_DESKTOP, 4);
        const fading     = picked(_fadingFavorites(now), SHELF_DESKTOP, 5);
        const oneAway    = picked(_oneTrackAway(), SHELF_DESKTOP, 6);
        const deepCut    = picked(_deepCutNeeded(), SHELF_DESKTOP, 7);
        const heardOnce  = picked(_onlyHeardOnce(now), SHELF_DESKTOP, 8);

        const moreResult = _moreFromThisArtist(now);
        const moreRows   = picked(moreResult.rows, SHELF_DESKTOP, 9);
        const moreDesc   = moreResult.name
            ? `Other albums by ${moreResult.name}, your most-played artist this fortnight.`
            : 'Other albums by your most-played artist this fortnight.';

        const anniv      = picked(_anniversary(cm, now), SHELF_DESKTOP, 10);
        const soundtrack = picked(_soundtrackSpotlight(now), SHELF_DESKTOP, 11);
        const shortForm  = picked(_shortFormFavourites(), SHELF_DESKTOP, 12);

        const shelves = [
            // ── Decreasing urgency: current → timely → retrospective → exploratory ──
            ['Favorites This Year',        'Top albums by play count in the past 90 days.',                             favYear],
            ['Rising',                     'Albums with more plays in the last 30 days than the 30 before.',           rising],
            ['This Month, Past Years',     'Albums you played most during this calendar month in prior years.',         thisMonth],
            ['Favorites From Last 5 Years','Most-played album from each of the past five calendar years.',              perYear],
            ['Fading Favorites',           'Top 50 all-time albums with no plays in over a year.',                     fading],
            ['Throwbacks',                 'All-time top albums released more than five years ago.',                    throwback],
            ['Anniversary',               'Albums first heard during this calendar month in a previous year.',          anniv],
            ['One Track Away',             'Albums where every eligible track has been heard except one.',              oneAway],
            ['Deep Cut Needed',            'Albums where one track accounts for more than 5× the per-track average.', deepCut],
            ['Only Heard Once',            'Albums with five or fewer total plays, first listened to over a year ago.', heardOnce],
            ['More From This Artist',      moreDesc,                                                                    moreRows],
            ['Soundtrack Spotlight',       'Soundtracks in your top 20 not played in the past 90 days.',               soundtrack],
            ['Short-Form Favourites',      'Most-played EPs and singles.',                                              shortForm],
        ];

        el.innerHTML = shelves.map(([title, desc, rows]) => _shelf(title, desc, rows)).join('');
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    function mount(container, db) {
        _db   = db;
        _seed = _db.exec('SELECT COUNT(*) FROM listens')[0].values[0][0];
        document.title = 'aswin.db/music – Recommendations';

        container.innerHTML = `
            <header class="rec-header">
                <h1>Recommendations</h1>
            </header>
            <div id="recShelves" class="rec-shelves"></div>
        `;
        _load();
    }

    function unmount() { _db = null; }

    return { mount, unmount };
})();
