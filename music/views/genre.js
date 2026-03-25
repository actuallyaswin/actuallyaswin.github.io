const ViewGenre = (() => {
    let _db = null;
    let _genreId = null;

    function mount(container, db, params) {
        _db = db;
        _genreId = params.id;

        if (!_genreId) {
            navigate({ view: 'home' });
            return;
        }

        container.innerHTML = `
            <div class="site-header">
                <a href="?" class="site-logo-small">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </a>
                <a href="javascript:history.back()" class="back-button">← Back</a>
            </div>

            <header>
                <h1 id="genreName">Loading...</h1>
                <p class="subtitle" id="genreSubtitle"></p>
            </header>

            <section>
                <div id="releasesContainer" class="wide-grid">
                    <div class="loading">Loading releases...</div>
                </div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadGenreInfo();
        loadGenreReleases();
    }

    function unmount() {}

    function loadGenreInfo() {
        const safeId = parseInt(_genreId);
        if (isNaN(safeId)) {
            document.getElementById('genreName').textContent = 'Genre not found';
            return;
        }

        const result = _db.exec(`
            SELECT g.name,
                   COUNT(DISTINCT rg.release_mbid) as release_count,
                   COUNT(l.timestamp) as total_plays
            FROM overrides.genres g
            JOIN overrides.release_genres rg ON g.aoty_id = rg.aoty_genre_id
            JOIN tracks t ON rg.release_mbid = t.release_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE g.aoty_id = ${safeId}
            AND (tro.hidden IS NULL OR tro.hidden = 0)
        `)[0];

        if (!result || result.values.length === 0) {
            document.getElementById('genreName').textContent = 'Genre not found';
            return;
        }

        const [name, releaseCount, totalPlays] = result.values[0];
        document.getElementById('genreName').textContent = name || 'Unknown Genre';
        document.getElementById('genreSubtitle').textContent =
            `${formatNumber(releaseCount)} releases · ${formatNumber(totalPlays)} plays`;
        document.title = `aswin.db/music - ${name}`;
    }

    function loadGenreReleases() {
        const safeId = parseInt(_genreId);
        if (isNaN(safeId)) return;

        const result = _db.exec(`
            SELECT * FROM (
                SELECT
                    r.release_mbid,
                    r.release_name,
                    COALESCE(ro.release_year, r.release_year) as release_year,
                    COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
                    (SELECT a.artist_name FROM track_artists ta
                     JOIN artists a ON ta.artist_mbid = a.artist_mbid
                     WHERE ta.track_mbid = (SELECT t2.track_mbid FROM tracks t2
                                            WHERE t2.release_mbid = r.release_mbid LIMIT 1)
                     AND ta.role = 'main' LIMIT 1) as artist_name,
                    (SELECT COUNT(l2.timestamp)
                     FROM tracks t2
                     LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
                     JOIN listens l2 ON t2.track_mbid = l2.track_mbid
                     WHERE t2.release_mbid = r.release_mbid
                     AND (tro2.hidden IS NULL OR tro2.hidden = 0)) as total_plays,
                    (SELECT CAST(SUM(COALESCE(t2.duration_ms, 0)) / 60000.0 AS INTEGER)
                     FROM tracks t2
                     LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
                     JOIN listens l2 ON t2.track_mbid = l2.track_mbid
                     WHERE t2.release_mbid = r.release_mbid
                     AND (tro2.hidden IS NULL OR tro2.hidden = 0)) as total_minutes
                FROM overrides.release_genres rg
                JOIN releases r ON rg.release_mbid = r.release_mbid
                LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
                WHERE rg.aoty_genre_id = ${safeId}
            ) WHERE total_plays > 0
            ORDER BY total_plays DESC
        `)[0];

        const container = document.getElementById('releasesContainer');
        if (!container) return;
        container.innerHTML = '';

        if (!result || result.values.length === 0) {
            container.innerHTML = '<div class="loading">No releases found</div>';
            return;
        }

        result.values.forEach(([releaseMbid, releaseName, releaseYear, albumArtUrl, artistName, totalPlays, totalMinutes]) => {
            const card = createWideCard({
                href: `?view=release&id=${encodeURIComponent(releaseMbid)}`,
                imageUrl: albumArtUrl,
                name: releaseName,
                meta: `${escapeHtml(artistName || 'Unknown')} · ${releaseYear || 'Unknown'}`,
                totalListens: totalPlays,
                totalMinutes,
                rounded: false
            });
            container.appendChild(card);
        });

        lucide.createIcons();
    }

    return { mount, unmount };
})();
