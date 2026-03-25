const ViewTopTracks = (() => {
    let _db = null;
    let sortBy = 'listens';
    let countLimit = 10;
    let cachedResults = [];

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music – Top Tracks';

        container.innerHTML = `
            <div class="site-header">
                <a href="?" class="site-logo-small">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </a>
                <a href="?" class="back-button">← Home</a>
            </div>

            <header>
                <h1>Top Tracks</h1>
            </header>

            <div class="page-controls">
                <div class="control-block">
                    <span class="control-block-label">Sort By</span>
                    <div class="sort-controls">
                        <button class="sort-btn${sortBy === 'listens' ? ' active' : ''}" data-sort="listens" title="Sort by listens"><i data-lucide="headphones"></i></button>
                        <button class="sort-btn${sortBy === 'minutes' ? ' active' : ''}" data-sort="minutes" title="Sort by minutes"><i data-lucide="clock"></i></button>
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">#</span>
                    <div class="sort-controls">
                        ${[10, 20, 50, 100].map(n =>
                            `<button class="sort-btn${countLimit === n ? ' active' : ''}" data-count="${n}">${n}</button>`
                        ).join('')}
                    </div>
                </div>
            </div>

            <div id="tracksContainer" class="track-two-col">
                <div class="loading">Loading tracks...</div>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();
        setupControls();
        loadTracks();
    }

    function unmount() {}

    function loadTracks() {
        const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';

        const result = _db.exec(`
            SELECT
                t.track_mbid,
                t.track_name,
                a.artist_name,
                a.artist_mbid,
                COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
                r.release_mbid,
                COUNT(l.timestamp) as total_listens,
                CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000.0 AS INTEGER) as total_minutes
            FROM tracks t
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            LEFT JOIN releases r ON t.release_mbid = r.release_mbid
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE (tro.hidden IS NULL OR tro.hidden = 0)
            AND (ao.hidden IS NULL OR ao.hidden = 0)
            GROUP BY t.track_mbid
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 100
        `)[0];

        cachedResults = result ? result.values : [];
        renderTracks();
    }

    function renderTracks() {
        const container = document.getElementById('tracksContainer');
        if (!container) return;
        container.innerHTML = '';

        cachedResults.forEach((row, i) => {
            const [trackMbid, trackName, artistName, artistMbid, albumArtUrl, releaseMbid, totalListens, totalMinutes] = row;
            const href = releaseMbid ? `?view=release&id=${encodeURIComponent(releaseMbid)}` : '#';
            const imgSrc = albumArtUrl || getFallbackImageUrl();

            const card = document.createElement('a');
            card.className = 'track-row';
            card.href = href;
            card.innerHTML = `
                <div class="track-row-thumb" style="background-image: url('${imgSrc}')"></div>
                <div class="track-row-info">
                    <div class="track-row-name">${escapeHtml(trackName)}</div>
                    ${artistName ? `<div class="track-row-artist">${escapeHtml(artistName)}</div>` : ''}
                </div>
                <div class="track-row-stats">
                    <span class="stat-item">
                        <i data-lucide="headphones" style="width: 13px; height: 13px;"></i>
                        ${formatNumber(totalListens)}
                    </span>
                </div>
            `;

            if (i >= countLimit) card.style.display = 'none';
            container.appendChild(card);
        });

        lucide.createIcons();
    }

    function applyCount() {
        const container = document.getElementById('tracksContainer');
        if (!container) return;
        Array.from(container.children).forEach((el, i) => {
            el.style.display = i < countLimit ? '' : 'none';
        });
    }

    function setupControls() {
        setupToggleGroup('[data-sort]', btn => {
            sortBy = btn.dataset.sort;
            loadTracks();
        });

        setupToggleGroup('[data-count]', btn => {
            countLimit = parseInt(btn.dataset.count);
            applyCount();
        });
    }

    return { mount, unmount };
})();
