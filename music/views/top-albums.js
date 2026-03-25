const ViewTopAlbums = (() => {
    let _db = null;
    let sortBy = 'listens';
    let countLimit = 10;
    let viewMode = 'list';
    let cachedResults = [];

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music – Top Albums';

        const countBtns = [10, 20, 50, 100].map(n => {
            const label = viewMode === 'collage' ? (() => { const s = COLLAGE_SIZES[n]; return `${s}×${s}`; })() : n;
            return `<button class="sort-btn${countLimit === n ? ' active' : ''}" data-count="${n}">${label}</button>`;
        }).join('');

        container.innerHTML = `
            <div class="site-header">
                <a href="?" class="site-logo-small">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </a>
                <a href="?" class="back-button">← Home</a>
            </div>

            <header>
                <h1>Top Albums</h1>
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
                    <div class="sort-controls">${countBtns}</div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Display</span>
                    <div class="sort-controls">
                        <button class="sort-btn${viewMode === 'list' ? ' active' : ''}" data-view="list" title="List"><i data-lucide="layout-list"></i></button>
                        <button class="sort-btn${viewMode === 'tiles' ? ' active' : ''}" data-view="tiles" title="Tiles"><i data-lucide="layout-grid"></i></button>
                        <button class="sort-btn${viewMode === 'collage' ? ' active' : ''}" data-view="collage" title="Collage"><i data-lucide="grid-3x3"></i></button>
                    </div>
                </div>
            </div>

            <div id="albumsContainer" class="image-grid">
                <div class="loading">Loading albums...</div>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();
        setupControls();
        loadAlbums();
    }

    function unmount() {}

    function loadAlbums() {
        const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';

        const result = _db.exec(`
            SELECT
                r.release_mbid,
                r.release_name,
                COALESCE(ro.release_year, r.release_year) as release_year,
                COALESCE(ro.release_type_primary, r.release_type_primary) as release_type_primary,
                COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
                a.artist_name,
                a.artist_mbid,
                (SELECT COUNT(DISTINCT t2.track_mbid)
                 FROM tracks t2
                 LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
                 LEFT JOIN listens l2 ON t2.track_mbid = l2.track_mbid
                 WHERE t2.release_mbid = r.release_mbid
                 AND (tro2.hidden IS NULL OR tro2.hidden = 0)
                 AND l2.timestamp IS NOT NULL) as tracks_listened,
                (SELECT COUNT(l2.timestamp)
                 FROM tracks t2
                 LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
                 LEFT JOIN listens l2 ON t2.track_mbid = l2.track_mbid
                 WHERE t2.release_mbid = r.release_mbid
                 AND (tro2.hidden IS NULL OR tro2.hidden = 0)) as total_listens,
                (SELECT CAST(SUM(COALESCE(t2.duration_ms, 0)) / 60000.0 AS INTEGER)
                 FROM tracks t2
                 LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
                 LEFT JOIN listens l2 ON t2.track_mbid = l2.track_mbid
                 WHERE t2.release_mbid = r.release_mbid
                 AND (tro2.hidden IS NULL OR tro2.hidden = 0)) as total_minutes
            FROM releases r
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            JOIN tracks t ON r.release_mbid = t.release_mbid
            JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
            JOIN artists a ON ta.artist_mbid = a.artist_mbid
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            WHERE (ro.hidden IS NULL OR ro.hidden = 0)
            AND (ao.hidden IS NULL OR ao.hidden = 0)
            GROUP BY r.release_mbid, a.artist_mbid
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 100
        `)[0];

        cachedResults = result ? result.values : [];
        renderAlbums();
    }

    function renderAlbums() {
        const container = document.getElementById('albumsContainer');
        if (!container) return;
        container.innerHTML = '';
        container.style.gridTemplateColumns = '';

        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[countLimit];
            const show = n * n;
            container.className = 'collage-grid';
            container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;
            cachedResults.forEach((row, i) => {
                const [releaseMbid, releaseName, releaseYear, releaseType, albumArtUrl] = row;
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = `?view=release&id=${encodeURIComponent(releaseMbid)}`;
                const imgSrc = albumArtUrl || getFallbackImageUrl();
                card.innerHTML = `<div class="image-card-img" style="background-image: url('${imgSrc}')"></div>`;
                if (i >= show) card.style.display = 'none';
                container.appendChild(card);
            });
        } else if (viewMode === 'list') {
            container.className = 'wide-grid';
            cachedResults.forEach((row, i) => {
                const [releaseMbid, releaseName, releaseYear, releaseType, albumArtUrl, artistName, artistMbid, tracksListened, totalListens, totalMinutes] = row;
                const card = createWideCard({
                    href: `?view=release&id=${encodeURIComponent(releaseMbid)}`,
                    imageUrl: albumArtUrl,
                    name: releaseName,
                    meta: `${escapeHtml(artistName)} · ${releaseYear || 'Unknown'}`,
                    totalListens,
                    totalMinutes,
                    rounded: false
                });
                if (i >= countLimit) card.style.display = 'none';
                container.appendChild(card);
            });
        } else {
            container.className = 'image-grid';
            cachedResults.forEach((row, i) => {
                const [releaseMbid, releaseName, releaseYear, releaseType, albumArtUrl, artistName, artistMbid, tracksListened, totalListens, totalMinutes] = row;
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = `?view=release&id=${encodeURIComponent(releaseMbid)}`;
                const imgSrc = albumArtUrl || getFallbackImageUrl();
                card.innerHTML = `
                    <div class="image-card-img" style="background-image: url('${imgSrc}')"></div>
                    <div class="image-card-overlay">
                        <div class="image-card-name">${escapeHtml(releaseName)}</div>
                        <div class="image-card-artist">${escapeHtml(artistName)}</div>
                        <div class="image-card-stats">
                            <span class="stat-item">
                                <i data-lucide="headphones" style="width: 14px; height: 14px;"></i>
                                ${formatNumber(totalListens)}
                            </span>
                            <span class="stat-item">
                                <i data-lucide="clock" style="width: 14px; height: 14px;"></i>
                                ${formatNumber(totalMinutes)} min
                            </span>
                        </div>
                    </div>
                `;
                if (i >= countLimit) card.style.display = 'none';
                container.appendChild(card);
            });
        }

        lucide.createIcons();
    }

    function applyCount() {
        const container = document.getElementById('albumsContainer');
        if (!container) return;
        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[countLimit];
            const show = n * n;
            container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;
            Array.from(container.children).forEach((el, i) => {
                el.style.display = i < show ? '' : 'none';
            });
        } else {
            Array.from(container.children).forEach((el, i) => {
                el.style.display = i < countLimit ? '' : 'none';
            });
        }
    }

    function setupControls() {
        setupToggleGroup('[data-sort]', btn => {
            sortBy = btn.dataset.sort;
            loadAlbums();
        });

        setupToggleGroup('[data-count]', btn => {
            countLimit = parseInt(btn.dataset.count);
            applyCount();
        });

        setupToggleGroup('[data-view]', btn => {
            viewMode = btn.dataset.view;
            updateCountLabels(viewMode);
            renderAlbums();
        });
    }

    return { mount, unmount };
})();
