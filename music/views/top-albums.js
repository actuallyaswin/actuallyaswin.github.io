const ViewTopAlbums = (() => {
    let _db = null;
    let sortBy = 'listens';
    let countLimit = 10;
    let viewMode = 'list';
    let releaseYear = 'all';
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
                    <span class="control-block-label">Released</span>
                    <div class="sort-controls">
                        <select id="yearFilter" class="year-filter-select">
                            <option value="all">All years</option>
                        </select>
                    </div>
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

        setupControls();
        populateYearFilter();
        loadAlbums();
    }

    function unmount() {}

    function loadAlbums() {
        const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';
        const yearFilter = releaseYear !== 'all' ? `AND r.release_year = ${parseInt(releaseYear)}` : '';

        const result = _db.exec(`
            SELECT
                r.id,
                r.title,
                r.release_year,
                r.type,
                COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                a.name,
                a.id as artist_id,
                COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as tracks_listened,
                COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                CAST(SUM(CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
            FROM releases r
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            LEFT JOIN tracks t ON t.release_id = r.id
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0)
            AND NOT EXISTS (SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id)
            ${yearFilter}
            GROUP BY r.id
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
                const [id, title, year, type, albumArtUrl] = row;
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = `?view=release&id=${encodeURIComponent(id)}`;
                const imgSrc = albumArtUrl || getFallbackImageUrl();
                card.innerHTML = `<div class="image-card-img" style="background-image: url('${imgSrc}')"></div>`;
                if (i >= show) card.style.display = 'none';
                container.appendChild(card);
            });
        } else if (viewMode === 'list') {
            container.className = 'wide-grid';
            cachedResults.forEach((row, i) => {
                const [id, title, year, type, albumArtUrl, artistName, artistId, tracksListened, totalListens, totalMinutes] = row;
                const card = createWideCard({
                    href: `?view=release&id=${encodeURIComponent(id)}`,
                    imageUrl: albumArtUrl,
                    name: title,
                    meta: `${escapeHtml(artistName || 'Various Artists')} · ${year || 'Unknown'}`,
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
                const [id, title, year, type, albumArtUrl, artistName, artistId, tracksListened, totalListens, totalMinutes] = row;
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = `?view=release&id=${encodeURIComponent(id)}`;
                const imgSrc = albumArtUrl || getFallbackImageUrl();
                card.innerHTML = `
                    <div class="image-card-img" style="background-image: url('${imgSrc}')"></div>
                    <div class="image-card-overlay">
                        <div class="image-card-name">${escapeHtml(title)}</div>
                        <div class="image-card-artist">${escapeHtml(artistName || 'Various Artists')}</div>
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

    function populateYearFilter() {
        const sel = document.getElementById('yearFilter');
        if (!sel) return;
        const res = _db.exec(`
            SELECT DISTINCT release_year FROM releases
            WHERE release_year IS NOT NULL AND hidden = 0
            ORDER BY release_year DESC
        `)[0];
        if (res) {
            res.values.forEach(([yr]) => {
                const opt = document.createElement('option');
                opt.value = yr;
                opt.textContent = yr;
                if (String(yr) === String(releaseYear)) opt.selected = true;
                sel.appendChild(opt);
            });
        }
    }

    function setupControls() {
        setupToggleGroup('[data-sort]', btn => {
            sortBy = btn.dataset.sort;
            loadAlbums();
        });

        const yearSel = document.getElementById('yearFilter');
        if (yearSel) {
            yearSel.addEventListener('change', () => {
                releaseYear = yearSel.value;
                loadAlbums();
            });
        }

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
