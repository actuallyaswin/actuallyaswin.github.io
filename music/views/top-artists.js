const ViewTopArtists = (() => {
    let _db = null;
    let sortBy = 'listens';
    let countLimit = 10;
    let viewMode = 'list';
    let cachedResults = [];

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music – Top Artists';

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
                <h1>Top Artists</h1>
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

            <div id="artistsContainer" class="image-grid">
                <div class="loading">Loading artists...</div>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();
        setupControls();
        loadArtists();
    }

    function unmount() {}

    function loadArtists() {
        const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';

        const result = _db.exec(`
            SELECT
                a.id,
                a.name,
                a.image_url,
                COUNT(DISTINCT CASE WHEN t.hidden = 0 THEN l.id END) as unique_tracks,
                COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                CAST(SUM(CASE WHEN t.hidden = 0 THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
            FROM artists a
            LEFT JOIN track_artists ta ON a.id = ta.artist_id AND ta.role = 'main'
            LEFT JOIN tracks t ON ta.track_id = t.id
            LEFT JOIN listens l ON t.id = l.track_id
            WHERE a.hidden = 0
            GROUP BY a.id
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 100
        `)[0];

        cachedResults = result ? result.values : [];
        renderArtists();
    }

    function getCertTier(totalListens) {
        if (totalListens >= 1000) return 'diamond';
        if (totalListens >= 500)  return 'platinum';
        if (totalListens >= 250)  return 'gold';
        return null;
    }

    const CERT_LABELS = {
        gold: 'Gold — 250+ plays',
        platinum: 'Platinum — 500+ plays',
        diamond: 'Diamond — 1,000+ plays',
    };

    function renderArtists() {
        const container = document.getElementById('artistsContainer');
        if (!container) return;
        container.innerHTML = '';
        container.style.gridTemplateColumns = '';

        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[countLimit];
            const show = n * n;
            container.className = 'collage-grid';
            container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;
            cachedResults.forEach((row, i) => {
                const [id, name, imageUrl] = row;
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = `?view=artist&id=${encodeURIComponent(id)}`;
                const imgSrc = imageUrl || getFallbackImageUrl();
                card.innerHTML = `<div class="image-card-img" style="background-image: url('${imgSrc}')"></div>`;
                if (i >= show) card.style.display = 'none';
                container.appendChild(card);
            });
        } else if (viewMode === 'list') {
            container.className = 'wide-grid';
            cachedResults.forEach((row, i) => {
                const [id, name, imageUrl, uniqueTracks, totalListens, totalMinutes] = row;
                const card = createWideCard({
                    href: `?view=artist&id=${encodeURIComponent(id)}`,
                    imageUrl,
                    name,
                    meta: `${formatNumber(uniqueTracks)} tracks`,
                    totalListens,
                    totalMinutes,
                    rounded: true
                });
                const cert = getCertTier(totalListens);
                if (cert) card.classList.add(`release-card-cert-${cert}`);
                if (i >= countLimit) card.style.display = 'none';
                container.appendChild(card);
            });
        } else {
            container.className = 'image-grid';
            cachedResults.forEach((row, i) => {
                const [id, name, imageUrl, uniqueTracks, totalListens, totalMinutes] = row;
                const cert = getCertTier(totalListens);
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = `?view=artist&id=${encodeURIComponent(id)}`;
                const imgSrc = imageUrl || getFallbackImageUrl();
                card.innerHTML = `
                    <div class="image-card-img" style="background-image: url('${imgSrc}')"></div>
                    <div class="image-card-overlay">
                        <div class="image-card-name">${escapeHtml(name)}</div>
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
        const container = document.getElementById('artistsContainer');
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
            loadArtists();
        });

        setupToggleGroup('[data-count]', btn => {
            countLimit = parseInt(btn.dataset.count);
            applyCount();
        });

        setupToggleGroup('[data-view]', btn => {
            viewMode = btn.dataset.view;
            updateCountLabels(viewMode);
            renderArtists();
        });
    }

    return { mount, unmount };
})();
