const ViewYear = (() => {
    let _db = null;
    let currentYear = null;
    let MIN_YEAR = null;
    let MAX_YEAR = null;
    let sortBy = 'listens';
    let countLimit = 10;
    let viewMode = 'list';
    let filterMode = 'this-year';
    let cachedReleases = [];
    let cachedArtists = [];

    function mount(container, db, params) {
        _db = db;

        if (MIN_YEAR === null) {
            const yearRange = _db.exec(`
                SELECT MIN(year) as min_year, MAX(year) as max_year
                FROM listens WHERE year IS NOT NULL
            `)[0];
            if (yearRange && yearRange.values[0][0] !== null) {
                MIN_YEAR = yearRange.values[0][0];
                MAX_YEAR = yearRange.values[0][1];
            } else {
                MIN_YEAR = 1960;
                MAX_YEAR = new Date().getFullYear();
            }
        }

        currentYear = parseInt(params.year) || MAX_YEAR;
        if (currentYear < MIN_YEAR) currentYear = MIN_YEAR;
        if (currentYear > MAX_YEAR) currentYear = MAX_YEAR;

        document.title = `aswin.db/music - ${currentYear}`;

        container.innerHTML = buildTemplate();
        lucide.createIcons();

        populateYearSelector();
        setupControls();
        setupYearNavigation();
        loadYearStats();
        loadReleases();
        loadArtists();
        loadYearGenres();
    }

    function unmount() {}

    function buildTemplate() {
        const countBtns = [10, 20, 50, 100].map(n => {
            const label = viewMode === 'collage' ? (() => { const s = COLLAGE_SIZES[n]; return `${s}×${s}`; })() : n;
            return `<button class="sort-btn${countLimit === n ? ' active' : ''}" data-count="${n}">${label}</button>`;
        }).join('');

        return `
            <div class="site-header">
                <a href="?" class="site-logo-small">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </a>
                <a href="?" class="back-button">← Home</a>
            </div>

            <header>
                <div class="year-navigation">
                    <button id="prevYear" class="year-nav-arrow" aria-label="Previous year">←</button>
                    <h1 id="pageTitle">
                        <select id="yearSelect" class="year-select-hero"></select>
                    </h1>
                    <button id="nextYear" class="year-nav-arrow" aria-label="Next year">→</button>
                </div>
                <p class="subtitle" id="yearSummary">Loading...</p>
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
                <div class="control-block">
                    <span class="control-block-label">Filter</span>
                    <div class="sort-controls">
                        <button class="sort-btn${filterMode === 'this-year' ? ' active' : ''}" data-filter="this-year">THIS YEAR</button>
                        <button class="sort-btn${filterMode === 'all-years' ? ' active' : ''}" data-filter="all-years">ALL YEARS</button>
                    </div>
                </div>
            </div>

            <section class="year-section">
                <h2>Releases</h2>
                <div id="releasesContainer" class="image-grid">
                    <div class="loading">Loading releases...</div>
                </div>
            </section>

            <section class="year-section">
                <h2>Artists</h2>
                <div id="artistsContainer" class="image-grid">
                    <div class="loading">Loading artists...</div>
                </div>
            </section>

            <section class="year-section" id="genresSection">
                <h2>Genres</h2>
                <div id="genresContainer" class="genre-list">
                    <div class="loading">Loading...</div>
                </div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;
    }

    function populateYearSelector() {
        const select = document.getElementById('yearSelect');
        select.innerHTML = '';
        for (let year = MAX_YEAR; year >= MIN_YEAR; year--) {
            const option = document.createElement('option');
            option.value = year;
            option.textContent = year;
            if (year === currentYear) option.selected = true;
            select.appendChild(option);
        }
    }

    function loadYearStats() {
        const result = _db.exec(`
            SELECT
                (SELECT COUNT(DISTINCT ta.artist_id)
                 FROM listens l2
                 JOIN tracks t2 ON l2.track_id = t2.id
                 JOIN track_artists ta ON t2.id = ta.track_id AND ta.role = 'main'
                 JOIN artists a2 ON ta.artist_id = a2.id
                 WHERE l2.year = ${currentYear} AND t2.hidden = 0 AND a2.hidden = 0) as artist_count,
                (SELECT COUNT(DISTINCT t2.release_id)
                 FROM listens l2
                 JOIN tracks t2 ON l2.track_id = t2.id
                 JOIN releases r2 ON t2.release_id = r2.id
                 WHERE l2.year = ${currentYear} AND t2.hidden = 0 AND r2.hidden = 0) as album_count,
                (SELECT COUNT(*)
                 FROM listens l2
                 JOIN tracks t2 ON l2.track_id = t2.id
                 WHERE l2.year = ${currentYear} AND t2.hidden = 0) as total_listens
        `)[0];

        const el = document.getElementById('yearSummary');
        if (!el) return;
        if (result && result.values.length > 0) {
            const [artistCount, albumCount, totalListens] = result.values[0];
            el.textContent = `${formatNumber(artistCount)} artists · ${formatNumber(albumCount)} albums · ${formatNumber(totalListens)} plays`;
        } else {
            el.textContent = 'No data for this year';
        }
    }

    function loadReleases() {
        const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';
        const thisYearFilter = filterMode === 'this-year';

        const whereClause = thisYearFilter
            ? `r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0) AND r.release_year = ${currentYear}`
            : `l.year = ${currentYear} AND r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0)`;

        const fromClause = thisYearFilter
            ? `FROM releases r
               LEFT JOIN artists a ON a.id = r.primary_artist_id
               LEFT JOIN tracks t ON r.id = t.release_id AND t.hidden = 0
               LEFT JOIN listens l ON t.id = l.track_id`
            : `FROM listens l
               JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
               JOIN releases r ON t.release_id = r.id
               LEFT JOIN artists a ON a.id = r.primary_artist_id`;

        const result = _db.exec(`
            SELECT
                r.id,
                r.title,
                r.release_year,
                r.album_art_url,
                a.name,
                a.id as artist_id,
                (SELECT COUNT(*)
                 FROM tracks t2
                 JOIN listens l2 ON t2.id = l2.track_id
                 WHERE t2.release_id = r.id AND t2.hidden = 0
                 AND l2.year = ${currentYear}) as total_listens,
                (SELECT CAST(SUM(COALESCE(t2.duration_ms, 0)) / 60000.0 AS INTEGER)
                 FROM tracks t2
                 JOIN listens l2 ON t2.id = l2.track_id
                 WHERE t2.release_id = r.id AND t2.hidden = 0
                 AND l2.year = ${currentYear}) as total_minutes
            ${fromClause}
            WHERE ${whereClause}
            GROUP BY r.id
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 100
        `)[0];

        cachedReleases = result ? result.values : [];
        renderReleases();
    }

    function loadArtists() {
        const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';

        let query;
        if (filterMode === 'this-year') {
            query = `
                SELECT
                    a.id,
                    a.name,
                    a.image_url,
                    COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.year = ${currentYear} THEN l.id END) as unique_tracks,
                    COUNT(CASE WHEN t.hidden = 0 AND l.year = ${currentYear} THEN l.id END) as total_listens,
                    CAST(SUM(CASE WHEN t.hidden = 0 AND l.year = ${currentYear} THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
                FROM releases r
                JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
                JOIN artists a ON ra.artist_id = a.id
                LEFT JOIN tracks t ON r.id = t.release_id
                LEFT JOIN listens l ON t.id = l.track_id
                WHERE r.release_year = ${currentYear} AND r.hidden = 0 AND a.hidden = 0
                GROUP BY a.id
                HAVING total_listens > 0
                ORDER BY ${orderClause}
                LIMIT 100
            `;
        } else {
            query = `
                SELECT
                    a.id,
                    a.name,
                    a.image_url,
                    COUNT(DISTINCT CASE WHEN t.hidden = 0 THEN l.id END) as unique_tracks,
                    COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                    CAST(SUM(CASE WHEN t.hidden = 0 THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                JOIN artists a ON ta.artist_id = a.id
                WHERE l.year = ${currentYear} AND t.hidden = 0 AND a.hidden = 0
                GROUP BY a.id
                HAVING total_listens > 0
                ORDER BY ${orderClause}
                LIMIT 100
            `;
        }

        const result = _db.exec(query)[0];
        cachedArtists = result ? result.values : [];
        renderArtists();
    }

    function renderReleases() {
        const container = document.getElementById('releasesContainer');
        if (!container) return;
        container.innerHTML = '';
        container.style.gridTemplateColumns = '';

        if (cachedReleases.length === 0) {
            container.className = 'image-grid';
            container.innerHTML = '<div class="loading">No releases found</div>';
            return;
        }

        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[countLimit];
            const show = n * n;
            container.className = 'collage-grid';
            container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;
            cachedReleases.forEach((row, i) => {
                const [id, title, year, albumArtUrl] = row;
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
            cachedReleases.forEach((row, i) => {
                const [id, title, year, albumArtUrl, artistName, artistId, totalListens, totalMinutes] = row;
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
            cachedReleases.forEach((row, i) => {
                const [id, title, year, albumArtUrl, artistName, artistId, totalListens, totalMinutes] = row;
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

        lucide.createIcons();
    }

    function renderArtists() {
        const container = document.getElementById('artistsContainer');
        if (!container) return;
        container.innerHTML = '';
        container.style.gridTemplateColumns = '';

        if (cachedArtists.length === 0) {
            container.className = 'image-grid';
            container.innerHTML = '<div class="loading">No artists found</div>';
            return;
        }

        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[countLimit];
            const show = n * n;
            container.className = 'collage-grid';
            container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;
            cachedArtists.forEach((row, i) => {
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
            cachedArtists.forEach((row, i) => {
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
                if (i >= countLimit) card.style.display = 'none';
                container.appendChild(card);
            });
        } else {
            container.className = 'image-grid';
            cachedArtists.forEach((row, i) => {
                const [id, name, imageUrl, uniqueTracks, totalListens, totalMinutes] = row;
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
        const containers = [
            document.getElementById('releasesContainer'),
            document.getElementById('artistsContainer')
        ];
        containers.forEach(container => {
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
        });
    }

    function setupControls() {
        setupToggleGroup('[data-sort]', btn => {
            sortBy = btn.dataset.sort;
            loadReleases();
            loadArtists();
        });

        setupToggleGroup('[data-count]', btn => {
            countLimit = parseInt(btn.dataset.count);
            applyCount();
        });

        setupToggleGroup('[data-filter]', btn => {
            filterMode = btn.dataset.filter;
            loadReleases();
            loadArtists();
        });

        setupToggleGroup('[data-view]', btn => {
            viewMode = btn.dataset.view;
            updateCountLabels(viewMode);
            renderReleases();
            renderArtists();
        });
    }

    function setupYearNavigation() {
        const prevBtn = document.getElementById('prevYear');
        const nextBtn = document.getElementById('nextYear');
        const yearSelect = document.getElementById('yearSelect');

        prevBtn.addEventListener('click', () => {
            if (currentYear > MIN_YEAR) navigateToYear(currentYear - 1);
        });

        nextBtn.addEventListener('click', () => {
            if (currentYear < MAX_YEAR) navigateToYear(currentYear + 1);
        });

        yearSelect.addEventListener('change', e => {
            navigateToYear(parseInt(e.target.value));
        });

        updateNavigationButtons();
    }

    function updateNavigationButtons() {
        const prev = document.getElementById('prevYear');
        const next = document.getElementById('nextYear');
        if (prev) prev.disabled = currentYear <= MIN_YEAR;
        if (next) next.disabled = currentYear >= MAX_YEAR;
    }

    function navigateToYear(year) {
        navigate({ view: 'year', year: String(year) });
    }

    function loadYearGenres() {
        const result = _db.exec(`
            SELECT g.aoty_id, g.name, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN release_genres rg ON t.release_id = rg.release_id AND rg.is_primary = 1
            JOIN genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE l.year = ${currentYear} AND t.hidden = 0
            GROUP BY g.aoty_id
            ORDER BY listen_count DESC
            LIMIT 10
        `)[0];

        const container = document.getElementById('genresContainer');
        const section = document.getElementById('genresSection');
        if (!container) return;

        if (!result || result.values.length === 0) {
            if (section) section.style.display = 'none';
            return;
        }

        container.innerHTML = result.values.map(([id, name, count]) =>
            `<a href="?view=genre&id=${id}" class="genre-tag">${escapeHtml(name)}</a> <span class="genre-year-count">(${formatNumber(count)})</span>`
        ).join(', ');
    }

    return { mount, unmount };
})();
