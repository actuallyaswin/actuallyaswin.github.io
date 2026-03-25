const ViewArtist = (() => {
    let _db = null;
    let _artistId = null;
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    let _sortState = { tracks: 'listens', releases: 'listens' };
    let _themeObserver = null;

    function mount(container, db, params) {
        _db = db;
        _artistId = params.id;
        _currentChart = null;
        _chartData = { monthly: null, yearly: null };

        if (!_artistId) {
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

            <header id="artistHeader" class="artist-header-layout">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="artistPhoto">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <circle cx="50" cy="50" r="50" fill="#1e293b"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#475569">♪</text>
                        </svg>
                    </div>
                </div>
                <div class="artist-info-container">
                    <h1 id="artistName">Loading...</h1>
                    <p id="artistGenres" class="genre-list"></p>
                    <div class="stats-compact" id="artistStats">
                        <div class="stat-item">
                            <span class="stat-value" id="totalPlays">-</span>
                            <span class="stat-label">plays</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value" id="uniqueTracks">-</span>
                            <span class="stat-label">tracks</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value" id="totalReleases">-</span>
                            <span class="stat-label">releases</span>
                        </div>
                    </div>
                </div>
            </header>

            <section class="chart-container">
                <div class="chart-header">
                    <h3 class="chart-title">Listening History Over Time</h3>
                    <div class="chart-controls">
                        <div class="control-group">
                            <button class="control-btn${_chartState.granularity === 'monthly' ? ' active' : ''}" data-granularity="monthly">Monthly</button>
                            <button class="control-btn${_chartState.granularity === 'yearly' ? ' active' : ''}" data-granularity="yearly">Yearly</button>
                        </div>
                        <div class="control-group">
                            <button class="control-btn${_chartState.type === 'distribution' ? ' active' : ''}" data-type="distribution">Distribution</button>
                            <button class="control-btn${_chartState.type === 'cumulative' ? ' active' : ''}" data-type="cumulative">Cumulative</button>
                        </div>
                    </div>
                </div>
                <canvas id="historyChart"></canvas>
            </section>

            <div class="two-column-layout">
                <section class="column">
                    <div class="section-header">
                        <h2>Top Releases</h2>
                        <div class="sort-controls">
                            <button class="sort-btn${_sortState.releases === 'listens' ? ' active' : ''}" data-sort-releases="listens" title="Sort by listens"><i data-lucide="headphones"></i></button>
                            <button class="sort-btn${_sortState.releases === 'minutes' ? ' active' : ''}" data-sort-releases="minutes" title="Sort by minutes"><i data-lucide="clock"></i></button>
                            <button class="sort-btn${_sortState.releases === 'date' ? ' active' : ''}" data-sort-releases="date" title="Sort by release date"><i data-lucide="calendar"></i></button>
                        </div>
                    </div>
                    <div id="releases">
                        <div class="loading">Loading releases...</div>
                    </div>
                </section>

                <section class="column">
                    <div class="section-header">
                        <h2>Top Tracks</h2>
                        <div class="sort-controls">
                            <button class="sort-btn${_sortState.tracks === 'listens' ? ' active' : ''}" data-sort-tracks="listens" title="Sort by listens"><i data-lucide="headphones"></i></button>
                            <button class="sort-btn${_sortState.tracks === 'minutes' ? ' active' : ''}" data-sort-tracks="minutes" title="Sort by minutes"><i data-lucide="clock"></i></button>
                        </div>
                    </div>
                    <div class="track-two-col" id="topTracks">
                        <div class="loading">Loading tracks...</div>
                    </div>
                </section>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();

        loadArtistInfo();
        loadTopTracks();
        loadReleases();
        loadListeningHistory();
        setupChartControls();
        setupSortControls();
    }

    function unmount() {
        if (_currentChart) {
            _currentChart.destroy();
            _currentChart = null;
        }
        if (_themeObserver) {
            _themeObserver.disconnect();
            _themeObserver = null;
        }
    }

    function loadArtistInfo() {
        const result = _db.exec(`
            SELECT
                a.artist_name,
                COALESCE(ao.profile_image_url, a.profile_image_url) as profile_image_url,
                COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.track_mbid END) as unique_tracks,
                COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_plays,
                COUNT(DISTINCT t.release_mbid) as total_releases
            FROM artists a
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            LEFT JOIN (SELECT DISTINCT artist_mbid, track_mbid FROM track_artists WHERE role = 'main') ta ON a.artist_mbid = ta.artist_mbid
            LEFT JOIN tracks t ON ta.track_mbid = t.track_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE a.artist_mbid = '${_artistId.replace(/'/g, "''")}'
            GROUP BY a.artist_mbid
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('artistName');
            if (el) el.textContent = 'Artist not found';
            return;
        }

        const [name, profileImageUrl, uniqueTracks, totalPlays, totalReleases] = result.values[0];

        document.getElementById('artistName').textContent = name;
        document.getElementById('totalPlays').textContent = formatNumber(totalPlays);
        document.getElementById('uniqueTracks').textContent = formatNumber(uniqueTracks);
        document.getElementById('totalReleases').textContent = formatNumber(totalReleases);
        document.title = `aswin.db/music - ${name}`;

        if (profileImageUrl) {
            document.getElementById('artistPhoto').innerHTML = `<img src="${profileImageUrl}" alt="${escapeHtml(name)}">`;
        }

        const genreResult = _db.exec(`
            SELECT g.aoty_id, g.name, COUNT(DISTINCT rg.release_mbid) as freq
            FROM overrides.release_genres rg
            JOIN overrides.genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE rg.is_primary = 1
            AND rg.release_mbid IN (
                SELECT DISTINCT t.release_mbid
                FROM track_artists ta
                JOIN tracks t ON ta.track_mbid = t.track_mbid
                WHERE ta.artist_mbid = '${_artistId.replace(/'/g, "''")}' AND ta.role = 'main'
            )
            GROUP BY g.aoty_id
            ORDER BY freq DESC
            LIMIT 8
        `)[0];

        const genresEl = document.getElementById('artistGenres');
        if (genresEl && genreResult && genreResult.values.length > 0) {
            // Pass is_primary=1 for all (already filtered); renderGenreTags expects [id, name, is_primary]
            genresEl.innerHTML = renderGenreTags(genreResult.values.map(([id, name]) => [id, name, 1]));
        }
    }

    function loadTopTracks() {
        const orderClause = _sortState.tracks === 'minutes' ? 'total_minutes DESC' : 'play_count DESC';

        const result = _db.exec(`
            SELECT
                COALESCE(tro.track_name, t.track_name) as track_name,
                t.track_mbid,
                t.duration_ms,
                (SELECT COUNT(*) FROM listens l WHERE l.track_mbid = t.track_mbid) as play_count,
                CAST((SELECT COUNT(*) FROM listens l WHERE l.track_mbid = t.track_mbid) * COALESCE(t.duration_ms, 0) / 60000.0 AS INTEGER) as total_minutes,
                COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
                r.release_mbid
            FROM tracks t
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN releases r ON t.release_mbid = r.release_mbid
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            WHERE t.track_mbid IN (
                SELECT DISTINCT track_mbid FROM track_artists
                WHERE artist_mbid = '${_artistId.replace(/'/g, "''")}' AND role = 'main'
            )
            AND (tro.hidden IS NULL OR tro.hidden = 0)
            AND (SELECT COUNT(*) FROM listens l WHERE l.track_mbid = t.track_mbid) > 0
            ORDER BY ${orderClause}
            LIMIT 20
        `)[0];

        const container = document.getElementById('topTracks');
        if (!container) return;
        container.innerHTML = '';

        if (!result || result.values.length === 0) {
            container.innerHTML = '<div class="loading">No tracks found</div>';
            return;
        }

        result.values.forEach(([trackName, trackMbid, durationMs, playCount, totalMinutes, albumArtUrl, releaseMbid]) => {
            const card = document.createElement('a');
            card.className = 'track-row';
            card.href = releaseMbid ? `?view=release&id=${encodeURIComponent(releaseMbid)}` : '#';
            const imgSrc = albumArtUrl || getFallbackImageUrl();
            card.innerHTML = `
                <div class="track-row-thumb" style="background-image: url('${imgSrc}')"></div>
                <div class="track-row-info">
                    <div class="track-row-name">${escapeHtml(trackName)}</div>
                </div>
                <div class="track-row-stats">
                    <span class="stat-item">
                        <i data-lucide="headphones" style="width: 13px; height: 13px;"></i>
                        ${formatNumber(playCount)}
                    </span>
                </div>
            `;
            container.appendChild(card);
        });

        lucide.createIcons();
    }

    function loadReleases() {
        const sortBy = _sortState.releases;
        let orderClause;
        if (sortBy === 'minutes') {
            orderClause = 'total_minutes DESC';
        } else if (sortBy === 'date') {
            orderClause = 'release_year DESC, total_listens DESC';
        } else {
            orderClause = 'total_listens DESC, release_year DESC';
        }

        const result = _db.exec(`
            SELECT
                r.release_mbid,
                r.release_name,
                COALESCE(ro.release_year, r.release_year) as release_year,
                COALESCE(ro.release_type_primary, r.release_type_primary) as release_type_primary,
                COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN t.track_mbid END) as tracks_listened,
                COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_listens,
                CAST(SUM(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
            FROM releases r
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            JOIN tracks t ON r.release_mbid = t.release_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            JOIN (SELECT DISTINCT track_mbid FROM track_artists WHERE artist_mbid = '${_artistId.replace(/'/g, "''")}' AND role = 'main') ta ON t.track_mbid = ta.track_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE (ro.hidden IS NULL OR ro.hidden = 0)
            AND (ro.hidden IS NULL OR ro.hidden = 0)
            GROUP BY r.release_mbid
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 15
        `)[0];

        const container = document.getElementById('releases');
        if (!container) return;
        container.innerHTML = '';

        if (!result || result.values.length === 0) {
            container.innerHTML = '<div class="loading">No releases found</div>';
            return;
        }

        result.values.forEach(([releaseMbid, releaseName, releaseYear, releaseType, tracksListened, totalListens, totalMinutes]) => {
            const card = document.createElement('a');
            card.className = 'release-card';
            card.href = `?view=release&id=${encodeURIComponent(releaseMbid)}`;

            let statsText;
            if (sortBy === 'minutes') {
                statsText = `${formatNumber(totalMinutes)} min · ${formatNumber(totalListens)} plays`;
            } else if (sortBy === 'date') {
                statsText = `${formatNumber(totalListens)} plays · ${formatNumber(tracksListened)} tracks`;
            } else {
                statsText = `${formatNumber(totalListens)} plays · ${formatNumber(totalMinutes)} min`;
            }

            card.innerHTML = `
                <div class="release-header">
                    <div>
                        <div class="release-name">${escapeHtml(releaseName)}</div>
                        ${releaseType ? `<span class="release-type">${releaseType}</span>` : ''}
                    </div>
                    <div class="release-year">${releaseYear || 'Unknown'}</div>
                </div>
                <div class="release-stats">${statsText}</div>
            `;
            container.appendChild(card);
        });
    }

    function loadListeningHistory() {
        const safeId = _artistId.replace(/'/g, "''");

        const monthlyResult = _db.exec(`
            SELECT l.year, l.month, COUNT(*) as listen_count
            FROM listens l
            LEFT JOIN overrides.track_overrides tro ON l.track_mbid = tro.track_mbid
            WHERE l.main_artist_mbid = '${safeId}'
            AND (tro.hidden IS NULL OR tro.hidden = 0)
            GROUP BY l.year, l.month
            ORDER BY l.year, l.month
        `)[0];

        const yearlyResult = _db.exec(`
            SELECT l.year, COUNT(*) as listen_count
            FROM listens l
            LEFT JOIN overrides.track_overrides tro ON l.track_mbid = tro.track_mbid
            WHERE l.main_artist_mbid = '${safeId}'
            AND (tro.hidden IS NULL OR tro.hidden = 0)
            GROUP BY l.year
            ORDER BY l.year
        `)[0];

        if ((!monthlyResult || monthlyResult.values.length === 0) &&
            (!yearlyResult || yearlyResult.values.length === 0)) {
            const el = document.querySelector('.chart-container');
            if (el) el.innerHTML = '<div class="loading">No listening history found</div>';
            return;
        }

        if (monthlyResult && monthlyResult.values.length > 0) {
            _chartData.monthly = buildMonthlyChartData(monthlyResult.values);
        }

        if (yearlyResult && yearlyResult.values.length > 0) {
            _chartData.yearly = buildYearlyChartData(yearlyResult.values);
        }

        renderChart();
    }

    function buildMonthlyChartData(values) {
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const years = values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);
        const dataMap = new Map();
        values.forEach(([year, month, count]) => dataMap.set(`${year}-${month}`, count));
        const labels = [], data = [];
        for (let year = minYear; year <= maxYear; year++) {
            for (let month = 1; month <= 12; month++) {
                labels.push(`${monthNames[month - 1]} ${year}`);
                data.push(dataMap.get(`${year}-${month}`) || 0);
            }
        }
        return { labels, data };
    }

    function buildYearlyChartData(values) {
        const years = values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);
        const dataMap = new Map();
        values.forEach(([year, count]) => dataMap.set(year, count));
        const labels = [], data = [];
        for (let year = minYear; year <= maxYear; year++) {
            labels.push(year.toString());
            data.push(dataMap.get(year) || 0);
        }
        return { labels, data };
    }

    function setupChartControls() {
        setupToggleGroup('[data-granularity]', btn => {
            _chartState.granularity = btn.dataset.granularity;
            renderChart();
        });

        setupToggleGroup('[data-type]', btn => {
            _chartState.type = btn.dataset.type;
            renderChart();
        });

        _themeObserver = new MutationObserver(() => {
            if (_currentChart) renderChart();
        });
        _themeObserver.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ['data-theme']
        });
    }

    function setupSortControls() {
        setupToggleGroup('[data-sort-tracks]', btn => {
            _sortState.tracks = btn.dataset.sortTracks;
            loadTopTracks();
        });

        setupToggleGroup('[data-sort-releases]', btn => {
            _sortState.releases = btn.dataset.sortReleases;
            loadReleases();
        });
    }

    function renderChart() {
        const data = _chartData[_chartState.granularity];
        if (!data) return;

        const primaryColor = getCSSColor('--primary');
        const chartBg = getCSSColor('--chart-bg');
        const chartBgSolid = getCSSColor('--chart-bg-solid');
        const bgSecondary = getCSSColor('--bg-secondary');
        const textColor = getCSSColor('--text');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor = getCSSColor('--border');

        let chartValues = [...data.data];
        if (_chartState.type === 'cumulative') {
            chartValues = data.data.reduce((acc, val, idx) => {
                acc.push(idx === 0 ? val : acc[idx - 1] + val);
                return acc;
            }, []);
        }

        const skipFactor = Math.max(1, Math.ceil(data.labels.length / 15));
        const labelCallback = (value, index) => index % skipFactor === 0 ? data.labels[index] : '';

        if (_currentChart) _currentChart.destroy();

        const ctx = document.getElementById('historyChart');
        if (!ctx) return;

        _currentChart = new Chart(ctx.getContext('2d'), {
            type: _chartState.type === 'cumulative' ? 'line' : 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: _chartState.type === 'cumulative' ? 'Total Listens' : 'Listens per Period',
                    data: chartValues,
                    backgroundColor: _chartState.type === 'cumulative' ? chartBg : chartBgSolid,
                    borderColor: primaryColor,
                    borderWidth: _chartState.type === 'cumulative' ? 3 : 1,
                    fill: _chartState.type === 'cumulative',
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: bgSecondary,
                        titleColor: textColor,
                        bodyColor: textColor,
                        borderColor: borderColor,
                        borderWidth: 1
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: textSecondary },
                        grid: { color: borderColor }
                    },
                    x: {
                        ticks: {
                            color: textSecondary,
                            maxRotation: 45,
                            minRotation: 45,
                            autoSkip: false,
                            callback: labelCallback
                        },
                        grid: { color: borderColor }
                    }
                }
            }
        });
    }

    return { mount, unmount };
})();
