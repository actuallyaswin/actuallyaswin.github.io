const ViewRelease = (() => {
    let _db = null;
    let _releaseId = null;
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    let _themeObserver = null;

    function mount(container, db, params) {
        _db = db;
        _releaseId = params.id;
        _currentChart = null;
        _chartData = { monthly: null, yearly: null };

        if (!_releaseId) {
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

            <header id="releaseHeader" class="artist-header-layout">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="albumArt">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <rect width="100" height="100" fill="#1e293b"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#475569">♪</text>
                        </svg>
                    </div>
                </div>
                <div class="artist-info-container">
                    <h1 id="releaseName">Loading...</h1>
                    <p class="release-artist">
                        by <span id="releaseArtist"></span> ·
                        <span id="releaseYear"></span> ·
                        <span id="releaseType"></span>
                    </p>
                    <p id="releaseGenres" class="genre-list"></p>
                    <div class="stats-compact" id="releaseStats">
                        <div class="stat-item">
                            <span class="stat-value" id="totalPlays">-</span>
                            <span class="stat-label">plays</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value" id="tracksListened">-</span>
                            <span class="stat-label">tracks</span>
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

            <section>
                <h2>Tracks</h2>
                <div class="track-two-col" id="trackList">
                    <div class="loading">Loading tracks...</div>
                </div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();

        loadReleaseInfo();
        loadTracks();
        loadListeningHistory();
        setupChartControls();
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

    function loadReleaseInfo() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                r.release_name,
                COALESCE(ro.release_year, r.release_year) as release_year,
                COALESCE(ro.release_type_primary, r.release_type_primary) as release_type_primary,
                COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
                COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN t.track_mbid END) as tracks_listened,
                COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_plays
            FROM releases r
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            JOIN tracks t ON r.release_mbid = t.release_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE r.release_mbid = '${safeId}'
            GROUP BY r.release_mbid
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('releaseName');
            if (el) el.textContent = 'Release not found';
            return;
        }

        const [name, year, type, albumArtUrl, tracksListened, totalPlays] = result.values[0];

        const artistResult = _db.exec(`
            SELECT DISTINCT a.artist_name, a.artist_mbid
            FROM tracks t
            JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
            JOIN artists a ON ta.artist_mbid = a.artist_mbid
            WHERE t.release_mbid = '${safeId}'
            LIMIT 1
        `)[0];

        document.getElementById('releaseName').textContent = name || 'Unknown Release';
        document.getElementById('releaseYear').textContent = year || 'Unknown year';
        document.getElementById('releaseType').textContent = type || 'Unknown type';
        document.getElementById('totalPlays').textContent = formatNumber(totalPlays || 0);
        document.getElementById('tracksListened').textContent = formatNumber(tracksListened || 0);
        document.title = `aswin.db/music - ${name || 'Release'}`;

        if (albumArtUrl) {
            const albumArtDiv = document.getElementById('albumArt');
            albumArtDiv.style.backgroundImage = `url(${albumArtUrl})`;
            albumArtDiv.style.backgroundSize = 'cover';
            albumArtDiv.style.backgroundPosition = 'center';
            albumArtDiv.innerHTML = '';
        }

        if (artistResult && artistResult.values.length > 0) {
            const [artistName, artistMbid] = artistResult.values[0];
            const artistSpan = document.getElementById('releaseArtist');
            const artistLink = document.createElement('a');
            artistLink.href = `?view=artist&id=${encodeURIComponent(artistMbid)}`;
            artistLink.textContent = artistName;
            artistLink.style.color = getCSSColor('--primary');
            artistLink.style.textDecoration = 'none';
            artistSpan.appendChild(artistLink);
        }

        const genreResult = _db.exec(`
            SELECT g.aoty_id, g.name, rg.is_primary
            FROM overrides.release_genres rg
            JOIN overrides.genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE rg.release_mbid = '${safeId}'
            ORDER BY rg.is_primary DESC, g.name
        `)[0];

        const genresEl = document.getElementById('releaseGenres');
        if (genresEl && genreResult && genreResult.values.length > 0) {
            genresEl.innerHTML = renderGenreTags(genreResult.values);
        }
    }

    function loadTracks() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                COALESCE(tro.track_name, t.track_name) as track_name,
                t.track_mbid,
                GROUP_CONCAT(DISTINCT a.artist_name) as artists,
                (SELECT COUNT(*) FROM listens l WHERE l.track_mbid = t.track_mbid) as play_count,
                COALESCE(ro.album_art_url, r.album_art_url) as album_art_url
            FROM tracks t
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid
            LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
            LEFT JOIN releases r ON t.release_mbid = r.release_mbid
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            WHERE t.release_mbid = '${safeId}'
            AND (tro.hidden IS NULL OR tro.hidden = 0)
            GROUP BY t.track_mbid
            ORDER BY play_count DESC, track_name
        `)[0];

        const container = document.getElementById('trackList');
        if (!container) return;
        container.innerHTML = '';

        if (!result || result.values.length === 0) {
            container.innerHTML = '<div class="loading">No tracks found</div>';
            return;
        }

        result.values.forEach(([trackName, trackMbid, artists, playCount, albumArtUrl]) => {
            const row = document.createElement('div');
            row.className = 'track-row';
            const imgSrc = albumArtUrl || getFallbackImageUrl();
            row.innerHTML = `
                <div class="track-row-thumb" style="background-image: url('${imgSrc}')"></div>
                <div class="track-row-info">
                    <div class="track-row-name">${escapeHtml(trackName)}</div>
                    ${artists ? `<div class="track-row-artist">${escapeHtml(artists)}</div>` : ''}
                </div>
                <div class="track-row-stats">
                    <span class="stat-item">
                        <i data-lucide="headphones" style="width: 13px; height: 13px;"></i>
                        ${playCount > 0 ? formatNumber(playCount) : '—'}
                    </span>
                </div>
            `;
            container.appendChild(row);
        });

        lucide.createIcons();
    }

    function loadListeningHistory() {
        const safeId = _releaseId.replace(/'/g, "''");

        const monthlyResult = _db.exec(`
            SELECT l.year, l.month, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_mbid = t.track_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            WHERE t.release_mbid = '${safeId}'
            AND (tro.hidden IS NULL OR tro.hidden = 0)
            GROUP BY l.year, l.month
            ORDER BY l.year, l.month
        `)[0];

        const yearlyResult = _db.exec(`
            SELECT l.year, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_mbid = t.track_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            WHERE t.release_mbid = '${safeId}'
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
                        ticks: { color: textSecondary, stepSize: 1 },
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
