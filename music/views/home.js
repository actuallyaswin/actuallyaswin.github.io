const ViewHome = (() => {
    let _db = null;
    let _abortController = null;

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music';

        container.innerHTML = `
            <header>
                <div class="site-logo">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </div>
                <p class="subtitle">
                    Explore data through the years:
                    <a href="?view=year" class="year-link" id="yearRange">Loading...</a>
                </p>
            </header>

            <div class="stats" id="stats">
                <div class="stat-card">
                    <div class="stat-value" id="statListens">-</div>
                    <div class="stat-label">Total Listens</div>
                </div>
                <a href="?view=top-artists" class="stat-card">
                    <div class="stat-value" id="statArtists">-</div>
                    <div class="stat-label">Artists</div>
                </a>
                <a href="?view=top-albums" class="stat-card">
                    <div class="stat-value" id="statReleases">-</div>
                    <div class="stat-label">Releases</div>
                </a>
                <a href="?view=top-tracks" class="stat-card">
                    <div class="stat-value" id="statTracks">-</div>
                    <div class="stat-label">Tracks</div>
                </a>
            </div>

            <div class="search-section">
                <div class="search-col">
                    <input type="text" id="searchInput" class="search-input" placeholder="Search artists..." autocomplete="off">
                    <div id="searchResults" class="search-results"></div>
                </div>
                <div class="search-col">
                    <input type="text" id="releaseSearchInput" class="search-input" placeholder="Search releases..." autocomplete="off">
                    <div id="releaseSearchResults" class="search-results"></div>
                </div>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();

        loadYearRange();
        loadStats();
        setupSearch();
        setupReleaseSearch();
    }

    function unmount() {
        if (_abortController) {
            _abortController.abort();
            _abortController = null;
        }
    }

    function loadYearRange() {
        const yearRange = _db.exec(`
            SELECT MIN(year) as min_year, MAX(year) as max_year
            FROM listens WHERE year IS NOT NULL
        `)[0];

        if (yearRange && yearRange.values[0][0] !== null) {
            const [minYear, maxYear] = yearRange.values[0];
            const el = document.getElementById('yearRange');
            if (el) {
                el.textContent = `${minYear} – ${maxYear}`;
                el.href = `?view=year&year=${maxYear}`;
            }
        }
    }

    function loadStats() {
        const stats = _db.exec(`
            SELECT
                (SELECT COUNT(*) FROM listens l
                 LEFT JOIN overrides.track_overrides tro ON l.track_mbid = tro.track_mbid
                 WHERE (tro.hidden IS NULL OR tro.hidden = 0)) as total_listens,
                (SELECT COUNT(*) FROM artists a
                 LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
                 WHERE (ao.hidden IS NULL OR ao.hidden = 0)) as total_artists,
                (SELECT COUNT(*) FROM releases r
                 LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
                 WHERE (ro.hidden IS NULL OR ro.hidden = 0)) as total_releases,
                (SELECT COUNT(*) FROM tracks t
                 LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
                 WHERE (tro.hidden IS NULL OR tro.hidden = 0)) as total_tracks
        `)[0];

        const [totalListens, totalArtists, totalReleases, totalTracks] = stats.values[0];
        document.getElementById('statListens').textContent = formatNumber(totalListens);
        document.getElementById('statArtists').textContent = formatNumber(totalArtists);
        document.getElementById('statReleases').textContent = formatNumber(totalReleases);
        document.getElementById('statTracks').textContent = formatNumber(totalTracks);
    }

    function setupSearch() {
        const searchInput = document.getElementById('searchInput');
        const searchResults = document.getElementById('searchResults');
        let debounceTimer;

        _abortController = new AbortController();
        const signal = _abortController.signal;

        searchInput.addEventListener('input', e => {
            clearTimeout(debounceTimer);
            const query = e.target.value.trim();
            if (query.length < 2) {
                searchResults.classList.remove('active');
                return;
            }
            debounceTimer = setTimeout(() => performSearch(query), 300);
        });

        document.addEventListener('click', e => {
            if (searchInput && !searchInput.contains(e.target) && searchResults && !searchResults.contains(e.target)) {
                searchResults.classList.remove('active');
            }
        }, { signal });
    }

    function performSearch(query) {
        const searchResults = document.getElementById('searchResults');
        if (!searchResults) return;

        const result = _db.exec(`
            SELECT
                a.artist_mbid,
                a.artist_name,
                COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_listens
            FROM artists a
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            LEFT JOIN (SELECT DISTINCT artist_mbid, track_mbid FROM track_artists WHERE role = 'main') ta ON a.artist_mbid = ta.artist_mbid
            LEFT JOIN tracks t ON ta.track_mbid = t.track_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE a.artist_name LIKE '%${query.replace(/'/g, "''")}%'
            AND (ao.hidden IS NULL OR ao.hidden = 0)
            GROUP BY a.artist_mbid
            ORDER BY total_listens DESC
            LIMIT 10
        `)[0];

        searchResults.innerHTML = '';

        if (!result || result.values.length === 0) {
            searchResults.innerHTML = '<div class="search-result-item">No results found</div>';
            searchResults.classList.add('active');
            return;
        }

        result.values.forEach(([mbid, name, totalListens]) => {
            const item = document.createElement('a');
            item.className = 'search-result-item';
            item.href = `?view=artist&id=${encodeURIComponent(mbid)}`;
            item.innerHTML = `
                <div class="search-result-name">${escapeHtml(name)}</div>
                <div class="search-result-meta">${formatNumber(totalListens)} plays</div>
            `;
            searchResults.appendChild(item);
        });

        searchResults.classList.add('active');
    }

    function setupReleaseSearch() {
        const searchInput = document.getElementById('releaseSearchInput');
        const searchResults = document.getElementById('releaseSearchResults');
        let debounceTimer;

        const signal = _abortController.signal;

        searchInput.addEventListener('input', e => {
            clearTimeout(debounceTimer);
            const query = e.target.value.trim();
            if (query.length < 2) {
                searchResults.classList.remove('active');
                return;
            }
            debounceTimer = setTimeout(() => performReleaseSearch(query), 300);
        });

        document.addEventListener('click', e => {
            if (searchInput && !searchInput.contains(e.target) && searchResults && !searchResults.contains(e.target)) {
                searchResults.classList.remove('active');
            }
        }, { signal });
    }

    function performReleaseSearch(query) {
        const searchResults = document.getElementById('releaseSearchResults');
        if (!searchResults) return;

        const result = _db.exec(`
            SELECT
                r.release_mbid,
                r.release_name,
                COALESCE(ro.release_year, r.release_year) as release_year,
                a.artist_name,
                COUNT(l.timestamp) as total_listens
            FROM releases r
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            JOIN tracks t ON r.release_mbid = t.release_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            JOIN (SELECT DISTINCT track_mbid, artist_mbid FROM track_artists WHERE role = 'main') ta ON t.track_mbid = ta.track_mbid
            JOIN artists a ON ta.artist_mbid = a.artist_mbid
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE r.release_name LIKE '%${query.replace(/'/g, "''")}%'
            AND (ro.hidden IS NULL OR ro.hidden = 0)
            AND (ao.hidden IS NULL OR ao.hidden = 0)
            GROUP BY r.release_mbid
            ORDER BY total_listens DESC
            LIMIT 10
        `)[0];

        searchResults.innerHTML = '';

        if (!result || result.values.length === 0) {
            searchResults.innerHTML = '<div class="search-result-item">No results found</div>';
            searchResults.classList.add('active');
            return;
        }

        result.values.forEach(([mbid, name, year, artistName, totalListens]) => {
            const item = document.createElement('a');
            item.className = 'search-result-item';
            item.href = `?view=release&id=${encodeURIComponent(mbid)}`;
            item.innerHTML = `
                <div class="search-result-name">${escapeHtml(name)}</div>
                <div class="search-result-meta">${escapeHtml(artistName)}${year ? ` · ${year}` : ''}</div>
            `;
            searchResults.appendChild(item);
        });

        searchResults.classList.add('active');
    }

    return { mount, unmount };
})();
