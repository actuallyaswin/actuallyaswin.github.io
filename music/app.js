let db = null;

async function init() {
    try {
        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });

        const buffer = await DB_CONFIG.fetchDatabase();
        db = new SQL.Database(new Uint8Array(buffer));

        await loadOverridesDatabase(SQL, db);

        console.log('Database loaded successfully');

        // Calculate year range from database
        const yearRange = db.exec(`
            SELECT MIN(year) as min_year, MAX(year) as max_year
            FROM listens
            WHERE year IS NOT NULL
        `)[0];

        if (yearRange && yearRange.values.length > 0 && yearRange.values[0][0] !== null) {
            const minYear = yearRange.values[0][0];
            const maxYear = yearRange.values[0][1];
            document.getElementById('yearRange').textContent = `${minYear} – ${maxYear}`;
        }

        loadStats();
        setupSearch();
        setupReleaseSearch();

        lucide.createIcons();
    } catch (error) {
        console.error('Error loading database:', error);
    }
}

function loadStats() {
    const stats = db.exec(`
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

    const values = stats.values[0];
    const statCards = document.querySelectorAll('.stat-value');

    statCards[0].textContent = formatNumber(values[0]);
    statCards[1].textContent = formatNumber(values[1]);
    statCards[2].textContent = formatNumber(values[2]);
    statCards[3].textContent = formatNumber(values[3]);
}

function setupSearch() {
    const searchInput = document.getElementById('searchInput');
    const searchResults = document.getElementById('searchResults');
    let debounceTimer;

    searchInput.addEventListener('input', (e) => {
        clearTimeout(debounceTimer);
        const query = e.target.value.trim();

        if (query.length < 2) {
            searchResults.classList.remove('active');
            return;
        }

        debounceTimer = setTimeout(() => {
            performSearch(query);
        }, 300);
    });

    document.addEventListener('click', (e) => {
        if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
            searchResults.classList.remove('active');
        }
    });
}

function performSearch(query) {
    const searchResults = document.getElementById('searchResults');

    const result = db.exec(`
        SELECT
            a.artist_mbid,
            a.artist_name,
            COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_listens
        FROM artists a
        LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
        LEFT JOIN track_artists ta ON a.artist_mbid = ta.artist_mbid AND ta.role = 'main'
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

    result.values.forEach(row => {
        const [mbid, name, totalListens] = row;
        const item = document.createElement('a');
        item.className = 'search-result-item';
        item.href = `artist.html?id=${encodeURIComponent(mbid)}`;

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

    searchInput.addEventListener('input', (e) => {
        clearTimeout(debounceTimer);
        const query = e.target.value.trim();

        if (query.length < 2) {
            searchResults.classList.remove('active');
            return;
        }

        debounceTimer = setTimeout(() => {
            performReleaseSearch(query);
        }, 300);
    });

    document.addEventListener('click', (e) => {
        if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
            searchResults.classList.remove('active');
        }
    });
}

function performReleaseSearch(query) {
    const searchResults = document.getElementById('releaseSearchResults');

    const result = db.exec(`
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
        JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
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

    result.values.forEach(row => {
        const [mbid, name, year, artistName, totalListens] = row;
        const item = document.createElement('a');
        item.className = 'search-result-item';
        item.href = `release.html?id=${encodeURIComponent(mbid)}`;

        item.innerHTML = `
            <div class="search-result-name">${escapeHtml(name)}</div>
            <div class="search-result-meta">${escapeHtml(artistName)}${year ? ` · ${year}` : ''}</div>
        `;

        searchResults.appendChild(item);
    });

    searchResults.classList.add('active');
}

init();
