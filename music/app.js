let db = null;
let overridesDb = null;
let sortState = {
    artists: 'listens',
    albums: 'listens'
};
let viewState = {
    artists: { mode: 'grid', showStats: true },
    albums: { mode: 'grid', showStats: true }
};

async function init() {
    try {
        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });

        const buffer = await DB_CONFIG.fetchDatabase();
        db = new SQL.Database(new Uint8Array(buffer));

        overridesDb = await loadOverridesDatabase(SQL, db);

        console.log('Database loaded successfully');

        loadStats();
        loadTopArtists();
        loadTopAlbums();
        setupSearch();
        setupSortControls();

        lucide.createIcons();
    } catch (error) {
        console.error('Error loading database:', error);
        document.getElementById('topArtists').innerHTML = `
            <div class="loading" style="color: var(--error);">
                Error loading database. Please refresh the page.
            </div>
        `;
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

function loadTopArtists(limit = 20) {
    const sortBy = sortState.artists;
    const view = viewState.artists;
    let orderClause;

    if (sortBy === 'minutes') {
        orderClause = 'total_minutes DESC';
    } else {
        orderClause = 'total_listens DESC';
    }

    const result = db.exec(`
        SELECT
            a.artist_mbid,
            a.artist_name,
            COALESCE(ao.profile_image_url, a.profile_image_url) as profile_image_url,
            COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.track_mbid END) as unique_tracks,
            COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_listens,
            CAST(SUM(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
        FROM artists a
        LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
        LEFT JOIN track_artists ta ON a.artist_mbid = ta.artist_mbid AND ta.role = 'main'
        LEFT JOIN tracks t ON ta.track_mbid = t.track_mbid
        LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
        LEFT JOIN listens l ON t.track_mbid = l.track_mbid
        WHERE (ao.hidden IS NULL OR ao.hidden = 0)
        GROUP BY a.artist_mbid
        HAVING total_listens > 0
        ORDER BY ${orderClause}
        LIMIT ${limit}
    `)[0];

    const container = document.getElementById('topArtists');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<div class="loading">No artists found</div>';
        return;
    }

    if (view.mode === 'list') {
        container.className = 'list-view';
    } else {
        container.className = 'image-grid';
    }

    result.values.forEach(row => {
        const [mbid, name, imageUrl, uniqueTracks, totalListens, totalMinutes] = row;
        let card;
        if (view.mode === 'list') {
            card = createArtistListItem(mbid, name, uniqueTracks, totalListens, totalMinutes, view.showStats);
        } else {
            card = createArtistImageCard(mbid, name, imageUrl, uniqueTracks, totalListens, totalMinutes, view.showStats);
        }
        container.appendChild(card);
    });

    lucide.createIcons();
    updateViewButtonStates('artists');
}

function createArtistImageCard(mbid, name, imageUrl, uniqueTracks, totalListens, totalMinutes, showStats) {
    const card = document.createElement('a');
    card.className = 'image-card';
    card.href = `artist.html?id=${encodeURIComponent(mbid)}`;

    const imgSrc = imageUrl || getFallbackImageUrl();

    card.innerHTML = `
        <div class="image-card-img" style="background-image: url('${imgSrc}')"></div>
        ${showStats ? `
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
        ` : ''}
    `;

    return card;
}

function createArtistListItem(mbid, name, uniqueTracks, totalListens, totalMinutes, showStats) {
    const item = document.createElement('a');
    item.className = 'list-item';
    item.href = `artist.html?id=${encodeURIComponent(mbid)}`;

    if (showStats) {
        item.innerHTML = `
            <div class="list-item-info">
                <div class="list-item-name">${escapeHtml(name)}</div>
                <div class="list-item-meta">${formatNumber(uniqueTracks)} tracks</div>
            </div>
            <div class="list-item-stats">
                <span class="stat-item">
                    <i data-lucide="headphones" style="width: 14px; height: 14px;"></i>
                    ${formatNumber(totalListens)}
                </span>
                <span class="stat-item">
                    <i data-lucide="clock" style="width: 14px; height: 14px;"></i>
                    ${formatNumber(totalMinutes)} min
                </span>
            </div>
        `;
    } else {
        item.innerHTML = `
            <div class="list-item-info">
                <div class="list-item-name">${escapeHtml(name)}</div>
            </div>
        `;
    }

    return item;
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

    // Close search results when clicking outside
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

function loadTopAlbums(limit = 20) {
    const sortBy = sortState.albums;
    const view = viewState.albums;
    let orderClause;

    if (sortBy === 'minutes') {
        orderClause = 'total_minutes DESC';
    } else if (sortBy === 'date') {
        orderClause = 'release_year DESC, total_listens DESC';
    } else {
        orderClause = 'total_listens DESC';
    }

    const result = db.exec(`
        SELECT
            r.release_mbid,
            r.release_name,
            COALESCE(ro.release_year, r.release_year) as release_year,
            COALESCE(ro.release_type_primary, r.release_type_primary) as release_type_primary,
            COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
            a.artist_name,
            a.artist_mbid,
            COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN t.track_mbid END) as tracks_listened,
            COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_listens,
            CAST(SUM(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
        FROM releases r
        LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
        JOIN tracks t ON r.release_mbid = t.release_mbid
        LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
        JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        JOIN artists a ON ta.artist_mbid = a.artist_mbid
        LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
        LEFT JOIN listens l ON t.track_mbid = l.track_mbid
        WHERE (ro.hidden IS NULL OR ro.hidden = 0)
        AND (ao.hidden IS NULL OR ao.hidden = 0)
        GROUP BY r.release_mbid, a.artist_mbid
        HAVING total_listens > 0
        ORDER BY ${orderClause}
        LIMIT ${limit}
    `)[0];

    const container = document.getElementById('topAlbums');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<div class="loading">No albums found</div>';
        return;
    }

    if (view.mode === 'list') {
        container.className = 'list-view';
    } else {
        container.className = 'image-grid';
    }

    result.values.forEach(row => {
        const [releaseMbid, releaseName, releaseYear, releaseType, albumArtUrl, artistName, artistMbid, tracksListened, totalListens, totalMinutes] = row;
        let card;
        if (view.mode === 'list') {
            card = createAlbumListItem(releaseMbid, releaseName, releaseYear, releaseType, artistName, artistMbid, tracksListened, totalListens, totalMinutes, view.showStats);
        } else {
            card = createAlbumImageCard(releaseMbid, releaseName, releaseYear, releaseType, albumArtUrl, artistName, artistMbid, tracksListened, totalListens, totalMinutes, view.showStats);
        }
        container.appendChild(card);
    });

    lucide.createIcons();
    updateViewButtonStates('albums');
}

function createAlbumImageCard(releaseMbid, releaseName, releaseYear, releaseType, albumArtUrl, artistName, artistMbid, tracksListened, totalListens, totalMinutes, showStats) {
    const card = document.createElement('a');
    card.className = 'image-card';
    card.href = `release.html?id=${encodeURIComponent(releaseMbid)}`;

    const imgSrc = albumArtUrl || getFallbackImageUrl();

    card.innerHTML = `
        <div class="image-card-img" style="background-image: url('${imgSrc}')"></div>
        ${showStats ? `
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
        ` : ''}
    `;

    return card;
}

function createAlbumListItem(releaseMbid, releaseName, releaseYear, releaseType, artistName, artistMbid, tracksListened, totalListens, totalMinutes, showStats) {
    const item = document.createElement('a');
    item.className = 'list-item';
    item.href = `release.html?id=${encodeURIComponent(releaseMbid)}`;

    if (showStats) {
        item.innerHTML = `
            <div class="list-item-info">
                <div class="list-item-name">${escapeHtml(releaseName)}</div>
                <div class="list-item-meta">${escapeHtml(artistName)} · ${releaseYear || 'Unknown'} · ${releaseType || 'album'}</div>
            </div>
            <div class="list-item-stats">
                <span class="stat-item">
                    <i data-lucide="headphones" style="width: 14px; height: 14px;"></i>
                    ${formatNumber(totalListens)}
                </span>
                <span class="stat-item">
                    <i data-lucide="clock" style="width: 14px; height: 14px;"></i>
                    ${formatNumber(totalMinutes)} min
                </span>
            </div>
        `;
    } else {
        item.innerHTML = `
            <div class="list-item-info">
                <div class="list-item-name">${escapeHtml(releaseName)}</div>
            </div>
        `;
    }

    return item;
}

function toggleView(section, toggleType) {
    const view = viewState[section];

    if (toggleType === 'stats') {
        // Eye button: toggle showStats
        view.showStats = !view.showStats;
    } else if (toggleType === 'list') {
        // List button: toggle between grid and list mode
        view.mode = view.mode === 'list' ? 'grid' : 'list';
    }

    if (section === 'artists') {
        loadTopArtists();
    } else if (section === 'albums') {
        loadTopAlbums();
    }

    lucide.createIcons();
}

function updateViewButtonStates(section) {
    const view = viewState[section];
    const eyeButton = document.querySelector(`#${section}ViewEye`);
    const listButton = document.querySelector(`#${section}ViewList`);

    if (eyeButton) {
        if (view.showStats) {
            eyeButton.classList.add('active');
        } else {
            eyeButton.classList.remove('active');
        }
    }

    if (listButton) {
        if (view.mode === 'list') {
            listButton.classList.add('active');
        } else {
            listButton.classList.remove('active');
        }
    }
}

function setupSortControls() {
    // Artist sort buttons
    document.querySelectorAll('[data-sort-artists]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('[data-sort-artists]').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            sortState.artists = e.target.dataset.sortArtists;
            loadTopArtists();
        });
    });

    // Album sort buttons
    document.querySelectorAll('[data-sort-albums]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('[data-sort-albums]').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            sortState.albums = e.target.dataset.sortAlbums;
            loadTopAlbums();
        });
    });
}

init();
