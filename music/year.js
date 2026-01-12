let db = null;
let currentYear = null;
let MIN_YEAR = 2011;
let MAX_YEAR = 2025;
let albumFilterMode = 'listens'; // 'listens' or 'released'
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

        console.log('Database loaded successfully');

        const yearRange = db.exec(`
            SELECT MIN(year) as min_year, MAX(year) as max_year
            FROM listens
            WHERE year IS NOT NULL
        `)[0];

        if (yearRange && yearRange.values.length > 0 && yearRange.values[0][0] !== null) {
            MIN_YEAR = yearRange.values[0][0];
            MAX_YEAR = yearRange.values[0][1];
        } else {
            MIN_YEAR = 1960;
            MAX_YEAR = new Date().getFullYear();
        }

        const urlParams = new URLSearchParams(window.location.search);
        currentYear = parseInt(urlParams.get('year')) || MAX_YEAR;

        if (currentYear < MIN_YEAR) currentYear = MIN_YEAR;
        if (currentYear > MAX_YEAR) currentYear = MAX_YEAR;

        populateYearSelector();
        loadYearStats();
        loadTopArtists();
        loadTopAlbums();
        setupYearNavigation();
        setupSortControls();
        setupAlbumFilter();

        document.title = `Music of ${currentYear} - Music Browser`;

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

function populateYearSelector() {
    const select = document.getElementById('yearSelect');
    select.innerHTML = '';

    for (let year = MAX_YEAR; year >= MIN_YEAR; year--) {
        const option = document.createElement('option');
        option.value = year;
        option.textContent = year;
        if (year === currentYear) {
            option.selected = true;
        }
        select.appendChild(option);
    }
}

function loadYearStats() {
    const result = db.exec(`
        SELECT
            COUNT(DISTINCT ta.artist_mbid) as artist_count,
            COUNT(DISTINCT t.release_mbid) as album_count,
            COUNT(l.timestamp) as total_listens
        FROM listens l
        JOIN tracks t ON l.track_mbid = t.track_mbid
        LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        WHERE l.year = ${currentYear}
    `)[0];

    if (result && result.values.length > 0) {
        const [artistCount, albumCount, totalListens] = result.values[0];
        document.getElementById('yearSummary').textContent =
            `${formatNumber(artistCount)} artists · ${formatNumber(albumCount)} albums · ${formatNumber(totalListens)} plays`;
    } else {
        document.getElementById('yearSummary').textContent = 'No data for this year';
    }
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
            a.profile_image_url,
            COUNT(DISTINCT l.track_mbid) as unique_tracks,
            COUNT(l.timestamp) as total_listens,
            CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000.0 AS INTEGER) as total_minutes
        FROM listens l
        JOIN tracks t ON l.track_mbid = t.track_mbid
        LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
        WHERE l.year = ${currentYear}
        GROUP BY a.artist_mbid
        HAVING total_listens > 0
        ORDER BY ${orderClause}
        LIMIT ${limit}
    `)[0];

    const container = document.getElementById('topArtists');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<div class="loading">No artists found for this year</div>';
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

    const imgSrc = imageUrl || 'data:image/svg+xml,' + encodeURIComponent(`
        <svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
            <rect width="200" height="200" fill="#1e293b"/>
            <text x="100" y="115" text-anchor="middle" font-size="80" fill="#475569">♪</text>
        </svg>
    `);

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

function loadTopAlbums(limit = 20) {
    const sortBy = sortState.albums;
    const view = viewState.albums;
    let orderClause;
    let whereClause;

    if (albumFilterMode === 'released') {
        whereClause = `r.release_year = ${currentYear}`;
    } else {
        whereClause = `l.year = ${currentYear}`;
    }

    if (sortBy === 'minutes') {
        orderClause = 'total_minutes DESC';
    } else {
        orderClause = 'total_listens DESC';
    }

    const result = db.exec(`
        SELECT
            r.release_mbid,
            r.release_name,
            r.release_year,
            r.release_type_primary,
            r.album_art_url,
            a.artist_name,
            a.artist_mbid,
            COUNT(DISTINCT l.track_mbid) as tracks_listened,
            COUNT(l.timestamp) as total_listens,
            CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000.0 AS INTEGER) as total_minutes
        FROM listens l
        JOIN tracks t ON l.track_mbid = t.track_mbid
        JOIN releases r ON t.release_mbid = r.release_mbid
        LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
        WHERE ${whereClause}
        GROUP BY r.release_mbid
        HAVING total_listens > 0
        ORDER BY ${orderClause}
        LIMIT ${limit}
    `)[0];

    const container = document.getElementById('topAlbums');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<div class="loading">No albums found for this year</div>';
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

    const imgSrc = albumArtUrl || 'data:image/svg+xml,' + encodeURIComponent(`
        <svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
            <rect width="200" height="200" fill="#1e293b"/>
            <text x="100" y="115" text-anchor="middle" font-size="80" fill="#475569">♪</text>
        </svg>
    `);

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

function setupAlbumFilter() {
    const filterBtn = document.getElementById('albumFilterToggle');

    filterBtn.addEventListener('click', () => {
        // Toggle between 'listens' (listens from this year) and 'released' (albums released this year, all-time listens)
        albumFilterMode = albumFilterMode === 'listens' ? 'released' : 'listens';

        // Update button active state
        if (albumFilterMode === 'released') {
            filterBtn.classList.add('active');
            filterBtn.title = `Albums released in ${currentYear} (all-time listens)`;
        } else {
            filterBtn.classList.remove('active');
            filterBtn.title = `Albums listened to in ${currentYear}`;
        }

        loadTopAlbums();
    });

    // Set initial title
    filterBtn.title = `Albums listened to in ${currentYear}`;
}

function setupYearNavigation() {
    const prevBtn = document.getElementById('prevYear');
    const nextBtn = document.getElementById('nextYear');
    const yearSelect = document.getElementById('yearSelect');

    prevBtn.addEventListener('click', () => {
        if (currentYear > MIN_YEAR) {
            navigateToYear(currentYear - 1);
        }
    });

    nextBtn.addEventListener('click', () => {
        if (currentYear < MAX_YEAR) {
            navigateToYear(currentYear + 1);
        }
    });

    yearSelect.addEventListener('change', (e) => {
        navigateToYear(parseInt(e.target.value));
    });

    updateNavigationButtons();
}

function updateNavigationButtons() {
    document.getElementById('prevYear').disabled = currentYear <= MIN_YEAR;
    document.getElementById('nextYear').disabled = currentYear >= MAX_YEAR;
}

function navigateToYear(year) {
    window.location.href = `year.html?year=${year}`;
}

function formatNumber(num) {
    return num.toLocaleString();
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

init();
