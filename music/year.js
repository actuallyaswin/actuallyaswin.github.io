let db = null;
let currentYear = null;
let MIN_YEAR = 2011;
let MAX_YEAR = 2025;
let sortBy = 'listens';
let countLimit = 10;
let viewMode = 'list';
let filterMode = 'this-year';
let cachedReleases = [];
let cachedArtists = [];

async function init() {
    try {
        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });

        const buffer = await DB_CONFIG.fetchDatabase();
        db = new SQL.Database(new Uint8Array(buffer));
        await loadOverridesDatabase(SQL, db);

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
        loadReleases();
        loadArtists();
        setupYearNavigation();
        setupControls();

        document.title = `aswin.db/music - ${currentYear}`;
        lucide.createIcons();
    } catch (error) {
        console.error('Error loading database:', error);
        document.getElementById('releasesContainer').innerHTML =
            '<div class="loading" style="color: var(--error);">Error loading database. Please refresh.</div>';
    }
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
    const result = db.exec(`
        SELECT
            COUNT(DISTINCT CASE WHEN (ao.hidden IS NULL OR ao.hidden = 0) THEN ta.artist_mbid END) as artist_count,
            COUNT(DISTINCT CASE WHEN (ro.hidden IS NULL OR ro.hidden = 0) THEN t.release_mbid END) as album_count,
            COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) THEN l.timestamp END) as total_listens
        FROM listens l
        JOIN tracks t ON l.track_mbid = t.track_mbid
        LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
        LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
        LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
        LEFT JOIN releases r ON t.release_mbid = r.release_mbid
        LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
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

function loadReleases() {
    const orderClause = sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';
    const thisYearFilter = filterMode === 'this-year';

    const whereClause = thisYearFilter
        ? `(ro.hidden IS NULL OR ro.hidden = 0)
           AND (ao.hidden IS NULL OR ao.hidden = 0)
           AND COALESCE(ro.release_year, r.release_year) = ${currentYear}`
        : `l.year = ${currentYear}
           AND (ro.hidden IS NULL OR ro.hidden = 0)
           AND (ao.hidden IS NULL OR ao.hidden = 0)`;

    const fromClause = thisYearFilter
        ? `FROM releases r
           LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
           JOIN tracks t ON r.release_mbid = t.release_mbid
           LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
           LEFT JOIN listens l ON t.track_mbid = l.track_mbid
           JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
           JOIN artists a ON ta.artist_mbid = a.artist_mbid
           LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid`
        : `FROM listens l
           JOIN tracks t ON l.track_mbid = t.track_mbid
           LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
           JOIN releases r ON t.release_mbid = r.release_mbid
           LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
           JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
           JOIN artists a ON ta.artist_mbid = a.artist_mbid
           LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid`;

    const result = db.exec(`
        SELECT
            r.release_mbid,
            r.release_name,
            COALESCE(ro.release_year, r.release_year) as release_year,
            COALESCE(ro.album_art_url, r.album_art_url) as album_art_url,
            a.artist_name,
            a.artist_mbid,
            (SELECT COUNT(*)
             FROM tracks t2
             LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
             LEFT JOIN listens l2 ON t2.track_mbid = l2.track_mbid
             WHERE t2.release_mbid = r.release_mbid
             AND (tro2.hidden IS NULL OR tro2.hidden = 0)
             AND l2.year = ${currentYear}) as total_listens,
            (SELECT CAST(SUM(COALESCE(t2.duration_ms, 0)) / 60000.0 AS INTEGER)
             FROM tracks t2
             LEFT JOIN overrides.track_overrides tro2 ON t2.track_mbid = tro2.track_mbid
             LEFT JOIN listens l2 ON t2.track_mbid = l2.track_mbid
             WHERE t2.release_mbid = r.release_mbid
             AND (tro2.hidden IS NULL OR tro2.hidden = 0)
             AND l2.year = ${currentYear}) as total_minutes
        ${fromClause}
        WHERE ${whereClause}
        GROUP BY r.release_mbid, a.artist_mbid
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
                a.artist_mbid,
                a.artist_name,
                COALESCE(ao.profile_image_url, a.profile_image_url) as profile_image_url,
                COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) AND l.year = ${currentYear} THEN l.track_mbid END) as unique_tracks,
                COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) AND l.year = ${currentYear} THEN l.timestamp END) as total_listens,
                CAST(SUM(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) AND l.year = ${currentYear} THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
            FROM artists a
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            LEFT JOIN track_artists ta ON a.artist_mbid = ta.artist_mbid AND ta.role = 'main'
            LEFT JOIN tracks t ON ta.track_mbid = t.track_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN releases r ON t.release_mbid = r.release_mbid
            LEFT JOIN overrides.release_overrides ro ON r.release_mbid = ro.release_mbid
            LEFT JOIN listens l ON t.track_mbid = l.track_mbid
            WHERE (ao.hidden IS NULL OR ao.hidden = 0)
            AND COALESCE(ro.release_year, r.release_year) = ${currentYear}
            GROUP BY a.artist_mbid
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 100
        `;
    } else {
        query = `
            SELECT
                a.artist_mbid,
                a.artist_name,
                COALESCE(ao.profile_image_url, a.profile_image_url) as profile_image_url,
                COUNT(DISTINCT CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) AND l.year = ${currentYear} THEN l.track_mbid END) as unique_tracks,
                COUNT(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) AND l.year = ${currentYear} THEN l.timestamp END) as total_listens,
                CAST(SUM(CASE WHEN (tro.hidden IS NULL OR tro.hidden = 0) AND l.year = ${currentYear} THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes
            FROM listens l
            JOIN tracks t ON l.track_mbid = t.track_mbid
            LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
            LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
            LEFT JOIN overrides.artist_overrides ao ON a.artist_mbid = ao.artist_mbid
            WHERE l.year = ${currentYear}
            AND a.artist_mbid IS NOT NULL
            AND (ao.hidden IS NULL OR ao.hidden = 0)
            GROUP BY a.artist_mbid
            HAVING total_listens > 0
            ORDER BY ${orderClause}
            LIMIT 100
        `;
    }

    const result = db.exec(query)[0];
    cachedArtists = result ? result.values : [];
    renderArtists();
}

function renderReleases() {
    const container = document.getElementById('releasesContainer');
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
            const [releaseMbid, releaseName, releaseYear, albumArtUrl] = row;
            const card = document.createElement('a');
            card.className = 'image-card';
            card.href = `release.html?id=${encodeURIComponent(releaseMbid)}`;
            const imgSrc = albumArtUrl || getFallbackImageUrl();
            card.innerHTML = `<div class="image-card-img" style="background-image: url('${imgSrc}')"></div>`;
            if (i >= show) card.style.display = 'none';
            container.appendChild(card);
        });
    } else if (viewMode === 'list') {
        container.className = 'wide-grid';
        cachedReleases.forEach((row, i) => {
            const [releaseMbid, releaseName, releaseYear, albumArtUrl, artistName, artistMbid, totalListens, totalMinutes] = row;
            const card = createWideCard({
                href: `release.html?id=${encodeURIComponent(releaseMbid)}`,
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
        cachedReleases.forEach((row, i) => {
            const [releaseMbid, releaseName, releaseYear, albumArtUrl, artistName, artistMbid, totalListens, totalMinutes] = row;
            const card = document.createElement('a');
            card.className = 'image-card';
            card.href = `release.html?id=${encodeURIComponent(releaseMbid)}`;
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

function renderArtists() {
    const container = document.getElementById('artistsContainer');
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
            const [mbid, name, imageUrl] = row;
            const card = document.createElement('a');
            card.className = 'image-card';
            card.href = `artist.html?id=${encodeURIComponent(mbid)}`;
            const imgSrc = imageUrl || getFallbackImageUrl();
            card.innerHTML = `<div class="image-card-img" style="background-image: url('${imgSrc}')"></div>`;
            if (i >= show) card.style.display = 'none';
            container.appendChild(card);
        });
    } else if (viewMode === 'list') {
        container.className = 'wide-grid';
        cachedArtists.forEach((row, i) => {
            const [mbid, name, imageUrl, uniqueTracks, totalListens, totalMinutes] = row;
            const card = createWideCard({
                href: `artist.html?id=${encodeURIComponent(mbid)}`,
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
            const [mbid, name, imageUrl, uniqueTracks, totalListens, totalMinutes] = row;
            const card = document.createElement('a');
            card.className = 'image-card';
            card.href = `artist.html?id=${encodeURIComponent(mbid)}`;
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
    document.getElementById('prevYear').disabled = currentYear <= MIN_YEAR;
    document.getElementById('nextYear').disabled = currentYear >= MAX_YEAR;
}

function navigateToYear(year) {
    window.location.href = `year.html?year=${year}`;
}

init();
