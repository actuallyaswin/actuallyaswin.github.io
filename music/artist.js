let db = null;
let artistId = null;
let currentChart = null;
let chartData = {
    monthly: null,
    yearly: null
};
let chartState = {
    granularity: 'monthly',
    type: 'distribution'
};
let sortState = {
    tracks: 'listens',
    releases: 'listens'
};

async function init() {
    try {
        // Get artist ID from URL
        const urlParams = new URLSearchParams(window.location.search);
        artistId = urlParams.get('id');

        if (!artistId) {
            window.location.href = 'index.html';
            return;
        }

        // Load SQL.js WASM
        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });

        // Load database from GitHub Releases CDN
        const buffer = await DB_CONFIG.fetchDatabase();
        db = new SQL.Database(new Uint8Array(buffer));

        console.log('Database loaded successfully');

        // Load artist data
        loadArtistInfo();
        loadTopTracks();
        loadReleases();
        loadListeningHistory();
        setupChartControls();
        setupSortControls();
    } catch (error) {
        console.error('Error loading database:', error);
        document.getElementById('artistName').textContent = 'Error loading data';
    }
}

function loadArtistInfo() {
    const result = db.exec(`
        SELECT
            a.artist_name,
            a.profile_image_url,
            COUNT(DISTINCT l.track_mbid) as unique_tracks,
            COUNT(l.timestamp) as total_plays,
            COUNT(DISTINCT t.release_mbid) as total_releases
        FROM artists a
        LEFT JOIN track_artists ta ON a.artist_mbid = ta.artist_mbid AND ta.role = 'main'
        LEFT JOIN tracks t ON ta.track_mbid = t.track_mbid
        LEFT JOIN listens l ON t.track_mbid = l.track_mbid
        WHERE a.artist_mbid = '${artistId.replace(/'/g, "''")}'
        GROUP BY a.artist_mbid
    `)[0];

    if (!result || result.values.length === 0) {
        document.getElementById('artistName').textContent = 'Artist not found';
        return;
    }

    const [name, profileImageUrl, uniqueTracks, totalPlays, totalReleases] = result.values[0];

    document.getElementById('artistName').textContent = name;
    document.getElementById('totalPlays').textContent = formatNumber(totalPlays);
    document.getElementById('uniqueTracks').textContent = formatNumber(uniqueTracks);
    document.getElementById('totalReleases').textContent = formatNumber(totalReleases);
    document.title = `aswin.db/music - ${name}`;

    // Update artist photo
    const photoContainer = document.getElementById('artistPhoto');
    if (profileImageUrl) {
        photoContainer.innerHTML = `<img src="${profileImageUrl}" alt="${name}">`;
    }
    // else keep the default SVG placeholder
}

function loadTopTracks() {
    const sortBy = sortState.tracks;
    let orderClause;

    if (sortBy === 'minutes') {
        orderClause = 'total_minutes DESC';
    } else {
        orderClause = 'play_count DESC';
    }

    const result = db.exec(`
        SELECT
            t.track_name,
            t.track_mbid,
            t.duration_ms,
            COUNT(l.timestamp) as play_count,
            CAST(COUNT(l.timestamp) * COALESCE(t.duration_ms, 0) / 60000.0 AS INTEGER) as total_minutes
        FROM tracks t
        JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        JOIN listens l ON t.track_mbid = l.track_mbid
        WHERE ta.artist_mbid = '${artistId.replace(/'/g, "''")}'
        GROUP BY t.track_mbid
        ORDER BY ${orderClause}
        LIMIT 20
    `)[0];

    const container = document.getElementById('topTracks');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<li class="loading">No tracks found</li>';
        return;
    }

    result.values.forEach(([trackName, trackMbid, durationMs, playCount, totalMinutes]) => {
        const li = document.createElement('li');
        li.className = 'track-item';

        const statsText = sortBy === 'minutes'
            ? `${formatNumber(totalMinutes)} min · ${formatNumber(playCount)} plays`
            : `${formatNumber(playCount)} plays · ${formatNumber(totalMinutes)} min`;

        li.innerHTML = `
            <div class="track-info">
                <div class="track-name">${escapeHtml(trackName)}</div>
            </div>
            <div class="track-plays">${statsText}</div>
        `;
        container.appendChild(li);
    });
}

function loadReleases() {
    const sortBy = sortState.releases;
    let orderClause;

    if (sortBy === 'minutes') {
        orderClause = 'total_minutes DESC';
    } else if (sortBy === 'date') {
        orderClause = 'r.release_year DESC, total_listens DESC';
    } else {
        orderClause = 'total_listens DESC, r.release_year DESC';
    }

    const result = db.exec(`
        SELECT
            r.release_mbid,
            r.release_name,
            r.release_year,
            r.release_type_primary,
            COUNT(DISTINCT t.track_mbid) as tracks_listened,
            COUNT(l.timestamp) as total_listens,
            CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000.0 AS INTEGER) as total_minutes
        FROM releases r
        JOIN tracks t ON r.release_mbid = t.release_mbid
        JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        LEFT JOIN listens l ON t.track_mbid = l.track_mbid
        WHERE ta.artist_mbid = '${artistId.replace(/'/g, "''")}'
        GROUP BY r.release_mbid
        ORDER BY ${orderClause}
        LIMIT 15
    `)[0];

    const container = document.getElementById('releases');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<div class="loading">No releases found</div>';
        return;
    }

    result.values.forEach(([releaseMbid, releaseName, releaseYear, releaseType, tracksListened, totalListens, totalMinutes]) => {
        const card = document.createElement('a');
        card.className = 'release-card';
        card.href = `release.html?id=${encodeURIComponent(releaseMbid)}`;

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
            <div class="release-stats">
                ${statsText}
            </div>
        `;

        container.appendChild(card);
    });
}

function loadListeningHistory() {
    // Load monthly data
    const monthlyResult = db.exec(`
        SELECT
            l.year,
            l.month,
            COUNT(*) as listen_count
        FROM listens l
        WHERE l.main_artist_mbid = '${artistId.replace(/'/g, "''")}'
        GROUP BY l.year, l.month
        ORDER BY l.year, l.month
    `)[0];

    // Load yearly data
    const yearlyResult = db.exec(`
        SELECT
            l.year,
            COUNT(*) as listen_count
        FROM listens l
        WHERE l.main_artist_mbid = '${artistId.replace(/'/g, "''")}'
        GROUP BY l.year
        ORDER BY l.year
    `)[0];

    if ((!monthlyResult || monthlyResult.values.length === 0) &&
        (!yearlyResult || yearlyResult.values.length === 0)) {
        document.querySelector('.chart-container').innerHTML = '<div class="loading">No listening history found</div>';
        return;
    }

    // Process monthly data
    if (monthlyResult && monthlyResult.values.length > 0) {
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        chartData.monthly = {
            labels: monthlyResult.values.map(([year, month]) => `${monthNames[month - 1]} ${year}`),
            data: monthlyResult.values.map(([, , count]) => count)
        };
    }

    // Process yearly data
    if (yearlyResult && yearlyResult.values.length > 0) {
        chartData.yearly = {
            labels: yearlyResult.values.map(([year]) => year.toString()),
            data: yearlyResult.values.map(([, count]) => count)
        };
    }

    // Render initial chart
    renderChart();
}

function setupChartControls() {
    // Granularity buttons
    document.querySelectorAll('[data-granularity]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('[data-granularity]').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            chartState.granularity = e.target.dataset.granularity;
            renderChart();
        });
    });

    // Type buttons
    document.querySelectorAll('[data-type]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('[data-type]').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            chartState.type = e.target.dataset.type;
            renderChart();
        });
    });

    // Listen for theme changes to update chart colors
    const observer = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            if (mutation.type === 'attributes' && mutation.attributeName === 'data-theme') {
                if (currentChart) {
                    renderChart();
                }
            }
        });
    });

    observer.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ['data-theme']
    });
}

function setupSortControls() {
    // Track sort buttons
    document.querySelectorAll('[data-sort-tracks]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('[data-sort-tracks]').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            sortState.tracks = e.target.dataset.sortTracks;
            loadTopTracks();
        });
    });

    // Release sort buttons
    document.querySelectorAll('[data-sort-releases]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('[data-sort-releases]').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            sortState.releases = e.target.dataset.sortReleases;
            loadReleases();
        });
    });
}

function renderChart() {
    const data = chartData[chartState.granularity];

    if (!data) {
        return;
    }

    // Get theme colors
    const primaryColor = getCSSColor('--primary');
    const chartBg = getCSSColor('--chart-bg');
    const chartBgSolid = getCSSColor('--chart-bg-solid');
    const bgSecondary = getCSSColor('--bg-secondary');
    const textColor = getCSSColor('--text');
    const textSecondary = getCSSColor('--text-secondary');
    const borderColor = getCSSColor('--border');

    // Calculate cumulative if needed
    let chartValues = [...data.data];
    if (chartState.type === 'cumulative') {
        chartValues = data.data.reduce((acc, val, idx) => {
            acc.push(idx === 0 ? val : acc[idx - 1] + val);
            return acc;
        }, []);
    }

    // Smart label thinning: show ~15 labels max
    const totalLabels = data.labels.length;
    const targetLabels = 15;
    const skipFactor = Math.max(1, Math.ceil(totalLabels / targetLabels));

    // Create callback to hide labels based on skip factor
    const labelCallback = (value, index) => {
        return index % skipFactor === 0 ? data.labels[index] : '';
    };

    // Destroy existing chart
    if (currentChart) {
        currentChart.destroy();
    }

    // Create new chart
    const ctx = document.getElementById('historyChart').getContext('2d');
    currentChart = new Chart(ctx, {
        type: chartState.type === 'cumulative' ? 'line' : 'bar',
        data: {
            labels: data.labels,
            datasets: [{
                label: chartState.type === 'cumulative' ? 'Total Listens' : 'Listens per Period',
                data: chartValues,
                backgroundColor: chartState.type === 'cumulative' ? chartBg : chartBgSolid,
                borderColor: primaryColor,
                borderWidth: chartState.type === 'cumulative' ? 3 : 1,
                fill: chartState.type === 'cumulative',
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
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
                    ticks: {
                        color: textSecondary
                    },
                    grid: {
                        color: borderColor
                    }
                },
                x: {
                    ticks: {
                        color: textSecondary,
                        maxRotation: 45,
                        minRotation: 45,
                        autoSkip: false,
                        callback: labelCallback
                    },
                    grid: {
                        color: borderColor
                    }
                }
            }
        }
    });
}

function formatNumber(num) {
    return num.toLocaleString();
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

init();
