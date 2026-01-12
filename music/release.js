let db = null;
let overridesDb = null;
let releaseId = null;
let currentChart = null;
let chartData = {
    monthly: null,
    yearly: null
};
let chartState = {
    granularity: 'monthly',
    type: 'distribution'
};

async function init() {
    try {
        const urlParams = new URLSearchParams(window.location.search);
        releaseId = urlParams.get('id');

        if (!releaseId) {
            window.location.href = 'index.html';
            return;
        }

        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });

        const buffer = await DB_CONFIG.fetchDatabase();
        db = new SQL.Database(new Uint8Array(buffer));

        overridesDb = await loadOverridesDatabase(SQL, db);

        console.log('Database loaded successfully');

        loadReleaseInfo();
        loadTracks();
        loadListeningHistory();
        setupChartControls();
    } catch (error) {
        console.error('Error loading database:', error);
        document.getElementById('releaseName').textContent = 'Error loading data';
    }
}

function loadReleaseInfo() {
    const result = db.exec(`
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
        WHERE r.release_mbid = '${releaseId.replace(/'/g, "''")}'
        GROUP BY r.release_mbid
    `)[0];

    if (!result || result.values.length === 0) {
        document.getElementById('releaseName').textContent = 'Release not found';
        return;
    }

    const [name, year, type, albumArtUrl, tracksListened, totalPlays] = result.values[0];

    const artistResult = db.exec(`
        SELECT DISTINCT a.artist_name, a.artist_mbid
        FROM tracks t
        JOIN track_artists ta ON t.track_mbid = ta.track_mbid AND ta.role = 'main'
        JOIN artists a ON ta.artist_mbid = a.artist_mbid
        WHERE t.release_mbid = '${releaseId.replace(/'/g, "''")}'
        LIMIT 1
    `)[0];

    document.getElementById('releaseName').textContent = name || 'Unknown Release';
    document.getElementById('releaseYear').textContent = year || 'Unknown year';
    document.getElementById('releaseType').textContent = type || 'Unknown type';

    if (albumArtUrl) {
        const albumArtDiv = document.getElementById('albumArt');
        albumArtDiv.style.backgroundImage = `url(${albumArtUrl})`;
        albumArtDiv.style.backgroundSize = 'cover';
        albumArtDiv.style.backgroundPosition = 'center';
        albumArtDiv.innerHTML = '';
    }

    if (artistResult && artistResult.values.length > 0) {
        const [artistName, artistMbid] = artistResult.values[0];
        const artistLink = document.createElement('a');
        artistLink.href = `artist.html?id=${encodeURIComponent(artistMbid)}`;
        artistLink.textContent = artistName;
        artistLink.style.color = getCSSColor('--primary');
        artistLink.style.textDecoration = 'none';
        document.getElementById('releaseArtist').appendChild(artistLink);
        document.title = `aswin.db/music - ${name || 'Release'}`;
    } else {
        document.getElementById('releaseArtist').textContent = 'Unknown Artist';
        document.title = `aswin.db/music - ${name || 'Release'}`;
    }

    document.getElementById('totalPlays').textContent = formatNumber(totalPlays || 0);
    document.getElementById('tracksListened').textContent = formatNumber(tracksListened || 0);
}

function loadTracks() {
    const result = db.exec(`
        SELECT
            COALESCE(tro.track_name, t.track_name) as track_name,
            t.track_mbid,
            GROUP_CONCAT(DISTINCT a.artist_name) as artists,
            COUNT(l.timestamp) as play_count
        FROM tracks t
        LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
        LEFT JOIN track_artists ta ON t.track_mbid = ta.track_mbid
        LEFT JOIN artists a ON ta.artist_mbid = a.artist_mbid
        LEFT JOIN listens l ON t.track_mbid = l.track_mbid
        WHERE t.release_mbid = '${releaseId.replace(/'/g, "''")}'
        AND (tro.hidden IS NULL OR tro.hidden = 0)
        GROUP BY t.track_mbid
        ORDER BY play_count DESC, track_name
    `)[0];

    const container = document.getElementById('trackList');
    container.innerHTML = '';

    if (!result || result.values.length === 0) {
        container.innerHTML = '<li class="loading">No tracks found</li>';
        return;
    }

    result.values.forEach(([trackName, trackMbid, artists, playCount]) => {
        const li = document.createElement('li');
        li.className = 'track-item';
        li.innerHTML = `
            <div class="track-info">
                <div class="track-name">${escapeHtml(trackName)}</div>
                <div class="track-artists">${escapeHtml(artists)}</div>
            </div>
            <div class="track-plays">${playCount > 0 ? formatNumber(playCount) + ' plays' : 'Not played'}</div>
        `;
        container.appendChild(li);
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
        JOIN tracks t ON l.track_mbid = t.track_mbid
        LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
        WHERE t.release_mbid = '${releaseId.replace(/'/g, "''")}'
        AND (tro.hidden IS NULL OR tro.hidden = 0)
        GROUP BY l.year, l.month
        ORDER BY l.year, l.month
    `)[0];

    // Load yearly data
    const yearlyResult = db.exec(`
        SELECT
            l.year,
            COUNT(*) as listen_count
        FROM listens l
        JOIN tracks t ON l.track_mbid = t.track_mbid
        LEFT JOIN overrides.track_overrides tro ON t.track_mbid = tro.track_mbid
        WHERE t.release_mbid = '${releaseId.replace(/'/g, "''")}'
        AND (tro.hidden IS NULL OR tro.hidden = 0)
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

        const years = monthlyResult.values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);

        const dataMap = new Map();
        monthlyResult.values.forEach(([year, month, count]) => {
            dataMap.set(`${year}-${month}`, count);
        });

        const labels = [];
        const data = [];
        for (let year = minYear; year <= maxYear; year++) {
            for (let month = 1; month <= 12; month++) {
                const key = `${year}-${month}`;
                labels.push(`${monthNames[month - 1]} ${year}`);
                data.push(dataMap.get(key) || 0);
            }
        }

        chartData.monthly = { labels, data };
    }

    // Process yearly data
    if (yearlyResult && yearlyResult.values.length > 0) {
        const years = yearlyResult.values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);

        const dataMap = new Map();
        yearlyResult.values.forEach(([year, count]) => {
            dataMap.set(year, count);
        });

        const labels = [];
        const data = [];
        for (let year = minYear; year <= maxYear; year++) {
            labels.push(year.toString());
            data.push(dataMap.get(year) || 0);
        }

        chartData.yearly = { labels, data };
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
                        color: textSecondary,
                        stepSize: 1
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
