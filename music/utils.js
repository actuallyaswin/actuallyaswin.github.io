// Shared utilities for music browser

async function loadOverridesDatabase(SQL, mainDb) {
    const overridesBuffer = await DB_CONFIG.fetchOverridesDatabase();
    let overridesDb = null;

    if (overridesBuffer) {
        overridesDb = new SQL.Database(new Uint8Array(overridesBuffer));
    }

    mainDb.run("ATTACH DATABASE ':memory:' AS overrides");
    mainDb.run(`CREATE TABLE IF NOT EXISTS overrides.artist_overrides (
        artist_mbid TEXT PRIMARY KEY,
        profile_image_url TEXT,
        profile_image_source TEXT,
        profile_image_crop TEXT,
        spotify_artist_id TEXT,
        hidden INTEGER DEFAULT 0,
        updated_at INTEGER,
        notes TEXT
    )`);
    mainDb.run(`CREATE TABLE IF NOT EXISTS overrides.release_overrides (
        release_mbid TEXT PRIMARY KEY,
        album_art_url TEXT,
        album_art_source TEXT,
        album_art_crop TEXT,
        release_date TEXT,
        release_year INTEGER,
        release_type_primary TEXT,
        release_type_secondary TEXT,
        genre TEXT,
        spotify_album_id TEXT,
        hidden INTEGER DEFAULT 0,
        updated_at INTEGER,
        notes TEXT
    )`);
    mainDb.run(`CREATE TABLE IF NOT EXISTS overrides.track_overrides (
        track_mbid TEXT PRIMARY KEY,
        track_name TEXT,
        spotify_track_id TEXT,
        hidden INTEGER DEFAULT 0,
        updated_at INTEGER,
        notes TEXT
    )`);

    if (overridesDb) {
        const tables = ['artist_overrides', 'release_overrides', 'track_overrides'];
        for (const table of tables) {
            try {
                const rows = overridesDb.exec(`SELECT * FROM ${table}`);
                if (rows.length > 0 && rows[0].values.length > 0) {
                    const columns = rows[0].columns;
                    const values = rows[0].values;
                    values.forEach(row => {
                        const placeholders = columns.map(() => '?').join(',');
                        const columnNames = columns.join(',');
                        mainDb.run(`INSERT OR REPLACE INTO overrides.${table} (${columnNames}) VALUES (${placeholders})`, row);
                    });
                    console.log(`Loaded ${values.length} rows from ${table}`);
                }
            } catch (e) {
                console.log(`Table ${table} not found in overrides or empty (this is OK)`);
            }
        }
        console.log('Overrides database loaded and attached');
    } else {
        console.log('No overrides found, using raw data only');
    }

    return overridesDb;
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

function getFallbackImageUrl() {
    return 'data:image/svg+xml,' + encodeURIComponent(`
        <svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
            <rect width="200" height="200" fill="#1e293b"/>
            <text x="100" y="115" text-anchor="middle" font-size="80" fill="#475569">♪</text>
        </svg>
    `);
}

const COLLAGE_SIZES = { 10: 3, 20: 4, 50: 7, 100: 10 };

function updateCountLabels(viewMode) {
    document.querySelectorAll('[data-count]').forEach(btn => {
        const count = parseInt(btn.dataset.count);
        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[count];
            btn.textContent = `${n}×${n}`;
        } else {
            btn.textContent = count;
        }
    });
}

function setupToggleGroup(selector, onChange) {
    document.querySelectorAll(selector).forEach(btn => {
        btn.addEventListener('click', e => {
            document.querySelectorAll(selector).forEach(b => b.classList.remove('active'));
            e.currentTarget.classList.add('active');
            onChange(e.currentTarget);
        });
    });
}

function createWideCard({ href, imageUrl, name, meta, totalListens, totalMinutes, rounded = false }) {
    const card = document.createElement('a');
    card.className = 'wide-card';
    card.href = href;

    const imgSrc = imageUrl || getFallbackImageUrl();
    card.innerHTML = `
        <div class="wide-card-thumb${rounded ? ' rounded' : ''}" style="background-image: url('${imgSrc}')"></div>
        <div class="wide-card-info">
            <div class="wide-card-name">${escapeHtml(name)}</div>
            ${meta ? `<div class="wide-card-meta">${meta}</div>` : ''}
        </div>
        <div class="wide-card-stats">
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
    return card;
}
