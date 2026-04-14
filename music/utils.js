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
        notes TEXT,
        hero_image TEXT
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
        notes TEXT,
        aoty_url TEXT
    )`);
    mainDb.run(`CREATE TABLE IF NOT EXISTS overrides.track_overrides (
        track_mbid TEXT PRIMARY KEY,
        track_name TEXT,
        track_number INTEGER,
        disc_number INTEGER,
        spotify_track_id TEXT,
        hidden INTEGER DEFAULT 0,
        updated_at INTEGER,
        notes TEXT
    )`);
    mainDb.run(`CREATE TABLE IF NOT EXISTS overrides.genres (
        aoty_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        slug TEXT NOT NULL
    )`);
    mainDb.run(`CREATE TABLE IF NOT EXISTS overrides.release_genres (
        release_mbid TEXT NOT NULL,
        aoty_genre_id INTEGER NOT NULL,
        is_primary INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (release_mbid, aoty_genre_id)
    )`);

    if (overridesDb) {
        const tables = ['artist_overrides', 'release_overrides', 'track_overrides', 'genres', 'release_genres'];
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

function formatRelativeTime(ts) {
    const diffSec = Math.floor(Date.now() / 1000) - ts;
    if (diffSec < 86400)       return 'today';
    if (diffSec < 2 * 86400)   return 'yesterday';
    const days = Math.floor(diffSec / 86400);
    if (days < 14)             return `${days} days ago`;
    const weeks = Math.floor(days / 7);
    if (weeks < 9)             return `${weeks} weeks ago`;
    const months = Math.floor(days / 30);
    if (months < 12)           return `${months} months ago`;
    const years = Math.floor(days / 365);
    return years === 1 ? '1 year ago' : `${years} years ago`;
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

function renderGenreTags(rows) {
    // rows: [[aoty_id, name, is_primary], ...]
    if (!rows || !rows.length) return '';
    return rows.map(([id, name, isPrimary]) =>
        `<a href="?view=genre&id=${id}" class="genre-tag${isPrimary ? '' : ' genre-tag-secondary'}">${escapeHtml(name)}</a>`
    ).join(', ');
}

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

function createWideCard({ href, imageUrl, name, meta, totalListens, totalMinutes,
                          rounded = false, cert = null, viaArtist = null }) {
    const card = document.createElement('a');
    card.className = 'release-card';
    card.href = href;

    const imgSrc = imageUrl || getFallbackImageUrl();
    const metaParts = meta ? meta.split(' · ') : [];
    const metaHtml = metaParts.map((p, i) =>
        `<span class="${i === 0 ? 'release-year' : 'release-type-label'}">${p}</span>`
    ).join('');

    const certLabels = { gold: '50+ plays', platinum: '100+ plays', diamond: '250+ plays' };
    const certDot = cert
        ? `<span class="release-cert-dot release-cert-dot-${cert}" title="${certLabels[cert]}"></span>`
        : '';

    const statsHtml = totalListens != null ? `
        <div class="release-stats">
            <span class="stat-item">
                <i data-lucide="headphones" style="width: 13px; height: 13px;"></i>
                ${formatNumber(totalListens)}
            </span>
            <span class="stat-item">
                <i data-lucide="clock" style="width: 13px; height: 13px;"></i>
                ${formatNumber(totalMinutes)} min
            </span>
        </div>
    ` : '';

    const viaHtml = viaArtist ? `<span class="release-via-artist">${escapeHtml(viaArtist)}</span>` : '';

    card.innerHTML = `
        <div class="release-card-thumb${rounded ? ' rounded' : ''}" style="background-image: url('${imgSrc}')">${certDot}</div>
        <div class="release-card-body">
            <div class="release-name">${escapeHtml(name)}</div>
            ${statsHtml}
            ${metaHtml || viaHtml ? `<div class="release-meta">${metaHtml}${viaHtml}</div>` : ''}
        </div>
    `;
    return card;
}
