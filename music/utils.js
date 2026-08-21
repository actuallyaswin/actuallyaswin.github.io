// Shared utilities for music browser

const SITE_NAME = 'Aswin Sivaraman';

// Single source of the title suffix, so views don't each invent a format.
function setPageTitle(...parts) {
    const page = parts.filter(Boolean).join(' · ');
    document.title = page ? `${page} | ${SITE_NAME}` : SITE_NAME;
}

const _PILL_SVG_EXT = `<svg class="pill-ext" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>`;

function renderLinkPill(svc, href, name, sub) {
    const icon = svc === 'aoty'
        ? `<img src="images/links/aoty-icon.png" class="pill-aoty-img">`
        : `<span class="pill-mask"></span>`;
    return `<a href="${href}" target="_blank" rel="noopener" class="release-link-pill pill-${svc}">` +
        `<span class="pill-icon">${icon}</span>` +
        `<span class="pill-text"><span class="pill-service-name">${name}</span>` +
        (sub ? `<span class="pill-sub">${sub}</span>` : '') +
        `</span>${_PILL_SVG_EXT}</a>`;
}

// external_links Wikipedia values are usually a bare English Wikipedia page ID
// (e.g. "12326111"), but for artists/releases with no English article we store
// the full non-English Wikipedia URL instead (same convention as Bandcamp/RYM).
function wikipediaHref(value) {
    return /^\d+$/.test(value) ? `https://en.wikipedia.org/wiki/?curid=${value}` : value;
}

// Prefer clean slug-based URLs for artist/release links; fall back to the raw
// id if a slug isn't available (e.g. a query that didn't select it).
function artistHref(id, slug) {
    return `?view=artist&${slug ? `slug=${encodeURIComponent(slug)}` : `id=${encodeURIComponent(id)}`}`;
}
function releaseHref(id, slug) {
    return `?view=release&${slug ? `slug=${encodeURIComponent(slug)}` : `id=${encodeURIComponent(id)}`}`;
}

// Fine-grained "Xs/Xm/Xh/Xd ago" — for activity lists (Recent Plays, History).
// Distinct from formatRelativeTime() below, which is coarser (today/yesterday/
// weeks/months/years) and used for single "Last played" stat fields.
function formatTimeAgo(ts) {
    const diff = Math.floor(Date.now() / 1000) - ts;
    if (diff < 60)     return `${diff}s ago`;
    if (diff < 3600)   return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400)  return `${Math.floor(diff / 3600)}h ago`;
    if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function renderLoading(text = 'Loading...') {
    return `<div class="loading">${escapeHtml(text)}</div>`;
}

function formatNumber(num) {
    return num.toLocaleString();
}

// Mirrors mdb_strings.py's same_song_key() — groups track titles that are
// just edit/length cuts of one recording (radio edit, extended mix,
// original mix, club mix, ...) so hearing any one variant counts as having
// heard the song for completion purposes. A remix or edit credited to a
// specific artist/DJ is a distinct musical work and is never folded in.
const GENERIC_EDIT_TERMS = new Set([
    'radio edit', 'radio version', 'radio mix',
    'extended mix', 'extended', 'extended version',
    'original mix', 'original version', 'original',
    'club mix', 'dub mix', 'album mix', 'album version',
    'single version', 'single mix',
]);
const EDIT_SUFFIX_RE = /\s*[\(\[]([^\(\)\[\]]+)[\)\]]\s*$/;

function _isGenericEdit(inner) {
    if (GENERIC_EDIT_TERMS.has(inner)) return true;
    const stripped = inner.replace(/^original\s+/, '');
    return stripped !== inner && GENERIC_EDIT_TERMS.has(stripped);
}

function sameSongKey(title) {
    const m = EDIT_SUFFIX_RE.exec(title);
    if (!m || !_isGenericEdit(m[1].trim().toLowerCase())) {
        return title.trim().toLowerCase();
    }
    return title.slice(0, m.index).trim().toLowerCase();
}

// Tracks under 30s don't get scrobbled, so they're excluded from every
// "X / Y tracks heard" denominator (mdb.py's stats refresh mirrors this).
const SCROBBLABLE_TRACK_FILTER =
    "t.hidden = 0 AND t.variant_section IS NULL AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)";

function donutColor(pct) {
    if (pct <= 0)   return 'var(--border)';
    if (pct < 0.5)  return '#3b82f6';
    if (pct < 0.75) return '#f59e0b';
    if (pct < 1.0)  return '#f97316';
    return '#22c55e';
}

// "X / Y tracks" ring markup — pass heard/total already filtered by SCROBBLABLE_TRACK_FILTER.
function donutHtml(heard, total, { small = false, label = 'tracks' } = {}) {
    if (!total) return '';
    const pct = heard / total;
    const sizeClass = small ? ' donut-sm' : '';
    return `<div class="donut-wrap${sizeClass}" style="--p:${Math.round(pct * 100)};--c:${donutColor(pct)}" data-tooltip="${heard} / ${total} ${label}"><div class="donut"></div></div>`;
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

// Safe interpolation of an image URL into a style="background-image:url('…')"
// attribute. These URLs come from Spotify / MusicBrainz / Cover Art Archive /
// AOTY scraping, so a stray quote would terminate both the CSS string and the
// HTML attribute. Escapes for the CSS-string context, then the HTML-attribute
// context.
function cssUrl(url) {
    if (!url) return '';
    const cssEscaped = String(url).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    return escapeHtml(cssEscaped);
}

function getFallbackImageUrl() {
    return 'data:image/svg+xml,' + encodeURIComponent(`
        <svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
            <rect width="200" height="200" fill="#20232c"/>
            <text x="100" y="115" text-anchor="middle" font-size="80" fill="#767c85">♪</text>
        </svg>
    `);
}

const COLLAGE_SIZES = { 10: 3, 20: 4, 50: 7, 100: 10 };

// Given a target cols/rows ratio (e.g. 0.5625 for a 9:16 Story export) and an
// approximate desired total cell count, finds the closest-fitting integer
// {rows, cols} within a 1-10 bound per axis (matches the largest fixed
// preset, 10x10, and keeps PNG export capped at 100 images). Ratio fidelity
// is weighted 4x over exact cell-count match, since a "Story-shaped" grid
// that's slightly bigger/smaller than requested reads better than a
// correctly-sized grid that looks square when it should look tall.
function _nearestGridForRatio(targetRatio, approxCellCount) {
    let best = null, bestErr = Infinity;
    for (let rows = 1; rows <= 10; rows++) {
        for (let cols = 1; cols <= 10; cols++) {
            const cellCount = rows * cols;
            const ratioErr = Math.abs((cols / rows) - targetRatio);
            const countErr = Math.abs(cellCount - approxCellCount) / approxCellCount;
            const err = ratioErr * 4 + countErr;
            if (err < bestErr) { bestErr = err; best = { rows, cols }; }
        }
    }
    return best;
}

function renderGenreTags(rows) {
    // rows: [[aoty_id, name, is_primary], ...]
    if (!rows || !rows.length) return '';
    return rows.map(([id, name, isPrimary]) =>
        `<a href="?view=genre&id=${id}" class="genre-tag${isPrimary ? '' : ' genre-tag-secondary'}">${escapeHtml(name)}</a>`
    ).join(', ');
}

function updateCountLabels(viewMode) {
    const sel = document.getElementById('countFilter');
    if (!sel) return;
    Array.from(sel.options).forEach(opt => {
        const count = parseInt(opt.value);
        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[count];
            opt.textContent = `${n}×${n}`;
        } else {
            opt.textContent = count;
        }
    });
}

function setupToggleGroup(selector, onChange) {
    const sync = (active) => {
        document.querySelectorAll(selector).forEach(b => {
            const on = b === active;
            b.classList.toggle('active', on);
            // The `active` class alone isn't announced by screen readers.
            b.setAttribute('aria-pressed', String(on));
        });
    };

    const buttons = document.querySelectorAll(selector);
    buttons.forEach(btn => {
        if (!btn.hasAttribute('aria-pressed')) {
            btn.setAttribute('aria-pressed', String(btn.classList.contains('active')));
        }
        btn.addEventListener('click', e => {
            sync(e.currentTarget);
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

    const certLabels = { gold: '250+ plays', platinum: '500+ plays', diamond: '1,000+ plays' };
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
        <div class="release-card-thumb${rounded ? ' rounded' : ''}" style="background-image: url('${cssUrl(imgSrc)}')">${certDot}</div>
        <div class="release-card-body">
            <div class="release-name">${escapeHtml(name)}</div>
            ${statsHtml}
            ${metaHtml || viaHtml ? `<div class="release-meta">${metaHtml}${viaHtml}</div>` : ''}
        </div>
    `;
    return card;
}

function createImageCard({ href, imageUrl, title = null, subtitle = null,
                            totalListens = null, totalMinutes = null,
                            collageLabel = null, extraClass = '' }) {
    const card = document.createElement('a');
    card.className = extraClass ? `image-card ${extraClass}` : 'image-card';
    card.href = href;

    const imgHtml = `<div class="image-card-img" style="background-image: url('${cssUrl(imageUrl || getFallbackImageUrl())}')"></div>`;

    if (collageLabel != null) {
        card.innerHTML = `${imgHtml}<div class="image-card-collage-label">${escapeHtml(collageLabel)}</div>`;
        return card;
    }
    if (title == null) {
        card.innerHTML = imgHtml;
        return card;
    }

    const statsHtml = totalListens != null ? `
        <div class="image-card-stats">
            <span class="stat-item"><i data-lucide="headphones" style="width:14px;height:14px;"></i>${formatNumber(totalListens)}</span>
            <span class="stat-item"><i data-lucide="clock" style="width:14px;height:14px;"></i>${formatNumber(totalMinutes)} min</span>
        </div>` : '';

    card.innerHTML = `
        ${imgHtml}
        <div class="image-card-overlay">
            <div class="image-card-name">${escapeHtml(title)}</div>
            ${subtitle ? `<div class="image-card-artist">${escapeHtml(subtitle)}</div>` : ''}
            ${statsHtml}
        </div>`;
    return card;
}
