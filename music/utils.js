// Shared utilities for music browser

const SITE_NAME = 'Aswin Sivaraman';

// Single source of the title suffix, so views don't each invent a format.
function setPageTitle(...parts) {
    const page = parts.filter(Boolean).join(' · ');
    document.title = page ? `${page} | ${SITE_NAME}` : SITE_NAME;
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
