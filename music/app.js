let _db = null;
let _currentView = null;

const VIEWS = {
    'home':        () => ViewHome,
    'year':        () => ViewYear,
    'top-albums':  () => ViewTopAlbums,
    'top-artists': () => ViewTopArtists,
    'top-tracks':  () => ViewTopTracks,
    'artist':      () => ViewArtist,
    'release':     () => ViewRelease,
    'genre':       () => ViewGenre,
    'admin':       () => ViewAdmin,
};

function getParams() {
    return Object.fromEntries(new URLSearchParams(window.location.search));
}

function navigate(params, pushState = true) {
    if (_currentView && _currentView.unmount) _currentView.unmount();
    _currentView = null;

    const container = document.getElementById('view-container');
    const viewName = params.view || 'home';
    const viewFn = VIEWS[viewName] || VIEWS['home'];
    _currentView = viewFn();

    if (pushState) {
        const qs = new URLSearchParams(params).toString();
        history.pushState(params, '', qs ? `?${qs}` : '?');
    }

    _currentView.mount(container, _db, params);

    // Fade-in transition
    container.style.animation = 'none';
    void container.offsetWidth;
    container.style.animation = 'viewFadeIn 0.18s ease forwards';
}

// ── Search modal ──────────────────────────────────────────────────────────────

let _searchDebounce = null;

function _searchOpen() {
    const overlay = document.getElementById('searchOverlay');
    const input   = document.getElementById('searchInput');
    overlay.removeAttribute('hidden');
    input.value = '';
    document.getElementById('searchResults').innerHTML = '';
    requestAnimationFrame(() => input.focus());
}

function _searchClose() {
    document.getElementById('searchOverlay').setAttribute('hidden', '');
}

function _searchQuery(q) {
    const results = document.getElementById('searchResults');
    if (!_db || q.trim().length < 2) { results.innerHTML = ''; return; }

    const safe = q.replace(/'/g, "''");
    let html = '';

    // Releases
    const releases = _db.exec(`
        SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url), a.name, r.release_year
        FROM releases r
        LEFT JOIN artists a ON a.id = r.primary_artist_id
        WHERE r.hidden = 0
          AND NOT EXISTS (SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id)
          AND (lower(r.title) LIKE lower('%${safe}%') OR lower(a.name) LIKE lower('%${safe}%'))
        ORDER BY (lower(r.title) LIKE lower('${safe}%')) DESC, r.release_year DESC
        LIMIT 4
    `)[0];
    if (releases?.values.length) {
        html += `<div class="search-section-label">Releases</div>`;
        for (const [id, title, art, artist, year] of releases.values) {
            const thumb = art
                ? `<img class="search-result-thumb" src="${art}" alt="" loading="lazy">`
                : `<div class="search-result-thumb" style="background:var(--bg-tertiary)"></div>`;
            html += `<a class="search-result-row" href="?view=release&id=${encodeURIComponent(id)}">
                ${thumb}
                <div class="search-result-text">
                    <div class="search-result-name">${escapeHtml(title)}</div>
                    <div class="search-result-sub">${escapeHtml(artist || '')}${year ? ' · ' + year : ''}</div>
                </div></a>`;
        }
    }

    // Artists
    const artists = _db.exec(`
        SELECT a.id, a.name, a.image_url
        FROM artists a
        WHERE lower(a.name) LIKE lower('%${safe}%')
        ORDER BY (lower(a.name) LIKE lower('${safe}%')) DESC
        LIMIT 4
    `)[0];
    if (artists?.values.length) {
        html += `<div class="search-section-label">Artists</div>`;
        for (const [id, name, img] of artists.values) {
            const thumb = img
                ? `<img class="search-result-thumb round" src="${img}" alt="" loading="lazy">`
                : `<div class="search-result-thumb round" style="background:var(--bg-tertiary)"></div>`;
            html += `<a class="search-result-row" href="?view=artist&id=${encodeURIComponent(id)}">
                ${thumb}
                <div class="search-result-text">
                    <div class="search-result-name">${escapeHtml(name)}</div>
                </div></a>`;
        }
    }

    // Tracks
    const tracks = _db.exec(`
        SELECT t.id, t.title, r.title, r.id, a.name
        FROM tracks t
        JOIN releases r ON r.id = t.release_id
        LEFT JOIN artists a ON a.id = r.primary_artist_id
        WHERE t.hidden = 0 AND lower(t.title) LIKE lower('%${safe}%')
        ORDER BY (lower(t.title) LIKE lower('${safe}%')) DESC
        LIMIT 3
    `)[0];
    if (tracks?.values.length) {
        html += `<div class="search-section-label">Tracks</div>`;
        for (const [, title, rTitle, rId, artist] of tracks.values) {
            html += `<a class="search-result-row" href="?view=release&id=${encodeURIComponent(rId)}">
                <div class="search-result-thumb" style="background:var(--bg-tertiary);display:flex;align-items:center;justify-content:center">
                    <i data-lucide="music" style="width:16px;height:16px;color:var(--text-tertiary)"></i>
                </div>
                <div class="search-result-text">
                    <div class="search-result-name">${escapeHtml(title)}</div>
                    <div class="search-result-sub">${escapeHtml(artist || '')}${rTitle ? ' · ' + escapeHtml(rTitle) : ''}</div>
                </div></a>`;
        }
    }

    if (!html) html = `<div class="search-empty">No results for "${escapeHtml(q)}"</div>`;
    results.innerHTML = html;
    lucide.createIcons({ el: results });

    // Keyboard navigation within results
    results.querySelectorAll('.search-result-row').forEach((row, i, rows) => {
        row.addEventListener('click', e => {
            e.preventDefault();
            navigate(Object.fromEntries(new URLSearchParams(new URL(row.href).search.slice(1))));
            _searchClose();
        });
    });
}

// Wire up search controls once DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    lucide.createIcons();

    document.getElementById('searchBtn').addEventListener('click', _searchOpen);

    document.getElementById('searchInput').addEventListener('input', e => {
        clearTimeout(_searchDebounce);
        _searchDebounce = setTimeout(() => _searchQuery(e.target.value), 150);
    });

    document.querySelector('.search-backdrop').addEventListener('click', _searchClose);

    document.addEventListener('keydown', e => {
        if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
            e.preventDefault();
            const overlay = document.getElementById('searchOverlay');
            overlay.hasAttribute('hidden') ? _searchOpen() : _searchClose();
        }
        if (e.key === 'Escape') _searchClose();

        // Arrow key navigation inside results
        if (!document.getElementById('searchOverlay').hasAttribute('hidden') &&
            (e.key === 'ArrowDown' || e.key === 'ArrowUp')) {
            e.preventDefault();
            const rows = [...document.querySelectorAll('.search-result-row')];
            if (!rows.length) return;
            const cur = rows.findIndex(r => r.classList.contains('kb-focused'));
            rows.forEach(r => r.classList.remove('kb-focused'));
            const next = e.key === 'ArrowDown'
                ? (cur + 1) % rows.length
                : (cur - 1 + rows.length) % rows.length;
            rows[next].classList.add('kb-focused');
            rows[next].scrollIntoView({ block: 'nearest' });
        }
        if (e.key === 'Enter') {
            const focused = document.querySelector('.search-result-row.kb-focused');
            if (focused) { focused.click(); }
        }
    });
});

// Auto-process Lucide icons on any DOM mutation — replaces per-section lucide.createIcons() calls
let _lucideTimer = null;
new MutationObserver(mutations => {
    if (mutations.some(m =>
        [...m.addedNodes].some(n =>
            n.nodeType === 1 &&
            (n.matches('i[data-lucide]') || n.querySelector('i[data-lucide]'))
        )
    )) {
        clearTimeout(_lucideTimer);
        _lucideTimer = setTimeout(() => lucide.createIcons(), 0);
    }
}).observe(document.body, { subtree: true, childList: true });

// Intercept SPA-style links (href starting with ?)
document.addEventListener('click', e => {
    const a = e.target.closest('a[href]');
    if (!a) return;
    const href = a.getAttribute('href');
    if (!href || !href.startsWith('?')) return;
    e.preventDefault();
    navigate(Object.fromEntries(new URLSearchParams(href.slice(1))));
});

window.addEventListener('popstate', e => {
    const params = e.state || getParams();
    navigate(params, false);
});

// Bootstrap
(async function () {
    const container = document.getElementById('view-container');

    const params = getParams();
    const VIEW_LOADING = {
        'artist':       'Loading artist…',
        'release':      'Loading release…',
        'genre':        'Loading genre…',
        'year':         'Loading year…',
        'top-albums':   'Loading top albums…',
        'top-artists':  'Loading top artists…',
        'top-tracks':   'Loading top tracks…',
    };
    container.innerHTML = `<div class="loading">${VIEW_LOADING[params.view] || 'Loading…'}</div>`;

    try {
        console.time('[db] init-sql');
        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });
        console.timeEnd('[db] init-sql');

        console.time('[db] fetch-db');
        const buffer = await DB_CONFIG.fetchDatabase();
        console.timeEnd('[db] fetch-db');

        console.time('[db] parse-db');
        _db = new SQL.Database(new Uint8Array(buffer));
        console.timeEnd('[db] parse-db');

        console.time('[db] mount-view');
        navigate(params, false);
        console.timeEnd('[db] mount-view');
    } catch (err) {
        console.error('Error loading database:', err);
        container.innerHTML = '<div class="loading" style="color: var(--error);">Error loading database. Please refresh.</div>';
    }
})();
