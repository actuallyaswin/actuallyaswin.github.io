// Shared search modal — used by index.html (in-SPA) and the standalone
// artist/release/year pages (results link back to index.html). Expects a
// global `_db` (sql.js Database) and `escapeHtml` (utils.js) to be loaded.
// On index.html, `navigate()` (app.js) is used for a smooth in-SPA jump;
// on the standalone pages it's undefined, so result clicks fall through to
// a normal browser navigation to index.html.

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
    const isAdmin = typeof getParams === 'function' && getParams().view === 'admin';
    let html = '';

    // Static view shortcuts — hidden in admin mode (no page navigation from editor)
    if (!isAdmin) {
        const VIEW_SHORTCUTS = [
            { label: 'Recommendations', view: 'recommendations', icon: 'sparkles' },
            { label: 'History',         view: 'history',         icon: 'history' },
            { label: 'Stats',           view: 'stats',           icon: 'bar-chart-2' },
            { label: 'Top Albums',      view: 'top', params: '&type=albums',  icon: 'disc-album' },
            { label: 'Top Artists',     view: 'top', params: '&type=artists', icon: 'mic-vocal' },
            { label: 'Top Tracks',      view: 'top', params: '&type=tracks',  icon: 'music' },
        ];
        const matchedViews = VIEW_SHORTCUTS.filter(v =>
            v.label.toLowerCase().includes(q.toLowerCase())
        );
        if (matchedViews.length) {
            html += `<div class="search-section-label">Pages</div>`;
            matchedViews.forEach(v => {
                html += `<a class="search-result-row" href="index.html?view=${v.view}${v.params || ''}">
                    <div class="search-result-thumb" style="background:var(--bg-tertiary);display:flex;align-items:center;justify-content:center">
                        <i data-lucide="${v.icon}" style="width:16px;height:16px;color:var(--text-tertiary)"></i>
                    </div>
                    <div class="search-result-text">
                        <div class="search-result-name">${escapeHtml(v.label)}</div>
                    </div></a>`;
            });
        }
    }

    // Releases
    const releases = _db.exec(`
        SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url), a.name, r.release_year
        FROM releases r
        LEFT JOIN artists a ON a.id = r.primary_artist_id
        WHERE r.hidden = 0
          AND (lower(r.title) LIKE lower('%${safe}%')
            OR lower(a.name) LIKE lower('%${safe}%')
            OR EXISTS (SELECT 1 FROM artist_aliases aa
                       WHERE aa.artist_id = a.id AND lower(aa.alias) LIKE lower('%${safe}%')))
        ORDER BY (lower(r.title) LIKE lower('${safe}%')) DESC, r.release_year DESC
        LIMIT 4
    `)[0];
    if (releases?.values.length) {
        html += `<div class="search-section-label">Releases</div>`;
        for (const [id, title, art, artist, year] of releases.values) {
            const thumb = art
                ? `<img class="search-result-thumb" src="${escapeHtml(art)}" alt="" loading="lazy">`
                : `<div class="search-result-thumb" style="background:var(--bg-tertiary)"></div>`;
            html += `<a class="search-result-row" href="index.html?view=release&id=${encodeURIComponent(id)}">
                ${thumb}
                <div class="search-result-text">
                    <div class="search-result-name">${escapeHtml(title)}</div>
                    <div class="search-result-sub">${escapeHtml(artist || '')}${year ? ' · ' + year : ''}</div>
                </div></a>`;
        }
    }

    // Artists
    const artists = _db.exec(`
        SELECT a.id, a.name, COALESCE(a.image_thumb_url, a.image_url)
        FROM artists a
        WHERE (lower(a.name) LIKE lower('%${safe}%')
            OR EXISTS (SELECT 1 FROM artist_aliases aa
                       WHERE aa.artist_id = a.id AND lower(aa.alias) LIKE lower('%${safe}%')))
        ORDER BY (lower(a.name) LIKE lower('${safe}%')) DESC
        LIMIT 4
    `)[0];
    if (artists?.values.length) {
        html += `<div class="search-section-label">Artists</div>`;
        for (const [id, name, img] of artists.values) {
            const thumb = img
                ? `<img class="search-result-thumb round" src="${escapeHtml(img)}" alt="" loading="lazy">`
                : `<div class="search-result-thumb round" style="background:var(--bg-tertiary)"></div>`;
            html += `<a class="search-result-row" href="index.html?view=artist&id=${encodeURIComponent(id)}">
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
            html += `<a class="search-result-row" href="index.html?view=release&id=${encodeURIComponent(rId)}">
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

    // Result clicks: smooth in-SPA jump on index.html (navigate() defined),
    // normal browser navigation to index.html from the standalone pages.
    results.querySelectorAll('.search-result-row').forEach(row => {
        row.addEventListener('click', e => {
            if (typeof navigate === 'function') {
                e.preventDefault();
                navigate(Object.fromEntries(new URLSearchParams(new URL(row.href).search.slice(1))));
                _searchClose();
            }
        });
    });
}

document.addEventListener('DOMContentLoaded', () => {
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
