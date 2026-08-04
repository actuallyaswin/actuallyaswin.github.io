const ViewCollectionPhysical = (() => {
    let _db = null;

    // Deterministic spine colour — fallback when no art / CORS blocked
    function _spineColor(seed) {
        let h = 0;
        for (let i = 0; i < seed.length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
        return `hsl(${h % 360},30%,22%)`;
    }

    // Spine width from runtime (minutes). No data → 20px default.
    // Scale: ~20 min → 17px, ~45 min → 22px, ~80 min → 28px, 100+ → 30px max.
    function _spineWidth(runtime_min) {
        if (!runtime_min) return 20;
        return Math.round(Math.max(14, Math.min(30, 14 + (runtime_min / 80) * 16)));
    }

    // Sample the left-edge pixels of an image and return averaged { bg, text }.
    // Returns null on CORS failure or load error.
    const _colorCache = new Map(); // release_id → { bg, text }
    function _sampleEdgeColor(url) {
        return new Promise(resolve => {
            const img = new Image();
            img.crossOrigin = 'anonymous';
            img.onload = () => {
                try {
                    const W = 6, H = Math.min(img.naturalHeight, 300);
                    const cv = document.createElement('canvas');
                    cv.width = W; cv.height = H;
                    const ctx = cv.getContext('2d');
                    ctx.drawImage(img, 0, 0, W, H);
                    const d = ctx.getImageData(0, 0, W, H).data;
                    let r = 0, g = 0, b = 0, n = 0;
                    for (let i = 0; i < d.length; i += 4) { r += d[i]; g += d[i+1]; b += d[i+2]; n++; }
                    r = Math.round(r * 0.75 / n); g = Math.round(g * 0.75 / n); b = Math.round(b * 0.75 / n);
                    const lum = 0.2126 * (r/255) ** 2.2 + 0.7152 * (g/255) ** 2.2 + 0.0722 * (b/255) ** 2.2;
                    resolve({ bg: `rgb(${r},${g},${b})`, text: lum > 0.25 ? 'rgba(0,0,0,0.85)' : 'rgba(255,255,255,0.90)' });
                } catch (_) { resolve(null); }
            };
            img.onerror = () => resolve(null);
            img.src = url;
        });
    }

    // Derive coarse genre from Discogs folder name when DB genre isn't set.
    const FOLDER_GENRE = {
        'vinyl - hip-hop/rap':           'Hip-Hop / Rap',
        'vinyl - dance/electronic':      'Electronic',
        'vinyl - rock/alternative':      'Rock / Alternative',
        'vinyl - disco/funk/jazz/soul':  'Funk / Soul',
        'vinyl - pop/synthpop':          'Pop',
        'vinyl - abstract/experimental': 'Experimental',
        'vinyl - blues/classical':       'Classical / Orchestral',
    };

    const GENRE_ORDER = [
        'Hip-Hop / Rap', 'Rock / Alternative', 'Funk / Soul', 'Electronic',
        'Jazz', 'Pop', 'Classical / Orchestral', 'Experimental', 'Other',
    ];

    // Sections with this many items or fewer get compact rendering (no sort controls).
    const TINY = 6;

    function _sortItems(items, sortBy) {
        const copy = [...items];
        if (sortBy === 'alpha')   return copy.sort((a, b) => (a.title  || '').localeCompare(b.title));
        if (sortBy === 'artist')  return copy.sort((a, b) => (a.artist || '').localeCompare(b.artist));
        if (sortBy === 'year')    return copy.sort((a, b) => (a.year   || 0) - (b.year || 0));
        if (sortBy === 'listens') return copy.sort((a, b) => (b.listens || 0) - (a.listens || 0));
        return copy;
    }

    function _applySpineColor(spine, item) {
        if (!item.art_url) return;
        const cacheKey = item.release_id || item.art_url;
        if (_colorCache.has(cacheKey)) {
            const { bg, text } = _colorCache.get(cacheKey);
            spine.style.background = bg;
            spine.style.setProperty('--spine-text-color', text);
        } else {
            _sampleEdgeColor(item.art_url).then(result => {
                if (!result) return;
                _colorCache.set(cacheKey, result);
                spine.style.background = result.bg;
                spine.style.setProperty('--spine-text-color', result.text);
            });
        }
    }

    function _renderShelf(container, items, sortBy, listView) {
        container.innerHTML = '';
        const sorted = _sortItems(items, sortBy);

        if (listView) {
            const tbl = document.createElement('table');
            tbl.className = 'shelf-list active';
            tbl.innerHTML = `<thead><tr>
                <th>Title</th><th>Artist</th><th>Format</th><th>Year</th><th>Plays</th>
            </tr></thead><tbody></tbody>`;
            const tbody = tbl.querySelector('tbody');
            sorted.forEach(item => {
                const tr = document.createElement('tr');
                tr.style.cursor = 'pointer';
                tr.innerHTML = `
                    <td>${escapeHtml(item.title)}</td>
                    <td style="color:var(--text-secondary)">${escapeHtml(item.artist)}</td>
                    <td style="color:var(--text-tertiary)">${escapeHtml(item.format_short || '')}</td>
                    <td style="color:var(--text-tertiary)">${item.year || '—'}</td>
                    <td class="sl-listens">${item.listens ? formatNumber(item.listens) : '—'}</td>
                `;
                tr.addEventListener('click', () => _openDrawer(item));
                tbody.appendChild(tr);
            });
            container.appendChild(tbl);
            return;
        }

        const shelf = document.createElement('div');
        shelf.className = 'spine-shelf';
        sorted.forEach(item => {
            const spine = document.createElement('div');
            const htClass = item.format_coarse === 'ep' ? 'spine-ep' : 'spine-lp';
            spine.className = `spine ${htClass}`;
            spine.style.width = item.spine_width + 'px';
            spine.style.background = _spineColor(item.discogs_id || item.title);
            spine.innerHTML = `
                <span class="spine-text">${escapeHtml(item.title)}</span>
                <div class="spine-tip">
                    <div class="spine-tip-title">${escapeHtml(item.title)}</div>
                    <div class="spine-tip-artist">${escapeHtml(item.artist)}</div>
                    <div class="spine-tip-meta">${escapeHtml(item.label || '')}${item.label ? ' · ' : ''}${item.year || '?'} · ${escapeHtml(item.format_short || '')}${item.listens ? ' · ' + formatNumber(item.listens) + ' plays' : ''}</div>
                </div>
            `;
            spine.addEventListener('click', () => _openDrawer(item));
            shelf.appendChild(spine);
            _applySpineColor(spine, item);
        });
        container.appendChild(shelf);
    }

    let _drawer = null;

    function _openDrawer(item) {
        if (!_drawer) {
            _drawer = document.createElement('div');
            _drawer.className = 'detail-drawer';
            _drawer.innerHTML = `
                <button class="drawer-close" aria-label="Close">✕</button>
                <div id="drawer-body"></div>
            `;
            _drawer.querySelector('.drawer-close').addEventListener('click', () => {
                _drawer.classList.remove('open');
            });
            document.body.appendChild(_drawer);
        }
        const body = _drawer.querySelector('#drawer-body');
        const libLink = item.release_id
            ? `<a class="drawer-link" href="index.html?view=release&id=${encodeURIComponent(item.release_id)}" target="_blank">
                 <i data-lucide="library"></i> View in library
               </a>`
            : '';
        const metaRows = [
            ['Format',    item.format_short],
            ['Label',     item.label],
            ['Year',      item.year],
            ['Catalog #', item.catalog_number],
            ['Medium',    item.medium],
            ['Media',     item.media_condition],
            ['Sleeve',    item.sleeve_condition],
            ['Added',     item.date_added ? item.date_added.slice(0, 10) : null],
            ['Listens',   item.listens ? formatNumber(item.listens) : null],
            ['Genre',     item.coarse_genre],
        ].filter(([, v]) => v);

        body.innerHTML = `
            <div class="drawer-title">${escapeHtml(item.title)}</div>
            <div class="drawer-artist">${escapeHtml(item.artist)}</div>
            ${metaRows.map(([l, v]) => `
                <div class="drawer-row">
                    <span class="drawer-label">${l}</span>
                    <span class="drawer-value">${escapeHtml(String(v))}</span>
                </div>`).join('')}
            <div class="drawer-links">
                <a class="drawer-link" href="https://www.discogs.com/release/${encodeURIComponent(item.discogs_id)}"
                   target="_blank" rel="noopener">
                    <i data-lucide="external-link"></i> View on Discogs
                </a>
                ${libLink}
            </div>
        `;
        _drawer.classList.add('open');
        lucide.createIcons();
    }

    function _buildSection(parent, items, headingTag, headingText) {
        if (!items.length) return;

        const isTiny = items.length <= TINY;

        const heading = document.createElement(isTiny ? 'h3' : headingTag);
        heading.className = isTiny ? 'coll-h3 coll-tiny-label' : `coll-${headingTag}`;
        heading.textContent = isTiny ? `${headingText} (${items.length})` : headingText;

        const shelfWrap = document.createElement('div');
        shelfWrap.className = 'spine-shelf-wrap';

        if (isTiny) {
            _renderShelf(shelfWrap, items, 'alpha', false);
            parent.appendChild(heading);
            parent.appendChild(shelfWrap);
            return;
        }

        let sortBy = 'alpha';
        let listView = false;

        const controls = document.createElement('div');
        controls.className = 'shelf-controls';

        const sortLabels = { alpha: 'A–Z', artist: 'Artist', year: 'Year', listens: 'Plays' };
        Object.keys(sortLabels).forEach(s => {
            const btn = document.createElement('button');
            btn.className = 'shelf-sort-btn' + (s === sortBy ? ' active' : '');
            btn.textContent = sortLabels[s];
            btn.addEventListener('click', () => {
                sortBy = s;
                controls.querySelectorAll('.shelf-sort-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                _renderShelf(shelfWrap, items, sortBy, listView);
            });
            controls.appendChild(btn);
        });

        const toggleBtn = document.createElement('button');
        toggleBtn.className = 'list-view-toggle';
        toggleBtn.innerHTML = '<i data-lucide="list"></i> List';
        toggleBtn.addEventListener('click', () => {
            listView = !listView;
            toggleBtn.innerHTML = listView
                ? '<i data-lucide="layout-grid"></i> Shelf'
                : '<i data-lucide="list"></i> List';
            _renderShelf(shelfWrap, items, sortBy, listView);
            lucide.createIcons();
        });
        controls.appendChild(toggleBtn);

        const countEl = document.createElement('span');
        countEl.className = 'shelf-count';
        countEl.textContent = `${items.length} item${items.length !== 1 ? 's' : ''}`;
        controls.appendChild(countEl);

        _renderShelf(shelfWrap, items, sortBy, listView);

        parent.appendChild(heading);
        parent.appendChild(controls);
        parent.appendChild(shelfWrap);
    }

    function _loadItems() {
        const result = _db.exec(`
            SELECT ci.discogs_release_id  AS discogs_id,
                   ci.medium,
                   COALESCE(r.title, ci.discogs_release_id) AS title,
                   COALESCE(a.name, 'Unknown Artist')        AS artist,
                   r.album_art_url                           AS art_url,
                   ci.format,
                   ci.format_coarse,
                   ci.label,
                   ci.catalog_number,
                   ci.date_added,
                   ci.media_condition,
                   ci.sleeve_condition,
                   ci.coarse_genre,
                   ci.release_id,
                   ci.discogs_folder,
                   CAST(substr(ci.date_added, 1, 4) AS INTEGER) AS year,
                   sm.source_type,
                   sm.industry_region,
                   sm.original_language,
                   COALESCE(lc.listens, 0)    AS listens,
                   COALESCE(ts.track_count, 0) AS track_count,
                   COALESCE(ts.runtime_min, 0) AS runtime_min
            FROM   collection_items ci
            LEFT JOIN releases r  ON r.id  = ci.release_id
            LEFT JOIN artists  a  ON a.id  = r.primary_artist_id
            LEFT JOIN release_soundtrack_meta sm ON sm.release_id = r.id
            LEFT JOIN (
                SELECT t.release_id, COUNT(l.id) AS listens
                FROM   listens l JOIN tracks t ON t.id = l.track_id
                GROUP  BY t.release_id
            ) lc ON lc.release_id = r.id
            LEFT JOIN (
                SELECT release_id,
                       COUNT(id)                        AS track_count,
                       SUM(COALESCE(duration_ms,0))/60000.0 AS runtime_min
                FROM   tracks WHERE hidden = 0
                GROUP  BY release_id
            ) ts ON ts.release_id = r.id
            ORDER  BY ci.coarse_genre, COALESCE(r.title, ci.discogs_release_id)
        `)[0];

        if (!result) return [];
        const cols = result.columns;
        return result.values.map(row => {
            const obj = Object.fromEntries(cols.map((c, i) => [c, row[i]]));
            obj.format_short = (obj.format || '').split(',')[0].trim().replace(/"/g, '″');

            // Folder-based genre fallback (Discogs API is blocked on-network)
            if (!obj.coarse_genre || obj.coarse_genre === 'Other') {
                const folderKey = (obj.discogs_folder || '').toLowerCase();
                obj.coarse_genre = FOLDER_GENRE[folderKey] || 'Other';
            }

            // Treat items in a Soundtracks folder as soundtrack regardless of format_coarse
            if (obj.source_type || (obj.discogs_folder || '').toLowerCase().includes('soundtrack')) {
                obj.format_coarse = 'soundtrack';
            }

            obj.spine_width = _spineWidth(obj.runtime_min);
            return obj;
        });
    }

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/collection – Physical';

        container.innerHTML = `
            <nav class="coll-nav">
                <a href="index.html" class="back-link">
                    <i data-lucide="arrow-left"></i> Listening History
                </a>
                <div class="coll-tabs">
                    <a href="?page=physical" class="coll-tab active">Vinyl</a>
                    <a href="?page=digital"  class="coll-tab">Digital</a>
                </div>
            </nav>
            <div class="coll-page" id="collPhysicalPage">
                <div class="loading">Building shelf...</div>
            </div>
        `;
        lucide.createIcons();

        const page  = document.getElementById('collPhysicalPage');
        const items = _loadItems();

        if (!items.length) {
            page.innerHTML = `<p style="color:var(--text-secondary);margin-top:2rem">
                No collection items found.
                Run <code>python mdb.py collection import &lt;csv&gt;</code> to import your Discogs collection.
            </p>`;
            return;
        }

        page.innerHTML = '';

        const h1Groups = { album: [], ep: [], single: [], soundtrack: [] };
        items.forEach(item => {
            const key = h1Groups[item.format_coarse] ? item.format_coarse : 'album';
            h1Groups[key].push(item);
        });

        // ── Albums ──────────────────────────────────────────────────────────────
        if (h1Groups.album.length) {
            const h1 = document.createElement('h1');
            h1.className = 'coll-h1'; h1.textContent = 'Albums';
            page.appendChild(h1);
            const byGenre = {};
            h1Groups.album.forEach(i => {
                const g = i.coarse_genre || 'Other';
                (byGenre[g] = byGenre[g] || []).push(i);
            });
            GENRE_ORDER.forEach(g => { if (byGenre[g]) _buildSection(page, byGenre[g], 'h2', g); });
            Object.keys(byGenre).filter(g => !GENRE_ORDER.includes(g))
                .forEach(g => _buildSection(page, byGenre[g], 'h2', g));
        }

        // ── EPs & Singles ────────────────────────────────────────────────────────
        const epsAndSingles = [...h1Groups.ep, ...h1Groups.single];
        if (epsAndSingles.length) {
            const h1 = document.createElement('h1');
            h1.className = 'coll-h1'; h1.textContent = 'EPs & Singles';
            page.appendChild(h1);
            const byGenre = {};
            epsAndSingles.forEach(i => {
                const g = i.coarse_genre || 'Other';
                (byGenre[g] = byGenre[g] || []).push(i);
            });
            GENRE_ORDER.forEach(g => { if (byGenre[g]) _buildSection(page, byGenre[g], 'h2', g); });
            Object.keys(byGenre).filter(g => !GENRE_ORDER.includes(g))
                .forEach(g => _buildSection(page, byGenre[g], 'h2', g));
        }

        // ── Soundtracks ─────────────────────────────────────────────────────────
        if (h1Groups.soundtrack.length) {
            const h1 = document.createElement('h1');
            h1.className = 'coll-h1'; h1.textContent = 'Soundtracks';
            page.appendChild(h1);

            const bySource = {};
            h1Groups.soundtrack.forEach(i => {
                const src = i.source_type || 'film';
                (bySource[src] = bySource[src] || []).push(i);
            });

            const srcLabels = { film: 'Film', video_game: 'Video Game', tv_series: 'TV Series', other: 'Other' };
            ['film', 'video_game', 'tv_series', 'other'].forEach(src => {
                const srcItems = bySource[src];
                if (!srcItems) return;
                const h3 = document.createElement('h3');
                h3.className = 'coll-h3'; h3.textContent = srcLabels[src] || src;
                page.appendChild(h3);
                const byLang = {};
                srcItems.forEach(i => {
                    const lang = (i.original_language || 'en') + '-' + (i.industry_region || 'US');
                    (byLang[lang] = byLang[lang] || []).push(i);
                });
                Object.keys(byLang).sort().forEach(lang => {
                    _buildSection(page, byLang[lang], 'h4', lang);
                });
            });
        }

        lucide.createIcons();
    }

    function unmount() {
        if (_drawer) _drawer.classList.remove('open');
    }

    return { mount, unmount };
})();
