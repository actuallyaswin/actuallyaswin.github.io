const ViewCollectionDigital = (() => {
    let _db = null;

    function _spineColor(seed) {
        let h = 0;
        for (let i = 0; i < seed.length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
        return `hsl(${h % 360},28%,24%)`;
    }

    function _spineWidth(runtime_min) {
        if (!runtime_min) return 20;
        return Math.round(Math.max(14, Math.min(30, 14 + (runtime_min / 80) * 16)));
    }

    const GENRE_ORDER = [
        'Hip-Hop / Rap', 'Electronic', 'Rock / Alternative', 'Funk / Soul',
        'Jazz', 'Pop', 'Classical / Orchestral', 'Experimental', 'Other',
    ];

    const GENRE_MAP = {
        'hip-hop': 'Hip-Hop / Rap', 'hip hop': 'Hip-Hop / Rap', 'rap': 'Hip-Hop / Rap',
        'electronic': 'Electronic', 'dance': 'Electronic', 'techno': 'Electronic',
        'ambient': 'Electronic', 'idm': 'Electronic', 'house': 'Electronic',
        'rock': 'Rock / Alternative', 'alternative': 'Rock / Alternative',
        'indie': 'Rock / Alternative', 'post-rock': 'Rock / Alternative', 'punk': 'Rock / Alternative',
        'funk': 'Funk / Soul', 'soul': 'Funk / Soul', 'r&b': 'Funk / Soul',
        'disco': 'Funk / Soul', 'gospel': 'Funk / Soul',
        'jazz': 'Jazz', 'blues': 'Jazz',
        'pop': 'Pop', 'synth-pop': 'Pop', 'indie pop': 'Pop',
        'classical': 'Classical / Orchestral', 'orchestral': 'Classical / Orchestral',
        'experimental': 'Experimental', 'noise': 'Experimental', 'abstract': 'Experimental',
    };

    const TINY = 6;

    function _resolveGenre(release) {
        if (release.type_secondary === 'soundtrack' || release.source_type) return '__soundtrack__';
        const rawGenres = (release.genres || '').split(',').map(g => g.trim().toLowerCase());
        for (const g of rawGenres) {
            for (const [key, coarse] of Object.entries(GENRE_MAP)) {
                if (g.includes(key)) return coarse;
            }
        }
        return 'Other';
    }

    function _sortItems(items, sortBy) {
        const copy = [...items];
        if (sortBy === 'alpha')   return copy.sort((a, b) => (a.title || '').localeCompare(b.title));
        if (sortBy === 'artist')  return copy.sort((a, b) => (a.artist || '').localeCompare(b.artist));
        if (sortBy === 'year')    return copy.sort((a, b) => (b.release_year || 0) - (a.release_year || 0));
        if (sortBy === 'listens') return copy.sort((a, b) => (b.listens || 0) - (a.listens || 0));
        if (sortBy === 'first')   return copy.sort((a, b) => (a.first_listen || 0) - (b.first_listen || 0));
        return copy;
    }

    function _renderShelf(container, items, sortBy, listView) {
        container.innerHTML = '';
        const sorted = _sortItems(items, sortBy);

        if (listView) {
            const tbl = document.createElement('table');
            tbl.className = 'shelf-list active';
            tbl.innerHTML = `<thead><tr>
                <th>Title</th><th>Artist</th><th>Year</th><th>Plays</th>
            </tr></thead><tbody></tbody>`;
            const tbody = tbl.querySelector('tbody');
            sorted.forEach(r => {
                const tr = document.createElement('tr');
                tr.style.cursor = 'pointer';
                tr.innerHTML = `
                    <td>${escapeHtml(r.title || '')}</td>
                    <td style="color:var(--text-secondary)">${escapeHtml(r.artist || '')}</td>
                    <td style="color:var(--text-tertiary)">${r.release_year || '—'}</td>
                    <td class="sl-listens">${r.listens ? formatNumber(r.listens) : '—'}</td>
                `;
                tr.addEventListener('click', () => {
                    window.open(`index.html${releaseHref(r.id, r.slug)}`, '_blank');
                });
                tbody.appendChild(tr);
            });
            container.appendChild(tbl);
            return;
        }

        const shelf = document.createElement('div');
        shelf.className = 'spine-shelf';
        sorted.forEach(r => {
            const spine = document.createElement('div');
            spine.className = `spine spine-lp`;
            spine.style.width = _spineWidth(r.runtime_min) + 'px';
            spine.style.background = _spineColor(r.id || r.title);
            spine.innerHTML = `
                <span class="spine-text">${escapeHtml(r.title || '')}</span>
                <div class="spine-tip">
                    <div class="spine-tip-title">${escapeHtml(r.title || '')}</div>
                    <div class="spine-tip-artist">${escapeHtml(r.artist || '')}</div>
                    <div class="spine-tip-meta">${r.release_year || '?'}${r.listens ? ' · ' + formatNumber(r.listens) + ' plays' : ''}${r.owned_vinyl ? ' · ⬥ vinyl' : ''}</div>
                </div>
            `;
            spine.addEventListener('click', () => {
                window.open(`index.html${releaseHref(r.id, r.slug)}`, '_blank');
            });
            shelf.appendChild(spine);
        });
        container.appendChild(shelf);
    }

    function _buildSection(parent, items, headingTag, headingText, sortKey) {
        if (!items.length) return;

        const isTiny = items.length <= TINY;

        const heading = document.createElement(isTiny ? 'h3' : headingTag);
        heading.className = isTiny ? 'coll-h3 coll-tiny-label' : `coll-${headingTag}`;
        heading.textContent = isTiny ? `${headingText} (${items.length})` : headingText;

        const shelfWrap = document.createElement('div');
        shelfWrap.className = 'spine-shelf-wrap';

        if (isTiny) {
            _renderShelf(shelfWrap, items, sortKey, false);
            parent.appendChild(heading);
            parent.appendChild(shelfWrap);
            return;
        }

        let localSort = sortKey;
        let listView = false;

        const controls = document.createElement('div');
        controls.className = 'shelf-controls';

        const sortLabels = { listens: 'Plays', alpha: 'A–Z', year: 'Year', first: 'Discovery' };
        Object.keys(sortLabels).forEach(s => {
            const btn = document.createElement('button');
            btn.className = 'shelf-sort-btn' + (s === localSort ? ' active' : '');
            btn.textContent = sortLabels[s];
            btn.addEventListener('click', () => {
                localSort = s;
                controls.querySelectorAll('.shelf-sort-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                _renderShelf(shelfWrap, items, localSort, listView);
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
            _renderShelf(shelfWrap, items, localSort, listView);
            lucide.createIcons();
        });
        controls.appendChild(toggleBtn);

        const countEl = document.createElement('span');
        countEl.className = 'shelf-count';
        countEl.textContent = `${items.length} item${items.length !== 1 ? 's' : ''}`;
        controls.appendChild(countEl);

        _renderShelf(shelfWrap, items, localSort, listView);

        parent.appendChild(heading);
        parent.appendChild(controls);
        parent.appendChild(shelfWrap);
    }

    function _loadReleases() {
        const result = _db.exec(`
            SELECT r.id,
                   r.title,
                   r.slug,
                   a.name           AS artist,
                   r.release_year,
                   r.type,
                   r.type_secondary,
                   r.stat_total_plays     AS listens,
                   r.stat_first_listen_ts AS first_listen,
                   GROUP_CONCAT(DISTINCT g.name) AS genres,
                   (ci.id IS NOT NULL)    AS owned_vinyl,
                   sm.source_type,
                   sm.industry_region,
                   sm.original_language,
                   COALESCE(ts.runtime_min, 0) AS runtime_min
            FROM   releases r
            LEFT JOIN artists  a   ON a.id  = r.primary_artist_id
            LEFT JOIN release_genres rg ON rg.release_id = r.id AND rg.is_primary = 1
            LEFT JOIN genres   g   ON g.aoty_id = rg.aoty_genre_id
            LEFT JOIN collection_items ci ON ci.release_id = r.id
            LEFT JOIN release_soundtrack_meta sm ON sm.release_id = r.id
            LEFT JOIN (
                SELECT release_id,
                       SUM(COALESCE(duration_ms,0))/60000.0 AS runtime_min
                FROM   tracks WHERE hidden = 0
                GROUP  BY release_id
            ) ts ON ts.release_id = r.id
            WHERE  r.hidden = 0 AND r.stat_total_plays > 0
            GROUP  BY r.id
            ORDER  BY listens DESC
        `)[0];

        if (!result) return [];
        const cols = result.columns;
        return result.values.map(row => Object.fromEntries(cols.map((c, i) => [c, row[i]])));
    }

    function mount(container, db, params) {
        _db = db;
        setPageTitle('Collection', 'Digital');

        container.innerHTML = `
            <nav class="coll-nav">
                <a href="index.html" class="back-link">
                    <i data-lucide="arrow-left"></i> Listening History
                </a>
                <div class="coll-tabs">
                    <a href="?page=physical" class="coll-tab">Vinyl</a>
                    <a href="?page=digital"  class="coll-tab active">Digital</a>
                </div>
            </nav>
            <div class="coll-page" id="collDigitalPage">
                ${renderLoading()}
            </div>
        `;
        lucide.createIcons();

        const page     = document.getElementById('collDigitalPage');
        const releases = _loadReleases();

        page.innerHTML = '';

        const byH1 = { album: {}, ep: {}, soundtrack: {} };
        releases.forEach(r => {
            const genre = _resolveGenre(r);
            if (genre === '__soundtrack__') {
                const src = r.source_type || 'film';
                (byH1.soundtrack[src] = byH1.soundtrack[src] || []).push(r);
            } else {
                const fmt = (r.type === 'album') ? 'album' : 'ep';
                (byH1[fmt][genre] = byH1[fmt][genre] || []).push(r);
            }
        });

        // ── Albums ──────────────────────────────────────────────────────────────
        if (Object.keys(byH1.album).length) {
            const h1 = document.createElement('h1'); h1.className = 'coll-h1'; h1.textContent = 'Albums';
            page.appendChild(h1);
            GENRE_ORDER.forEach(g => {
                if (!byH1.album[g]) return;
                _buildSection(page, byH1.album[g], 'h2', g, 'listens');
            });
            Object.keys(byH1.album).filter(g => !GENRE_ORDER.includes(g))
                .forEach(g => _buildSection(page, byH1.album[g], 'h2', g, 'listens'));
        }

        // ── EPs & Singles ────────────────────────────────────────────────────────
        if (Object.keys(byH1.ep).length) {
            const h1 = document.createElement('h1'); h1.className = 'coll-h1'; h1.textContent = 'EPs & Singles';
            page.appendChild(h1);
            GENRE_ORDER.forEach(g => {
                if (!byH1.ep[g]) return;
                _buildSection(page, byH1.ep[g], 'h2', g, 'listens');
            });
            Object.keys(byH1.ep).filter(g => !GENRE_ORDER.includes(g))
                .forEach(g => _buildSection(page, byH1.ep[g], 'h2', g, 'listens'));
        }

        // ── Soundtracks ─────────────────────────────────────────────────────────
        if (Object.keys(byH1.soundtrack).length) {
            const h1 = document.createElement('h1'); h1.className = 'coll-h1'; h1.textContent = 'Soundtracks';
            page.appendChild(h1);
            const srcLabels = { film: 'Film', video_game: 'Video Game', tv_series: 'TV Series', other: 'Other' };
            ['film', 'video_game', 'tv_series', 'other'].forEach(src => {
                const srcItems = byH1.soundtrack[src];
                if (!srcItems) return;
                const h3 = document.createElement('h3'); h3.className = 'coll-h3'; h3.textContent = srcLabels[src] || src;
                page.appendChild(h3);
                const byLang = {};
                srcItems.forEach(i => {
                    const lang = (i.original_language || 'en') + '-' + (i.industry_region || 'US');
                    (byLang[lang] = byLang[lang] || []).push(i);
                });
                Object.keys(byLang).sort().forEach(lang => {
                    _buildSection(page, byLang[lang], 'h4', lang, 'listens');
                });
            });
        }

        lucide.createIcons();
    }

    function unmount() {}

    return { mount, unmount };
})();
