const ViewGenres = (() => {
    let _db = null;
    // [{id, name, plays, releases}]  (heard genres only)
    let _allGenres = [];
    let _query = '';
    // 'plays' | 'name' | 'tree'
    let _sort = 'plays';
    // expanded tree node ids, persisted across mount/back-nav
    let _openIds = new Set();

    // sessionStorage (not the URL) — per-node expand state would make for an
    // unreadable URL, and this only needs to survive back/forward within the
    // same tab, which sessionStorage already does.
    const OPEN_IDS_KEY = 'genresTreeOpenIds';

    function _loadOpenIds() {
        try {
            const raw = sessionStorage.getItem(OPEN_IDS_KEY);
            _openIds = raw ? new Set(JSON.parse(raw)) : new Set();
        } catch (_) {
            _openIds = new Set();
        }
    }

    function _saveOpenIds() {
        try {
            sessionStorage.setItem(OPEN_IDS_KEY, JSON.stringify([..._openIds]));
        } catch (_) {}
    }

    function mount(container, db, params) {
        _db = db;
        _query = params.q || '';
        _sort = ['tree', 'plays', 'name'].includes(params.sort) ? params.sort : 'tree';
        _loadOpenIds();
        setPageTitle('Genres');

        container.innerHTML = `
            <header>
                <nav class="genre-breadcrumb">
                    <a href="?" class="bc-home"><i data-lucide="home"></i></a>
                    <i data-lucide="chevron-right" class="bc-sep"></i>
                    <span class="bc-current">Genres</span>
                </nav>
                <h1>Genres</h1>
                <p class="subtitle" id="genresSubtitle"></p>
            </header>

            <div class="page-controls">
                <div class="control-block" style="flex:1;max-width:320px">
                    <div class="filter-search" style="width:100%">
                        <i data-lucide="search" class="filter-search-icon"></i>
                        <input id="genresSearch" class="filter-search-input" placeholder="Filter genres…"
                               autocomplete="off" value="${escapeHtml(_query)}">
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Sort</span>
                    <div class="sort-controls">
                        <button class="sort-btn${_sort === 'tree' ? ' active' : ''}" data-sort="tree">Tree</button>
                        <button class="sort-btn${_sort === 'plays' ? ' active' : ''}" data-sort="plays">Most played</button>
                        <button class="sort-btn${_sort === 'name' ? ' active' : ''}" data-sort="name">A–Z</button>
                    </div>
                </div>
            </div>

            <div id="genresGroups"></div>
        `;

        _load();
    }

    function unmount() {}

    function _syncUrl() {
        const p = new URLSearchParams({ view: 'genres', sort: _sort });
        if (_query) p.set('q', _query);
        history.replaceState(Object.fromEntries(p), '', '?' + p.toString());
    }

    function _load() {
        // Precomputed by `mdb.py stats refresh` — a live join across all
        // genres x releases x tracks x listens was taking seconds to render.
        const cacheRes = _db.exec("SELECT value_json FROM stats_cache WHERE key = 'genresIndex'")[0];
        const rows = cacheRes ? JSON.parse(cacheRes.values[0][0]) : [];

        _allGenres = rows.map(({ id, name, releases, plays }) => ({ id: String(id), name, releases, plays }));

        // Not the sum of each genre's own `plays` — releases average ~4 genre
        // tags each, so that sum triple-counts most listens. This is the
        // real distinct-listen count instead (each scrobble counted once).
        const totalRes = _db.exec("SELECT value_json FROM stats_cache WHERE key = 'genresTotalListens'")[0];
        const totalListens = totalRes ? JSON.parse(totalRes.values[0][0]) : 0;
        document.getElementById('genresSubtitle').textContent =
            `${formatNumber(_allGenres.length)} genres heard, spanning ${formatNumber(totalListens)} listens`;

        _setupControls();
        _render();
    }

    function _setupControls() {
        document.getElementById('genresSearch')?.addEventListener('input', e => {
            _query = e.target.value.toLowerCase().trim();
            _syncUrl();
            _render();
        });
        setupToggleGroup('[data-sort]', btn => {
            _sort = btn.dataset.sort;
            _syncUrl();
            _render();
        });
    }

    // Delegated 'toggle' listener — <details> elements are recreated on every
    // _render(), so this re-attaches each time rather than per-node.
    function _wireTreeToggles(container) {
        container.querySelectorAll('.genre-tree-node').forEach(details => {
            details.addEventListener('toggle', () => {
                const id = details.dataset.genreId;
                if (!id) return;
                if (details.open) _openIds.add(id);
                else _openIds.delete(id);
                _saveOpenIds();
            });
        });
    }

    // GENRE_TREE is a DAG (a genre can have >1 parent) — building a strict
    // tree would either drop or duplicate those genres. Duplicating is the
    // honest choice: e.g. Mambo genuinely belongs under Latin American Music,
    // Regional, AND Spanish Caribbean Music on AOTY, so it appears under each.
    function _buildForest(genreById) {
        const heardIds = new Set(genreById.keys());

        // Every id that's either heard or an ancestor of a heard genre —
        // ancestors-only nodes render as unplayed connective tissue.
        const relevant = new Set();
        const markAncestors = (id, depth) => {
            // guard against a cyclic/malformed tree
            if (depth > 30) return;
            relevant.add(id);
            (GENRE_TREE[id]?.parents || []).forEach(pid => markAncestors(String(pid), depth + 1));
        };
        heardIds.forEach(id => markAncestors(id, 0));

        const roots = [...relevant].filter(id => {
            const parents = (GENRE_TREE[id]?.parents || []).map(String);
            return !parents.some(p => relevant.has(p));
        });

        function buildNode(id, ancestry) {
            // cycle guard
            if (ancestry.has(id)) return null;
            const g = genreById.get(id);
            const name = g?.name || GENRE_TREE[id]?.name || 'Unknown';
            const children = (GENRE_TREE[id]?.children || [])
                .map(String)
                .filter(cid => relevant.has(cid))
                .map(cid => buildNode(cid, new Set([...ancestry, id])))
                .filter(Boolean)
                .sort((a, b) => b.subtreePlays - a.subtreePlays || a.name.localeCompare(b.name));
            const ownPlays = g?.plays || 0;
            const subtreePlays = ownPlays + children.reduce((s, c) => s + c.subtreePlays, 0);
            return { id, name, plays: ownPlays, releases: g?.releases || 0, heard: !!g, children, subtreePlays };
        }

        return roots
            .map(id => buildNode(id, new Set()))
            .filter(Boolean)
            .sort((a, b) => b.subtreePlays - a.subtreePlays || a.name.localeCompare(b.name));
    }

    function _nodeHtml(node) {
        const label = node.heard
            ? `<a href="?view=genre&id=${node.id}" class="genre-tree-link">${escapeHtml(node.name)}</a>`
            : `<span class="genre-tree-unheard">${escapeHtml(node.name)}</span>`;
        const countHtml = node.heard
            ? `<span class="genre-tree-count">${formatNumber(node.plays)} plays</span>`
            : `<span class="genre-tree-count genre-tree-count-muted">not yet heard</span>`;

        if (!node.children.length) {
            return `<li class="genre-tree-leaf">${label} ${countHtml}</li>`;
        }

        const open = _openIds.has(node.id);
        return `
            <li>
                <details class="genre-tree-node" data-genre-id="${node.id}"${open ? ' open' : ''}>
                    <summary>${label} ${countHtml}
                        <span class="genre-tree-child-count">${node.children.length}</span>
                    </summary>
                    <ul class="genre-tree-children">
                        ${node.children.map(c => _nodeHtml(c)).join('')}
                    </ul>
                </details>
            </li>`;
    }

    function _render() {
        const container = document.getElementById('genresGroups');
        if (!container) return;

        let genres = _query
            ? _allGenres.filter(g => g.name.toLowerCase().includes(_query))
            : _allGenres;

        if (!genres.length) {
            container.innerHTML = '<div class="empty-state"><div class="empty-state-title">No genres match</div></div>';
            return;
        }

        if (_sort === 'name') {
            const sorted = [...genres].sort((a, b) => a.name.localeCompare(b.name));
            container.innerHTML = `<div class="genre-list" style="margin-top:1rem">` +
                sorted.map(g => `<a href="?view=genre&id=${g.id}" class="genre-tag">${escapeHtml(g.name)}</a>`).join(', ') +
                `</div>`;
            return;
        }

        if (_sort === 'plays') {
            const sorted = [...genres].sort((a, b) => b.plays - a.plays);
            const max = Math.max(...sorted.map(g => g.plays), 1);
            const rows = sorted.map(g => {
                const pct = ((g.plays / max) * 100).toFixed(1);
                return `
                    <div class="lang-row lang-row-static">
                        <a href="?view=genre&id=${g.id}" class="lang-code lang-code-link">${escapeHtml(g.name)}</a>
                        <div class="lang-bar-track">
                            <div class="lang-bar-fill" style="width:${pct}%;background:var(--primary)"></div>
                        </div>
                        <span class="lang-count">${formatNumber(g.plays)}</span>
                        <span class="lang-pct">${formatNumber(g.releases)} rel.</span>
                        <span></span>
                    </div>`;
            }).join('');
            container.innerHTML = `<div class="lang-list has-drilldown">${rows}</div>`;
            return;
        }

        // Tree mode while searching — a DAG hit can have several parent
        // chains (genres routinely have 2-3 parents on AOTY), and rendering
        // the whole forest repeats the same node under every chain. Past a
        // couple of matches that reads as chaos rather than a search result,
        // so show a flat list instead: one row per match, with its primary
        // ancestor chain as a breadcrumb above the name.
        if (_query) {
            const sorted = [...genres].sort((a, b) => b.plays - a.plays);
            const rows = sorted.map(g => `
                <div class="genre-search-hit">
                    <div class="genre-search-hit-path">${_breadcrumbHtml(g.id)}</div>
                    <div class="genre-search-hit-row">
                        <a href="?view=genre&id=${g.id}" class="genre-tree-link">${escapeHtml(g.name)}</a>
                        <span class="genre-tree-count">${formatNumber(g.plays)} plays</span>
                    </div>
                </div>`).join('');
            container.innerHTML = `<div class="genre-search-hits">${rows}</div>`;
            return;
        }

        // Tree mode, no search — full forest, roots collapsed by default
        // (36 roots auto-expanded made the page enormous).
        const genreById = new Map(genres.map(g => [g.id, g]));
        const forest = _buildForest(genreById);
        container.innerHTML = `<ul class="genre-tree-root">${forest.map(n => _nodeHtml(n)).join('')}</ul>`;
        _wireTreeToggles(container);
    }

    // Depth = longest chain to a root, memoized. Used to pick the most
    // specific parent below when a genre has several (AOTY routinely lists
    // one under both a broad umbrella and a narrower category at once —
    // French House sits under both "Electronic" and "House" directly).
    const _depthCache = new Map();
    function _genreDepth(id) {
        if (_depthCache.has(id)) return _depthCache.get(id);
        // cycle guard while this id is being computed
        _depthCache.set(id, 0);
        const parents = (GENRE_TREE[id]?.parents || []).map(String);
        const depth = parents.length ? 1 + Math.max(...parents.map(_genreDepth)) : 0;
        _depthCache.set(id, depth);
        return depth;
    }

    // One representative parent chain for breadcrumb display — at each step,
    // follow the deepest (most specific) parent rather than an arbitrary one.
    function _breadcrumbPath(id) {
        const path = [];
        const seen = new Set();
        let cur = id;
        while (cur != null && !seen.has(cur)) {
            seen.add(cur);
            const node = GENRE_TREE[cur];
            path.unshift(node?.name || 'Unknown');
            const parents = (node?.parents || []).map(String);
            cur = parents.length
                ? parents.reduce((best, p) => _genreDepth(p) > _genreDepth(best) ? p : best)
                : null;
        }
        return path;
    }

    function _breadcrumbHtml(id) {
        const ancestors = _breadcrumbPath(id).slice(0, -1);
        return ancestors.length ? escapeHtml(ancestors.join(' › ')) : '';
    }

    return { mount, unmount };
})();
