// Index of every canonical album list (RS500, Apple Music 100, etc.),
// grouped by publication — AOTY's own lists page groups this way, and it
// scales far better than one flat grid once there are a dozen+ lists.
// Lists with no publication (or publication:'other') fall into a trailing
// "Other" group, mirroring AOTY's "Miscellaneous Lists" catch-all.
const ViewLists = (() => {
    let _db = null;

    // Display name + icon for each known `canonical_lists.publication`
    // value. A publication with no entry here still renders (falls back to
    // a generic label, no icon) rather than disappearing — this map only
    // controls presentation, not which lists exist.
    const PUBLICATIONS = {
        rollingstone:  { name: 'Rolling Stone', icon: 'images/publications/rollingstone.svg' },
        pitchfork:     { name: 'Pitchfork',     icon: 'images/publications/pitchfork.svg' },
        theguardian:   { name: 'The Guardian',  icon: 'images/publications/theguardian.svg' },
        npr:           { name: 'NPR Music',     icon: 'images/publications/npr.svg' },
        complex:       { name: 'Complex',       icon: 'images/publications/complex.svg' },
        theneedledrop: { name: 'theneedledrop', icon: 'images/publications/theneedledrop.svg' },
        'apple-music': { name: 'Apple Music' },
        aoty:          { name: 'Album of the Year' },
        nme:           { name: 'NME' },
    };
    const OTHER_KEY = 'other';
    const OTHER_LABEL = 'Other';

    function _cache(key) {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : null;
    }

    function _groupLists(lists) {
        const groups = new Map();
        for (const lst of lists) {
            const pub = lst.publication && lst.publication !== OTHER_KEY ? lst.publication : OTHER_KEY;
            if (!groups.has(pub)) groups.set(pub, []);
            groups.get(pub).push(lst);
        }
        // Named publications first (by display name), "Other" always last —
        // matches AOTY's own pattern of appending its misc bucket at the end
        // rather than interleaving it alphabetically.
        const keys = [...groups.keys()].filter(k => k !== OTHER_KEY)
            .sort((a, b) => (PUBLICATIONS[a]?.name || a).localeCompare(PUBLICATIONS[b]?.name || b));
        if (groups.has(OTHER_KEY)) keys.push(OTHER_KEY);
        return keys.map(key => ({ key, lists: groups.get(key).sort((a, b) => b.total - a.total) }));
    }

    function _listCardHtml(lst) {
        const pct = lst.total ? Math.round((lst.heard / lst.total) * 100) : 0;
        const label = lst.short_name || lst.name;
        const mainstream = lst.mainstream_mean != null
            ? `<span class="pub-list-card-mainstream" title="Average Spotify popularity (0-100) of this list's artists">
                   <i data-lucide="trending-up"></i>${Math.round(lst.mainstream_mean)}
               </span>`
            : '';
        return `
            <a class="pub-list-card" href="?view=list&id=${encodeURIComponent(lst.id)}"
               aria-label="View all ${escapeHtml(label)} albums" title="${escapeHtml(lst.name)}">
                <div class="pub-list-card-top">
                    <span class="pub-list-card-title">${escapeHtml(label)}</span>
                    <span class="pub-list-card-pct">${pct}%</span>
                </div>
                <div class="pub-list-card-track">
                    <div class="pub-list-card-fill" style="width:${pct}%"></div>
                </div>
                <div class="pub-list-card-count">
                    <span>${lst.heard} of ${lst.total} heard</span>
                    ${mainstream}
                </div>
            </a>`;
    }

    function _groupHtml({ key, lists }) {
        const pub = PUBLICATIONS[key];
        const name = pub?.name || (key === OTHER_KEY ? OTHER_LABEL : key);
        const iconHtml = pub?.icon
            ? `<span class="pub-group-icon" style="-webkit-mask-image:url('${pub.icon}');mask-image:url('${pub.icon}')"></span>`
            : '';
        return `
            <section class="pub-group">
                <div class="pub-group-header">
                    ${iconHtml}
                    <h2 class="pub-group-title">${escapeHtml(name)}</h2>
                    <span class="pub-group-count">${lists.length}</span>
                </div>
                <div class="pub-group-grid">
                    ${lists.map(_listCardHtml).join('')}
                </div>
            </section>`;
    }

    function mount(container, db, params) {
        _db = db;
        setPageTitle('Lists');

        const lists = _cache('canonicalLists') || [];
        const groups = _groupLists(lists);
        const totalLists = lists.length;
        const totalAlbums = lists.reduce((s, l) => s + l.total, 0);
        const totalHeard = lists.reduce((s, l) => s + l.heard, 0);

        container.innerHTML = `
            <header>
                <nav class="genre-breadcrumb">
                    <a href="?" class="bc-home"><i data-lucide="home"></i></a>
                    <i data-lucide="chevron-right" class="bc-sep"></i>
                    <span class="bc-current">Lists</span>
                </nav>
                <h1>Lists</h1>
                <p class="subtitle">${formatNumber(totalLists)} curated album rankings · ${formatNumber(totalHeard)} of ${formatNumber(totalAlbums)} albums heard</p>
            </header>

            ${groups.length ? groups.map(_groupHtml).join('') : `
                <div class="empty-state">
                    <i data-lucide="list" class="app-error-icon"></i>
                    <div class="empty-state-title">No lists yet</div>
                    <p class="empty-state-hint">Run <code>mdb.py list import-csv</code> to add one.</p>
                </div>`}

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons({ el: container });
    }

    function unmount() {}

    return { mount, unmount };
})();
