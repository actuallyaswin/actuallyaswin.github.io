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
}

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

    // Show a view-specific loading message based on URL params
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
