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
    try {
        const SQL = await initSqlJs({
            locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });
        const buffer = await DB_CONFIG.fetchDatabase();
        _db = new SQL.Database(new Uint8Array(buffer));
        await loadOverridesDatabase(SQL, _db);
        navigate(getParams(), false);
    } catch (err) {
        console.error('Error loading database:', err);
        container.innerHTML = '<div class="loading" style="color: var(--error);">Error loading database. Please refresh.</div>';
    }
})();
