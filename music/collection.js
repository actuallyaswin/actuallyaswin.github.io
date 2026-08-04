let _db = null;
let _currentView = null;

const COLLECTION_VIEWS = {
    'physical': () => ViewCollectionPhysical,
    'digital':  () => ViewCollectionDigital,
};

function getParams() {
    return Object.fromEntries(new URLSearchParams(window.location.search));
}

function navigate(params, pushState = true) {
    if (_currentView && _currentView.unmount) _currentView.unmount();
    _currentView = null;

    const container = document.getElementById('view-container');
    const pageName  = params.page || 'physical';
    const viewFn    = COLLECTION_VIEWS[pageName] || COLLECTION_VIEWS['physical'];
    _currentView    = viewFn();

    if (pushState) {
        const qs = new URLSearchParams(params).toString();
        history.pushState(params, '', qs ? `?${qs}` : '?');
    }

    _currentView.mount(container, _db, params);
}

// SPA-style link interception (same pattern as app.js)
document.addEventListener('click', e => {
    const a = e.target.closest('a[href]');
    if (!a) return;
    const href = a.getAttribute('href');
    if (!href || !href.startsWith('?')) return;
    e.preventDefault();
    navigate(Object.fromEntries(new URLSearchParams(href.slice(1))));
});

window.addEventListener('popstate', e => {
    navigate(e.state || getParams(), false);
});

// Bootstrap — identical pattern to app.js
(async function () {
    const container = document.getElementById('view-container');
    try {
        const SQL = await initSqlJs({
            locateFile: f => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${f}`
        });
        const buffer = await DB_CONFIG.fetchDatabase();
        _db = new SQL.Database(new Uint8Array(buffer));
        navigate(getParams(), false);
    } catch (err) {
        console.error('Error loading database:', err);
        container.innerHTML = '<div class="loading" style="color:var(--error)">Error loading database. Please refresh.</div>';
    }
})();
