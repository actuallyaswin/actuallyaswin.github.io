let _db = null;
let _currentView = null;

// Don't add an 'admin' route: the editor lives outside this repo and runs
// against a local DB. Shipping it puts its PIN gate in every visitor's hands.
const VIEWS = {
    'home':            () => ViewHome,
    'year':            () => ViewYear,
    'top':             () => ViewTop,
    'artist':          () => ViewArtist,
    'release':         () => ViewRelease,
    'genre':           () => ViewGenre,
    'recommendations': () => ViewRecommendations,
    'history':         () => ViewHistory,
    'stats':           () => ViewStats,
};

function getParams() {
    return Object.fromEntries(new URLSearchParams(window.location.search));
}

function renderErrorPage(container, title, message, options = {}) {
    const { retry = false } = options;
    container.innerHTML = `
        <div class="app-error">
            <i data-lucide="${retry ? 'cloud-off' : 'compass'}" class="app-error-icon"></i>
            <h1 class="app-error-title">${escapeHtml(title)}</h1>
            <p class="app-error-message">${escapeHtml(message)}</p>
            <div class="app-error-actions">
                ${retry ? '<button type="button" class="app-error-btn" id="appErrorRetry">Try again</button>' : ''}
                <a class="app-error-link" href="?">Back to home</a>
            </div>
        </div>`;
    if (retry) {
        const btn = document.getElementById('appErrorRetry');
        if (btn) btn.addEventListener('click', () => window.location.reload());
    }
}

function navigate(params, pushState = true) {
    if (_currentView && _currentView.unmount) {
        // A throw here must not prevent the next view from mounting.
        try {
            _currentView.unmount();
        } catch (err) {
            console.error('View unmount failed:', err);
        }
    }
    _currentView = null;

    const container = document.getElementById('view-container');
    const viewName = params.view || 'home';

    if (pushState) {
        const qs = new URLSearchParams(params).toString();
        history.pushState(params, '', qs ? `?${qs}` : '?');
        // Forward navigation only — popstate keeps the browser's restored position.
        window.scrollTo(0, 0);
    }

    // Don't fall through to home — that leaves a bogus ?view= in the URL.
    if (!VIEWS[viewName]) {
        renderErrorPage(container, 'Page not found', `There's no “${viewName}” view here.`);
        return;
    }

    _currentView = VIEWS[viewName]();

    // Error boundary: views run many synchronous queries with little internal
    // handling, and one bad query would otherwise white-screen the SPA.
    try {
        _currentView.mount(container, _db, params);
    } catch (err) {
        console.error(`View "${viewName}" failed to mount:`, err);
        _currentView = null;
        renderErrorPage(
            container,
            'Something went wrong',
            'This page could not be rendered. The error has been logged to the console.',
            { retry: true }
        );
        return;
    }

    // Fade-in transition
    container.style.animation = 'none';
    void container.offsetWidth;
    container.style.animation = 'viewFadeIn 0.18s ease forwards';
}

// Search lives in search.js.
document.addEventListener('DOMContentLoaded', () => {
    lucide.createIcons();
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
        'top':          'Loading…',
    };
    const label = VIEW_LOADING[params.view] || 'Loading…';
    container.innerHTML = `
        <div class="db-loading">
            <div class="db-loading-label">${label}</div>
            <div class="db-progress"><div class="db-progress-fill" id="dbProgressFill"></div></div>
            <div class="db-loading-hint" id="dbProgressHint">Fetching listening database…</div>
        </div>`;

    const fill = document.getElementById('dbProgressFill');
    const hint = document.getElementById('dbProgressHint');
    const MB = 1024 * 1024;

    try {
        const SQL = await initSqlJs({
            locateFile: file => `https://cdn.jsdelivr.net/npm/sql.js@1.8.0/dist/${file}`
        });

        const buffer = await DB_CONFIG.fetchDatabase((ratio, loaded, total) => {
            if (fill) fill.style.width = `${(ratio * 100).toFixed(1)}%`;
            if (hint) {
                hint.textContent =
                    `${(loaded / MB).toFixed(1)} of ${(total / MB).toFixed(0)} MB`;
            }
        });

        if (hint) hint.textContent = 'Opening database…';
        // Yield a frame so the label above actually paints before sql.js blocks
        // the main thread parsing ~47 MB.
        await new Promise(requestAnimationFrame);

        _db = new SQL.Database(new Uint8Array(buffer));

        navigate(params, false);
    } catch (err) {
        console.error('Error loading database:', err);
        const offline = typeof navigator !== 'undefined' && navigator.onLine === false;
        renderErrorPage(
            container,
            offline ? 'You appear to be offline' : 'Could not load the database',
            offline
                ? 'Reconnect and try again — the listening database is about 18 MB.'
                : 'The listening database failed to download or was corrupt.',
            { retry: true }
        );
    }
})();
