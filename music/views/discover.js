// Discover — merges what used to be two separate shelf-of-cards pages
// (Recommendations, Trends) into one view with a tab toggle. Both share the
// same shelfCard/shelfSection rendering from views/shelf-helpers.js; the only
// real difference is where the data comes from — Recommendations reuses
// views/recommendation-helpers.js's computeRecommendationShelves() (shared
// with Home's teaser shelf), Trends reads precomputed results from
// stats_cache (computed offline in Python by mdb.py's _stats_trends() during
// `stats refresh`). Both load eagerly on mount and are toggled via CSS
// visibility rather than re-queried on tab switch, since neither is
// expensive enough to justify lazy loading.
const ViewDiscover = (() => {
    let _db   = null;
    let _ac = null;
    let _recShelvesEl = null;
    let _trendShelvesEl = null;

    // ── Recommendations shelf loading ───────────────────────────────────────────

    function _loadRecommendations() {
        const shelves = computeRecommendationShelves(_db);
        renderShelfPage(_recShelvesEl, 'recSubtitle', shelves, 'shelf', 'shelves');
    }

    // ── Trends shelf loaders ─────────────────────────────────────────────────────

    function _cache(key) {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : [];
    }

    function _trendCardsFrom(key) {
        return _cache(key).map(r => ({
            id: r.release_id, title: r.title, art: r.art_url, artist: r.artist,
            year: r.release_year, spotifyId: r.spotify_id,
        }));
    }

    function _loadTrends() {
        const shelves = [
            [
                'Hyper-Fixation Phases',
                'Albums where at least 80% of all-time plays landed inside a single 14-day window.',
                _trendCardsFrom('trendsHyperFixation'),
            ],
            [
                'Burnout Trajectory',
                'Albums with a burst of plays that dominated their total and were the last thing ever played on them.',
                _trendCardsFrom('trendsBurnout'),
            ],
            [
                'Sequential Album Loop',
                'Albums played straight through in track order, start to finish, with no long gaps between tracks.',
                _trendCardsFrom('trendsSequentialLoop'),
            ],
            [
                'Post-Concert Spike',
                "Albums that got a surge of plays in the week after Aswin saw that artist live.",
                _trendCardsFrom('trendsPostConcertSpike'),
            ],
            [
                'One-Hit Wonders',
                "Artists where a single track accounts for at least 90% of all their plays.",
                _trendCardsFrom('trendsOneHitWonder'),
            ],
        ];

        renderShelfPage(_trendShelvesEl, 'trendSubtitle', shelves, 'trend');
    }

    // ── Tabs ─────────────────────────────────────────────────────────────────────

    function _showTab(tab) {
        const showRec = tab === 'recommendations';
        document.getElementById('recPane').hidden = !showRec;
        document.getElementById('trendPane').hidden = showRec;
    }

    // ── Public API ─────────────────────────────────────────────────────────────

    function mount(container, db) {
        _db   = db;
        setPageTitle('Discover');

        container.innerHTML = `
            <header class="rec-header">
                <h1>Discover</h1>
                <div class="control-block" style="margin:0.5rem auto 0">
                    <div class="sort-controls">
                        <button class="sort-btn active" data-tab="recommendations">Recommendations</button>
                        <button class="sort-btn" data-tab="trends">Trends</button>
                    </div>
                </div>
            </header>
            <div id="recPane">
                <p class="subtitle" id="recSubtitle"></p>
                <div id="recShelves" class="rec-shelves"></div>
            </div>
            <div id="trendPane" hidden>
                <p class="subtitle" id="trendSubtitle"></p>
                <div id="trendShelves" class="rec-shelves"></div>
            </div>
        `;

        _recShelvesEl = document.getElementById('recShelves');
        _trendShelvesEl = document.getElementById('trendShelves');
        _ac = new AbortController();
        _recShelvesEl.addEventListener('click', shelfStreamingOnActivate, { signal: _ac.signal });
        _recShelvesEl.addEventListener('keydown', shelfStreamingOnActivate, { signal: _ac.signal });
        _trendShelvesEl.addEventListener('click', shelfStreamingOnActivate, { signal: _ac.signal });
        _trendShelvesEl.addEventListener('keydown', shelfStreamingOnActivate, { signal: _ac.signal });

        setupToggleGroup('[data-tab]', btn => _showTab(btn.dataset.tab));

        _loadRecommendations();
        _loadTrends();
    }

    function unmount() {
        if (_ac) { _ac.abort(); _ac = null; }
        _db = null;
    }

    return { mount, unmount };
})();
