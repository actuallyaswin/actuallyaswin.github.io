// Trends — behavioral listening-pattern shelves, mirroring recommendations.js's
// layout but reading precomputed results from stats_cache instead of running
// SQL live. Session/window clustering (e.g. hyper-fixation phases) is
// computed offline in Python (mdb.py's _stats_trends) during `stats refresh`;
// this view just renders whatever it finds.
const ViewTrends = (() => {
    let _db = null;
    let _ac = null;
    let _shelvesEl = null;

    function _cache(key) {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : [];
    }

    function _cardsFrom(key) {
        return _cache(key).map(r => ({
            id: r.release_id, title: r.title, art: r.art_url, artist: r.artist,
            year: r.release_year, spotifyId: r.spotify_id,
        }));
    }

    function _load() {
        const shelves = [
            [
                'Hyper-Fixation Phases',
                'Albums where at least 80% of all-time plays landed inside a single 14-day window.',
                _cardsFrom('trendsHyperFixation'),
            ],
            [
                'Burnout Trajectory',
                'Albums with a burst of plays that dominated their total and were the last thing ever played on them.',
                _cardsFrom('trendsBurnout'),
            ],
            [
                'Sequential Album Loop',
                'Albums played straight through in track order, start to finish, with no long gaps between tracks.',
                _cardsFrom('trendsSequentialLoop'),
            ],
            [
                'Post-Concert Spike',
                "Albums that got a surge of plays in the week after Aswin saw that artist live.",
                _cardsFrom('trendsPostConcertSpike'),
            ],
            [
                'One-Hit Wonders',
                "Artists where a single track accounts for at least 90% of all their plays.",
                _cardsFrom('trendsOneHitWonder'),
            ],
        ];

        renderShelfPage(_shelvesEl, 'trendSubtitle', shelves, 'trend');
    }

    function mount(container, db) {
        _db = db;
        const { shelvesEl, ac } = shelfPageMount(container, {
            title: 'Trends', shelvesId: 'trendShelves', subtitleId: 'trendSubtitle',
        });
        _shelvesEl = shelvesEl;
        _ac = ac;
        _load();
    }

    function unmount() {
        if (_ac) { _ac.abort(); _ac = null; }
        _db = null;
    }

    return { mount, unmount };
})();
