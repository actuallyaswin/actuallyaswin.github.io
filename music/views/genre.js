const ViewGenre = (() => {
    let _db = null;
    let _genreId = null;

    function mount(container, db, params) {
        _db = db;
        _genreId = params.id;

        if (!_genreId) {
            navigate({ view: 'home' });
            return;
        }

        container.innerHTML = `            <header>
                <nav class="genre-breadcrumb" id="genreBreadcrumb"></nav>
                <h1 id="genreName">Loading...</h1>
                <p class="subtitle" id="genreSubtitle"></p>
                <div class="genre-list" id="genreChildren"></div>
            </header>

            <section>
                <div id="releasesContainer" class="wide-grid">
                    <div class="loading">Loading releases...</div>
                </div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadGenreInfo();
        loadGenreReleases();
    }

    function unmount() {}

    // Ancestors of `id`, grouped into levels (root-most first, immediate
    // parents last) via BFS over the static GENRE_TREE. Each ancestor
    // appears once, at the shallowest depth it's reachable from `id` —
    // GENRE_TREE is a DAG (a genre can have multiple parents), and the
    // `visited` set also guards against a cycle if the data ever had one.
    function _ancestorLevels(id) {
        const levels = [];
        let frontier = [id];
        const visited = new Set([id]);

        while (true) {
            const next = new Set();
            for (const nid of frontier) {
                const node = GENRE_TREE[nid];
                if (!node) continue;
                for (const pid of node.parents) {
                    if (!visited.has(pid)) {
                        visited.add(pid);
                        next.add(pid);
                    }
                }
            }
            if (next.size === 0) break;
            levels.unshift([...next].map(nid => ({ id: nid, name: GENRE_TREE[nid]?.name || 'Unknown' })));
            frontier = [...next];
        }
        return levels;
    }

    function loadGenreBreadcrumb(genreName, safeId) {
        const el = document.getElementById('genreBreadcrumb');
        if (!el) return;

        const home = `<a href="?" class="bc-home"><i data-lucide="home"></i></a>`;
        const sep  = `<i data-lucide="chevron-right" class="bc-sep"></i>`;
        const cur  = `<span class="bc-current">${escapeHtml(genreName)}</span>`;

        const levels = _ancestorLevels(safeId);
        if (!levels.length) {
            // Top-level genre: home > Current
            el.innerHTML = `${home}${sep}${cur}`;
        } else {
            const levelHtml = levels
                .map(level => level
                    .map(g => `<a href="?view=genre&id=${g.id}" class="bc-link">${escapeHtml(g.name)}</a>`)
                    .join(`<span class="bc-dot">·</span>`))
                .join(sep);
            el.innerHTML = `${home}${sep}${levelHtml}${sep}${cur}`;
        }
    }

    function loadChildGenres(safeId) {
        const el = document.getElementById('genreChildren');
        if (!el) return;

        const children = GENRE_TREE[safeId]?.children || [];
        if (!children.length) { el.innerHTML = ''; return; }

        const links = children
            .map(cid => `<a href="?view=genre&id=${cid}" class="genre-tag">${escapeHtml(GENRE_TREE[cid]?.name || 'Unknown')}</a>`)
            .join(', ');
        el.innerHTML = `<strong>Sub-genres:</strong> ${links}`;
    }

    function loadGenreInfo() {
        const safeId = parseInt(_genreId);
        if (isNaN(safeId)) {
            document.getElementById('genreName').textContent = 'Genre not found';
            return;
        }

        const result = _db.exec(`
            SELECT g.name,
                   COUNT(DISTINCT rg.release_id) as release_count,
                   COUNT(l.id) as total_plays
            FROM genres g
            JOIN release_genres rg ON g.aoty_id = rg.aoty_genre_id
            JOIN tracks t ON rg.release_id = t.release_id AND t.hidden = 0
            JOIN listens l ON t.id = l.track_id
            WHERE g.aoty_id = ${safeId}
        `)[0];

        if (!result || result.values.length === 0) {
            document.getElementById('genreName').textContent = 'Genre not found';
            return;
        }

        const [name, releaseCount, totalPlays] = result.values[0];
        document.getElementById('genreName').textContent = name || 'Unknown Genre';
        document.getElementById('genreSubtitle').textContent =
            `${formatNumber(releaseCount)} releases · ${formatNumber(totalPlays)} plays`;
        document.title = `${name} | Aswin Sivaraman`;
        loadGenreBreadcrumb(name || 'Unknown Genre', safeId);
        loadChildGenres(safeId);
    }

    function loadGenreReleases() {
        const safeId = parseInt(_genreId);
        if (isNaN(safeId)) return;

        const result = _db.exec(`
            SELECT * FROM (
                SELECT
                    r.id,
                    r.title,
                    r.release_year,
                    COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                    (SELECT a.name FROM artists a WHERE a.id = r.primary_artist_id) as artist_name,
                    (SELECT COUNT(l2.id)
                     FROM tracks t2
                     LEFT JOIN listens l2 ON t2.id = l2.track_id
                     WHERE t2.release_id = r.id AND t2.hidden = 0) as total_plays,
                    (SELECT CAST(SUM(COALESCE(t2.duration_ms, 0)) / 60000.0 AS INTEGER)
                     FROM tracks t2
                     JOIN listens l2 ON t2.id = l2.track_id
                     WHERE t2.release_id = r.id AND t2.hidden = 0) as total_minutes
                FROM release_genres rg
                JOIN releases r ON rg.release_id = r.id
                JOIN genres g ON rg.aoty_genre_id = g.aoty_id
                WHERE g.aoty_id = ${safeId} AND r.hidden = 0
            ) WHERE total_plays > 0
            ORDER BY total_plays DESC
        `)[0];

        const container = document.getElementById('releasesContainer');
        if (!container) return;
        container.innerHTML = '';

        if (!result || result.values.length === 0) {
            container.innerHTML = '<div class="loading">No releases found</div>';
            return;
        }

        result.values.forEach(([id, title, year, albumArtUrl, artistName, totalPlays, totalMinutes]) => {
            const card = createWideCard({
                href: `?view=release&id=${encodeURIComponent(id)}`,
                imageUrl: albumArtUrl,
                name: title,
                meta: `${escapeHtml(artistName || 'Various Artists')} · ${year || 'Unknown'}`,
                totalListens: totalPlays,
                totalMinutes,
                rounded: false
            });
            container.appendChild(card);
        });

    }

    return { mount, unmount };
})();
