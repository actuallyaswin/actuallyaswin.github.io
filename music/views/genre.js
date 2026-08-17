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

            <div class="stats-row">
                <section id="genreArtistsSection" hidden>
                    <h2>Top Artists</h2>
                    <div class="lang-list" id="genreArtistsList"></div>
                </section>
                <section id="genreTrendSection" hidden>
                    <h2>Listens by Year</h2>
                    <div class="lang-list" id="genreTrendList"></div>
                </section>
            </div>

            <section>
                <div class="section-header">
                    <h2>Releases</h2>
                </div>
                <div id="releasesContainer" class="wide-grid">
                    ${renderLoading("Loading releases...")}
                </div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadGenreInfo();
        loadGenreArtists();
        loadGenreTrend();
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
        const genres = `<a href="?view=genres" class="bc-link">Genres</a>`;
        const sep  = `<i data-lucide="chevron-right" class="bc-sep"></i>`;
        const cur  = `<span class="bc-current">${escapeHtml(genreName)}</span>`;

        const levels = _ancestorLevels(safeId);
        if (!levels.length) {
            // Top-level genre: home > Genres > Current
            el.innerHTML = `${home}${sep}${genres}${sep}${cur}`;
        } else {
            const levelHtml = levels
                .map(level => level
                    .map(g => `<a href="?view=genre&id=${g.id}" class="bc-link">${escapeHtml(g.name)}</a>`)
                    .join(`<span class="bc-dot">·</span>`))
                .join(sep);
            el.innerHTML = `${home}${sep}${genres}${sep}${levelHtml}${sep}${cur}`;
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

        // LEFT JOIN + GROUP BY, not the aggregate-with-no-GROUP-BY this used
        // to be — that collapsed a genre with zero release_genres rows (a
        // real AOTY genre from the taxonomy sync that just hasn't been heard
        // yet) into a single phantom row with a NULL name, showing "Unknown
        // Genre" instead of the genre's real name with a 0/0 count.
        const result = _db.exec(`
            SELECT g.name,
                   COUNT(DISTINCT rg.release_id) as release_count,
                   COUNT(l.id) as total_plays
            FROM genres g
            LEFT JOIN release_genres rg ON g.aoty_id = rg.aoty_genre_id
            LEFT JOIN tracks t ON rg.release_id = t.release_id AND t.hidden = 0
            LEFT JOIN listens l ON t.id = l.track_id
            WHERE g.aoty_id = ${safeId}
            GROUP BY g.aoty_id
        `)[0];

        if (!result || result.values.length === 0) {
            document.getElementById('genreName').textContent = 'Genre not found';
            return;
        }

        const [name, releaseCount, totalPlays] = result.values[0];
        document.getElementById('genreName').textContent = name || 'Unknown Genre';
        document.getElementById('genreSubtitle').textContent =
            `${formatNumber(releaseCount)} releases · ${formatNumber(totalPlays)} plays`;
        setPageTitle(name);
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
                     WHERE t2.release_id = r.id AND t2.hidden = 0) as total_minutes,
                    r.slug
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
            const nameEl = document.getElementById('genreName');
            const name = (nameEl && nameEl.textContent !== 'Loading...' && nameEl.textContent !== 'Genre not found')
                ? nameEl.textContent : 'this genre';
            container.innerHTML = `<div class="empty-state">
                <i data-lucide="tags" class="app-error-icon"></i>
                <div class="empty-state-title">No releases heard yet for ${escapeHtml(name)}</div>
                <p class="empty-state-hint">This is a real genre in the taxonomy, but nothing tagged with it
                    is in your listening history yet.</p>
            </div>`;
            return;
        }

        result.values.forEach(([id, title, year, albumArtUrl, artistName, totalPlays, totalMinutes, slug]) => {
            const card = createWideCard({
                href: releaseHref(id, slug),
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

    function _breakdownBarsHtml(items, formatLabel) {
        const total = items.reduce((s, it) => s + it.n, 0);
        const max   = Math.max(...items.map(it => it.n), 1);
        return items.map(({ label, n, href }) => {
            const pct     = total ? ((n / total) * 100).toFixed(1) : '0.0';
            const opacity = (0.35 + 0.65 * (n / max)).toFixed(2);
            const labelHtml = formatLabel ? formatLabel(label) : escapeHtml(String(label));
            const codeHtml = href
                ? `<a href="${href}" class="lang-code lang-code-link">${labelHtml}</a>`
                : `<span class="lang-code">${labelHtml}</span>`;
            return `
                <div class="lang-row lang-row-static">
                    ${codeHtml}
                    <div class="lang-bar-track">
                        <div class="lang-bar-fill" style="width:${pct}%;background:var(--primary);opacity:${opacity}"></div>
                    </div>
                    <span class="lang-count">${formatNumber(n)}</span>
                    <span class="lang-pct">${pct}%</span>
                    <span></span>
                </div>`;
        }).join('');
    }

    function loadGenreArtists() {
        const safeId = parseInt(_genreId);
        if (isNaN(safeId)) return;

        const result = _db.exec(`
            SELECT a.id, a.name, COUNT(l.id) as plays, a.slug
            FROM release_genres rg
            JOIN releases r ON rg.release_id = r.id
            JOIN artists a ON a.id = r.primary_artist_id
            JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
            JOIN listens l ON l.track_id = t.id
            WHERE rg.aoty_genre_id = ${safeId} AND r.hidden = 0 AND (a.hidden IS NULL OR a.hidden = 0)
            GROUP BY a.id
            ORDER BY plays DESC
            LIMIT 10
        `)[0];

        const section = document.getElementById('genreArtistsSection');
        const list = document.getElementById('genreArtistsList');
        if (!section || !list || !result || result.values.length === 0) return;

        const items = result.values.map(([id, name, plays, slug]) => ({
            label: name, n: plays, href: artistHref(id, slug),
        }));
        list.innerHTML = _breakdownBarsHtml(items);
        section.removeAttribute('hidden');
    }

    function loadGenreTrend() {
        const safeId = parseInt(_genreId);
        if (isNaN(safeId)) return;

        const result = _db.exec(`
            SELECT l.year, COUNT(l.id) as plays
            FROM release_genres rg
            JOIN releases r ON rg.release_id = r.id
            JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
            JOIN listens l ON l.track_id = t.id
            WHERE rg.aoty_genre_id = ${safeId} AND r.hidden = 0
            GROUP BY l.year
            ORDER BY l.year
        `)[0];

        const section = document.getElementById('genreTrendSection');
        const list = document.getElementById('genreTrendList');
        if (!section || !list || !result || result.values.length === 0) return;

        // Most-recent-first reads better as a trend list than chronological.
        const items = result.values
            .map(([year, plays]) => ({ label: year, n: plays }))
            .reverse();
        list.innerHTML = _breakdownBarsHtml(items);
        section.removeAttribute('hidden');
    }

    return { mount, unmount };
})();
