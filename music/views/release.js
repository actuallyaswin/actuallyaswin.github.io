const ViewRelease = (() => {
    let _db = null;
    let _releaseId = null;
    let _primaryArtistId = null;
    let _releaseType = null;
    let _typeSecondary = null;
    let _artistsWithReleases = new Set();
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null, monthlyRaw: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    let _themeObserver = null;

    // Set to true to re-enable the Chart.js listening history chart
    const CHART_ENABLED = false;
    const AUDIO_FEATURES_ENABLED = true;
    const HIDE_DUPES = true;

    const _SVG_CHEVRON = `<svg class="pill-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="6 9 12 15 18 9"/></svg>`;

    // release_soundtrack_meta.source_type — mirrors mdb_cli.py's _SOURCE_TYPES.
    const SOUNDTRACK_MEDIUM_LABELS = {
        video_game: 'Video Game', film: 'Film', tv_series: 'TV Series',
        musical: 'Musical', podcast: 'Podcast', other: 'Other',
    };

    const _regionNames = (() => {
        try { return new Intl.DisplayNames(['en'], { type: 'region' }); }
        catch { return null; }
    })();
    const _languageNames = (() => {
        try { return new Intl.DisplayNames(['en'], { type: 'language' }); }
        catch { return null; }
    })();
    function _regionName(code) { return _regionNames?.of(String(code).toUpperCase()) || code; }
    function _languageName(code) { return _languageNames?.of(String(code).toLowerCase()) || code; }

    // Edition-tag-info (ETI) suffixes — "(Remastered 2022)", "(Deluxe Edition)", etc. —
    // stripped from display everywhere a title is shown (release header, tracklist).
    const _ETI_CONTENT = 'original\\s+(?:motion\\s+picture\\s+)?(?:soundtrack|score)(?:\\s+from\\s+[^()\\[\\]]+)?|music\\s+from\\s+(?:the\\s+)?(?:original\\s+)?(?:motion\\s+picture|film|movie)(?:\\s+soundtrack)?|soundtrack\\s+from\\s+(?:the\\s+)?[^()\\[\\]]*|(?:original\\s+)?(?:game|video\\s+game)\\s+soundtrack|deluxe(?:\\s+edition)?|anniversary\\s+edition|(?:\\d{4}\\s+)?remaster(?:ed)?(?:\\s+\\d{4})?|special\\s+edition|expanded\\s+edition|complete\\s+edition|soundtrack';
    const _ETI_RE = new RegExp(
        `(?:(\\s*:\\s*|\\s+[\\-–—]\\s*|\\s+)(?:${_ETI_CONTENT})|\\s*\\((?:${_ETI_CONTENT})\\)|\\s*\\[(?:${_ETI_CONTENT})\\])\\s*$`,
        'i'
    );
    // Splits a title into {base, eti} — eti is null when no edition-tag suffix matches.
    function _splitEti(title) {
        const m = (title || '').match(_ETI_RE);
        return m ? { base: title.slice(0, m.index), eti: m[0].trim() } : { base: title, eti: null };
    }

    function mount(container, db, params) {
        _db = db;
        _primaryArtistId = null;
        _releaseType = null;
        _artistsWithReleases = new Set();
        _currentChart = null;
        _chartData = { monthly: null, yearly: null, monthlyRaw: null };

        // Accept either a slug or a raw ULID in either param — resolve to the
        // canonical id up front so every query below (all keyed on the real
        // id) keeps working unchanged, then canonicalize the address bar to
        // the slug-based URL for clean copy-paste.
        const key = params.slug || params.id;
        if (!key) {
            navigate({ view: 'home' });
            return;
        }
        const safeKey = String(key).replace(/'/g, "''");
        const resolved = db.exec(`SELECT id, slug FROM releases WHERE slug = '${safeKey}' OR id = '${safeKey}' LIMIT 1`)[0];
        if (resolved) {
            const [realId, slug] = resolved.values[0];
            _releaseId = realId;
            if (slug && params.id && !params.slug) {
                history.replaceState({ view: 'release', slug }, '', `?view=release&slug=${encodeURIComponent(slug)}`);
            }
        } else {
            // No match — keep the original key so downstream queries fail the
            // same way they always did, surfacing "Release not found".
            _releaseId = key;
        }

        container.innerHTML = `            <nav class="genre-breadcrumb" id="releaseBreadcrumb">
                <a href="?" class="bc-home"><i data-lucide="home"></i></a>
                <i data-lucide="chevron-right" class="bc-sep"></i>
                <span class="bc-current" id="releaseBreadcrumbName">Loading…</span>
            </nav>

            <header id="releaseHeader" class="entity-header entity-header-grid">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="albumArt">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <rect width="100" height="100" fill="#20232c"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#767c85">♪</text>
                        </svg>
                    </div>
                </div>
                <div class="artist-info-container">
                    <h1 id="releaseName">Loading...</h1>
                    <p class="release-artist">
                        <span id="releaseArtist"></span>
                    </p>
                    <dl id="releaseStatsTable" class="release-stats-table" hidden></dl>
                    <p id="releaseAka" class="release-aka" hidden></p>
                    <p id="releaseGenres" class="genre-list" style="margin-top:0.5rem"></p>
                </div>
                <nav id="releaseLinkPills" class="release-link-pills"></nav>
            </header>

            ${CHART_ENABLED ? `
            <section class="chart-container">
                <div class="chart-header">
                    <h3 class="chart-title">Listening History Over Time</h3>
                    <div class="chart-controls">
                        <div class="control-group">
                            <button class="control-btn${_chartState.granularity === 'monthly' ? ' active' : ''}" data-granularity="monthly">Monthly</button>
                            <button class="control-btn${_chartState.granularity === 'yearly' ? ' active' : ''}" data-granularity="yearly">Yearly</button>
                        </div>
                        <div class="control-group">
                            <button class="control-btn${_chartState.type === 'distribution' ? ' active' : ''}" data-type="distribution">Distribution</button>
                            <button class="control-btn${_chartState.type === 'cumulative' ? ' active' : ''}" data-type="cumulative">Cumulative</button>
                        </div>
                    </div>
                </div>
                <canvas id="historyChart"></canvas>
            </section>
            ` : ''}

            <div class="stats-row">
                <section class="tracks-section">
                    <h2>Tracks</h2>
                    <div class="tracklist" id="trackList">
                        ${renderLoading("Loading tracks...")}
                    </div>
                    <div id="variantsSection"></div>
                </section>

                <section class="pulse-section" id="pulseSection">
                    <h2>Timeline</h2>
                    <div class="pulse-rows" id="pulseRows"></div>
                </section>
            </div>

            <div id="editorialSection"></div>


            <div id="sourcesSection"></div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadReleaseInfo();
        loadReleaseAliases();
        loadEditorialNotes();
        loadTracks();
        loadListeningHistory();
        loadVariants();
        loadSources();
        loadCanonicalBacklink();
        loadListRankings();
        if (CHART_ENABLED) setupChartControls();
    }

    function unmount() {
        if (_currentChart) {
            _currentChart.destroy();
            _currentChart = null;
        }
        if (_themeObserver) {
            _themeObserver.disconnect();
            _themeObserver = null;
        }
    }

    function loadReleaseInfo() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                r.title,
                r.release_date,
                r.type,
                r.type_secondary,
                r.album_art_url,
                r.stat_total_plays,
                r.spotify_id,
                r.release_group_mbid,
                r.mbid,
                r.aoty_url,
                r.aoty_id,
                r.aoty_score_critic,
                r.aoty_score_user,
                r.aoty_ratings_critic,
                r.aoty_ratings_user,
                r.primary_artist_id,
                r.credited_as,
                r.label,
                r.stat_album_total_ms,
                r.stat_first_listen_ts,
                r.stat_last_listen_ts,
                r.stat_drift_days,
                r.apple_music_id
            FROM releases r
            WHERE r.id = '${safeId}'
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('releaseName');
            if (el) el.textContent = 'Release not found';
            return;
        }

        const [title, releaseDate, type, typeSecondary, albumArtUrl,
               totalPlaysRaw,
               spotifyId, releaseGroupMbid, mbid, aotyUrl, aotyId,
               aotyScoreCritic, aotyScoreUser, aotyRatingsCritic, aotyRatingsUser,
               primaryArtistId, creditedAs, label, albumTotalMs, firstListenTs, lastListenTs, driftDays,
               appleMusicIdCol] = result.values[0];
        const totalPlays  = totalPlaysRaw || 0;

        // Group edit/length variants (radio edit, extended mix, ...) of the
        // same recording — hearing any one counts as having heard the song.
        const trackRowsResult = _db.exec(`
            SELECT t.title,
                   EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id) as heard
            FROM tracks t
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
              AND t.variant_section IS NULL
              AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
        `)[0];
        const songGroups = new Map();
        if (trackRowsResult) {
            trackRowsResult.values.forEach(([trackTitle, heard]) => {
                const key = sameSongKey(trackTitle);
                songGroups.set(key, songGroups.get(key) || !!heard);
            });
        }
        const totalTracksInDb = songGroups.size;
        const tracksHeard     = [...songGroups.values()].filter(Boolean).length;


        const extLinks = new Map();
        try {
            const linksResult = _db.exec(`
                SELECT service, link_value
                FROM external_links
                WHERE entity_type = 1 AND entity_id = '${safeId}'
            `)[0];
            if (linksResult) linksResult.values.forEach(([svc, val]) => extLinks.set(svc, val));
        } catch (_) {}
        const wikiPageId   = extLinks.get(0) || null;   // EL_SVC_WIKIPEDIA

        let vgSeries = null, vgPlatform = null, vgRegion = null, vgLanguage = null, soundtrackMedium = null;
        if (typeSecondary === 'soundtrack') {
            try {
                const smResult = _db.exec(`
                    SELECT source_type, series, platform, industry_region, original_language
                    FROM release_soundtrack_meta
                    WHERE release_id = '${safeId}'
                `)[0];
                if (smResult && smResult.values.length) {
                    let sourceType;
                    [sourceType, vgSeries, vgPlatform, vgRegion, vgLanguage] = smResult.values[0];
                    soundtrackMedium = sourceType;
                    // series/platform only ever apply to video_game rows
                    if (sourceType !== 'video_game') { vgSeries = null; vgPlatform = null; }
                }
            } catch (_) {}
        }
        // Spotify: prefer the column (canonical owner); fall back to external_links
        // service 2 for releases that are variants of a Spotify-listed edition.
        const effectiveSpotifyId = spotifyId || extLinks.get(2) || null;
        const typeLabel = [type, typeSecondary].filter(Boolean).join(' / ');

        _primaryArtistId = primaryArtistId || null;
        _releaseType     = type || null;
        _typeSecondary   = typeSecondary || null;

        const artistResult = _db.exec(`
            SELECT DISTINCT a.name, a.id, a.slug
            FROM releases r
            JOIN artists a ON (
                a.id = r.primary_artist_id
                OR a.id IN (SELECT artist_id FROM release_artists WHERE release_id = r.id AND role = 'main')
            )
            WHERE r.id = '${safeId}' AND a.id IS NOT NULL
            ORDER BY (r.primary_artist_id = a.id) DESC, a.name
        `)[0];
        const artistSlugById = new Map();
        if (artistResult) artistResult.values.forEach(([, id, slug]) => artistSlugById.set(id, slug));

        // Inline transliteration for non-Latin release titles + ETI stripping
        const isNonLatin = s => /[^ -]/.test(s);
        const titleAliasResult = _db.exec(`
            SELECT alias FROM release_aliases
            WHERE release_id = '${safeId}'
              AND alias_norm != lower('${(title || '').replace(/'/g, "''")}')
              AND language IS NOT NULL
            ORDER BY is_definitive DESC LIMIT 1
        `)[0];
        const titleAlias = titleAliasResult && titleAliasResult.values[0]?.[0];

        const { base: baseTitle, eti: etiPart } = _splitEti(title);

        const nameEl = document.getElementById('releaseName');
        if (isNonLatin(baseTitle) && titleAlias) {
            nameEl.innerHTML = `${escapeHtml(baseTitle)} <span class="artist-romanized">(${escapeHtml(titleAlias)})</span>${etiPart ? ` <span class="tracklist-eti">${escapeHtml(etiPart)}</span>` : ''}`;
        } else {
            nameEl.innerHTML = escapeHtml(baseTitle || 'Unknown Release') + (etiPart ? ` <span class="tracklist-eti">${escapeHtml(etiPart)}</span>` : '');
        }
        // artistResult is ordered primary-artist-first.
        const titleEdition = [baseTitle || title || 'Release', etiPart].filter(Boolean).join(' ');
        const titleArtist  = artistResult?.values?.length
            ? (creditedAs && artistResult.values[0][1] === primaryArtistId ? creditedAs : artistResult.values[0][0])
            : 'Various Artists';
        setPageTitle(`“${titleEdition}” by ${titleArtist}`);

        const breadcrumbEl = document.getElementById('releaseBreadcrumb');
        if (breadcrumbEl) {
            const home = `<a href="?" class="bc-home"><i data-lucide="home"></i></a>`;
            const sep  = `<i data-lucide="chevron-right" class="bc-sep"></i>`;
            const cur  = `<span class="bc-current">${escapeHtml(baseTitle || title || 'Release')}</span>`;
            const artistCrumb = primaryArtistId
                ? `<a href="${artistHref(primaryArtistId, artistSlugById.get(primaryArtistId))}" class="bc-link">${escapeHtml(titleArtist)}</a>`
                : `<span class="bc-link">${escapeHtml(titleArtist)}</span>`;
            breadcrumbEl.innerHTML = `${home}${sep}${artistCrumb}${sep}${cur}`;
        }

        const statsEl = document.getElementById('releaseStatsTable');
        if (statsEl) {
            const rows = [];
            // Ordered so each visual row (two cells, paired sequentially
            // below) groups related facts: classification (Type+Medium,
            // Series+Platform), then origin/logistics (Region+Language,
            // Released+Label), then content facts, then personal listening
            // stats. Any row skipped by a falsy check just shifts everything
            // after it — degrades cleanly for non-soundtrack releases, which
            // never populate Medium/Series/Platform/Region/Language at all.
            if (typeLabel)       rows.push(['Type',       escapeHtml(typeLabel.toUpperCase())]);
            if (soundtrackMedium && SOUNDTRACK_MEDIUM_LABELS[soundtrackMedium]) {
                rows.push(['Medium', escapeHtml(SOUNDTRACK_MEDIUM_LABELS[soundtrackMedium])]);
            }
            // Video-game series/platform, styled as the same clickable
            // .stat-genre-tag pill the Genre/Lists rows below already use —
            // consistent with the rest of this table (every other value
            // here is plain text or a tag, never an outlined badge). No
            // "Series" row at all when the game has no real series — a
            // generic "Video Game" tag would just restate the Type row.
            if (vgSeries) {
                const seriesTag = `<a href="?view=soundtracks&group=series&series=${encodeURIComponent(vgSeries)}" class="stat-genre-tag is-primary">${escapeHtml(vgSeries)}</a>`;
                rows.push(['Series', seriesTag]);
            }
            if (vgPlatform) {
                const platformIcon = platformIconMarkup(vgPlatform);
                const iconHtml = platformIcon ? `<span class="vgst-group-icon">${platformIcon}</span>` : '';
                const platformTag = `<a href="?view=soundtracks&group=platform&platform=${encodeURIComponent(vgPlatform)}" class="stat-genre-tag stat-genre-tag-icon">${iconHtml}${escapeHtml(platformLabel(vgPlatform))}</a>`;
                rows.push(['Platform', platformTag]);
            }
            if (vgRegion)         rows.push(['Region',     escapeHtml(_regionName(vgRegion))]);
            if (vgLanguage)       rows.push(['Language',   escapeHtml(_languageName(vgLanguage))]);
            if (releaseDate)      rows.push(['Released',   escapeHtml(_formatReleaseDate(releaseDate))]);
            if (label)            rows.push(['Label',      escapeHtml(label)]);
            if (albumTotalMs > 0) rows.push(['Length',     escapeHtml(_formatAlbumDuration(albumTotalMs))]);
            if (totalTracksInDb > 0) rows.push(['Tracks',  `${tracksHeard} / ${totalTracksInDb}`]);
            if (totalPlays > 0)   rows.push(['Listens',    formatNumber(totalPlays)]);
            if (firstListenTs)    rows.push(['First heard',escapeHtml(_fmtTs(firstListenTs))]);
            if (lastListenTs && lastListenTs !== firstListenTs)
                                  rows.push(['Last played',escapeHtml(formatRelativeTime(lastListenTs))]);
            if (driftDays !== null && totalPlays >= 3) rows.push(['Drift', escapeHtml(driftDays < 1
                ? 'Same-day repeats'
                : `${driftDays.toFixed(1)} days between plays`)]);
            if (rows.length > 0) {
                let html = '';
                for (let i = 0; i < rows.length; i += 2) {
                    const makeCell = ([lbl, val]) =>
                        `<div class="rst-row"><dt class="rst-label">${lbl}</dt><dd class="rst-value">${val}</dd></div>`;
                    html += `<div class="rst-pair">${makeCell(rows[i])}${rows[i + 1] ? makeCell(rows[i + 1]) : ''}</div>`;
                }
                statsEl.innerHTML = html;
                statsEl.removeAttribute('hidden');
            }
        }

        if (albumArtUrl) {
            const albumArtDiv = document.getElementById('albumArt');
            albumArtDiv.style.backgroundImage = `url(${albumArtUrl})`;
            albumArtDiv.style.backgroundSize = 'cover';
            albumArtDiv.style.backgroundPosition = 'center';
            albumArtDiv.innerHTML = '';
            albumArtDiv.classList.add('has-art');
            albumArtDiv.addEventListener('click', () => _openArtModal(albumArtUrl));
        }

        const artistSpan = document.getElementById('releaseArtist');
        if (!artistResult || artistResult.values.length === 0) {
            artistSpan.textContent = 'Various Artists';
        } else {
            const allArtists = artistResult.values; // [[name, id], ...]
            // Seed _artistsWithReleases with any header artist that has a primary release.
            // loadTracks() will extend this set for track-level credits after it runs.
            const headerIdList = allArtists.map(([, id]) => `'${id}'`).join(',');
            const hrResult = _db.exec(`
                SELECT DISTINCT primary_artist_id FROM releases
                WHERE hidden = 0 AND primary_artist_id IN (${headerIdList})
            `)[0];
            if (hrResult) hrResult.values.forEach(([id]) => _artistsWithReleases.add(id));

            const makeLink = (n, i) =>
                _artistsWithReleases.has(i)
                    ? `<a href="${artistHref(i, artistSlugById.get(i))}" class="release-artist-link">${escapeHtml(n)}</a>`
                    : escapeHtml(n);
            // credited_as shows the name this release was actually put out under
            // (e.g. a pseudonym) while still linking to the canonical artist page.
            const displayName = (n, i) => (creditedAs && i === primaryArtistId) ? creditedAs : n;

            let html;
            if (allArtists.length === 1) {
                html = makeLink(displayName(allArtists[0][0], allArtists[0][1]), allArtists[0][1]);
            } else {
                const idList = allArtists.map(([, id]) => `'${id}'`).join(',');
                const suppressedIds = new Set();

                // Suppress past_name aliases: if artist A is a past_name alias of artist B
                // and both appear on this release, hide A and keep B
                const aliasDedup = _db.exec(`
                    SELECT a_alias.id
                    FROM artists a_alias
                    JOIN artist_aliases aa
                        ON lower(aa.alias) = lower(a_alias.name) AND aa.alias_type = 'past_name'
                    WHERE a_alias.id IN (${idList})
                      AND aa.artist_id IN (${idList})
                `)[0];
                if (aliasDedup) aliasDedup.values.forEach(([id]) => suppressedIds.add(id));

                // Suppress members of supergroups: if group G has members M1, M2 all on this
                // release, render "G (M1 and M2)" and hide M1/M2 as standalone entries
                const groupMemberMap = new Map(); // groupId -> [{id, name}, ...]
                const memberResult = _db.exec(`
                    SELECT am.group_artist_id, am.member_artist_id, a.name
                    FROM artist_members am
                    JOIN artists a ON a.id = am.member_artist_id
                    WHERE am.group_artist_id IN (${idList})
                      AND am.member_artist_id IN (${idList})
                    ORDER BY am.sort_order
                `)[0];
                if (memberResult) {
                    memberResult.values.forEach(([groupId, memberId, memberName]) => {
                        if (!groupMemberMap.has(groupId)) groupMemberMap.set(groupId, []);
                        groupMemberMap.get(groupId).push({ id: memberId, name: memberName });
                        suppressedIds.add(memberId);
                    });
                }

                const joinLinks = arr =>
                    arr.length === 1 ? arr[0]
                    : arr.length === 2 ? `${arr[0]} and ${arr[1]}`
                    : `${arr.slice(0, -1).join(', ')}, and ${arr[arr.length - 1]}`;

                const parts = [];
                for (const [name, id] of allArtists) {
                    if (suppressedIds.has(id)) continue;
                    if (groupMemberMap.has(id)) {
                        const memberLinks = groupMemberMap.get(id).map(m => makeLink(m.name, m.id));
                        parts.push(`${makeLink(displayName(name, id), id)} (${joinLinks(memberLinks)})`);
                    } else {
                        parts.push(makeLink(displayName(name, id), id));
                    }
                }

                html = parts.length === 0 ? 'Various Artists' : joinLinks(parts);
            }
            artistSpan.innerHTML = html;
        }

        const genreResult = _db.exec(`
            SELECT g.aoty_id, g.name, rg.is_primary
            FROM release_genres rg
            JOIN genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE rg.release_id = '${safeId}'
            ORDER BY rg.is_primary DESC, g.name
        `)[0];

        const genresEl = document.getElementById('releaseGenres');
        if (genreResult && genreResult.values.length > 0) {
            // Standalone genre list (mobile / non-grid fallback)
            if (genresEl) genresEl.innerHTML = renderGenreTags(genreResult.values);

            // Append genre row to stats table (desktop grid — CSS hides the standalone list)
            const statsEl2 = document.getElementById('releaseStatsTable');
            if (statsEl2) {
                const pills = genreResult.values.map(([aotyId, name, isPrimary]) =>
                    `<a href="?view=genre&id=${encodeURIComponent(aotyId)}" class="stat-genre-tag${isPrimary ? ' is-primary' : ''}">${escapeHtml(name)}</a>`
                ).join('');
                statsEl2.insertAdjacentHTML('beforeend',
                    `<div class="rst-row rst-genres-row"><span class="rst-label">Genre</span><span class="rst-value">${pills}</span></div>`
                );
                statsEl2.removeAttribute('hidden');
            }
        }

        const pillsEl = document.getElementById('releaseLinkPills');
        if (pillsEl) {
            // Query variant Spotify IDs from release_service_links (new model)
            const varSpotResult = _db.exec(`
                SELECT service_id, variant_label
                FROM release_service_links
                WHERE release_id = '${safeId}'
                  AND service = 2
                  AND variant_label IS NOT NULL
                ORDER BY id
            `)[0];
            const variantSpotify = varSpotResult ? varSpotResult.values : [];

            const resolvedAotyUrl = aotyUrl || (aotyId ? `https://www.albumoftheyear.org/album/${aotyId}/` : null);
            const amId = appleMusicIdCol || extLinks.get(3) || null;

            let html = '';

            // ── Streaming ───────────────────────────────────────────────
            if (effectiveSpotifyId) {
                if (variantSpotify.length > 0) {
                    const groupId = 'sp-pill-group';
                    html += `<button class="release-link-pill pill-spotify pill-collapsible" data-group="${groupId}">` +
                        `<span class="pill-icon"><span class="pill-mask"></span></span>` +
                        `<span class="pill-text"><span class="pill-service-name">Spotify</span>` +
                        `<span class="pill-sub">${variantSpotify.length + 1} versions</span></span>` +
                        `${_SVG_CHEVRON}</button>` +
                        `<div class="pill-variant-group pill-spotify" id="${groupId}">` +
                        `<a href="https://open.spotify.com/album/${effectiveSpotifyId}" target="_blank" rel="noopener" class="pill-variant-row pill-spotify">` +
                        `<span class="pill-variant-name">Canonical pressing</span>${_PILL_SVG_EXT}</a>`;
                    for (const [vspId, vLabel] of variantSpotify) {
                        html += `<a href="https://open.spotify.com/album/${vspId}" target="_blank" rel="noopener" class="pill-variant-row pill-spotify">` +
                            `<span class="pill-variant-name">${escapeHtml(vLabel)}</span>${_PILL_SVG_EXT}</a>`;
                    }
                    html += `</div>`;
                } else {
                    html += renderLinkPill('spotify', `https://open.spotify.com/album/${effectiveSpotifyId}`, 'Spotify');
                }
            }
            if (amId)
                html += renderLinkPill('apple', `https://music.apple.com/album/${amId}`, 'Apple Music');
            if (extLinks.get(4))
                html += renderLinkPill('deezer', `https://www.deezer.com/album/${extLinks.get(4)}`, 'Deezer');
            if (extLinks.get(5))
                html += renderLinkPill('tidal', `https://tidal.com/browse/album/${extLinks.get(5)}`, 'Tidal');
            if (extLinks.get(7))
                html += renderLinkPill('beatport', `https://www.beatport.com/release/-/${extLinks.get(7)}`, 'Beatport');

            // ── Purchase ─────────────────────────────────────────────────
            if (extLinks.get(6))
                html += renderLinkPill('bandcamp', extLinks.get(6), 'Bandcamp');

            // ── Metadata / editorial ─────────────────────────────────────
            if (releaseGroupMbid)
                html += renderLinkPill('musicbrainz', `https://musicbrainz.org/release-group/${releaseGroupMbid}`, 'MusicBrainz');
            else if (mbid)
                html += renderLinkPill('musicbrainz', `https://musicbrainz.org/release/${mbid}`, 'MusicBrainz');
            if (wikiPageId)
                html += renderLinkPill('wikipedia', wikipediaHref(wikiPageId), 'Wikipedia');
            if (resolvedAotyUrl) {
                const aotyScore = aotyScoreCritic != null && aotyScoreUser != null
                    ? `C ${aotyScoreCritic} · U ${Math.round(aotyScoreUser)}`
                    : aotyScoreCritic != null ? `Critic ${aotyScoreCritic}`
                    : aotyScoreUser  != null ? `User ${Math.round(aotyScoreUser)}`
                    : null;
                html += renderLinkPill('aoty', resolvedAotyUrl, 'AOTY', aotyScore);
            }
            if (extLinks.get(8))
                html += renderLinkPill('genius', `https://genius.com/artists/${extLinks.get(8)}`, 'Genius');
            if (extLinks.get(10))
                html += renderLinkPill('discogs', `https://www.discogs.com/release/${extLinks.get(10)}`, 'Discogs');

            if (html) {
                pillsEl.innerHTML = html;
                // Wire up collapsible Spotify group
                pillsEl.querySelectorAll('.pill-collapsible').forEach(btn => {
                    btn.addEventListener('click', () => {
                        const group = document.getElementById(btn.dataset.group);
                        if (!group) return;
                        const open = group.classList.toggle('open');
                        btn.classList.toggle('expanded', open);
                    });
                });
            } else {
                pillsEl.style.display = 'none';
            }
        }

        lucide.createIcons();
    }

    function _formatReleaseDate(dateStr) {
        if (!dateStr) return '';
        const parts = dateStr.split('-');
        if (parts.length === 3 && !(parts[1] === '01' && parts[2] === '01')) {
            const d = new Date(dateStr + 'T00:00:00');
            if (!isNaN(d)) return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        } else if (parts.length === 2) {
            const d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1, 1);
            if (!isNaN(d)) return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
        }
        return parts[0]; // fallback to year
    }

    function _formatAlbumDuration(ms) {
        if (!ms) return '';
        const totalMin = Math.floor(ms / 60000);
        if (totalMin < 60) return `${totalMin} min`;
        const h = Math.floor(totalMin / 60);
        const m = totalMin % 60;
        return m > 0 ? `${h} hr ${m} min` : `${h} hr`;
    }

    function _fmtTs(ts) {
        if (!ts) return '';
        const d = new Date(ts * 1000);
        if (isNaN(d)) return '';
        return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
    }

    function loadReleaseAliases() {
        const safeId = _releaseId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT alias, is_definitive, language
            FROM release_aliases
            WHERE release_id = '${safeId}'
            ORDER BY is_definitive DESC, alias
        `)[0];

        const el = document.getElementById('releaseAka');
        if (!el || !result || result.values.length === 0) return;

        // Exclude aliases already shown inline as the title transliteration
        const titleEl = document.getElementById('releaseName');
        const inlineAlias = titleEl?.querySelector('.artist-romanized')?.textContent?.replace(/[()]/g, '').trim();

        const parts = result.values
            .filter(([alias]) => alias !== inlineAlias)
            .map(([alias, isDef]) => isDef ? `<strong>${escapeHtml(alias)}</strong>` : escapeHtml(alias));

        if (parts.length === 0) return;
        el.innerHTML = `Also known as ${parts.join(', ')}`;
        el.removeAttribute('hidden');
    }

    function formatDuration(ms) {
        if (!ms) return '?:??';
        const totalSec = Math.floor(ms / 1000);
        const min = Math.floor(totalSec / 60);
        const sec = String(totalSec % 60).padStart(2, '0');
        return `${min}:${sec}`;
    }

    // ── Track matching ──────────────────────────────────────────────────────────

    function _normTitle(s) {
        return (s || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
    }

    // Render a track title, dimming the trailing edition/mix tag.
    // When mix_name is present, split on the last '(' so tracks.title and
    // tracks.mix_name casing differences don't matter. Otherwise fall back to
    // the shared ETI regex so bare edition suffixes ("(Remastered 2022)") are
    // still dimmed even when no mix_name was captured for the track.
    function _renderTrackName(title, mixName) {
        if (mixName) {
            const split = title.lastIndexOf(' (');
            if (split !== -1) {
                const base = title.slice(0, split);
                const eti  = title.slice(split);          // includes the leading space
                return `${escapeHtml(base)}<span class="tracklist-eti">${escapeHtml(eti)}</span>`;
            }
        }
        const { base, eti } = _splitEti(title);
        if (!eti) return escapeHtml(title);
        return `${escapeHtml(base)}<span class="tracklist-eti"> ${escapeHtml(eti)}</span>`;
    }

    // ── Shared tracklist renderer ───────────────────────────────────────────────

    function _renderTracklist(container, tracks, showPlayCounts, opts = {}) {
        const { artistsByTrack = new Map(), primaryArtistId = null, artistsWithReleases = new Set(), aliasesByTrack = new Map(), alsoOnByTrack = new Map() } = opts;
        container.innerHTML = '';
        if (!tracks.length) {
            container.innerHTML = '<div class="tracklist-empty">No tracks found</div>';
            return;
        }

        // Only render BPM column when at least one track has data
        const showBpm = tracks.some(t => t.tempoBpm != null);

        if (showPlayCounts) {
            const colHeader = document.createElement('div');
            colHeader.className = 'tracklist-col-header';
            colHeader.innerHTML = `
                <span class="tracklist-num"></span>
                <div class="tracklist-info"></div>
                ${showBpm ? `<div class="tracklist-bpm" title="BPM (Beats per Minute)"><i data-lucide="metronome"></i></div>` : ''}
                <div class="tracklist-plays" title="Your play count for this track"><i data-lucide="headphones"></i></div>
            `;
            container.appendChild(colHeader);
        }

        const maxDisc   = Math.max(...tracks.map(t => t.discNumber || 1));
        const multiDisc = maxDisc > 1;
        let currentDisc = null;

        tracks.forEach((t, i) => {
            const disc = t.discNumber || 1;
            if (multiDisc && disc !== currentDisc) {
                currentDisc = disc;
                const header = document.createElement('div');
                header.className = 'tracklist-disc-header';
                header.textContent = `Disc ${disc}`;
                container.appendChild(header);
            }
            const row        = document.createElement('div');
            const displayNum = t.trackNumber != null ? t.trackNumber : (i + 1);
            const playsCell  = showPlayCounts
                ? `<div class="tracklist-plays">${(t.playCount > 0) ? formatNumber(t.playCount) : '—'}</div>`
                : '<div class="tracklist-plays">—</div>';

            let afHtml = '';
            if (AUDIO_FEATURES_ENABLED && t.audioFeaturesJson) {
                try {
                    const af = JSON.parse(t.audioFeaturesJson);
                    const features = [['Energy', af.energy], ['Mood', af.valence], ['Dance', af.danceability]];
                    const bars = features.filter(([, val]) => val != null).map(([label, val]) => `
                        <div class="af-bar-row">
                            <span class="af-bar-label">${label}</span>
                            <div class="af-bar-track"><div class="af-bar-fill" style="width:${Math.round(val * 100)}%"></div></div>
                        </div>`).join('');
                    if (bars) afHtml = `<div class="af-bars">${bars}</div>`;
                } catch (e) { /* ignore malformed JSON */ }
            }

            const bpmCell = showBpm
                ? `<div class="tracklist-bpm">${t.tempoBpm != null ? Math.round(t.tempoBpm) : '—'}</div>`
                : '';

            // Per-track artist credits
            let trackArtistsHtml = '';
            if (artistsByTrack.has(t.id)) {
                const artists = artistsByTrack.get(t.id);
                // Any co-billed main/performer artist beyond this release's own
                // primary artist is a genuine extra credit worth surfacing (e.g.
                // a normally solo-artist album with one collaborative track) —
                // filtering on primaryArtistId alone already excludes the
                // redundant self-credit case, no extra release-type gate needed.
                const mainArtists = artists.filter(a => (a.role === 'main' || a.role === 'performer') && a.id !== primaryArtistId);
                const featArtists = artists.filter(a => a.role === 'featured');

                const parts = [];
                if (mainArtists.length > 0) {
                    const mainLinks = mainArtists.map(a =>
                        artistsWithReleases.has(a.id)
                            ? `<a href="${artistHref(a.id, a.slug)}" class="tracklist-artist-link">${escapeHtml(a.name)}</a>`
                            : escapeHtml(a.name)
                    );
                    const mainStr = mainLinks.length <= 2
                        ? mainLinks.join(' and ')
                        : mainLinks.slice(0, -1).join(', ') + ', and ' + mainLinks[mainLinks.length - 1];
                    parts.push(mainStr);
                }
                if (featArtists.length > 0) {
                    const featLinks = featArtists.map(a =>
                        artistsWithReleases.has(a.id)
                            ? `<a href="${artistHref(a.id, a.slug)}" class="tracklist-artist-link">${escapeHtml(a.name)}</a>`
                            : escapeHtml(a.name)
                    );
                    const featStr = featLinks.length <= 2
                        ? featLinks.join(' and ')
                        : featLinks.slice(0, -1).join(', ') + ', and ' + featLinks[featLinks.length - 1];
                    parts.push(`<span class="tracklist-feat">feat. ${featStr}</span>`);
                }
                if (parts.length > 0) {
                    trackArtistsHtml = `<div class="tracklist-track-artists">${parts.join(' · ')}</div>`;
                }
            }

            let alsoOnHtml = '';
            if (alsoOnByTrack.has(t.id)) {
                const others = alsoOnByTrack.get(t.id);
                const links = others.map(o =>
                    `<a href="${releaseHref(o.releaseId, o.releaseSlug)}" class="tracklist-artist-link">${escapeHtml(o.releaseTitle)}</a>`
                );
                const linksStr = links.length <= 2
                    ? links.join(' and ')
                    : links.slice(0, -1).join(', ') + ', and ' + links[links.length - 1];
                alsoOnHtml = `<div class="tracklist-also-on">Also on ${linksStr}</div>`;
            }

            const aliasEntry = aliasesByTrack.get(t.id);
            let aliasRowsHtml = '';
            if (aliasEntry) {
                if (aliasEntry.transliteration) {
                    aliasRowsHtml += `<div class="tracklist-alias"><i data-lucide="corner-down-right" class="tracklist-alias-icon"></i>${escapeHtml(aliasEntry.transliteration)}</div>`;
                }
                if (aliasEntry.translation) {
                    aliasRowsHtml += `<div class="tracklist-alias"><i data-lucide="languages" class="tracklist-alias-icon"></i>${escapeHtml(aliasEntry.translation)}</div>`;
                }
                if (!aliasEntry.transliteration && !aliasEntry.translation && aliasEntry.fallback) {
                    aliasRowsHtml += `<div class="tracklist-alias">${escapeHtml(aliasEntry.fallback)}</div>`;
                }
            }

            row.className = 'tracklist-row' + (trackArtistsHtml || alsoOnHtml ? ' has-track-artists' : '');
            row.dataset.trackId = t.id;
            row.innerHTML = `
                <span class="tracklist-num">${displayNum}</span>
                <div class="tracklist-info">
                    <div class="tracklist-title-row">
                        <div class="tracklist-name">${_renderTrackName(t.title, t.mixName)}${aliasRowsHtml}</div>
                        <div class="tracklist-duration">${formatDuration(t.durationMs)}</div>
                    </div>
                    ${trackArtistsHtml}
                    ${alsoOnHtml}
                    ${afHtml}
                </div>
                ${bpmCell}
                <div class="tracklist-plays">${(showPlayCounts && t.playCount > 0) ? formatNumber(t.playCount) : '—'}</div>
            `;
            container.appendChild(row);
        });
    }

    // Same recording (by ISRC) appearing on other non-hidden releases — surfaced as "Also on"
    function _computeAlsoOnByTrack(safeId, tracks) {
        const alsoOnByTrack = new Map();
        const isrcs = [...new Set(tracks.map(t => t.isrc).filter(Boolean))];
        if (isrcs.length === 0) return alsoOnByTrack;

        const safeIsrcs = isrcs.map(i => `'${i.replace(/'/g, "''")}'`).join(',');
        const alsoOnResult = _db.exec(`
            SELECT t.isrc, t.id, r.id, r.title, r.slug
            FROM tracks t
            JOIN releases r ON r.id = t.release_id
            WHERE t.isrc IN (${safeIsrcs})
              AND t.hidden = 0 AND r.hidden = 0
              AND t.release_id != '${safeId}'
            ORDER BY r.release_date, r.title
        `)[0];
        if (alsoOnResult) {
            const byIsrc = new Map();
            alsoOnResult.values.forEach(([isrc, otherTrackId, otherReleaseId, otherReleaseTitle, otherReleaseSlug]) => {
                if (!byIsrc.has(isrc)) byIsrc.set(isrc, []);
                byIsrc.get(isrc).push({ trackId: otherTrackId, releaseId: otherReleaseId, releaseTitle: otherReleaseTitle, releaseSlug: otherReleaseSlug });
            });
            tracks.forEach(t => {
                if (t.isrc && byIsrc.has(t.isrc)) alsoOnByTrack.set(t.id, byIsrc.get(t.isrc));
            });
        }
        return alsoOnByTrack;
    }

    // ── Main tracklist ──────────────────────────────────────────────────────────

    function loadTracks() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT t.title, t.id, t.track_number, t.disc_number, t.duration_ms, t.isrc,
                   COUNT(l.id) as play_count, t.tempo_bpm, t.audio_features, t.mix_name
            FROM tracks t
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0 AND t.variant_section IS NULL
            GROUP BY t.id
            ORDER BY t.disc_number, t.track_number, t.title
        `)[0];

        const container = document.getElementById('trackList');
        if (!container) return;

        const tracks = (result ? result.values : []).map(
            ([title, id, trackNumber, discNumber, durationMs, isrc, playCount, tempoBpm, audioFeaturesJson, mixName]) =>
                ({ title, id, trackNumber, discNumber, durationMs, isrc, playCount, tempoBpm, audioFeaturesJson, mixName })
        );

        let artistsByTrack = new Map();
        if (tracks.length > 0) {
            const taResult = _db.exec(`
                SELECT ta.track_id, a.id, a.name, ta.role, a.slug
                FROM track_artists ta
                JOIN artists a ON a.id = ta.artist_id
                WHERE ta.track_id IN (
                    SELECT id FROM tracks WHERE release_id = '${safeId}' AND hidden = 0
                )
                ORDER BY ta.track_id,
                         CASE ta.role WHEN 'main' THEN 0 WHEN 'performer' THEN 1 WHEN 'featured' THEN 2 ELSE 3 END,
                         a.name
            `)[0];

            if (taResult) {
                taResult.values.forEach(([trackId, artistId, artistName, role, slug]) => {
                    if (!artistsByTrack.has(trackId)) artistsByTrack.set(trackId, []);
                    artistsByTrack.get(trackId).push({ id: artistId, name: artistName, role, slug });
                });
            }
        }

        // Extend _artistsWithReleases with every track-level credited artist.
        // These ids are already resolved artist rows (real foreign keys, not
        // raw text), so their artist page always renders something valid —
        // no need to additionally require they own a release of their own
        // (a featured-only artist like a guest rapper still deserves a link).
        artistsByTrack.forEach(list => list.forEach(a => _artistsWithReleases.add(a.id)));

        // Load transliteration/translation/native-script aliases to show as
        // secondary dim lines under the title. Transliteration and
        // translation each get their own row (prefaced by a distinct icon,
        // since they answer different questions — "how do I say this" vs.
        // "what does this mean"); native_script/unicode is a same-purpose
        // fallback for the more common case where the title itself is
        // already Latin script and the alias is what needs disambiguating,
        // so no icon there (nothing to distinguish it from).
        let aliasesByTrack = new Map();
        if (tracks.length > 0) {
            const aliasResult = _db.exec(`
                SELECT ta.track_id, ta.alias, ta.alias_type
                FROM track_aliases ta
                WHERE ta.track_id IN (
                    SELECT id FROM tracks WHERE release_id = '${safeId}' AND hidden = 0
                )
                AND ta.alias_type IN ('transliteration', 'unicode', 'native_script', 'translation')
                ORDER BY ta.track_id
            `)[0];
            if (aliasResult) {
                aliasResult.values.forEach(([trackId, alias, aliasType]) => {
                    if (!aliasesByTrack.has(trackId)) aliasesByTrack.set(trackId, {});
                    const entry = aliasesByTrack.get(trackId);
                    if (aliasType === 'transliteration' && !entry.transliteration) entry.transliteration = alias;
                    else if (aliasType === 'translation' && !entry.translation) entry.translation = alias;
                    else if ((aliasType === 'native_script' || aliasType === 'unicode') && !entry.fallback) entry.fallback = alias;
                });
            }
        }

        const alsoOnByTrack = _computeAlsoOnByTrack(safeId, tracks);

        _renderTracklist(container, tracks, true, { artistsByTrack, primaryArtistId: _primaryArtistId, artistsWithReleases: _artistsWithReleases, aliasesByTrack, alsoOnByTrack });
    }

    // ── Release variants ────────────────────────────────────────────────────────

    function loadVariants() {
        const safeId = _releaseId.replace(/'/g, "''");

        // Fetch distinct variant sections ordered by their first track number
        const sectionsResult = _db.exec(`
            SELECT variant_section
            FROM (
                SELECT t.variant_section, MIN(t.track_number) AS min_tn
                FROM tracks t
                WHERE t.release_id = '${safeId}' AND t.variant_section IS NOT NULL AND t.hidden = 0
                GROUP BY t.variant_section
            )
            ORDER BY min_tn
        `)[0];

        const section = document.getElementById('variantsSection');
        if (!section) return;

        if (!sectionsResult || sectionsResult.values.length === 0) return;

        // Per-track artist credits — used both for the dedup key below (same
        // title, different features = different recording, not a re-list)
        // and to render the "feat." / co-main credit line on each row. The
        // dedup key only ever looks at featured credits (that's the signal
        // that distinguishes an otherwise-identical title), but all roles
        // are surfaced in artistsByTrack so a variant-section track with a
        // non-primary main/performer artist (e.g. a bonus track performed
        // by someone other than the release's own primary artist) still
        // shows that credit, same as the main tracklist does.
        const featResult = _db.exec(`
            SELECT ta.track_id, a.id, a.name, ta.role, a.slug
            FROM track_artists ta
            JOIN tracks t ON t.id = ta.track_id
            JOIN artists a ON a.id = ta.artist_id
            WHERE t.release_id = '${safeId}' AND ta.role IN ('featured', 'main', 'performer')
            ORDER BY a.name
        `)[0];
        const featuredByTrack = new Map();
        const artistsByTrack = new Map();
        (featResult ? featResult.values : []).forEach(([trackId, artistId, name, role, slug]) => {
            if (role === 'featured') {
                if (!featuredByTrack.has(trackId)) featuredByTrack.set(trackId, []);
                featuredByTrack.get(trackId).push(name);
            }
            if (!artistsByTrack.has(trackId)) artistsByTrack.set(trackId, []);
            artistsByTrack.get(trackId).push({ id: artistId, name, role, slug });
        });
        const dedupKey = (id, title) =>
            `${_normTitle(title)}|${(featuredByTrack.get(id) || []).join(',').toLowerCase()}`;

        // Canonical track set for HIDE_DUPES dedup
        const canonResult = _db.exec(`
            SELECT t.id, t.title, t.isrc, t.duration_ms
            FROM tracks t
            WHERE t.release_id = '${safeId}' AND t.hidden = 0 AND t.variant_section IS NULL
        `)[0];
        const canonTracks = (canonResult ? canonResult.values : [])
            .map(([id, title, isrc, durationMs]) => ({ id, title, isrc, durationMs }));
        const shownIsrcs = new Set(canonTracks.map(t => t.isrc).filter(Boolean));
        const canonByKey = new Map();
        canonTracks.forEach(t => {
            const key = dedupKey(t.id, t.title);
            if (!canonByKey.has(key)) canonByKey.set(key, []);
            canonByKey.get(key).push(t);
        });

        // A variant track counts as the same recording as a canonical one if:
        // both have an ISRC and they match: definite dupe.
        // both have an ISRC and they differ: never a dupe, even if title+features
        //   match (distinct masters/mixes can legitimately reuse a title).
        // otherwise (ISRC missing on either side): fall back to the title+features
        //   key, gated by a duration tolerance so an unrelated same-titled track
        //   with no ISRC data isn't wrongly folded in.
        const DURATION_TOLERANCE_MS = 3000;
        const isDupe = t => {
            if (t.isrc && shownIsrcs.has(t.isrc)) return true;
            const key = dedupKey(t.id, t.title);
            const candidates = canonByKey.get(key);
            if (!candidates) return false;
            return candidates.some(c => {
                if (t.isrc && c.isrc) return false; // both known, already handled above as non-match
                if (t.durationMs == null || c.durationMs == null) return true; // no signal to refute the title match
                return Math.abs(t.durationMs - c.durationMs) <= DURATION_TOLERANCE_MS;
            });
        };

        for (const [variantSection] of sectionsResult.values) {
            const safeSection = variantSection.replace(/'/g, "''");

            const vtResult = _db.exec(`
                SELECT t.title, t.id, t.track_number, t.disc_number, t.duration_ms, t.isrc,
                       COUNT(l.id) as play_count
                FROM tracks t
                LEFT JOIN listens l ON l.track_id = t.id
                WHERE t.release_id = '${safeId}'
                  AND t.variant_section = '${safeSection}'
                  AND t.hidden = 0
                GROUP BY t.id
                ORDER BY t.disc_number, t.track_number, t.title
            `)[0];

            const allTracks = (vtResult ? vtResult.values : []).map(
                ([title, id, trackNumber, discNumber, durationMs, isrc, playCount]) =>
                    ({ title, id, trackNumber, discNumber, durationMs, isrc, playCount })
            );

            const exclusive = allTracks.filter(t => !isDupe(t));
            const dupes = allTracks.filter(t => isDupe(t));
            exclusive.forEach(t => {
                if (t.isrc) shownIsrcs.add(t.isrc);
                const key = dedupKey(t.id, t.title);
                if (!canonByKey.has(key)) canonByKey.set(key, []);
                canonByKey.get(key).push(t);
            });

            const tracksToShow = HIDE_DUPES ? exclusive : allTracks;
            if (tracksToShow.length === 0) continue;

            // Service indicator: check release_service_links for a Spotify entry for this section
            const svcResult = _db.exec(`
                SELECT service FROM release_service_links
                WHERE release_id = '${safeId}' AND variant_label = '${safeSection}'
                LIMIT 1
            `)[0];
            const svcNum = svcResult?.values[0]?.[0];
            const svcIconClass = svcNum === 2 ? 'vsi-spotify' : null;
            const serviceIndicator = svcIconClass
                ? `<span class="variant-section-service"><span class="variant-service-icon ${svcIconClass}"></span>Spotify</span>`
                : '';

            const wrap = document.createElement('section');
            wrap.className = 'variant-section';
            wrap.innerHTML = `
                <div class="variant-section-header">
                    <span class="variant-section-title">${escapeHtml(variantSection)}</span>
                    ${serviceIndicator}
                </div>
                <div class="tracklist variant-tracklist" id="vt-vs-${encodeURIComponent(variantSection)}"></div>
            `;
            section.appendChild(wrap);

            const trackContainer = document.getElementById(`vt-vs-${encodeURIComponent(variantSection)}`);
            const alsoOnByTrack = _computeAlsoOnByTrack(safeId, tracksToShow);
            const renderOpts = { alsoOnByTrack, artistsByTrack, primaryArtistId: _primaryArtistId, artistsWithReleases: _artistsWithReleases };
            if (HIDE_DUPES) {
                _renderTracklist(trackContainer, exclusive, true, renderOpts);
            } else {
                _renderTracklist(trackContainer, tracksToShow, true, renderOpts);
            }
        }
    }

    // ── About this album (Apple Music editorial note) ───────────────────────────

    function loadEditorialNotes() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT editorial_note FROM releases WHERE id = '${safeId}'
        `)[0];

        const note = result && result.values[0] && result.values[0][0];
        const section = document.getElementById('editorialSection');
        if (!note || !section) return;

        const paragraphs = note.split(/\n+/).filter(Boolean)
            .map(p => `<p class="editorial-note">${escapeHtml(p)}</p>`).join('');

        section.innerHTML = `
            <section class="editorial-notes">
                <h2>Editorial Notes</h2>
                <div class="editorial-clamp" id="editorialClamp">
                    ${paragraphs}
                    <div class="editorial-fade"></div>
                </div>
                <button class="editorial-toggle" id="editorialToggle">Read more</button>
            </section>
        `;

        const clampEl = document.getElementById('editorialClamp');
        const btnEl = document.getElementById('editorialToggle');

        // Clamp, measure, then unclamp if it wasn't worth it. Tolerance of one
        // line so the toggle never appears just to hide a single trailing line.
        clampEl.classList.add('is-clamped');
        const lineHeight = parseFloat(getComputedStyle(clampEl).lineHeight) || 24;
        if (clampEl.scrollHeight <= clampEl.clientHeight + lineHeight) {
            clampEl.classList.remove('is-clamped');
            btnEl.remove();
        } else {
            btnEl.addEventListener('click', () => {
                const clamped = clampEl.classList.toggle('is-clamped');
                btnEl.textContent = clamped ? 'Read more' : 'Show less';
            });
        }
    }

    // ── Compilation sources ─────────────────────────────────────────────────────

    function loadSources() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                rs.source_id,
                rs.disc_number,
                r.title,
                COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                r.release_year,
                r.slug,
                (SELECT COUNT(*)
                 FROM listens l JOIN tracks t ON l.track_id = t.id
                 WHERE t.release_id = rs.source_id AND t.hidden = 0) AS total_listens,
                (SELECT COALESCE(SUM(t.duration_ms), 0) / 60000
                 FROM tracks t
                 WHERE t.release_id = rs.source_id AND t.hidden = 0)  AS total_minutes
            FROM release_sources rs
            JOIN releases r ON r.id = rs.source_id
            WHERE rs.compilation_id = '${safeId}'
            ORDER BY rs.disc_number
        `)[0];

        if (!result || result.values.length === 0) return;

        const section = document.getElementById('sourcesSection');
        if (!section) return;

        const heading       = document.createElement('h2');
        heading.textContent = 'Compiled From';
        section.appendChild(heading);

        const grid     = document.createElement('div');
        grid.className = 'wide-grid';

        for (const [sourceId, discNumber, title, artUrl, year, sourceSlug, totalListens, totalMinutes]
                of result.values) {
            const discLabel = discNumber != null ? `Disc ${discNumber}` : null;
            const card = createWideCard({
                href:         releaseHref(sourceId, sourceSlug),
                imageUrl:     artUrl,
                name:         title,
                meta:         [year, discLabel].filter(Boolean).join(' · '),
                totalListens: totalListens || 0,
                totalMinutes: totalMinutes || 0,
            });
            grid.appendChild(card);
        }

        section.appendChild(grid);
    }

    // ── Canonical backlink ──────────────────────────────────────────────────────

    // When this release is itself a variant, show a "This is a remaster of X" note.
    function loadCanonicalBacklink() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT rv.canonical_id, r.title, rv.variant_type, r.slug
            FROM release_variants rv
            JOIN releases r ON r.id = rv.canonical_id
            WHERE rv.variant_id = '${safeId}'
            LIMIT 1
        `)[0];

        if (!result || result.values.length === 0) return;

        const [canonicalId, canonicalTitle, variantType, canonicalSlug] = result.values[0];
        const typeLabel = variantType ? `${variantType} of` : 'edition of';

        const artistContainer = document.querySelector('.release-artist');
        if (!artistContainer) return;

        const p     = document.createElement('p');
        p.className = 'variant-backlink';
        p.innerHTML = `A ${escapeHtml(typeLabel)} <a href="${releaseHref(canonicalId, canonicalSlug)}">${escapeHtml(canonicalTitle)}</a>`;
        artistContainer.insertAdjacentElement('afterend', p);
    }

    // Row appended after Genre in the stats table (same rst-genres-row pattern) —
    // shows every canonical list (RS500, NME AOTY, etc.) this release appears on.
    function loadListRankings() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT cl.id, cl.short_name, cl.name, cle.rank, cle.position_label
            FROM canonical_list_entries cle
            JOIN canonical_lists cl ON cl.id = cle.list_id
            WHERE cle.release_id = '${safeId}'
            ORDER BY cle.rank
        `)[0];

        if (!result || result.values.length === 0) return;

        const statsEl = document.getElementById('releaseStatsTable');
        if (!statsEl) return;

        const pills = result.values.map(([listId, shortName, name, rank, posLabel]) => {
            const label = posLabel || `#${rank}`;
            return `<a href="?view=list&id=${encodeURIComponent(listId)}" class="stat-genre-tag" title="${escapeHtml(name)}">${escapeHtml(shortName || name)} ${escapeHtml(label)}</a>`;
        }).join('');

        statsEl.insertAdjacentHTML('beforeend',
            `<div class="rst-row rst-genres-row"><span class="rst-label">Lists</span><span class="rst-value">${pills}</span></div>`
        );
        statsEl.removeAttribute('hidden');
    }

    // ── Listening history ───────────────────────────────────────────────────────

    function loadListeningHistory() {
        const safeId = _releaseId.replace(/'/g, "''");

        const monthlyResult = _db.exec(`
            SELECT l.year, l.month, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year, l.month
            ORDER BY l.year, l.month
        `)[0];

        const yearlyResult = _db.exec(`
            SELECT l.year, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year
            ORDER BY l.year
        `)[0];

        if ((!monthlyResult || monthlyResult.values.length === 0) &&
            (!yearlyResult  || yearlyResult.values.length  === 0)) {
            const rowsEl = document.getElementById('pulseRows');
            if (rowsEl) rowsEl.innerHTML = '<p class="no-data">No listening history yet.</p>';
            const el = document.querySelector('.chart-container');
            if (el) el.innerHTML = renderLoading('No listening history found');
            return;
        }

        if (monthlyResult && monthlyResult.values.length > 0) {
            _chartData.monthly    = buildMonthlyChartData(monthlyResult.values);
            _chartData.monthlyRaw = monthlyResult.values;
        }

        if (yearlyResult && yearlyResult.values.length > 0) {
            _chartData.yearly = buildYearlyChartData(yearlyResult.values);
            renderPulse(yearlyResult.values);
        }

        if (CHART_ENABLED) renderChart();
    }

    function renderPulse(yearlyValues) {
        const pulseEl = document.getElementById('pulseSection');
        const rowsEl  = document.getElementById('pulseRows');
        if (!pulseEl || !rowsEl || !yearlyValues || yearlyValues.length === 0) return;

        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const max = Math.max(...yearlyValues.map(([, count]) => count));

        const monthlyByYear = new Map();
        if (_chartData.monthlyRaw) {
            _chartData.monthlyRaw.forEach(([year, month, count]) => {
                if (!monthlyByYear.has(year)) monthlyByYear.set(year, new Map());
                monthlyByYear.get(year).set(month, count);
            });
        }

        rowsEl.innerHTML = yearlyValues.map(([year, count]) => {
            const pct = Math.round((count / max) * 100);
            return `
                <div class="pulse-row" data-year="${year}">
                    <span class="pulse-year">${year}</span>
                    <span class="pulse-count">${formatNumber(count)}</span>
                    <div class="pulse-bar-track">
                        <div class="pulse-bar-fill" style="width: ${pct}%"></div>
                    </div>
                    <span class="pulse-chevron">▶</span>
                </div>
                <div class="pulse-monthly" id="pulse-monthly-${year}" style="display:none"></div>
            `;
        }).join('');

        rowsEl.addEventListener('click', e => {
            const row = e.target.closest('.pulse-row');
            if (!row) return;
            const year      = parseInt(row.dataset.year);
            const monthlyEl = document.getElementById(`pulse-monthly-${year}`);
            if (!monthlyEl) return;

            const isExpanded = row.classList.contains('expanded');
            if (isExpanded) {
                monthlyEl.style.display = 'none';
                row.classList.remove('expanded');
                return;
            }

            if (!monthlyEl.innerHTML) {
                const monthMap = monthlyByYear.get(year) || new Map();
                const monthMax = Math.max(...[...monthMap.values()], 1);
                monthlyEl.innerHTML = Array.from({ length: 12 }, (_, i) => {
                    const m = i + 1;
                    const c = monthMap.get(m) || 0;
                    const p = Math.round((c / monthMax) * 100);
                    return `
                        <div class="pulse-month-row">
                            <span class="pulse-month-name">${monthNames[i]}</span>
                            <span class="pulse-month-count">${c > 0 ? formatNumber(c) : ''}</span>
                            <div class="pulse-month-bar-track">
                                <div class="pulse-month-bar-fill" style="width: ${p}%"></div>
                            </div>
                        </div>
                    `;
                }).join('');
            }

            monthlyEl.style.display = '';
            row.classList.add('expanded');
        });
    }

    function buildMonthlyChartData(values) {
        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const years      = values.map(([year]) => year);
        const minYear    = Math.min(...years);
        const maxYear    = Math.max(...years);
        const dataMap    = new Map();
        values.forEach(([year, month, count]) => dataMap.set(`${year}-${month}`, count));
        const labels = [], data = [];
        for (let year = minYear; year <= maxYear; year++) {
            for (let month = 1; month <= 12; month++) {
                labels.push(`${monthNames[month - 1]} ${year}`);
                data.push(dataMap.get(`${year}-${month}`) || 0);
            }
        }
        return { labels, data };
    }

    function buildYearlyChartData(values) {
        const years   = values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);
        const dataMap = new Map();
        values.forEach(([year, count]) => dataMap.set(year, count));
        const labels = [], data = [];
        for (let year = minYear; year <= maxYear; year++) {
            labels.push(year.toString());
            data.push(dataMap.get(year) || 0);
        }
        return { labels, data };
    }

    function setupChartControls() {
        setupToggleGroup('[data-granularity]', btn => {
            _chartState.granularity = btn.dataset.granularity;
            renderChart();
        });

        setupToggleGroup('[data-type]', btn => {
            _chartState.type = btn.dataset.type;
            renderChart();
        });

        _themeObserver = new MutationObserver(() => {
            if (_currentChart) renderChart();
        });
        _themeObserver.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ['data-theme']
        });
    }

    function renderChart() {
        const data = _chartData[_chartState.granularity];
        if (!data) return;

        const primaryColor  = getCSSColor('--primary');
        const chartBg       = getCSSColor('--chart-bg');
        const chartBgSolid  = getCSSColor('--chart-bg-solid');
        const bgSecondary   = getCSSColor('--bg-secondary');
        const textColor     = getCSSColor('--text');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor   = getCSSColor('--border');

        let chartValues = [...data.data];
        if (_chartState.type === 'cumulative') {
            chartValues = data.data.reduce((acc, val, idx) => {
                acc.push(idx === 0 ? val : acc[idx - 1] + val);
                return acc;
            }, []);
        }

        const skipFactor    = Math.max(1, Math.ceil(data.labels.length / 15));
        const labelCallback = (value, index) => index % skipFactor === 0 ? data.labels[index] : '';

        if (_currentChart) _currentChart.destroy();

        const ctx = document.getElementById('historyChart');
        if (!ctx) return;

        _currentChart = new Chart(ctx.getContext('2d'), {
            type: _chartState.type === 'cumulative' ? 'line' : 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: _chartState.type === 'cumulative' ? 'Total Listens' : 'Listens per Period',
                    data: chartValues,
                    backgroundColor: _chartState.type === 'cumulative' ? chartBg : chartBgSolid,
                    borderColor: primaryColor,
                    borderWidth: _chartState.type === 'cumulative' ? 3 : 1,
                    fill: _chartState.type === 'cumulative',
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: bgSecondary,
                        titleColor: textColor,
                        bodyColor: textColor,
                        borderColor: borderColor,
                        borderWidth: 1
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: textSecondary, stepSize: 1 },
                        grid:  { color: borderColor }
                    },
                    x: {
                        ticks: {
                            color: textSecondary,
                            maxRotation: 45,
                            minRotation: 45,
                            autoSkip: false,
                            callback: labelCallback
                        },
                        grid: { color: borderColor }
                    }
                }
            }
        });
    }

    return { mount, unmount };

    function _openArtModal(url) {
        const existing = document.getElementById('artModal');
        if (existing) existing.remove();
        const modal = document.createElement('div');
        modal.id = 'artModal';
        modal.className = 'art-modal';
        // Build the <img> via the DOM rather than innerHTML: the URL comes from
        // scraped third-party metadata, and a quote in it would break out of
        // the attribute.
        const inner = document.createElement('div');
        inner.className = 'art-modal-inner';
        const img = document.createElement('img');
        img.src = url;
        img.alt = '';
        inner.appendChild(img);
        modal.appendChild(inner);
        document.body.appendChild(modal);

        // AbortController so click-to-dismiss tears down the keydown handler
        // too, rather than leaking one per image opened.
        const ac = new AbortController();
        const close = () => { ac.abort(); modal.remove(); };
        modal.addEventListener('click', close, { signal: ac.signal });
        document.addEventListener('keydown', e => {
            if (e.key === 'Escape') close();
        }, { signal: ac.signal });
    }
})();
