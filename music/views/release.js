const ViewRelease = (() => {
    let _db = null;
    let _releaseId = null;
    let _primaryArtistId = null;
    let _releaseType = null;
    let _artistsWithReleases = new Set();
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null, monthlyRaw: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    let _themeObserver = null;

    // Set to true to re-enable the Chart.js listening history chart
    const CHART_ENABLED = false;

    // Set to false to disable per-track audio feature bars (Energy, Mood, Dance)
    const AUDIO_FEATURES_ENABLED = true;

    function mount(container, db, params) {
        _db = db;
        _releaseId = params.id;
        _primaryArtistId = null;
        _releaseType = null;
        _artistsWithReleases = new Set();
        _currentChart = null;
        _chartData = { monthly: null, yearly: null, monthlyRaw: null };

        if (!_releaseId) {
            navigate({ view: 'home' });
            return;
        }

        container.innerHTML = `
            <div class="site-header">
                <a href="?" class="site-logo-small">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </a>
                <a href="javascript:history.back()" class="back-button">← Back</a>
            </div>

            <header id="releaseHeader" class="artist-header-layout">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="albumArt">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <rect width="100" height="100" fill="#1e293b"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#475569">♪</text>
                        </svg>
                    </div>
                </div>
                <div class="artist-info-container">
                    <h1 id="releaseName">Loading...</h1>
                    <p class="release-artist">
                        <span id="releaseArtist"></span>
                    </p>
                    <p class="release-meta-line" id="releaseMeta"></p>
                    <p id="releaseListenStats" class="release-listen-stats" hidden></p>
                    <p id="releaseAka" class="release-aka" hidden></p>
                    <div class="release-footer-row">
                        <p id="releaseGenres" class="genre-list"></p>
                        <div id="releaseLinks" class="release-links"></div>
                    </div>
                </div>
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
                <section class="pulse-section" id="pulseSection">
                    <h2>Timeline</h2>
                    <div class="pulse-rows" id="pulseRows"></div>
                </section>

                <section class="tracks-section">
                    <h2>Tracks</h2>
                    <div class="tracklist" id="trackList">
                        <div class="loading">Loading tracks...</div>
                    </div>
                    <div id="variantsSection"></div>
                </section>
            </div>

            <div id="sourcesSection"></div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadReleaseInfo();
        loadReleaseAliases();
        loadTracks();
        loadListeningHistory();
        loadVariants();
        loadSources();
        loadCanonicalBacklink();
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
                COUNT(DISTINCT CASE WHEN t.hidden = 0 THEN t.id END)                      as total_tracks_in_db,
                COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as tracks_heard,
                COUNT(CASE WHEN t.hidden = 0 THEN l.id END)                               as total_plays,
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
                r.label,
                (SELECT CAST(SUM(COALESCE(t2.duration_ms, 0)) AS INTEGER)
                 FROM tracks t2 WHERE t2.release_id = r.id AND t2.hidden = 0) as album_total_ms,
                MIN(CASE WHEN t.hidden = 0 THEN l.timestamp END) as first_listen_ts,
                MAX(CASE WHEN t.hidden = 0 THEN l.timestamp END) as last_listen_ts,
                r.apple_music_id
            FROM releases r
            LEFT JOIN tracks t ON t.release_id = r.id
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE r.id = '${safeId}'
            GROUP BY r.id
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('releaseName');
            if (el) el.textContent = 'Release not found';
            return;
        }

        const [title, releaseDate, type, typeSecondary, albumArtUrl,
               totalTracksInDb, tracksHeard, totalPlays,
               spotifyId, releaseGroupMbid, mbid, aotyUrl, aotyId,
               aotyScoreCritic, aotyScoreUser, aotyRatingsCritic, aotyRatingsUser,
               primaryArtistId, label, albumTotalMs, firstListenTs, lastListenTs,
               appleMusicIdCol] = result.values[0];

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
        // Spotify: prefer the column (canonical owner); fall back to external_links
        // service 2 for releases that are variants of a Spotify-listed edition.
        const effectiveSpotifyId = spotifyId || extLinks.get(2) || null;
        const typeLabel = [type, typeSecondary].filter(Boolean).join(' / ');

        _primaryArtistId = primaryArtistId || null;
        _releaseType = type || null;

        const artistResult = _db.exec(`
            SELECT DISTINCT a.name, a.id
            FROM releases r
            JOIN artists a ON (
                a.id = r.primary_artist_id
                OR a.id IN (SELECT artist_id FROM release_artists WHERE release_id = r.id AND role = 'main')
            )
            WHERE r.id = '${safeId}' AND a.id IS NOT NULL
            ORDER BY (r.primary_artist_id = a.id) DESC, a.name
        `)[0];

        // Inline transliteration for non-Latin release titles
        const isNonLatin = s => /[^ -]/.test(s);
        const titleAliasResult = _db.exec(`
            SELECT alias FROM release_aliases
            WHERE release_id = '${safeId}'
              AND alias_norm != lower('${(title || '').replace(/'/g, "''")}')
              AND language IS NOT NULL
            ORDER BY is_definitive DESC LIMIT 1
        `)[0];
        const titleAlias = titleAliasResult && titleAliasResult.values[0]?.[0];

        const nameEl = document.getElementById('releaseName');
        if (isNonLatin(title) && titleAlias) {
            nameEl.innerHTML = `${escapeHtml(title)} <span class="artist-romanized">(${escapeHtml(titleAlias)})</span>`;
        } else {
            nameEl.textContent = title || 'Unknown Release';
        }
        document.title = `aswin.db/music - ${title || 'Release'}`;

        const metaParts = [];
        if (releaseDate) metaParts.push(_formatReleaseDate(releaseDate));
        if (label) metaParts.push(label);
        const metaEl = document.getElementById('releaseMeta');
        if (metaEl) {
            const badge = typeLabel
                ? `<span class="release-type-badge">${escapeHtml(typeLabel)}</span> `
                : '';
            metaEl.innerHTML = badge + escapeHtml(metaParts.join(' · '));
        }

        const scoreColor = n => `hsl(${Math.round(Math.min(n, 100) * 1.2)}, 65%, 40%)`;

        const statsEl = document.getElementById('releaseListenStats');
        if (statsEl) {
            const chips = [];
            if (totalPlays > 0) {
                chips.push(`<span class="stat-chip"><i data-lucide="headphones"></i> ${formatNumber(totalPlays)}</span>`);
                if (totalTracksInDb > 0) {
                    chips.push(`<span class="stat-chip"><i data-lucide="music"></i> ${tracksHeard} / ${totalTracksInDb} tracks</span>`);
                }
                if (albumTotalMs > 0) {
                    chips.push(`<span class="stat-chip"><i data-lucide="clock"></i> ${_formatAlbumDuration(albumTotalMs)}</span>`);
                }
                if (firstListenTs) {
                    chips.push(`<span class="stat-chip"><i data-lucide="calendar"></i> First heard ${_fmtTs(firstListenTs)}</span>`);
                }
                if (lastListenTs && lastListenTs !== firstListenTs) {
                    chips.push(`<span class="stat-chip"><i data-lucide="clock"></i> Last played ${formatRelativeTime(lastListenTs)}</span>`);
                }
            }
            if (aotyScoreCritic != null) {
                chips.push(`<span class="stat-chip" title="Critic Score${aotyRatingsCritic != null ? ` (${formatNumber(aotyRatingsCritic)} reviews)` : ''}"><i data-lucide="message-square-warning" style="color:${scoreColor(aotyScoreCritic)}"></i> <span style="color:${scoreColor(aotyScoreCritic)}">${aotyScoreCritic}</span></span>`);
            }
            if (aotyScoreUser != null) {
                chips.push(`<span class="stat-chip" title="User Score${aotyRatingsUser != null ? ` (${formatNumber(aotyRatingsUser)} ratings)` : ''}"><i data-lucide="message-square-heart" style="color:${scoreColor(aotyScoreUser)}"></i> <span style="color:${scoreColor(aotyScoreUser)}">${Number(aotyScoreUser).toFixed(1)}</span></span>`);
            }
            if (chips.length > 0) {
                statsEl.innerHTML = chips.join('');
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
                    ? `<a href="?view=artist&id=${encodeURIComponent(i)}" class="release-artist-link">${escapeHtml(n)}</a>`
                    : escapeHtml(n);

            let html;
            if (allArtists.length === 1) {
                html = makeLink(allArtists[0][0], allArtists[0][1]);
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
                        parts.push(`${makeLink(name, id)} (${joinLinks(memberLinks)})`);
                    } else {
                        parts.push(makeLink(name, id));
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
        if (genresEl && genreResult && genreResult.values.length > 0) {
            genresEl.innerHTML = renderGenreTags(genreResult.values);
        }

        const linksEl = document.getElementById('releaseLinks');
        if (linksEl) {
            const _svcLink = (href, service, label) =>
                `<a href="${href}" target="_blank" rel="noopener" class="release-link-icon" data-service="${service}" title="${label}">` +
                `<span class="link-icon-mask" style="--icon-url: url('images/links/${service}.svg')"></span></a>`;

            const iconLinks = [];
            if (effectiveSpotifyId) {
                iconLinks.push(_svcLink(`https://open.spotify.com/album/${effectiveSpotifyId}`, 'spotify', 'Spotify'));
            }
            if (releaseGroupMbid) {
                iconLinks.push(_svcLink(`https://musicbrainz.org/release-group/${releaseGroupMbid}`, 'musicbrainz', 'MusicBrainz'));
            } else if (mbid) {
                iconLinks.push(_svcLink(`https://musicbrainz.org/release/${mbid}`, 'musicbrainz', 'MusicBrainz'));
            }
            const resolvedAotyUrl = aotyUrl || (aotyId ? `https://www.albumoftheyear.org/album/${aotyId}/` : null);
            if (resolvedAotyUrl) {
                iconLinks.push(
                    `<a href="${resolvedAotyUrl}" target="_blank" rel="noopener" class="release-link-icon" data-service="aoty" title="Album of the Year">` +
                    `<img src="images/links/aoty-icon.png" alt="Album of the Year" style="width:20px;height:20px;object-fit:contain;display:block"></a>`
                );
            }
            if (wikiPageId) {
                iconLinks.push(_svcLink(`https://en.wikipedia.org/wiki/?curid=${wikiPageId}`, 'wikipedia', 'Wikipedia'));
            }
            const amId = appleMusicIdCol || extLinks.get(3) || null;
            if (amId) {
                iconLinks.push(_svcLink(`https://music.apple.com/album/${amId}`, 'applemusic', 'Apple Music'));
            }
            const beatportId = extLinks.get(7) || null;
            if (beatportId) {
                iconLinks.push(_svcLink(`https://www.beatport.com/release/-/${beatportId}`, 'beatport', 'Beatport'));
            }
            const tidalId = extLinks.get(5) || null;
            if (tidalId) {
                iconLinks.push(_svcLink(`https://tidal.com/browse/album/${tidalId}`, 'tidal', 'Tidal'));
            }
            const deezerId = extLinks.get(4) || null;
            if (deezerId) {
                iconLinks.push(_svcLink(`https://www.deezer.com/album/${deezerId}`, 'deezer', 'Deezer'));
            }
            const bandcampUrl = extLinks.get(6) || null;
            if (bandcampUrl) {
                iconLinks.push(_svcLink(bandcampUrl, 'bandcamp', 'Bandcamp'));
            }
            linksEl.innerHTML = iconLinks.join('');
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

    // Render a track title, optionally dimming the ETI when mix_name is present.
    // Uses lastIndexOf(' (') to split so it works regardless of casing differences
    // between tracks.title and tracks.mix_name.
    function _renderTrackName(title, mixName) {
        if (!mixName) return escapeHtml(title);
        const split = title.lastIndexOf(' (');
        if (split === -1) return escapeHtml(title);
        const base = title.slice(0, split);
        const eti  = title.slice(split);          // includes the leading space
        return `${escapeHtml(base)}<span class="tracklist-eti">${escapeHtml(eti)}</span>`;
    }

    // ── Shared tracklist renderer ───────────────────────────────────────────────

    function _renderTracklist(container, tracks, showPlayCounts, opts = {}) {
        const { showTrackArtists = false, artistsByTrack = new Map(), primaryArtistId = null, artistsWithReleases = new Set(), aliasesByTrack = new Map() } = opts;
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
                <div class="tracklist-plays"><i data-lucide="headphones"></i></div>
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
                const mainArtists = showTrackArtists ? artists.filter(a => a.role === 'main' && a.id !== primaryArtistId) : [];
                const featArtists = artists.filter(a => a.role === 'featured');

                const parts = [];
                if (mainArtists.length > 0) {
                    const mainLinks = mainArtists.map(a =>
                        artistsWithReleases.has(a.id)
                            ? `<a href="?view=artist&id=${encodeURIComponent(a.id)}" class="tracklist-artist-link">${escapeHtml(a.name)}</a>`
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
                            ? `<a href="?view=artist&id=${encodeURIComponent(a.id)}" class="tracklist-artist-link">${escapeHtml(a.name)}</a>`
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

            row.className = 'tracklist-row' + (trackArtistsHtml ? ' has-track-artists' : '');
            row.dataset.trackId = t.id;
            row.innerHTML = `
                <span class="tracklist-num">${displayNum}</span>
                <div class="tracklist-info">
                    <div class="tracklist-title-row">
                        <div class="tracklist-name">${_renderTrackName(t.title, t.mixName)}${aliasesByTrack.has(t.id) ? `<div class="tracklist-alias">${escapeHtml(aliasesByTrack.get(t.id))}</div>` : ''}</div>
                        <div class="tracklist-duration">${formatDuration(t.durationMs)}</div>
                    </div>
                    ${trackArtistsHtml}
                    ${afHtml}
                </div>
                ${bpmCell}
                <div class="tracklist-plays">${(showPlayCounts && t.playCount > 0) ? formatNumber(t.playCount) : '—'}</div>
            `;
            container.appendChild(row);
        });
    }

    // ── Main tracklist ──────────────────────────────────────────────────────────

    function loadTracks() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT t.title, t.id, t.track_number, t.disc_number, t.duration_ms, t.isrc,
                   COUNT(l.id) as play_count, t.tempo_bpm, t.audio_features, t.mix_name
            FROM tracks t
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
            GROUP BY t.id
            ORDER BY t.disc_number, t.track_number, t.title
        `)[0];

        const container = document.getElementById('trackList');
        if (!container) return;

        const tracks = (result ? result.values : []).map(
            ([title, id, trackNumber, discNumber, durationMs, isrc, playCount, tempoBpm, audioFeaturesJson, mixName]) =>
                ({ title, id, trackNumber, discNumber, durationMs, isrc, playCount, tempoBpm, audioFeaturesJson, mixName })
        );

        const showTrackArtists = (_releaseType === 'compilation' || _primaryArtistId === null);

        let artistsByTrack = new Map();
        if (tracks.length > 0) {
            const taResult = _db.exec(`
                SELECT ta.track_id, a.id, a.name, ta.role
                FROM track_artists ta
                JOIN artists a ON a.id = ta.artist_id
                WHERE ta.track_id IN (
                    SELECT id FROM tracks WHERE release_id = '${safeId}' AND hidden = 0
                )
                ORDER BY ta.track_id,
                         CASE ta.role WHEN 'main' THEN 0 ELSE 1 END,
                         a.name
            `)[0];

            if (taResult) {
                taResult.values.forEach(([trackId, artistId, artistName, role]) => {
                    if (!artistsByTrack.has(trackId)) artistsByTrack.set(trackId, []);
                    artistsByTrack.get(trackId).push({ id: artistId, name: artistName, role });
                });
            }
        }

        // Extend _artistsWithReleases with track-level credited artists.
        // (Header artists were already seeded by loadReleaseInfo.)
        const allTrackArtistIds = new Set();
        artistsByTrack.forEach(list => list.forEach(a => allTrackArtistIds.add(`'${a.id}'`)));
        if (allTrackArtistIds.size > 0) {
            const arResult = _db.exec(`
                SELECT DISTINCT primary_artist_id
                FROM releases
                WHERE hidden = 0
                  AND primary_artist_id IN (${[...allTrackArtistIds].join(',')})
            `)[0];
            if (arResult) arResult.values.forEach(([id]) => _artistsWithReleases.add(id));
        }

        // Load transliteration/unicode aliases to show as secondary dim lines
        let aliasesByTrack = new Map();
        if (tracks.length > 0) {
            const aliasResult = _db.exec(`
                SELECT ta.track_id, ta.alias
                FROM track_aliases ta
                WHERE ta.track_id IN (
                    SELECT id FROM tracks WHERE release_id = '${safeId}' AND hidden = 0
                )
                AND ta.alias_type IN ('transliteration', 'unicode')
                ORDER BY ta.track_id
            `)[0];
            if (aliasResult) {
                aliasResult.values.forEach(([trackId, alias]) => {
                    if (!aliasesByTrack.has(trackId)) aliasesByTrack.set(trackId, alias);
                });
            }
        }

        _renderTracklist(container, tracks, true, { showTrackArtists, artistsByTrack, primaryArtistId: _primaryArtistId, artistsWithReleases: _artistsWithReleases, aliasesByTrack });
    }

    // ── Release variants ────────────────────────────────────────────────────────

    function loadVariants() {
        const safeId = _releaseId.replace(/'/g, "''");

        const varResult = _db.exec(`
            SELECT rv.variant_id, rv.variant_type, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url, r.release_year, r.hidden
            FROM release_variants rv
            JOIN releases r ON r.id = rv.variant_id
            WHERE rv.canonical_id = '${safeId}'
            ORDER BY rv.sort_order, r.release_year
        `)[0];

        if (!varResult || varResult.values.length === 0) return;

        // Fetch canonical tracks once for all variant comparisons
        const canonResult = _db.exec(`
            SELECT t.title, t.isrc
            FROM tracks t
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
        `)[0];
        const canonTracks = (canonResult ? canonResult.values : [])
            .map(([title, isrc]) => ({ title, isrc }));

        const section = document.getElementById('variantsSection');
        if (!section) return;

        // Accumulate shown tracks across variants so the same track isn't shown twice
        const shownIsrcs  = new Set(canonTracks.map(t => t.isrc).filter(Boolean));
        const shownTitles = new Set(canonTracks.map(t => _normTitle(t.title)));

        for (const [variantId, variantType, variantTitle, artUrl, year, variantHidden] of varResult.values) {
            const safeVarId = variantId.replace(/'/g, "''");

            const vtResult = _db.exec(`
                SELECT t.title, t.id, t.track_number, t.disc_number, t.duration_ms, t.isrc,
                       COUNT(l.id) as play_count
                FROM tracks t
                LEFT JOIN listens l ON l.track_id = t.id
                WHERE t.release_id = '${safeVarId}' AND t.hidden = 0
                GROUP BY t.id
                ORDER BY t.disc_number, t.track_number, t.title
            `)[0];

            const variantTracks = (vtResult ? vtResult.values : []).map(
                ([title, id, trackNumber, discNumber, durationMs, isrc, playCount]) =>
                    ({ title, id, trackNumber, discNumber, durationMs, isrc, playCount })
            );

            const exclusive = variantTracks.filter(t =>
                !(t.isrc && shownIsrcs.has(t.isrc)) && !shownTitles.has(_normTitle(t.title))
            );

            // Add this variant's exclusive tracks to the cumulative shown set
            exclusive.forEach(t => {
                if (t.isrc) shownIsrcs.add(t.isrc);
                shownTitles.add(_normTitle(t.title));
            });

            // Nothing new to show — skip rendering this variant entirely
            if (exclusive.length === 0) continue;

            const wrap     = document.createElement('section');
            wrap.className = 'variant-section';

            const typeBadge = variantType
                ? `<span class="variant-type-badge">${escapeHtml(variantType.toUpperCase())}</span>`
                : '';
            const artStyle = artUrl
                ? `background-image: url('${artUrl}'); background-size: cover; background-position: center;`
                : '';

            wrap.innerHTML = `
                <div class="variant-header">
                    <div class="variant-art" style="${artStyle}"></div>
                    <div class="variant-header-info">
                        <h3>
                            ${variantHidden
                                ? escapeHtml(variantTitle)
                                : `<a href="?view=release&id=${encodeURIComponent(variantId)}">${escapeHtml(variantTitle)}</a>`}
                        </h3>
                        <span class="variant-meta">
                            ${typeBadge}
                            ${year ? `<span class="variant-year">${year}</span>` : ''}
                        </span>
                    </div>
                </div>
                <div class="tracklist variant-tracklist" id="vt-${variantId}"></div>
            `;
            section.appendChild(wrap);

            const trackContainer = document.getElementById(`vt-${variantId}`);
            if (exclusive.length === 0) {
                trackContainer.innerHTML = '<div class="tracklist-empty">No additional tracks</div>';
            } else {
                _renderTracklist(trackContainer, exclusive, true);
            }
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

        for (const [sourceId, discNumber, title, artUrl, year, totalListens, totalMinutes]
                of result.values) {
            const discLabel = discNumber != null ? `Disc ${discNumber}` : null;
            const card = createWideCard({
                href:         `?view=release&id=${encodeURIComponent(sourceId)}`,
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
            SELECT rv.canonical_id, r.title, rv.variant_type
            FROM release_variants rv
            JOIN releases r ON r.id = rv.canonical_id
            WHERE rv.variant_id = '${safeId}'
            LIMIT 1
        `)[0];

        if (!result || result.values.length === 0) return;

        const [canonicalId, canonicalTitle, variantType] = result.values[0];
        const typeLabel = variantType ? `${variantType} of` : 'edition of';

        const artistContainer = document.querySelector('.release-artist');
        if (!artistContainer) return;

        const p     = document.createElement('p');
        p.className = 'variant-backlink';
        p.innerHTML = `A ${escapeHtml(typeLabel)} <a href="?view=release&id=${encodeURIComponent(canonicalId)}">${escapeHtml(canonicalTitle)}</a>`;
        artistContainer.insertAdjacentElement('afterend', p);
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
            if (el) el.innerHTML = '<div class="loading">No listening history found</div>';
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
        modal.innerHTML = `<div class="art-modal-inner"><img src="${url}" alt=""></div>`;
        document.body.appendChild(modal);
        const close = () => modal.remove();
        modal.addEventListener('click', close);
        document.addEventListener('keydown', function onKey(e) {
            if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); }
        });
    }
})();
