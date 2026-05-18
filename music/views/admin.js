const ViewAdmin = (() => {
    const ROW_H = 52;
    const BUFFER = 12;

    const LANGS = [
        { code: 'en',           name: 'English' },
        { code: 'hi',           name: 'Hindi' },
        { code: 'ta',           name: 'Tamil' },
        { code: 'te',           name: 'Telugu' },
        { code: 'bn',           name: 'Bengali' },
        { code: 'mr',           name: 'Marathi' },
        { code: 'kn',           name: 'Kannada' },
        { code: 'ml',           name: 'Malayalam' },
        { code: 'gu',           name: 'Gujarati' },
        { code: 'ur',           name: 'Urdu' },
        { code: 'ja',           name: 'Japanese' },
        { code: 'ko',           name: 'Korean' },
        { code: 'zh',           name: 'Chinese' },
        { code: 'es',           name: 'Spanish' },
        { code: 'fr',           name: 'French' },
        { code: 'de',           name: 'German' },
        { code: 'pt',           name: 'Portuguese' },
        { code: 'ru',           name: 'Russian' },
        { code: 'instrumental', name: 'Instrumental' },
    ];

    const SOURCE_TYPES = [
        { code: 'film',       name: 'Film' },
        { code: 'video_game', name: 'Video Game' },
        { code: 'tv_series',  name: 'TV Series' },
        { code: 'musical',    name: 'Musical' },
        { code: 'podcast',    name: 'Podcast' },
        { code: 'other',      name: 'Other' },
    ];

    const MEDIUMS = [
        { code: 'digital',  name: 'Digital' },
        { code: 'cd',       name: 'CD' },
        { code: 'vinyl',    name: 'Vinyl' },
        { code: 'cassette', name: 'Cassette' },
        { code: 'bluray',   name: 'Blu-ray' },
        { code: 'dvd',      name: 'DVD' },
    ];

    const COUNTRIES = [
        { code: 'US', name: 'United States' }, { code: 'GB', name: 'United Kingdom' },
        { code: 'JP', name: 'Japan' },          { code: 'KR', name: 'South Korea' },
        { code: 'IN', name: 'India' },          { code: 'DE', name: 'Germany' },
        { code: 'FR', name: 'France' },         { code: 'CA', name: 'Canada' },
        { code: 'AU', name: 'Australia' },      { code: 'BR', name: 'Brazil' },
        { code: 'SE', name: 'Sweden' },         { code: 'NO', name: 'Norway' },
        { code: 'NL', name: 'Netherlands' },    { code: 'ES', name: 'Spain' },
        { code: 'IT', name: 'Italy' },          { code: 'MX', name: 'Mexico' },
        { code: 'CN', name: 'China' },          { code: 'TW', name: 'Taiwan' },
        { code: 'PH', name: 'Philippines' },    { code: 'PK', name: 'Pakistan' },
        { code: 'BD', name: 'Bangladesh' },     { code: 'LK', name: 'Sri Lanka' },
        { code: 'NG', name: 'Nigeria' },        { code: 'ZA', name: 'South Africa' },
        { code: 'AR', name: 'Argentina' },      { code: 'CL', name: 'Chile' },
    ];

    let _db = null;
    let _allRows = [];
    let _rows = [];
    let _pending = new Map();  // id -> {track_id?, hidden?, _trackName?, _artistName?, _artistId?, _releaseName?, _releaseId?}
    let _artistPending = new Map();
    let _releasePending = new Map();
    let _fileHandle = null;
    let _trackOpts = [];
    let _searchOpts = [];
    let _matchFilter = 'unmatched';
    let _filters = {};
    let _scrollEl = null;
    let _activeTs = null;  // { instance }
    let _container = null;
    let _onAdminKey = null;
    let _params = {};

    // -- Auth --

    async function _hashPin(pin, salt) {
        const enc = new TextEncoder();
        const key = await crypto.subtle.importKey('raw', enc.encode(pin), { name: 'PBKDF2' }, false, ['deriveBits']);
        const bits = await crypto.subtle.deriveBits(
            { name: 'PBKDF2', hash: 'SHA-256', salt: enc.encode(salt), iterations: 100000 },
            key, 256
        );
        return Array.from(new Uint8Array(bits)).map(b => b.toString(16).padStart(2, '0')).join('');
    }

    function _getStoredPin() {
        try {
            const res = _db.exec("SELECT key, value FROM settings WHERE key IN ('admin_pin_hash','admin_pin_salt')");
            if (!res.length || !res[0].values.length) return null;
            const map = Object.fromEntries(res[0].values);
            return map.admin_pin_hash && map.admin_pin_salt ? map : null;
        } catch { return null; }
    }

    // -- Data --

    function _loadRows() {
        const res = _db.exec(`
            SELECT l.id, l.timestamp, l.source,
                   l.raw_track_name, l.raw_artist_name, l.raw_album_name,
                   l.ms_played, COALESCE(l.skipped,0) as skipped,
                   COALESCE(l.hidden,0) as hidden, l.track_id,
                   t.title as matched_track_name, t.language as matched_track_language,
                   r.id as matched_release_id, r.title as matched_release_name,
                   a.id as matched_artist_id, a.name as matched_artist_name
            FROM listens l
            LEFT JOIN tracks t ON l.track_id = t.id
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_id = a.id
            ORDER BY l.timestamp DESC
        `);
        if (!res.length) return [];
        const cols = res[0].columns;
        return res[0].values.map(row => Object.fromEntries(cols.map((c, i) => [c, row[i]])));
    }

    function _loadTrackOpts() {
        const res = _db.exec(`
            SELECT t.id, t.title, r.id as release_id, r.title as release_title,
                   a.id as artist_id, a.name as artist_name
            FROM tracks t
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_id = a.id
            WHERE t.hidden = 0 AND (r.hidden IS NULL OR r.hidden = 0)
            ORDER BY a.name, r.title, t.disc_number, t.track_number
        `);
        if (!res.length) return [];
        return res[0].values.map(([id, title, releaseId, releaseTitle, artistId, artistName]) => ({
            value: String(id),
            text: title,
            releaseId: releaseId ? String(releaseId) : null,
            releaseTitle: releaseTitle || '',
            artistId: artistId ? String(artistId) : null,
            artistName: artistName || '',
            searchText: `${title} ${releaseTitle || ''} ${artistName || ''}`.toLowerCase(),
        }));
    }

    function _loadSearchOpts() {
        const artistRows = _db.exec(`
            SELECT id, name FROM artists WHERE hidden=0 ORDER BY name LIMIT 500
        `)[0]?.values || [];
        const releaseRows = _db.exec(`
            SELECT r.id, r.title,
                   COALESCE(a.name || ' · ', '') || COALESCE(CAST(r.release_year AS TEXT), '')
            FROM releases r
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.hidden=0 ORDER BY a.name, r.release_year LIMIT 2000
        `)[0]?.values || [];

        _searchOpts = [
            ...artistRows.map(([id, name]) => ({
                value: `artist:${id}`, text: name, sub: '', entityType: 'artist', entityId: id,
                searchText: name.toLowerCase(),
            })),
            ...releaseRows.map(([id, title, sub]) => ({
                value: `release:${id}`, text: title, sub: sub || '', entityType: 'release', entityId: id,
                searchText: `${title} ${sub || ''}`.toLowerCase(),
            })),
        ];
    }

    // -- Filtering --

    function _applyFilters() {
        _rows = _allRows.filter(row => {
            const pending = _pending.get(row.id) || {};
            const trackId = 'track_id' in pending ? pending.track_id : row.track_id;

            if (_matchFilter === 'matched' && !trackId) return false;
            if (_matchFilter === 'unmatched' && trackId) return false;

            for (const [col, val] of Object.entries(_filters)) {
                if (!val) continue;
                const cell = String(row[col] ?? '').toLowerCase();
                // Space-separated terms; prefix with - for negation
                const terms = val.trim().split(/\s+/);
                for (const term of terms) {
                    if (!term) continue;
                    if (term.startsWith('-')) {
                        const neg = term.slice(1).toLowerCase();
                        if (neg && cell.includes(neg)) return false;
                    } else {
                        if (!cell.includes(term.toLowerCase())) return false;
                    }
                }
            }
            return true;
        });

        const countEl = document.getElementById('adminRowCount');
        if (countEl) countEl.textContent = `${_rows.length.toLocaleString()} listens`;
        if (_scrollEl) _scrollEl.scrollTop = 0;
        _renderWindow();
    }

    let _raf = null;

    // -- Virtual scroll --

    function _renderWindow() {
        if (!_scrollEl) return;
        // Capture scrollTop before any DOM mutations — reflow from innerHTML='' can shift it
        const scrollTop = _scrollEl.scrollTop;
        const start = Math.max(0, Math.floor(scrollTop / ROW_H) - BUFFER);
        const end = Math.min(_rows.length, start + Math.ceil(_scrollEl.clientHeight / ROW_H) + BUFFER * 2);

        const tbody = document.getElementById('adminTbody');
        tbody.innerHTML = '';
        for (let i = start; i < end; i++) tbody.appendChild(_buildRow(_rows[i]));

        // Update spacers after content so total table height stays stable during mutation
        document.getElementById('adminSpacerTop').style.height = `${start * ROW_H}px`;
        document.getElementById('adminSpacerBot').style.height = `${(_rows.length - end) * ROW_H}px`;
        lucide.createIcons({ context: tbody });
    }

    function _scheduleRender() {
        if (_raf) cancelAnimationFrame(_raf);
        _raf = requestAnimationFrame(() => { _raf = null; _renderWindow(); });
    }

    // -- Formatting --

    function _fmtTs(ts) {
        return new Date(ts * 1000).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    }

    function _fmtMs(ms) {
        if (!ms) return '—';
        const s = Math.floor(ms / 1000);
        return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
    }

    // -- Row building --

    function _buildRow(row) {
        const pending = _pending.get(row.id) || {};
        const trackId = 'track_id' in pending ? pending.track_id : row.track_id;
        const hidden = 'hidden' in pending ? pending.hidden : row.hidden;
        const isMatched = !!trackId;

        const displayTrack = pending._trackName ?? row.matched_track_name ?? '';
        const displayArtist = pending._artistName ?? row.matched_artist_name ?? '';
        const artistId = pending._artistId ?? row.matched_artist_id ?? null;
        const displayRelease = pending._releaseName ?? row.matched_release_name ?? '';
        const releaseId = pending._releaseId ?? row.matched_release_id ?? null;

        const rawSub = [row.raw_artist_name, row.raw_album_name].filter(Boolean).map(escapeHtml).join(' · ') || '—';

        const artistPart = artistId
            ? `<a href="?view=artist&id=${artistId}" class="admin-link">${escapeHtml(displayArtist)}</a>`
            : (displayArtist ? escapeHtml(displayArtist) : '');
        const releasePart = releaseId
            ? `<a href="?view=release&id=${releaseId}" class="admin-link">${escapeHtml(displayRelease)}</a>`
            : (displayRelease ? escapeHtml(displayRelease) : '');
        const matchedSub = [artistPart, releasePart].filter(Boolean).join(' · ') || '<span class="admin-empty">—</span>';

        const langBadge = row.matched_track_language
            ? `<span class="admin-lang">${escapeHtml(row.matched_track_language)}</span>`
            : '';

        const pencilLink = releaseId
            ? `<a href="?view=admin&content=releases&release_id=${releaseId}" class="admin-edit-link" title="Edit release"><i data-lucide="pencil"></i></a>`
            : '';

        const tr = document.createElement('tr');
        tr.dataset.id = row.id;
        tr.className = `admin-row${hidden ? ' admin-row--hidden' : ''}${!isMatched ? ' admin-row--unmatched' : ''}`;

        tr.innerHTML = `
            <td class="admin-cell admin-cell--ts">${_fmtTs(row.timestamp)}</td>
            <td class="admin-cell"><span class="admin-source admin-source--${row.source}">${row.source}</span></td>
            <td class="admin-cell admin-cell--meta admin-cell--raw">
                <div class="admin-meta-name" title="${escapeHtml(row.raw_track_name || '')}">${escapeHtml(row.raw_track_name || '—')}</div>
                <div class="admin-meta-sub" title="${escapeHtml([row.raw_artist_name, row.raw_album_name].filter(Boolean).join(' · '))}">${rawSub}</div>
            </td>
            <td class="admin-cell admin-cell--num">${_fmtMs(row.ms_played)}</td>
            <td class="admin-cell admin-cell--center">${row.skipped ? '<span class="admin-skip" title="Skipped">↩</span>' : ''}</td>
            <td class="admin-cell admin-cell--meta">
                <div class="admin-meta-name admin-select-text">${displayTrack
                    ? `${escapeHtml(displayTrack)}${langBadge}${pencilLink}`
                    : '<span class="admin-match-prompt">match…</span>'
                }</div>
                <div class="admin-meta-sub">${matchedSub}</div>
            </td>
            <td class="admin-cell admin-cell--action">
                <button class="admin-btn admin-btn--eject" data-id="${row.id}" title="Unmatch" ${!isMatched ? 'disabled' : ''}>
                    <i data-lucide="disc-3"></i>
                </button>
            </td>
            <td class="admin-cell admin-cell--action">
                <button class="admin-btn admin-btn--hide${hidden ? ' is-active' : ''}" data-id="${row.id}" title="${hidden ? 'Unhide' : 'Hide'}">
                    <i data-lucide="${hidden ? 'eye' : 'eye-off'}"></i>
                </button>
            </td>
            <td class="admin-cell admin-cell--action">
                <button class="admin-btn admin-btn--rematch" data-id="${row.id}" title="Re-match">
                    <i data-lucide="arrow-right-left"></i>
                </button>
            </td>
        `;

        return tr;
    }

    // -- Track match modal --

    function _closeActive() {
        if (!_activeTs) return;
        _activeTs.instance.destroy();
        document.getElementById('adminMatchModal')?.remove();
        document.getElementById('adminMemberModal')?.remove();
        document.getElementById('adminTrackArtistModal')?.remove();
        _activeTs = null;
    }

    function _activateTrackSelect(cell, rowId) {
        _closeActive();

        const row = _allRows.find(r => r.id === rowId);
        if (!row) return;

        const pending = _pending.get(rowId) || {};
        const currentTrackId = 'track_id' in pending ? pending.track_id : row.track_id;

        // Count siblings with the same raw triplet
        const siblings = _allRows.filter(r =>
            r.raw_track_name === row.raw_track_name &&
            r.raw_artist_name === row.raw_artist_name &&
            r.raw_album_name === row.raw_album_name
        );
        const siblingCount = siblings.length;

        // Build modal
        const modal = document.createElement('div');
        modal.id = 'adminMatchModal';
        modal.className = 'admin-match-modal';
        modal.innerHTML = `
            <div class="admin-match-modal-inner">
                <div class="admin-match-modal-header">
                    <span class="admin-match-modal-raw">${escapeHtml(row.raw_track_name || '')}
                        ${row.raw_artist_name ? `<span class="admin-match-modal-artist">· ${escapeHtml(row.raw_artist_name)}</span>` : ''}
                    </span>
                    <button class="admin-match-modal-close" title="Cancel">✕</button>
                </div>
                <select id="adminMatchSelect"></select>
                ${siblingCount > 1 ? `
                <label class="admin-match-modal-bulk">
                    <input type="checkbox" id="adminMatchBulk" checked>
                    Apply to all ${siblingCount} listens with this exact raw track · artist · album
                </label>` : ''}
                <p class="admin-match-modal-hint">Type at least 2 characters to search</p>
            </div>
        `;
        document.body.appendChild(modal);

        modal.querySelector('.admin-match-modal-close').addEventListener('click', _closeActive);

        const sel = document.getElementById('adminMatchSelect');
        const instance = new TomSelect(sel, {
            valueField: 'value',
            labelField: 'text',
            searchField: ['text'],
            maxOptions: 50,
            create: false,
            closeAfterSelect: true,
            openOnFocus: false,
            placeholder: 'Search by track, release, or artist…',
            load(query, callback) {
                if (query.length < 2) return callback([]);
                const q = query.toLowerCase();
                callback(_trackOpts.filter(o => o.searchText.includes(q)).slice(0, 50));
            },
            render: {
                option(data) {
                    const sub = [data.releaseTitle, data.artistName].filter(Boolean).join(' · ');
                    return `<div class="admin-ts-option">
                        <span class="admin-ts-option-title">${escapeHtml(data.text)}</span>
                        ${sub ? `<span class="admin-ts-option-sub">${escapeHtml(sub)}</span>` : ''}
                    </div>`;
                },
                no_results() {
                    return `<div class="admin-ts-option admin-ts-option-sub" style="padding:10px">No results</div>`;
                },
            },
            items: currentTrackId ? [String(currentTrackId)] : [],
            onItemAdd(value) {
                const opt = _trackOpts.find(o => o.value === value);
                if (!opt) return;
                const change = {
                    track_id: value,
                    _trackName: opt.text,
                    _artistId: opt.artistId,
                    _artistName: opt.artistName,
                    _releaseId: opt.releaseId,
                    _releaseName: opt.releaseTitle,
                };

                const bulk = document.getElementById('adminMatchBulk');
                const applyBulk = bulk ? bulk.checked : false;
                const targets = applyBulk ? siblings : [row];

                for (const r of targets) {
                    _pending.set(r.id, { ...(_pending.get(r.id) || {}), ...change });
                }

                _markDirty();
                _closeActive();
                // Refresh all affected visible rows
                for (const r of targets) _refreshRow(r.id);
            },
        });

        instance.focus();
        _activeTs = { instance };
    }

    function _refreshRow(rowId) {
        const tr = document.querySelector(`tr[data-id="${rowId}"]`);
        if (!tr) return;
        const row = _allRows.find(r => r.id === rowId);
        if (!row) return;
        const newTr = _buildRow(row);
        tr.replaceWith(newTr);
        lucide.createIcons({ context: newTr });
    }

    function _markDirty() {
        const btn = document.getElementById('adminSaveBtn');
        if (btn) btn.classList.add('is-dirty');
    }

    // -- Actions --

    function _handleEject(id) {
        const p = _pending.get(id) || {};
        _pending.set(id, {
            ...p,
            track_id: null, _trackName: null,
            _artistId: null, _artistName: null,
            _releaseId: null, _releaseName: null,
        });
        _markDirty();
        _refreshRow(id);
    }

    function _handleHide(id) {
        const row = _allRows.find(r => r.id === id);
        if (!row) return;
        const p = _pending.get(id) || {};
        const current = 'hidden' in p ? p.hidden : row.hidden;
        _pending.set(id, { ...p, hidden: current ? 0 : 1 });
        _markDirty();
        _refreshRow(id);
    }

    // -- Save --

    async function _save() {
        const saveBtn = document.getElementById('adminSaveBtn');
        if (saveBtn) saveBtn.textContent = 'Saving…';

        try {
            // Collect any new alias inputs not yet committed via blur
            const activeArtistId = _params && _params.artist_id;
            if (activeArtistId) {
                document.querySelectorAll('.admin-alias-row[data-alias=""]').forEach(row => {
                    const alias = row.querySelector('.admin-alias-input')?.value.trim();
                    if (!alias) return;
                    const type = row.querySelector('.admin-alias-type')?.value || 'common';
                    const lang = row.querySelector('.admin-alias-lang')?.value.trim() || null;
                    _setArtistPending(activeArtistId, 'alias_insert', alias, type, lang);
                    // Mark the row as committed so it isn't re-collected on repeated saves
                    row.dataset.alias = alias;
                });
            }

            // Collect uncommitted track alias inputs
            const activeReleaseId = _params && _params.release_id;
            if (activeReleaseId) {
                document.querySelectorAll('.admin-track-aliases .admin-alias-row[data-alias=""]').forEach(row => {
                    const alias = row.querySelector('.admin-alias-input')?.value.trim();
                    if (!alias) return;
                    const type = row.querySelector('.admin-alias-type')?.value || 'common';
                    const lang = row.querySelector('.admin-alias-lang')?.value.trim() || null;
                    const trackId = row.querySelector('[data-track-id]')?.dataset.trackId
                        || row.closest('.admin-track-aliases')?.dataset.trackId;
                    if (!trackId) return;
                    const tp = _getTrackPending(activeReleaseId, trackId);
                    tp.aliasInserts.push({ alias, alias_type: type, language: lang });
                    row.dataset.alias = alias;
                });
            }

            for (const [id, changes] of _pending) {
                if ('track_id' in changes) {
                    _db.run('UPDATE listens SET track_id=? WHERE id=?', [changes.track_id, id]);
                }
                if ('hidden' in changes) {
                    _db.run('UPDATE listens SET hidden=? WHERE id=?', [changes.hidden, id]);
                }
            }
            _pending.clear();

            // Flush artist pending changes
            for (const [artistId, changes] of _artistPending) {
                _flushFields('artists', artistId, changes.fields,
                    ['name','sort_name','image_url','image_thumb_url','country','gender','type','hidden','notes']);
                for (const { alias, alias_type, language } of changes.aliasInserts) {
                    _db.run('INSERT OR REPLACE INTO artist_aliases (artist_id, alias, alias_norm, alias_type, language) VALUES (?,?,lower(?),?,?)',
                        [artistId, alias, alias, alias_type || 'common', language || null]);
                }
                for (const alias of changes.aliasDeletes) {
                    _db.run('DELETE FROM artist_aliases WHERE artist_id=? AND alias=?', [artistId, alias]);
                }
                for (const memberId of changes.memberInserts) {
                    _db.run('INSERT OR IGNORE INTO artist_members (group_artist_id, member_artist_id) VALUES (?,?)', [artistId, memberId]);
                }
                for (const memberId of changes.memberDeletes) {
                    _db.run('DELETE FROM artist_members WHERE group_artist_id=? AND member_artist_id=?', [artistId, memberId]);
                }
            }
            _artistPending.clear();

            // Flush release pending changes
            for (const [releaseId, rp] of _releasePending) {
                // Release fields
                _flushFields('releases', releaseId, rp.fields,
                    ['title','release_date','type','type_secondary','label','hidden','notes','album_art_url','album_art_thumb_url','spotify_id','mbid','apple_music_id','aoty_id']);
                // Soundtrack meta (separate table)
                if (rp.soundtrackMeta && Object.keys(rp.soundtrackMeta).length) {
                    // Read current row to merge (INSERT OR REPLACE overwrites all columns)
                    const cur = _db.exec(
                        'SELECT source_type, industry_region, original_language FROM release_soundtrack_meta WHERE release_id=?',
                        [releaseId]
                    );
                    const existing = cur.length && cur[0].values.length
                        ? Object.fromEntries(cur[0].columns.map((c, i) => [c, cur[0].values[0][i]]))
                        : {};
                    const merged = { ...existing, ...rp.soundtrackMeta };
                    _db.run(
                        `INSERT OR REPLACE INTO release_soundtrack_meta (release_id, source_type, industry_region, original_language) VALUES (?,?,?,?)`,
                        [releaseId, merged.source_type ?? null, merged.industry_region ?? null, merged.original_language ?? null]
                    );
                }
                // Track changes
                for (const [trackId, tp] of Object.entries(rp.tracks)) {
                    _flushFields('tracks', trackId, tp.fields,
                        ['title','track_number','disc_number','duration_ms','tempo_bpm','hidden','mix_name','variant_section','language']);
                    for (const { artist_id, role } of tp.artistInserts) {
                        _db.run('INSERT OR IGNORE INTO track_artists (track_id, artist_id, role) VALUES (?,?,?)',
                            [trackId, artist_id, role]);
                    }
                    for (const { artist_id, role } of tp.artistDeletes) {
                        _db.run('DELETE FROM track_artists WHERE track_id=? AND artist_id=? AND role=?',
                            [trackId, artist_id, role]);
                    }
                    for (const { alias, alias_type, language } of tp.aliasInserts) {
                        _db.run('INSERT OR REPLACE INTO track_aliases (track_id, alias, alias_norm, alias_type, language) VALUES (?,?,lower(?),?,?)',
                            [trackId, alias, alias, alias_type || 'common', language || null]);
                    }
                    for (const alias of tp.aliasDeletes) {
                        _db.run('DELETE FROM track_aliases WHERE track_id=? AND alias=?', [trackId, alias]);
                    }
                }
            }
            _releasePending.clear();

            const content = _params.content || 'listens';
            if (content === 'listens') {
                _allRows = _loadRows();
                _applyFilters();
            }
            _loadSearchOpts();

            const data = _db.export();
            const blob = new Blob([data], { type: 'application/octet-stream' });

            if (window.showSaveFilePicker) {
                try {
                    if (!_fileHandle) {
                        _fileHandle = await window.showSaveFilePicker({
                            suggestedName: 'master.sqlite',
                            types: [{ description: 'SQLite Database', accept: { 'application/octet-stream': ['.sqlite'] } }],
                        });
                    }
                    const writable = await _fileHandle.createWritable();
                    await writable.write(blob);
                    await writable.close();
                    if (saveBtn) { saveBtn.textContent = 'Saved'; saveBtn.classList.remove('is-dirty'); }
                    setTimeout(() => { if (saveBtn) saveBtn.textContent = 'Save'; }, 1500);
                    return;
                } catch (e) {
                    if (e.name === 'AbortError') { if (saveBtn) saveBtn.textContent = 'Save'; return; }
                }
            }

            const url = URL.createObjectURL(blob);
            Object.assign(document.createElement('a'), { href: url, download: 'master.sqlite' }).click();
            URL.revokeObjectURL(url);
            if (saveBtn) { saveBtn.textContent = 'Downloaded'; saveBtn.classList.remove('is-dirty'); }
            setTimeout(() => { if (saveBtn) saveBtn.textContent = 'Save'; }, 1500);
        } catch (err) {
            console.error('Save failed', err);
            if (saveBtn) saveBtn.textContent = 'Save failed';
        }
    }

    // -- Toolbar helpers --

    function _makeTomSelect(el, items, value, wrapperClass = 'ts-uline') {
        if (!el || el.tomselect) return;
        const ts = new TomSelect(el, {
            valueField: 'code', labelField: 'name', searchField: ['name', 'code'],
            maxItems: 1, create: false, allowEmptyOption: true,
            placeholder: '—',
            wrapperClass: `ts-wrapper ${wrapperClass}`,
            controlClass: 'ts-control',
            dropdownClass: 'ts-dropdown',
            render: {
                item:   d => `<span title="${escapeHtml(d.name)}">${escapeHtml(d.code || '—')}</span>`,
                option: d => `<div class="admin-ts-option"><span class="admin-ts-option-title">${escapeHtml(d.name)}</span>${d.code ? `<span class="admin-ts-option-sub">${escapeHtml(d.code)}</span>` : ''}</div>`,
            },
            options: [{ code: '', name: '—' }, ...items],
            items: value ? [value] : [''],
        });
        return ts;
    }

function _jsonp(url) {
        return new Promise((resolve, reject) => {
            const cb = '_itunes_' + Date.now();
            window[cb] = data => { delete window[cb]; script.remove(); resolve(data); };
            const script = Object.assign(document.createElement('script'), {
                src: `${url}&callback=${cb}`,
                onerror: () => { delete window[cb]; script.remove(); reject(new Error('JSONP failed')); },
            });
            document.head.appendChild(script);
        });
    }

    async function _fetchAppleArt(appleId) {
        const data = await _jsonp(
            `https://itunes.apple.com/lookup?id=${encodeURIComponent(appleId)}&entity=album&country=us`
        );
        const art = data.results?.[0]?.artworkUrl100;
        if (!art) return null;
        // Replace the trailing size specifier (e.g. 100x100bb) with 3000x3000bb
        return art.replace(/\d+x\d+bb\.jpg$/, '3000x3000bb.jpg');
    }

function _flushFields(table, id, fields, allowedCols) {
        const setClauses = [], vals = [];
        for (const f of allowedCols) {
            if (f in fields) {
                setClauses.push(`${f}=?`);
                vals.push(fields[f] === '' ? null : fields[f]);
            }
        }
        if (setClauses.length) {
            vals.push(id);
            _db.run(`UPDATE ${table} SET ${setClauses.join(',')} WHERE id=?`, vals);
        }
    }

    function _buildToolbarHtml(activeTab) {
        if (activeTab !== 'listens') return '';
        return `
            <div class="admin-toolbar">
                <div class="admin-toggle-group">
                    <button class="admin-toggle-btn${_matchFilter === 'all' ? ' is-active' : ''}" data-filter="all">All</button>
                    <button class="admin-toggle-btn${_matchFilter === 'matched' ? ' is-active' : ''}" data-filter="matched">Matched</button>
                    <button class="admin-toggle-btn${_matchFilter === 'unmatched' ? ' is-active' : ''}" data-filter="unmatched">Unmatched</button>
                </div>
                <span id="adminRowCount" class="admin-row-count"></span>
                <div class="admin-filter-sep"></div>
                <div class="admin-filter-group">
                    <span class="admin-filter-group-label">Raw</span>
                    <input class="admin-filter-uline" data-col="raw_track_name"     placeholder="track"   value="${escapeHtml(_filters['raw_track_name']    || '')}">
                    <input class="admin-filter-uline" data-col="raw_artist_name"    placeholder="artist"  value="${escapeHtml(_filters['raw_artist_name']   || '')}">
                    <input class="admin-filter-uline" data-col="raw_album_name"     placeholder="album"   value="${escapeHtml(_filters['raw_album_name']    || '')}">
                </div>
                <div class="admin-filter-sep"></div>
                <div class="admin-filter-group">
                    <span class="admin-filter-group-label">Matched</span>
                    <input class="admin-filter-uline" data-col="matched_track_name"   placeholder="track"   value="${escapeHtml(_filters['matched_track_name']   || '')}">
                    <input class="admin-filter-uline" data-col="matched_artist_name"  placeholder="artist"  value="${escapeHtml(_filters['matched_artist_name']  || '')}">
                    <input class="admin-filter-uline" data-col="matched_release_name" placeholder="release" value="${escapeHtml(_filters['matched_release_name'] || '')}">
                </div>
            </div>
        `;
    }

    function _injectHeaderSlot(activeTab) {
        // Remove any existing slot first
        document.getElementById('adminHeaderSlot')?.remove();
        document.getElementById('adminSaveBtn')?.remove();

        const tabs = [
            ['listens', 'Listens'],
            ['artists', 'Artists'],
            ['releases', 'Releases'],
        ];
        const searchBtn = document.getElementById('searchBtn');
        if (!searchBtn) return;

        // Tab slot — inserted before searchBtn
        const slot = document.createElement('div');
        slot.id = 'adminHeaderSlot';
        slot.className = 'admin-header-slot';
        slot.innerHTML = tabs.map(([t, label]) =>
            `<a class="admin-tab${t === activeTab ? ' is-active' : ''}" href="?view=admin&content=${t}">${label}</a>`
        ).join('');
        searchBtn.parentNode.insertBefore(slot, searchBtn);

        // Save button — inserted immediately before searchBtn
        const saveBtn = document.createElement('button');
        saveBtn.id = 'adminSaveBtn';
        saveBtn.className = 'admin-save-btn';
        saveBtn.textContent = 'Save';
        searchBtn.parentNode.insertBefore(saveBtn, searchBtn);
        saveBtn.addEventListener('click', _save);
    }

    function _wireToolbar(container) {
        _injectHeaderSlot((_params && _params.content) || 'listens');
        requestAnimationFrame(() => {
            const h = document.querySelector('.site-header-sticky')?.offsetHeight;
            if (h) document.documentElement.style.setProperty('--header-height', `${h}px`);
        });
        _onAdminKey = function(e) {
            if (e.key === 'Escape') _closeActive();
        };
        document.addEventListener('keydown', _onAdminKey);
    }

    // -- Table mount --

    function _mountTable(container, params) {
        document.title = 'aswin.db/music – Admin';

        _allRows = _loadRows();
        _trackOpts = _loadTrackOpts();
        _loadSearchOpts();

        const filterCols = [
            ['raw_track_name', 'raw track'],
            ['raw_artist_name', 'raw artist'],
            ['raw_album_name', 'raw album'],
            ['matched_track_name', 'matched track'],
            ['matched_artist_name', 'matched artist'],
            ['matched_release_name', 'matched release'],
        ];

        container.innerHTML = `
            <div class="admin-wrap">
                ${_buildToolbarHtml('listens')}
                <div class="admin-scroll" id="adminScroll">
                    <table class="admin-table">
                        <thead>
                            <tr>
                                <th>Date</th>
                                <th>Src</th>
                                <th>Raw Metadata</th>
                                <th>Dur</th>
                                <th title="Skipped">↩</th>
                                <th>Matched Metadata</th>
                                <th></th>
                                <th></th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody><tr><td id="adminSpacerTop" colspan="9" style="padding:0;height:0"></td></tr></tbody>
                        <tbody id="adminTbody"></tbody>
                        <tbody><tr><td id="adminSpacerBot" colspan="9" style="padding:0;height:0"></td></tr></tbody>
                    </table>
                </div>
            </div>
        `;

        _wireToolbar(container);

        _scrollEl = document.getElementById('adminScroll');
        container.querySelector('.admin-wrap')?.classList.add('admin-wrap--listens');
        _applyFilters();

        _scrollEl.addEventListener('scroll', _scheduleRender, { passive: true });

        container.querySelector('.admin-toggle-group').addEventListener('click', e => {
            const btn = e.target.closest('[data-filter]');
            if (!btn) return;
            _matchFilter = btn.dataset.filter;
            container.querySelectorAll('.admin-toggle-btn').forEach(b => b.classList.toggle('is-active', b === btn));
            _applyFilters();
        });

        container.querySelector('.admin-toolbar').addEventListener('input', e => {
            const col = e.target.dataset.col;
            if (col) { _filters[col] = e.target.value; _applyFilters(); }
        });

        container.addEventListener('click', e => {
            if (e.target.closest('.admin-btn--eject:not([disabled])')) {
                _handleEject(Number(e.target.closest('.admin-btn--eject').dataset.id));
                return;
            }
            if (e.target.closest('.admin-btn--hide')) {
                _handleHide(Number(e.target.closest('.admin-btn--hide').dataset.id));
                return;
            }
            if (e.target.closest('.admin-btn--rematch')) {
                _activateTrackSelect(null, Number(e.target.closest('.admin-btn--rematch').dataset.id));
                return;
            }
            if (_activeTs && !e.target.closest('#adminMatchModal') && !e.target.closest('#adminSearchModal')) {
                _closeActive();
            }
        });
    }

    // -- Artist pending helpers --

    function _getArtistPending(artistId) {
        if (!_artistPending.has(artistId)) {
            _artistPending.set(artistId, { fields: {}, aliasInserts: [], aliasDeletes: [], memberInserts: [], memberDeletes: [] });
        }
        return _artistPending.get(artistId);
    }

    function _setArtistPending(artistId, type, ...args) {
        const p = _getArtistPending(artistId);
        if (type === 'field') {
            const [field, value] = args;
            p.fields[field] = value;
        } else if (type === 'alias_insert') {
            const [alias, alias_type, language] = args;
            p.aliasInserts.push({ alias, alias_type, language });
        } else if (type === 'alias_delete') {
            const [alias] = args;
            p.aliasDeletes.push(alias);
            p.aliasInserts = p.aliasInserts.filter(a => a.alias !== alias);
        } else if (type === 'member_insert') {
            const [memberId] = args;
            if (!p.memberInserts.includes(memberId)) p.memberInserts.push(memberId);
            p.memberDeletes = p.memberDeletes.filter(id => id !== memberId);
        } else if (type === 'member_delete') {
            const [memberId] = args;
            p.memberDeletes.push(memberId);
            p.memberInserts = p.memberInserts.filter(id => id !== memberId);
        }
    }

    // -- Artist panel mount --

    function _mountArtistPanel(container, params) {
        document.title = 'aswin.db/music – Admin · Artists';
        _loadSearchOpts();

        container.innerHTML = `
            <div class="admin-wrap">
                ${_buildToolbarHtml('artists')}
                <div id="adminEditorContent"></div>
            </div>
        `;

        _wireToolbar(container);

        if (params.artist_id) {
            _loadArtistEditor(params.artist_id);
        } else {
            document.getElementById('adminEditorContent').innerHTML = `
                <div class="admin-editor-empty">
                    <p>Use ⌘K to jump to an artist.</p>
                </div>
            `;
        }
    }

    // -- Artist editor --

    function _loadArtistEditor(artistId) {
        const contentEl = document.getElementById('adminEditorContent');
        if (!contentEl) return;

        // Query artist fields
        const artistRes = _db.exec(
            `SELECT id, name, sort_name, image_url, image_thumb_url,
                    country, gender, type, hidden, notes
             FROM artists WHERE id = ?`, [artistId]
        );
        if (!artistRes.length || !artistRes[0].values.length) {
            contentEl.innerHTML = `<div class="admin-editor-empty"><p>Artist not found.</p></div>`;
            return;
        }
        const cols = artistRes[0].columns;
        const artist = Object.fromEntries(cols.map((c, i) => [c, artistRes[0].values[0][i]]));

        // Query aliases
        const aliasRes = _db.exec(
            `SELECT alias, alias_type, language
             FROM artist_aliases WHERE artist_id = ?
             ORDER BY sort_order, alias`, [artistId]
        );
        const aliases = aliasRes.length
            ? aliasRes[0].values.map(([alias, alias_type, language]) => ({ alias, alias_type, language }))
            : [];

        // Query members
        const memberRes = _db.exec(
            `SELECT am.member_artist_id, a.name
             FROM artist_members am JOIN artists a ON a.id = am.member_artist_id
             WHERE am.group_artist_id = ?
             ORDER BY am.sort_order, a.name`, [artistId]
        );
        const members = memberRes.length
            ? memberRes[0].values.map(([id, name]) => ({ id, name }))
            : [];

        // Render form
        contentEl.innerHTML = `
            <div class="admin-editor">
                <div class="admin-editor-section">
                    <p class="admin-editor-title">${escapeHtml(artist.name)}</p>
                </div>

                <div class="admin-editor-section">
                    <h3>Identity</h3>
                    <div class="admin-field-row"><label class="admin-field-label">Name</label><input class="admin-uline" data-field="name" value="${escapeHtml(artist.name || '')}"></div>
                    <div class="admin-field-row"><label class="admin-field-label">Sort name</label><input class="admin-uline" data-field="sort_name" value="${escapeHtml(artist.sort_name || '')}"></div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Type</label>
                        <select class="admin-uline" data-field="type">
                            <option value="">—</option>
                            ${['Person','Group','Orchestra','Choir','Character','Other'].map(v =>
                                `<option${artist.type === v ? ' selected' : ''}>${v}</option>`
                            ).join('')}
                        </select>
                    </div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Gender</label>
                        <select class="admin-uline" data-field="gender">
                            <option value="">—</option>
                            ${['Male','Female','Other'].map(v =>
                                `<option${artist.gender === v ? ' selected' : ''}>${v}</option>`
                            ).join('')}
                        </select>
                    </div>
                    <div class="admin-field-row"><label class="admin-field-label">Country</label><input class="admin-uline" data-field="country" maxlength="2" value="${escapeHtml(artist.country || '')}"></div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Hidden</label>
                        <input type="checkbox" data-field="hidden"${artist.hidden ? ' checked' : ''}>
                    </div>
                </div>

                <div class="admin-editor-section">
                    <h3>Images</h3>
                    <div class="admin-field-row"><label class="admin-field-label">Full URL</label><input class="admin-uline" data-field="image_url" value="${escapeHtml(artist.image_url || '')}"></div>
                    <div class="admin-field-row"><label class="admin-field-label">Thumb URL</label><input class="admin-uline" data-field="image_thumb_url" value="${escapeHtml(artist.image_thumb_url || '')}"></div>
                </div>

                <div class="admin-editor-section">
                    <h3>Notes</h3>
                    <textarea class="admin-uline admin-uline-textarea" data-field="notes">${escapeHtml(artist.notes || '')}</textarea>
                </div>

                <div class="admin-editor-section" id="artistAliasSection">
                    <h3>Aliases <button class="admin-add-btn" id="addAliasBtn">+ Add</button></h3>
                    <div id="artistAliasList">
                        ${aliases.map(a => _renderAliasRow(a.alias, a.alias_type, a.language)).join('')}
                    </div>
                </div>

                <div class="admin-editor-section" id="artistMemberSection">
                    <h3>Members <button class="admin-add-btn" id="addMemberBtn">+ Add</button></h3>
                    <div id="artistMemberList" class="admin-chip-list">
                        ${members.map(m => `
                            <div class="admin-chip" data-member-id="${m.id}">
                                ${escapeHtml(m.name)}
                                <span class="admin-chip-remove" data-remove-member="${m.id}">×</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;

        // Wire up field change tracking
        const editorEl = contentEl.querySelector('.admin-editor');
        editorEl.addEventListener('input', e => {
            const field = e.target.dataset.field;
            if (!field) return;
            const value = e.target.type === 'checkbox' ? (e.target.checked ? 1 : 0) : e.target.value;
            _setArtistPending(artistId, 'field', field, value);
            _markDirty();
        });

        // Add alias button
        document.getElementById('addAliasBtn').addEventListener('click', () => {
            const list = document.getElementById('artistAliasList');
            const row = document.createElement('div');
            row.className = 'admin-alias-row';
            row.dataset.alias = '';
            row.innerHTML = `
                <input class="admin-field-input admin-alias-input" value="" data-orig="" placeholder="Alias name">
                <select class="admin-field-input admin-alias-type">
                    <option selected>common</option>
                    <option>past_name</option>
                    <option>native_script</option>
                    <option>transliteration</option>
                </select>
                <input class="admin-field-input admin-alias-lang" placeholder="lang" value="">
                <button class="admin-remove-btn" data-remove-alias="">×</button>
            `;
            list.appendChild(row);
            row.querySelector('.admin-alias-input').focus();
            // Track the new alias on blur
            const input = row.querySelector('.admin-alias-input');
            input.addEventListener('blur', () => {
                const alias = input.value.trim();
                if (!alias) return;
                const type = row.querySelector('.admin-alias-type').value;
                const lang = row.querySelector('.admin-alias-lang').value.trim();
                row.dataset.alias = alias;
                row.querySelector('[data-remove-alias]').dataset.removeAlias = alias;
                _setArtistPending(artistId, 'alias_insert', alias, type, lang);
                _markDirty();
            });
        });

        // Remove alias (delegated)
        document.getElementById('artistAliasList').addEventListener('click', e => {
            const btn = e.target.closest('[data-remove-alias]');
            if (!btn) return;
            const alias = btn.dataset.removeAlias;
            if (alias) _setArtistPending(artistId, 'alias_delete', alias);
            btn.closest('.admin-alias-row')?.remove();
            _markDirty();
        });

        // Add member button
        document.getElementById('addMemberBtn').addEventListener('click', () => {
            _openMemberSearch(artistId);
        });

        // Remove member (delegated)
        document.getElementById('artistMemberList').addEventListener('click', e => {
            const btn = e.target.closest('[data-remove-member]');
            if (!btn) return;
            const memberId = btn.dataset.removeMember;
            _setArtistPending(artistId, 'member_delete', memberId);
            btn.closest('.admin-chip')?.remove();
            _markDirty();
        });
    }

    function _renderAliasRow(alias, type, lang) {
        return `<div class="admin-alias-row" data-alias="${escapeHtml(alias)}">
            <span class="admin-alias-text">${escapeHtml(alias)}</span>
            <span class="admin-alias-type-badge">${escapeHtml(type || 'common')}</span>
            ${lang ? `<span class="admin-alias-lang-badge">${escapeHtml(lang)}</span>` : ''}
            <button class="admin-remove-btn" data-remove-alias="${escapeHtml(alias)}" title="Remove alias">×</button>
        </div>`;
    }

    function _openMemberSearch(artistId) {
        _closeActive();
        const modal = document.createElement('div');
        modal.id = 'adminMemberModal';
        modal.className = 'admin-match-modal';
        modal.innerHTML = `
            <div class="admin-match-modal-inner">
                <div class="admin-match-modal-header">
                    <span class="admin-match-modal-raw">Add member</span>
                    <button class="admin-match-modal-close" title="Cancel">✕</button>
                </div>
                <select id="adminMemberSelect"></select>
                <p class="admin-match-modal-hint">Type to search artists</p>
            </div>
        `;
        document.body.appendChild(modal);
        modal.querySelector('.admin-match-modal-close').addEventListener('click', _closeActive);

        const artistOpts = _searchOpts.filter(o => o.entityType === 'artist' && o.entityId !== artistId);
        const sel = document.getElementById('adminMemberSelect');
        const instance = new TomSelect(sel, {
            valueField: 'value',
            labelField: 'text',
            searchField: ['text'],
            maxOptions: 50,
            create: false,
            closeAfterSelect: true,
            openOnFocus: true,
            placeholder: 'Search artists…',
            load(query, callback) {
                if (!query) return callback(artistOpts.slice(0, 50));
                const q = query.toLowerCase();
                callback(artistOpts.filter(o => o.searchText.includes(q)).slice(0, 50));
            },
            render: {
                option(data) {
                    return `<div class="admin-ts-option"><span class="admin-ts-option-title">${escapeHtml(data.text)}</span></div>`;
                },
            },
            onItemAdd(value) {
                const opt = artistOpts.find(o => o.value === value);
                _closeActive();
                if (!opt) return;
                const memberId = opt.entityId;
                const memberName = opt.text;
                const list = document.getElementById('artistMemberList');
                if (list) {
                    const chip = document.createElement('div');
                    chip.className = 'admin-chip';
                    chip.dataset.memberId = memberId;
                    chip.innerHTML = `${escapeHtml(memberName)}<span class="admin-chip-remove" data-remove-member="${memberId}">×</span>`;
                    list.appendChild(chip);
                }
                _setArtistPending(artistId, 'member_insert', memberId);
                _markDirty();
            },
        });
        instance.open();
        _activeTs = { instance };
    }

    // -- Release pending helpers --

    function _getReleasePending(releaseId) {
        if (!_releasePending.has(releaseId)) {
            _releasePending.set(releaseId, { fields: {}, tracks: {}, soundtrackMeta: {} });
        }
        return _releasePending.get(releaseId);
    }

    function _getTrackPending(releaseId, trackId) {
        const rp = _getReleasePending(releaseId);
        if (!rp.tracks[trackId]) {
            rp.tracks[trackId] = { fields: {}, artistInserts: [], artistDeletes: [], aliasInserts: [], aliasDeletes: [] };
        }
        return rp.tracks[trackId];
    }

    function _fmtDuration(ms) {
        if (ms == null) return '';
        const s = Math.round(ms / 1000);
        return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
    }

    function _parseDuration(str) {
        const trimmed = str.trim();
        if (trimmed === '') return null;
        const m = trimmed.match(/^(\d+):(\d{2})$/);
        return m ? (parseInt(m[1]) * 60 + parseInt(m[2])) * 1000 : null;
    }

    // -- Release panel mount --

    function _mountReleasePanel(container, params) {
        document.title = 'aswin.db/music – Admin · Releases';
        _loadSearchOpts();

        container.innerHTML = `
            <div class="admin-wrap">
                ${_buildToolbarHtml('releases')}
                <div id="adminEditorContent"></div>
            </div>
        `;

        _wireToolbar(container);

        if (params.release_id) {
            _loadReleaseEditor(params.release_id);
        } else {
            document.getElementById('adminEditorContent').innerHTML = `
                <div class="admin-editor-empty">
                    <p>Use ⌘K to jump to a release.</p>
                </div>
            `;
        }
    }

    // -- Release editor --

    function _loadReleaseEditor(releaseId) {
        const contentEl = document.getElementById('adminEditorContent');
        if (!contentEl) return;

        // Query release fields
        const releaseRes = _db.exec(
            `SELECT id, title, release_date, type, type_secondary, label, hidden, notes,
                    album_art_url, album_art_thumb_url, album_art_source,
                    spotify_id, mbid, apple_music_id, aoty_id
             FROM releases WHERE id = ?`, [releaseId]
        );
        if (!releaseRes.length || !releaseRes[0].values.length) {
            contentEl.innerHTML = `<div class="admin-editor-empty"><p>Release not found.</p></div>`;
            return;
        }
        const cols = releaseRes[0].columns;
        const release = Object.fromEntries(cols.map((c, i) => [c, releaseRes[0].values[0][i]]));

        // Query soundtrack meta (separate table — may not have a row yet)
        const stRes = _db.exec(
            `SELECT source_type, industry_region, original_language
             FROM release_soundtrack_meta WHERE release_id = ?`, [releaseId]
        );
        const stMeta = stRes.length && stRes[0].values.length
            ? Object.fromEntries(stRes[0].columns.map((c, i) => [c, stRes[0].values[0][i]]))
            : { source_type: null, industry_region: null, original_language: null };

        // Query tracks
        const trackRes = _db.exec(
            `SELECT t.id, t.title, t.track_number, t.disc_number, t.duration_ms,
                    t.tempo_bpm, t.hidden, t.variant_section, t.mix_name, t.language
             FROM tracks t WHERE t.release_id = ?
             ORDER BY (CASE WHEN t.variant_section IS NULL THEN 0 ELSE 1 END),
                      t.variant_section,
                      t.disc_number,
                      t.track_number`, [releaseId]
        );
        const trackCols = trackRes.length ? trackRes[0].columns : [];
        const tracks = trackRes.length
            ? trackRes[0].values.map(row => Object.fromEntries(trackCols.map((c, i) => [c, row[i]])))
            : [];

        // Query track artists
        const taRes = _db.exec(
            `SELECT ta.track_id, ta.artist_id, a.name, ta.role
             FROM track_artists ta
             JOIN artists a ON a.id = ta.artist_id
             WHERE ta.track_id IN (SELECT id FROM tracks WHERE release_id = ?)
             ORDER BY ta.track_id, ta.role, a.name`, [releaseId]
        );
        const trackArtistMap = new Map();
        if (taRes.length) {
            for (const [trackId, artistId, name, role] of taRes[0].values) {
                if (!trackArtistMap.has(trackId)) trackArtistMap.set(trackId, []);
                trackArtistMap.get(trackId).push({ artistId, name, role });
            }
        }

        // Query track aliases
        const aliasRes = _db.exec(
            `SELECT ta.track_id, ta.alias, ta.alias_type, ta.language
             FROM track_aliases ta
             WHERE ta.track_id IN (SELECT id FROM tracks WHERE release_id = ?)
             ORDER BY ta.track_id`, [releaseId]
        );
        const trackAliasMap = new Map();
        if (aliasRes.length) {
            for (const [trackId, alias, aliasType, language] of aliasRes[0].values) {
                if (!trackAliasMap.has(trackId)) trackAliasMap.set(trackId, []);
                trackAliasMap.get(trackId).push({ alias, alias_type: aliasType, language });
            }
        }

        // Render form
        const artThumb = release.album_art_thumb_url || release.album_art_url || '';
        const artFull  = release.album_art_url || '';
        const artSrc   = release.album_art_source || '';

        contentEl.innerHTML = `
            <div class="admin-editor">
                <div class="admin-editor-section">
                    <p class="admin-editor-title">${escapeHtml(release.title)}</p>
                </div>

                <div class="admin-editor-section">
                    <h3>Release Info</h3>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Title</label>
                        <input class="admin-uline" data-rel-field="title" value="${escapeHtml(release.title || '')}">
                    </div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Date</label>
                        <input class="admin-uline" data-rel-field="release_date" placeholder="YYYY-MM-DD" value="${escapeHtml(release.release_date || '')}">
                    </div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Type</label>
                        <select class="admin-uline" data-rel-field="type">
                            <option value="">—</option>
                            ${['album','ep','single','broadcast','other'].map(v =>
                                `<option${release.type === v ? ' selected' : ''} value="${v}">${v}</option>`
                            ).join('')}
                        </select>
                    </div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Secondary</label>
                        <select class="admin-uline" data-rel-field="type_secondary">
                            <option value="">—</option>
                            ${['compilation','soundtrack','live','remix','dj-mix','mixtape','demo',
                               'spokenword','interview','audiobook','audio drama','field recording'].map(v =>
                                `<option${release.type_secondary === v ? ' selected' : ''} value="${v}">${v}</option>`
                            ).join('')}
                        </select>
                    </div>
                    <div class="admin-field-row" id="releaseMediaRow">
                        <label class="admin-field-label">Soundtrack</label>
                        <div style="display:flex;gap:0.5rem;min-width:0">
                            <select id="rSourceType" data-st-field="source_type"   style="flex:1.2;min-width:0"></select>
                            <select id="rRegion"     data-st-field="industry_region" style="flex:0.8;min-width:0"></select>
                            <select id="rOrigLang"   data-st-field="original_language" style="flex:0.8;min-width:0"></select>
                        </div>
                    </div>
                    <div class="admin-field-row">
                        <label class="admin-field-label">Label</label>
                        <input class="admin-uline" data-rel-field="label" value="${escapeHtml(release.label || '')}">
                    </div>
                    <div class="admin-field-row" style="align-items:center">
                        <label class="admin-field-label">Hidden</label>
                        <input type="checkbox" data-rel-field="hidden"${release.hidden ? ' checked' : ''}>
                    </div>
                </div>

                <div class="admin-editor-section">
                    <h3>Art &amp; Links</h3>
                    <div class="admin-art-links-grid">
                        <div class="admin-art-block">
                            <div class="admin-art-preview${!artThumb ? ' admin-art-preview-empty' : ''}" id="artPreviewBox">
                                ${artThumb
                                    ? `<img src="${escapeHtml(artThumb)}" alt="" onerror="this.closest('.admin-art-preview').classList.add('admin-art-preview-empty');this.remove()">`
                                    : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="m21 15-5-5L5 21"/></svg><span>No art</span>`
                                }
                            </div>
                            <div class="admin-art-fields">
                                <div class="admin-art-field-row">
                                    <span class="admin-art-field-label">Full URL</span>
                                    <input class="admin-uline" data-rel-field="album_art_url" value="${escapeHtml(artFull)}" placeholder="https://…" style="font-size:0.75rem;font-family:ui-monospace,monospace;color:var(--text-secondary)">
                                </div>
                                <div class="admin-art-field-row">
                                    <span class="admin-art-field-label">Thumb URL</span>
                                    <input class="admin-uline" data-rel-field="album_art_thumb_url" value="${escapeHtml(release.album_art_thumb_url || '')}" placeholder="Leave blank to use full" style="font-size:0.75rem;font-family:ui-monospace,monospace;color:var(--text-secondary)">
                                </div>
                                ${artSrc ? `<span class="admin-art-source">Source: ${escapeHtml(artSrc)}</span>` : ''}
                                <div class="admin-art-pull-row">
                                    <button class="admin-pull-btn" data-pull="spotify" title="${release.spotify_id ? 'Re-fetch art from Spotify' : 'No Spotify ID linked'}">↓ Spotify</button>
                                    <button class="admin-pull-btn" data-pull="apple" title="${release.apple_music_id ? 'Re-fetch art from Apple Music' : 'No Apple Music ID linked'}">↓ Apple Music</button>
                                </div>
                            </div>
                        </div>
                        <div class="admin-links-block">
                            <div class="admin-link-row">
                                <span class="admin-link-label"><span class="admin-link-dot dot-spotify"></span>Spotify</span>
                                <input class="admin-link-input" data-rel-field="spotify_id" value="${escapeHtml(release.spotify_id || '')}" placeholder="not linked">
                            </div>
                            <div class="admin-link-row">
                                <span class="admin-link-label"><span class="admin-link-dot dot-mb"></span>MusicBrainz</span>
                                <input class="admin-link-input" data-rel-field="mbid" value="${escapeHtml(release.mbid || '')}" placeholder="not linked">
                            </div>
                            <div class="admin-link-row">
                                <span class="admin-link-label"><span class="admin-link-dot dot-apple"></span>Apple Music</span>
                                <input class="admin-link-input" data-rel-field="apple_music_id" value="${escapeHtml(release.apple_music_id || '')}" placeholder="not linked">
                            </div>
                            <div class="admin-link-row">
                                <span class="admin-link-label"><span class="admin-link-dot dot-aoty"></span>AOTY ID</span>
                                <input class="admin-link-input" data-rel-field="aoty_id" value="${escapeHtml(release.aoty_id ? String(release.aoty_id) : '')}" placeholder="not linked">
                            </div>
                        </div>
                    </div>
                </div>

                <div class="admin-editor-section">
                    <h3>Notes</h3>
                    <textarea class="admin-uline admin-uline-textarea" data-rel-field="notes">${escapeHtml(release.notes || '')}</textarea>
                </div>

                <div class="admin-editor-section" id="releaseTrackSection">
                    <h3>Tracks</h3>
                    <div id="releaseTrackList">
                        ${_renderTrackList(tracks, trackArtistMap, trackAliasMap)}
                    </div>
                </div>
            </div>
        `;

        // Initialise TomSelect for soundtrack meta
        const tsSourceType = _makeTomSelect(document.getElementById('rSourceType'), SOURCE_TYPES, stMeta.source_type);
        const tsRegion     = _makeTomSelect(document.getElementById('rRegion'),     COUNTRIES,    stMeta.industry_region);
        const tsOrigLang   = _makeTomSelect(document.getElementById('rOrigLang'),   LANGS,        stMeta.original_language);

        [
            [tsSourceType, 'source_type'],
            [tsRegion,     'industry_region'],
            [tsOrigLang,   'original_language'],
        ].forEach(([ts, field]) => {
            if (!ts) return;
            ts.on('change', v => {
                _getReleasePending(releaseId).soundtrackMeta = {
                    ...(_getReleasePending(releaseId).soundtrackMeta || {}),
                    [field]: v || null,
                };
                _markDirty();
            });
        });

        // Initialise TomSelect on every track language select
        contentEl.querySelectorAll('.admin-track-lang-select').forEach(sel => {
            const current = sel.value;
            _makeTomSelect(sel, LANGS, current, 'ts-lang-tag');
        });

        // Wire release-level field changes
        const editorEl = contentEl.querySelector('.admin-editor');
        editorEl.addEventListener('input', e => {
            // variant section rename — bulk update all tracks in section
            if (e.target.classList.contains('admin-variant-name-input')) {
                const oldSection = e.target.dataset.variantSection;
                const newSection = e.target.value;
                // Queue a rename for all tracks with this variant_section
                tracks.filter(t => t.variant_section === oldSection).forEach(t => {
                    _getTrackPending(releaseId, t.id).fields['variant_section'] = newSection;
                });
                e.target.dataset.variantSection = newSection;
                _markDirty();
                return;
            }
            if (e.target.closest('#releaseTrackList')) return;
            const f = e.target.dataset.relField;
            if (!f) return;
            const v = e.target.type === 'checkbox' ? (e.target.checked ? 1 : 0) : e.target.value;
            _getReleasePending(releaseId).fields[f] = v;
            _markDirty();
        });

        // Pull buttons
        editorEl.addEventListener('click', async e => {
            const pullBtn = e.target.closest('[data-pull]');
            if (!pullBtn) return;

            if (pullBtn.dataset.pull === 'spotify') {
                alert(`Spotify art fetching requires the CLI:\npython mdb.py enrich art --release-id ${releaseId}`);
                return;
            }

            // Apple Music
            const appleId = release.apple_music_id;
            if (!appleId) {
                alert('No Apple Music ID linked for this release. Add one in the Links section first.');
                return;
            }

            pullBtn.textContent = '…';
            pullBtn.disabled = true;
            try {
                const url = await _fetchAppleArt(appleId);
                if (!url) throw new Error('No artwork found');

                // Populate the Full URL field and pending
                const fullInput = editorEl.querySelector('[data-rel-field="album_art_url"]');
                if (fullInput) {
                    fullInput.value = url;
                    _getReleasePending(releaseId).fields['album_art_url'] = url;
                }

                // Update the preview box
                const preview = document.getElementById('artPreviewBox');
                if (preview) {
                    preview.classList.remove('admin-art-preview-empty');
                    preview.innerHTML = `<img src="${url}" alt="">`;
                }

                _markDirty();
                pullBtn.textContent = '✓';
                setTimeout(() => { pullBtn.textContent = '↓ Apple Music'; pullBtn.disabled = false; }, 1500);
            } catch (err) {
                pullBtn.textContent = '✕ Failed';
                pullBtn.disabled = false;
                setTimeout(() => { pullBtn.textContent = '↓ Apple Music'; }, 2000);
            }
        });

        // Wire track-level changes
        const trackList = document.getElementById('releaseTrackList');

        trackList.addEventListener('input', e => {
            const row = e.target.closest('.admin-track-row-grid');
            if (!row) return;
            const trackId = row.dataset.trackId;
            const field = e.target.dataset.trackField;
            if (!field) return;
            let value = e.target.type === 'checkbox' ? (e.target.checked ? 1 : 0) : e.target.value;
            if (field === 'duration_ms') {
                const parsed = _parseDuration(value);
                if (parsed !== null) value = parsed;
                else return;
            }
            if (field === 'tempo_bpm') value = value ? parseFloat(value) : null;
            _getTrackPending(releaseId, trackId).fields[field] = value;
            _markDirty();
        });

        // Language select (fires change, not input)
        trackList.addEventListener('change', e => {
            const row = e.target.closest('.admin-track-row-grid');
            if (!row) return;
            const trackId = row.dataset.trackId;
            const field = e.target.dataset.trackField;
            if (!field) return;
            const value = e.target.value || null;
            _getTrackPending(releaseId, trackId).fields[field] = value;
            _markDirty();
        });

        // Duration + BPM blur: reformat and validate
        trackList.addEventListener('blur', e => {
            if (e.target.dataset.trackField === 'tempo_bpm') {
                const v = parseFloat(e.target.value);
                if (!e.target.value.trim() || v === 0) {
                    e.target.value = '';
                    _getTrackPending(releaseId, e.target.closest('.admin-track-row-grid')?.dataset.trackId || '').fields['tempo_bpm'] = null;
                    e.target.classList.remove('warn');
                } else {
                    e.target.classList.toggle('warn', v > 300 || v < 20);
                }
            }
            if (e.target.dataset.trackField === 'duration_ms') {
                const parsed = _parseDuration(e.target.value);
                if (parsed === null) {
                    e.target.value = '';
                } else if (parsed > 99 * 60 * 1000 + 59 * 1000) {
                    e.target.classList.add('warn');
                } else {
                    e.target.classList.remove('warn');
                    e.target.value = _fmtDuration(parsed);
                }
            }
        }, true);

        // Track artist chips: remove and add + eye button + aliases button
        trackList.addEventListener('click', e => {
            const removeKey = e.target.dataset.removeTrackArtist;
            if (removeKey) {
                const [trackId, artistId, role] = removeKey.split(':');
                const tp = _getTrackPending(releaseId, trackId);
                tp.artistDeletes.push({ artist_id: artistId, role });
                tp.artistInserts = tp.artistInserts.filter(a => !(a.artist_id === artistId && a.role === role));
                e.target.closest('.admin-chip')?.remove();
                _markDirty();
                return;
            }
            // Add artist button
            const addBtn = e.target.closest('.admin-add-artist-btn');
            if (addBtn) {
                _openTrackArtistSearch(releaseId, addBtn.dataset.trackId, addBtn.dataset.role);
                return;
            }
            // Eye button
            const eyeBtn = e.target.closest('.admin-eye-btn');
            if (eyeBtn) {
                const trackId = eyeBtn.dataset.trackId;
                const row = eyeBtn.closest('.admin-track-row-grid');
                const isHidden = !eyeBtn.classList.contains('eye-off');
                eyeBtn.classList.toggle('eye-off', isHidden);
                row?.classList.toggle('track-hidden', isHidden);
                const eyeVisible = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`;
                const eyeOff = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/><path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/><line x1="1" y1="1" x2="23" y2="23"/></svg>`;
                eyeBtn.innerHTML = isHidden ? eyeOff : eyeVisible;
                _getTrackPending(releaseId, trackId).fields['hidden'] = isHidden ? 1 : 0;
                _markDirty();
                return;
            }
            // Aliases button
            const aliasesBtn = e.target.closest('.admin-aliases-btn');
            if (aliasesBtn) {
                const trackId = aliasesBtn.dataset.trackId;
                const trackRow = aliasesBtn.closest('.admin-track-row-grid');
                const trackTitle = trackRow?.querySelector('.admin-track-title-input')?.value || trackId;
                _openTrackAliasModal(releaseId, trackId, trackTitle);
                return;
            }
        });
    }

    function _renderTrackList(tracks, trackArtistMap, trackAliasMap) {
        if (!tracks.length) return '<p style="color:var(--text-tertiary);font-size:0.82rem">No tracks.</p>';

        const eyeIconSvg = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:11px;height:11px;opacity:0.4"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`;

        const headers = `
            <div class="admin-track-col-headers">
                <span class="admin-col-hdr right">#</span>
                <span class="admin-col-hdr">Title / ETI</span>
                <span class="admin-col-hdr center">Dur</span>
                <span class="admin-col-hdr center">BPM</span>
                <span class="admin-col-hdr center">Lang</span>
                <span class="admin-col-hdr">Artists</span>
                <span class="admin-col-hdr center">Aliases</span>
                <span class="admin-col-hdr center">${eyeIconSvg}</span>
            </div>
        `;

        let html = headers;
        let currentSection = undefined;
        const sectionCounts = {};
        tracks.forEach(t => {
            const s = t.variant_section || null;
            if (s) sectionCounts[s] = (sectionCounts[s] || 0) + 1;
        });

        let openSections = [];

        for (const t of tracks) {
            const section = t.variant_section || null;

            if (section !== currentSection) {
                // Close any open variant group
                if (currentSection !== null && currentSection !== undefined) {
                    html += '</div></div>';
                    openSections.pop();
                }
                currentSection = section;
                if (section !== null) {
                    const count = sectionCounts[section] || 0;
                    html += `
                        <div class="admin-variant-group-new">
                            <div class="admin-variant-summary" onclick="
                                const body=this.nextElementSibling;
                                const arrow=this.querySelector('.admin-variant-arrow');
                                body.hidden=!body.hidden;
                                arrow.classList.toggle('open',!body.hidden);
                            ">
                                <span class="admin-variant-arrow">▸</span>
                                <input class="admin-variant-name-input" value="${escapeHtml(section)}"
                                    data-variant-section="${escapeHtml(section)}"
                                    onclick="event.stopPropagation()"
                                    title="Rename applies to all tracks in this section">
                                <span class="admin-variant-count">${count} track${count !== 1 ? 's' : ''}</span>
                            </div>
                            <div class="admin-variant-body" hidden>
                    `;
                    openSections.push(section);
                }
            }
            html += _renderTrackRow(t, trackArtistMap, trackAliasMap);
        }

        // Close any open variant group
        if (currentSection !== null && currentSection !== undefined && openSections.length) {
            html += '</div></div>';
        }

        return html;
    }

    function _renderTrackRow(t, trackArtistMap, trackAliasMap) {
        const trackId = t.id;
        const artists = trackArtistMap.get(trackId) || [];
        const aliases = trackAliasMap.get(trackId) || [];

        // Group artists by role
        const mainArtists  = artists.filter(a => a.role === 'main');
        const featArtists  = artists.filter(a => a.role === 'featured');
        const remixArtists = artists.filter(a => a.role === 'remixer');

        function artistChips(list, role, chipClass) {
            const chips = list.map(a =>
                `<span class="admin-chip ${chipClass}">${escapeHtml(a.name)}<span class="admin-chip-remove" data-remove-track-artist="${trackId}:${a.artistId}:${a.role}">×</span></span>`
            ).join('');
            return `<div class="admin-track-role-row">
                <span class="admin-role-label role-${role}">${role === 'featured' ? 'Feat.' : role === 'remixer' ? 'Remix' : 'Main'}</span>
                ${chips}
                <button class="admin-add-btn admin-add-artist-btn" data-track-id="${trackId}" data-role="${role}">+</button>
            </div>`;
        }

        // Language select — populated now for SSR fallback; TomSelect initialised after render
        const langOpts = [{ code: '', name: '—' }, ...LANGS].map(({ code, name }) =>
            `<option value="${code}"${t.language === code ? ' selected' : ''}>${name}</option>`
        ).join('');

        const aliasCount = aliases.length;
        const eyeVisible = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`;
        const eyeOff     = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/><path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/><line x1="1" y1="1" x2="23" y2="23"/></svg>`;

        return `
            <div class="admin-track-row-grid${t.hidden ? ' track-hidden' : ''}" data-track-id="${trackId}">
                <span class="admin-track-num-grid">${t.track_number || '—'}</span>
                <div class="admin-track-title-cell">
                    <input class="admin-track-title-input" data-track-field="title" value="${escapeHtml(t.title || '')}">
                    <input class="admin-track-eti-input" data-track-field="mix_name" value="${escapeHtml(t.mix_name || '')}" placeholder="ETI…">
                </div>
                <input class="admin-track-dur-input" data-track-field="duration_ms" value="${_fmtDuration(t.duration_ms)}" placeholder="m:ss">
                <input class="admin-track-bpm-input" data-track-field="tempo_bpm" value="${t.tempo_bpm || ''}" placeholder="BPM">
                <select class="admin-track-lang-select" data-track-field="language">${langOpts}</select>
                <div class="admin-track-artists-grid">
                    ${artistChips(mainArtists,  'main',     'chip-main')}
                    ${artistChips(featArtists,  'featured', 'chip-feat')}
                    ${artistChips(remixArtists, 'remixer',  'chip-remix')}
                </div>
                <div style="display:flex;align-items:flex-start;justify-content:center;padding-top:0.28rem">
                    <button class="admin-aliases-btn${aliasCount ? ' has-aliases' : ''}" data-track-id="${trackId}">${aliasCount || '—'}</button>
                </div>
                <div style="display:flex;align-items:flex-start;justify-content:center">
                    <button class="admin-eye-btn${t.hidden ? ' eye-off' : ''}" data-track-id="${trackId}" data-track-field="hidden">${t.hidden ? eyeOff : eyeVisible}</button>
                </div>
            </div>
        `;
    }

    function _openTrackArtistSearch(releaseId, trackId, defaultRole) {
        _closeActive();
        const modal = document.createElement('div');
        modal.id = 'adminTrackArtistModal';
        modal.className = 'admin-match-modal';
        modal.innerHTML = `
            <div class="admin-match-modal-inner">
                <div class="admin-match-modal-header">
                    <span class="admin-match-modal-raw">Add track artist</span>
                    <button class="admin-match-modal-close" title="Cancel">✕</button>
                </div>
                <div style="padding:0.5rem 0.75rem">
                    <label style="font-size:0.78rem;color:var(--text-secondary)">Role</label>
                    <select id="trackArtistRole" class="admin-field-input" style="width:120px;margin-left:0.5rem">
                        <option value="main">main</option>
                        <option value="featured">featured</option>
                        <option value="remixer">remixer</option>
                    </select>
                </div>
                <select id="adminTrackArtistSelect"></select>
                <p class="admin-match-modal-hint">Type to search artists</p>
            </div>
        `;
        document.body.appendChild(modal);
        modal.querySelector('.admin-match-modal-close').addEventListener('click', _closeActive);
        if (defaultRole) {
            const roleSelect = document.getElementById('trackArtistRole');
            if (roleSelect) roleSelect.value = defaultRole;
        }

        const artistOpts = _searchOpts.filter(o => o.entityType === 'artist');
        const sel = document.getElementById('adminTrackArtistSelect');
        const instance = new TomSelect(sel, {
            valueField: 'value',
            labelField: 'text',
            searchField: ['text'],
            maxOptions: 50,
            create: false,
            closeAfterSelect: true,
            openOnFocus: true,
            placeholder: 'Search artists…',
            load(query, callback) {
                if (!query) return callback(artistOpts.slice(0, 50));
                const q = query.toLowerCase();
                callback(artistOpts.filter(o => o.searchText.includes(q)).slice(0, 50));
            },
            render: {
                option(data) {
                    return `<div class="admin-ts-option"><span class="admin-ts-option-title">${escapeHtml(data.text)}</span></div>`;
                },
            },
            onItemAdd(value) {
                const opt = artistOpts.find(o => o.value === value);
                const role = document.getElementById('trackArtistRole')?.value || 'main';
                _closeActive();
                if (!opt) return;
                const artistId = opt.entityId;
                const artistName = opt.text;
                const tp = _getTrackPending(releaseId, trackId);
                tp.artistInserts.push({ artist_id: artistId, role });
                tp.artistDeletes = tp.artistDeletes.filter(a => !(a.artist_id === artistId && a.role === role));
                // Add chip to DOM
                const row = document.querySelector(`.admin-track-row-grid[data-track-id="${trackId}"] .admin-track-artists-grid`);
                if (row) {
                    const chipClass = role === 'featured' ? 'chip-feat' : role === 'remixer' ? 'chip-remix' : 'chip-main';
                    const chip = document.createElement('span');
                    chip.className = `admin-chip ${chipClass}`;
                    chip.innerHTML = `${escapeHtml(artistName)}<span class="admin-chip-remove" data-remove-track-artist="${trackId}:${artistId}:${role}">×</span>`;
                    // Find the role row's add button and insert before it
                    const roleBtn = row.querySelector(`.admin-add-artist-btn[data-role="${role}"]`);
                    if (roleBtn) roleBtn.parentNode.insertBefore(chip, roleBtn);
                }
                _markDirty();
            },
        });
        instance.open();
        _activeTs = { instance };
    }

    function _openTrackAliasModal(releaseId, trackId, trackTitle) {
        _closeActive();
        const tp = _getTrackPending(releaseId, trackId);

        // Load existing aliases from DB (plus any pending inserts, minus pending deletes)
        const aliasRes = _db.exec(
            'SELECT alias, alias_type, language FROM track_aliases WHERE track_id = ? ORDER BY alias',
            [trackId]
        );
        const dbAliases = aliasRes.length ? aliasRes[0].values.map(([alias, alias_type, language]) => ({ alias, alias_type, language })) : [];
        // Merge: db aliases minus pending deletes, plus pending inserts
        const deletedSet = new Set(tp.aliasDeletes);
        const merged = [
            ...dbAliases.filter(a => !deletedSet.has(a.alias)),
            ...tp.aliasInserts,
        ];

        const TYPE_LABELS = {
            common: 'Common', native_script: 'Native script',
            transliteration: 'Transliteration', past_name: 'Past name',
        };

        function renderRows(aliases) {
            if (!aliases.length) return '<p style="font-size:0.8rem;color:var(--text-tertiary)">No aliases yet.</p>';
            return aliases.map(a => `
                <div class="admin-alias-modal-row">
                    <span class="admin-alias-modal-text">${escapeHtml(a.alias)}</span>
                    <span class="admin-alias-modal-type alias-type-${escapeHtml(a.alias_type || 'common')}">${TYPE_LABELS[a.alias_type] || 'Common'}</span>
                    <span class="admin-alias-modal-lang">${escapeHtml(a.language || '')}</span>
                    <button class="admin-alias-modal-remove" data-alias="${escapeHtml(a.alias)}">×</button>
                </div>`).join('');
        }

        const modal = document.createElement('div');
        modal.id = 'adminMatchModal';
        modal.className = 'admin-match-modal';
        modal.innerHTML = `
            <div class="admin-match-modal-inner">
                <div class="admin-match-modal-header">
                    <div>
                        <span class="admin-match-modal-raw">${escapeHtml(trackTitle)}</span>
                        <div style="font-size:0.72rem;color:var(--text-tertiary);margin-top:0.1rem">Alternate titles · transliterations · search aliases</div>
                    </div>
                    <button class="admin-match-modal-close" title="Close">✕</button>
                </div>
                <div class="admin-alias-modal-body" style="padding:0.85rem 1rem;display:flex;flex-direction:column;gap:0.5rem">
                    <div id="aliasModalRows">${renderRows(merged)}</div>
                    <div class="admin-alias-add-form">
                        <div class="admin-alias-add-label">Add alias</div>
                        <div class="admin-alias-add-fields">
                            <input class="admin-alias-add-input" id="aliasAddText" placeholder="Alternate title or transliteration">
                            <select class="admin-alias-add-select" id="aliasAddType">
                                <option value="common">Common</option>
                                <option value="native_script">Native script</option>
                                <option value="transliteration">Transliteration</option>
                                <option value="past_name">Past name</option>
                            </select>
                            <input class="admin-alias-add-input" id="aliasAddLang" placeholder="lang" style="text-align:center">
                        </div>
                        <div class="admin-alias-add-footer">
                            <span class="admin-alias-add-hint">e.g. <code>en</code>, <code>ja</code>, <code>ko</code>, <code>zh</code></span>
                            <button class="admin-alias-add-btn" id="aliasAddBtn">Add</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        document.body.appendChild(modal);
        modal.querySelector('.admin-match-modal-close').addEventListener('click', _closeActive);

        // Remove alias
        modal.querySelector('#aliasModalRows').addEventListener('click', e => {
            const btn = e.target.closest('[data-alias]');
            if (!btn || !btn.classList.contains('admin-alias-modal-remove')) return;
            const alias = btn.dataset.alias;
            const row = btn.closest('.admin-alias-modal-row');
            row?.remove();
            const tp2 = _getTrackPending(releaseId, trackId);
            tp2.aliasDeletes.push(alias);
            tp2.aliasInserts = tp2.aliasInserts.filter(a => a.alias !== alias);
            _markDirty();
            // Update the aliases button count in the track row
            _updateAliasBtn(trackId);
        });

        // Add alias
        modal.querySelector('#aliasAddBtn').addEventListener('click', () => {
            const alias = modal.querySelector('#aliasAddText').value.trim();
            if (!alias) return;
            const alias_type = modal.querySelector('#aliasAddType').value;
            const language = modal.querySelector('#aliasAddLang').value.trim() || null;
            const tp2 = _getTrackPending(releaseId, trackId);
            // Avoid duplicate
            if (tp2.aliasInserts.some(a => a.alias === alias)) return;
            tp2.aliasInserts.push({ alias, alias_type, language });
            tp2.aliasDeletes = tp2.aliasDeletes.filter(a => a !== alias);
            _markDirty();
            // Simpler: just append a new row
            const rowsEl = modal.querySelector('#aliasModalRows');
            const noYet = rowsEl.querySelector('p');
            if (noYet) noYet.remove();
            const rowEl = document.createElement('div');
            rowEl.className = 'admin-alias-modal-row';
            rowEl.innerHTML = `
                <span class="admin-alias-modal-text">${escapeHtml(alias)}</span>
                <span class="admin-alias-modal-type alias-type-${escapeHtml(alias_type)}">${TYPE_LABELS[alias_type] || 'Common'}</span>
                <span class="admin-alias-modal-lang">${escapeHtml(language || '')}</span>
                <button class="admin-alias-modal-remove" data-alias="${escapeHtml(alias)}">×</button>
            `;
            rowsEl.appendChild(rowEl);
            modal.querySelector('#aliasAddText').value = '';
            modal.querySelector('#aliasAddLang').value = '';
            _updateAliasBtn(trackId);
        });

        // Allow Enter key to add
        modal.querySelector('#aliasAddText').addEventListener('keydown', e => {
            if (e.key === 'Enter') modal.querySelector('#aliasAddBtn').click();
        });

        _activeTs = { instance: { destroy: () => {} } };  // sentinel so _closeActive works
    }

    function _updateAliasBtn(trackId) {
        const btn = document.querySelector(`.admin-aliases-btn[data-track-id="${trackId}"]`);
        if (!btn) return;
        // Count = db aliases minus deletes + inserts (approximate from pending)
        // For simplicity just reload count from DB
        const res = _db.exec('SELECT COUNT(*) FROM track_aliases WHERE track_id=?', [trackId]);
        const dbCount = res.length ? (res[0].values[0][0] || 0) : 0;
        // TODO: adjust for pending, for now show db count
        btn.textContent = dbCount > 0 ? String(dbCount) : '—';
        btn.classList.toggle('has-aliases', dbCount > 0);
    }

    // -- Auth mount --

    function _mountAuth(container) {
        document.title = 'aswin.db/music – Admin';
        const noPinSet = !_getStoredPin();

        container.innerHTML = `
            <div class="admin-auth">
                <div class="admin-auth-card">
                    <h2>Admin</h2>
                    ${noPinSet
                        ? `<p class="admin-auth-hint">No PIN configured. Run:<br>
                           <code>python mdb.py admin-pin</code></p>`
                        : `<p class="admin-auth-hint">Enter PIN to continue.</p>
                           <form id="pinForm">
                               <input type="password" id="pinInput" class="admin-pin-input" placeholder="PIN" autofocus autocomplete="current-password">
                               <button type="submit" class="admin-pin-submit">Unlock</button>
                           </form>
                           <p id="pinError" class="admin-auth-error" hidden>Incorrect PIN.</p>`
                    }
                </div>
            </div>
        `;

        if (noPinSet) return;

        document.getElementById('pinForm').addEventListener('submit', async e => {
            e.preventDefault();
            const pin = document.getElementById('pinInput').value;
            const stored = _getStoredPin();
            const hash = await _hashPin(pin, stored.admin_pin_salt);
            if (hash === stored.admin_pin_hash) {
                sessionStorage.setItem('admin_authed', '1');
                const content = (_params && _params.content) || 'listens';
                if (content === 'artists') {
                    _mountArtistPanel(container, _params);
                } else if (content === 'releases') {
                    _mountReleasePanel(container, _params);
                } else {
                    _mountTable(container, _params);
                }
            } else {
                document.getElementById('pinError').hidden = false;
                document.getElementById('pinInput').value = '';
                document.getElementById('pinInput').focus();
            }
        });
    }

    // -- Public --

    function mount(container, db, params) {
        _container = container;
        _db = db;
        _params = params || {};
        _pending.clear();
        _artistPending.clear();
        _releasePending.clear();
        _fileHandle = null;
        _activeTs = null;
        _filters = {};
        _matchFilter = 'unmatched';

        if (sessionStorage.getItem('admin_authed') !== '1') {
            _mountAuth(container);
            return;
        }

        const content = _params.content || 'listens';
        if (content === 'artists') {
            _mountArtistPanel(container, _params);
        } else if (content === 'releases') {
            _mountReleasePanel(container, _params);
        } else {
            _mountTable(container, _params);
        }
    }

    function unmount() {
        _closeActive();
        if (_onAdminKey) {
            document.removeEventListener('keydown', _onAdminKey);
            _onAdminKey = null;
        }
        document.getElementById('adminHeaderSlot')?.remove();
        document.getElementById('adminSaveBtn')?.remove();
        _scrollEl = null;
        _container = null;
    }

    return { mount, unmount };
})();
