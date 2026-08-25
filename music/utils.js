// Shared utilities for music browser

const SITE_NAME = 'Aswin Sivaraman';

// Single source of the title suffix, so views don't each invent a format.
function setPageTitle(...parts) {
    const page = parts.filter(Boolean).join(' · ');
    document.title = page ? `${page} | ${SITE_NAME}` : SITE_NAME;
}

const _PILL_SVG_EXT = `<svg class="pill-ext" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>`;

function renderLinkPill(svc, href, name, sub) {
    const icon = svc === 'aoty'
        ? `<img src="images/links/aoty-icon.png" class="pill-aoty-img">`
        : `<span class="pill-mask"></span>`;
    return `<a href="${href}" target="_blank" rel="noopener" class="release-link-pill pill-${svc}">` +
        `<span class="pill-icon">${icon}</span>` +
        `<span class="pill-text"><span class="pill-service-name">${name}</span>` +
        (sub ? `<span class="pill-sub">${sub}</span>` : '') +
        `</span>${_PILL_SVG_EXT}</a>`;
}

// external_links Wikipedia values are usually a bare English Wikipedia page ID
// (e.g. "12326111"), but for artists/releases with no English article we store
// the full non-English Wikipedia URL instead (same convention as Bandcamp/RYM).
function wikipediaHref(value) {
    return /^\d+$/.test(value) ? `https://en.wikipedia.org/wiki/?curid=${value}` : value;
}

// Prefer clean slug-based URLs for artist/release links; fall back to the raw
// id if a slug isn't available (e.g. a query that didn't select it).
function artistHref(id, slug) {
    return `?view=artist&${slug ? `slug=${encodeURIComponent(slug)}` : `id=${encodeURIComponent(id)}`}`;
}
function releaseHref(id, slug) {
    return `?view=release&${slug ? `slug=${encodeURIComponent(slug)}` : `id=${encodeURIComponent(id)}`}`;
}

// Fine-grained "Xs/Xm/Xh/Xd ago" — for activity lists (Recent Plays, History).
// Distinct from formatRelativeTime() below, which is coarser (today/yesterday/
// weeks/months/years) and used for single "Last played" stat fields.
function formatTimeAgo(ts) {
    const diff = Math.floor(Date.now() / 1000) - ts;
    if (diff < 60)     return `${diff}s ago`;
    if (diff < 3600)   return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400)  return `${Math.floor(diff / 3600)}h ago`;
    if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;
    const d = new Date(ts * 1000);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function renderLoading(text = 'Loading...') {
    return `<div class="loading">${escapeHtml(text)}</div>`;
}

function formatNumber(num) {
    return num.toLocaleString();
}

// Mirrors mdb_strings.py's same_song_key() — groups track titles that are
// just edit/length cuts of one recording (radio edit, extended mix,
// original mix, club mix, ...) so hearing any one variant counts as having
// heard the song for completion purposes. A remix or edit credited to a
// specific artist/DJ is a distinct musical work and is never folded in.
const GENERIC_EDIT_TERMS = new Set([
    'radio edit', 'radio version', 'radio mix',
    'extended mix', 'extended', 'extended version',
    'original mix', 'original version', 'original',
    'club mix', 'dub mix', 'album mix', 'album version',
    'single version', 'single mix',
]);
const EDIT_SUFFIX_RE = /\s*[\(\[]([^\(\)\[\]]+)[\)\]]\s*$/;

function _isGenericEdit(inner) {
    if (GENERIC_EDIT_TERMS.has(inner)) return true;
    const stripped = inner.replace(/^original\s+/, '');
    return stripped !== inner && GENERIC_EDIT_TERMS.has(stripped);
}

function sameSongKey(title) {
    const m = EDIT_SUFFIX_RE.exec(title);
    if (!m || !_isGenericEdit(m[1].trim().toLowerCase())) {
        return title.trim().toLowerCase();
    }
    return title.slice(0, m.index).trim().toLowerCase();
}

// Tracks under 30s don't get scrobbled, so they're excluded from every
// "X / Y tracks heard" denominator (mdb.py's stats refresh mirrors this).
const SCROBBLABLE_TRACK_FILTER =
    "t.hidden = 0 AND t.variant_section IS NULL AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)";

// Owned-copy medium for a release, correlated to r.id -- 'vinyl' takes
// priority over 'cd' when a release is owned on both (matches the record-pull
// modal's own priority on the release page). Covers collection_item_releases
// too, since a box set's physical item can point at more than one release.
const OWNED_MEDIUM_SQL = `(
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM collection_items ci JOIN collection_item_media cim ON cim.collection_item_id = ci.id
            WHERE cim.medium = 'vinyl' AND (ci.release_id = r.id
                OR ci.id IN (SELECT collection_item_id FROM collection_item_releases WHERE release_id = r.id))
        ) THEN 'vinyl'
        WHEN EXISTS (
            SELECT 1 FROM collection_items ci JOIN collection_item_media cim ON cim.collection_item_id = ci.id
            WHERE cim.medium = 'cd' AND (ci.release_id = r.id
                OR ci.id IN (SELECT collection_item_id FROM collection_item_releases WHERE release_id = r.id))
        ) THEN 'cd'
        WHEN EXISTS (
            SELECT 1 FROM collection_items ci
            WHERE ci.release_id = r.id
               OR ci.id IN (SELECT collection_item_id FROM collection_item_releases WHERE release_id = r.id)
        ) THEN 'other'
    END
)`;

// Small "owned" indicator for grid/list tiles -- gold to match the release
// page's owned ribbon, medium-specific tooltip. The tile itself is too small
// for the ribbon's descriptor detail (color/weight/packaging); that stays on
// the release page, reachable by clicking through.
function ownedBadgeHtml(medium) {
    if (!medium) return '';
    const label = medium === 'vinyl' ? 'Owned · Vinyl' : medium === 'cd' ? 'Owned · CD' : 'Owned';
    return `<div class="disc-owned-badge" title="${label}"></div>`;
}

function donutColor(pct) {
    if (pct <= 0)   return 'var(--border)';
    if (pct < 0.5)  return '#3b82f6';
    if (pct < 0.75) return '#f59e0b';
    if (pct < 1.0)  return '#f97316';
    return '#22c55e';
}

// "X / Y tracks" ring markup — pass heard/total already filtered by SCROBBLABLE_TRACK_FILTER.
function donutHtml(heard, total, { small = false, label = 'tracks' } = {}) {
    if (!total) return '';
    const pct = heard / total;
    const sizeClass = small ? ' donut-sm' : '';
    return `<div class="donut-wrap${sizeClass}" style="--p:${Math.round(pct * 100)};--c:${donutColor(pct)}" data-tooltip="${heard} / ${total} ${label}"><div class="donut"></div></div>`;
}

// "How mainstream is this artist" bar for an inline dt/dd stats row —
// same hand-rolled track/fill convention as .lang-bar-fill/.pub-list-card-fill
// elsewhere in this app, rather than a native <meter> (rendering differs
// enough across browsers' UA shadow parts that it wouldn't match the rest
// of the app's bars). role="meter" + aria-value* keep the accessibility
// semantics a real <meter> would give, without native rendering.
function popularityMeterHtml(popularity) {
    if (popularity == null) return '';
    return `<span class="popularity-meter" role="meter" aria-valuenow="${popularity}" aria-valuemin="0" aria-valuemax="100"
                  aria-label="Spotify popularity" title="Spotify popularity: ${popularity}/100">
        <span class="popularity-meter-track"><span class="popularity-meter-fill" style="width:${popularity}%"></span></span>
        <span class="popularity-meter-value">${popularity}</span>
    </span>`;
}

function formatRelativeTime(ts) {
    const diffSec = Math.floor(Date.now() / 1000) - ts;
    if (diffSec < 86400)       return 'today';
    if (diffSec < 2 * 86400)   return 'yesterday';
    const days = Math.floor(diffSec / 86400);
    if (days < 14)             return `${days} days ago`;
    const weeks = Math.floor(days / 7);
    if (weeks < 9)             return `${weeks} weeks ago`;
    const months = Math.floor(days / 30);
    if (months < 12)           return `${months} months ago`;
    const years = Math.floor(days / 365);
    return years === 1 ? '1 year ago' : `${years} years ago`;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Safe interpolation of an image URL into a style="background-image:url('…')"
// attribute. These URLs come from Spotify / MusicBrainz / Cover Art Archive /
// AOTY scraping, so a stray quote would terminate both the CSS string and the
// HTML attribute. Escapes for the CSS-string context, then the HTML-attribute
// context.
function cssUrl(url) {
    if (!url) return '';
    const cssEscaped = String(url).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    return escapeHtml(cssEscaped);
}

function getFallbackImageUrl() {
    return 'data:image/svg+xml,' + encodeURIComponent(`
        <svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
            <rect width="200" height="200" fill="#20232c"/>
            <text x="100" y="115" text-anchor="middle" font-size="80" fill="#767c85">♪</text>
        </svg>
    `);
}

const COLLAGE_SIZES = { 10: 3, 20: 4, 50: 7, 100: 10 };

// Given a target cols/rows ratio (e.g. 0.5625 for a 9:16 Story export) and an
// approximate desired total cell count, finds the closest-fitting integer
// {rows, cols} within a 1-10 bound per axis (matches the largest fixed
// preset, 10x10, and keeps PNG export capped at 100 images). Ratio fidelity
// is weighted 4x over exact cell-count match, since a "Story-shaped" grid
// that's slightly bigger/smaller than requested reads better than a
// correctly-sized grid that looks square when it should look tall.
function _nearestGridForRatio(targetRatio, approxCellCount) {
    let best = null, bestErr = Infinity;
    for (let rows = 1; rows <= 10; rows++) {
        for (let cols = 1; cols <= 10; cols++) {
            const cellCount = rows * cols;
            const ratioErr = Math.abs((cols / rows) - targetRatio);
            const countErr = Math.abs(cellCount - approxCellCount) / approxCellCount;
            const err = ratioErr * 4 + countErr;
            if (err < bestErr) { bestErr = err; best = { rows, cols }; }
        }
    }
    return best;
}

function renderGenreTags(rows) {
    // rows: [[aoty_id, name, is_primary], ...]
    if (!rows || !rows.length) return '';
    return rows.map(([id, name, isPrimary]) =>
        `<a href="?view=genre&id=${id}" class="genre-tag${isPrimary ? '' : ' genre-tag-secondary'}">${escapeHtml(name)}</a>`
    ).join(', ');
}

function updateCountLabels(viewMode) {
    const sel = document.getElementById('countFilter');
    if (!sel) return;
    Array.from(sel.options).forEach(opt => {
        const count = parseInt(opt.value);
        if (viewMode === 'collage') {
            const n = COLLAGE_SIZES[count];
            opt.textContent = `${n}×${n}`;
        } else {
            opt.textContent = count;
        }
    });
}

function setupToggleGroup(selector, onChange) {
    const sync = (active) => {
        document.querySelectorAll(selector).forEach(b => {
            const on = b === active;
            b.classList.toggle('active', on);
            // The `active` class alone isn't announced by screen readers.
            b.setAttribute('aria-pressed', String(on));
        });
    };

    const buttons = document.querySelectorAll(selector);
    buttons.forEach(btn => {
        if (!btn.hasAttribute('aria-pressed')) {
            btn.setAttribute('aria-pressed', String(btn.classList.contains('active')));
        }
        btn.addEventListener('click', e => {
            sync(e.currentTarget);
            onChange(e.currentTarget);
        });
    });
}

// Shared toolbar dropdown -- Letterboxd's own .smenu-menu is the model: a
// sticky category header, the current pick shown once right below it (not
// buried mid-list with just a checkmark), a divider, the rest of the list,
// and a bottom scroll-fade that only appears when the list is actually
// taller than the panel. Any view with a small "pick one of these" filter
// (Sort/Status/Decade/Range/Period/Count/...) can use this instead of
// hand-rolling its own <select> or button row, so a Letterboxd-style
// restyle only has to happen once instead of once per view.
//
// container: the view's root element (dropdowns are looked up/wired within
//   it, so a full container.innerHTML replace on view re-render doesn't
//   leave stale references).
// key: unique id for this dropdown within the container (becomes the
//   data-dropdown-key value).
// label: category header shown in the panel and as the trigger's prefix.
// options: [{ value, label }, ...] in display order, OR a function
//   returning that array -- a function is for options whose labels depend
//   on state outside this dropdown (year.js/top.js's Count filter relabels
//   10/20/50/100 as 3x3/4x4/7x7/10x10 once Display switches to Collage;
//   the label is a live view of that state, not a fixed set of choices).
// getValue: () => current value.
// onPick: (value) => void, called after the trigger/panel are already
//   updated to reflect the pick -- do the actual filter/reload here.
function _resolveDropdownOptions(options) {
    return typeof options === 'function' ? options() : options;
}

function dropdownHtml(key, label, options, getValue) {
    const list = _resolveDropdownOptions(options);
    const current = getValue();
    const currentOpt = list.find(o => o.value === current) || list[0];
    return `<div class="toolbar-dropdown" data-dropdown="${escapeHtml(key)}">
        <button type="button" class="toolbar-dropdown-trigger" data-dropdown-trigger="${escapeHtml(key)}">
            <span class="toolbar-dropdown-label">${escapeHtml(label)}</span>
            <span class="toolbar-dropdown-value" data-dropdown-value="${escapeHtml(key)}">${escapeHtml(currentOpt ? currentOpt.label : '')}</span>
        </button>
        <div class="toolbar-dropdown-panel" data-dropdown-panel="${escapeHtml(key)}" hidden></div>
    </div>`;
}

// Re-syncs a dropdown's trigger label after its options/value changed via
// some OTHER control -- e.g. year.js's Display toggle (not this dropdown)
// changes what "10" means for Count. dropdownHtml() only sets the label at
// initial render; setupDropdowns() only refreshes it after a pick through
// this dropdown itself. This covers the third case: an outside change.
function refreshDropdownTrigger(container, key, options, getValue) {
    const valueEl = container.querySelector(`[data-dropdown-value="${key}"]`);
    if (!valueEl) return;
    const list = _resolveDropdownOptions(options);
    const current = getValue();
    const currentOpt = list.find(o => o.value === current) || list[0];
    valueEl.textContent = currentOpt ? currentOpt.label : '';
}

function _dropdownPanelContentHtml(label, options, current) {
    const currentOpt = options.find(o => o.value === current) || options[0];
    const rest = options.filter(o => o !== currentOpt);
    // icon is either a bare lucide icon name (e.g. browse.js's Sort options:
    // 'sparkles', 'clock') or pre-built HTML (e.g. browse.js's Soundtrack
    // dropdown, which reuses the same Controllercons markup its former
    // TomSelect render.option did) -- most callers have no use for it and
    // leave it undefined.
    const iconHtml = icon => (icon.includes('<') ? icon : `<i data-lucide="${escapeHtml(icon)}"></i>`);
    const rowInner = o => `${o.icon ? `<span class="toolbar-dropdown-row-icon">${iconHtml(o.icon)}</span>` : ''}<span>${escapeHtml(o.label)}</span>`;
    // `indent: true` -- a sub-option nested under the row directly above it
    // (browse.js's Soundtrack dropdown: platforms nested under "Video Game")
    // rather than a sibling of equal standing in the flat list.
    const row = o => `<button type="button" class="toolbar-dropdown-row${o.indent ? ' toolbar-dropdown-row--indent' : ''}" data-dropdown-pick="${escapeHtml(String(o.value))}">
        ${rowInner(o)}
    </button>`;
    return `
        <div class="toolbar-dropdown-panel-scroll">
            <div class="toolbar-dropdown-panel-header">${escapeHtml(label)}</div>
            ${currentOpt ? `<button type="button" class="toolbar-dropdown-row toolbar-dropdown-row-current${currentOpt.indent ? ' toolbar-dropdown-row--indent' : ''}" data-dropdown-pick="${escapeHtml(String(currentOpt.value))}">
                ${rowInner(currentOpt)}
                <i data-lucide="check"></i>
            </button>
            <div class="toolbar-dropdown-divider"></div>` : ''}
            ${rest.map(row).join('')}
        </div>
        <div class="toolbar-dropdown-fade" hidden></div>`;
}

// Flips the panel to whichever side/direction actually has room -- a naive
// top-left anchor would run a dropdown near the right edge off-screen, or
// one near the bottom under the fold.
function _positionDropdownPanel(trigger, panel) {
    panel.style.left = '';
    panel.style.right = '';
    panel.style.top = '';
    panel.style.bottom = '';
    const triggerRect = trigger.getBoundingClientRect();
    const panelRect = panel.getBoundingClientRect();
    const overflowsRight = triggerRect.left + panelRect.width > window.innerWidth - 8;
    const overflowsBottom = triggerRect.bottom + panelRect.height > window.innerHeight - 8;
    if (overflowsRight) panel.style.right = '0';
    else panel.style.left = '0';
    if (overflowsBottom) panel.style.bottom = 'calc(100% + 4px)';
    else panel.style.top = 'calc(100% + 4px)';
}

// Shows the bottom scroll-fade only while there's more list below the fold
// -- a short list (Sort, Status) never scrolls and never shows one; a long
// one (Decade, Genre) does, matching Letterboxd's own
// .smenu-overflowindicator.-scrollable toggle rather than always/never.
function _updateDropdownFade(panel) {
    if (!panel) return;
    const scroll = panel.querySelector('.toolbar-dropdown-panel-scroll');
    const fade = panel.querySelector('.toolbar-dropdown-fade');
    if (!scroll || !fade) return;
    const hasMore = scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight > 2;
    fade.hidden = !hasMore;
}

// Wires every `[data-dropdown-trigger]`/`[data-dropdown-panel]` pair inside
// `container` that was rendered via dropdownHtml(). `specs` is
// `{ [key]: { label, options, getValue, onPick } }`. Call once per view
// mount (not per re-render of a reused container node) -- see browse.js's
// own mount()/_ac AbortController pattern for why binding this inside a
// function that reruns on every filter change stacks duplicate listeners.
// Panel content is rendered lazily on open (not once up front), since a
// view like browse.js can rebuild the whole toolbar's markup independently
// of this one-time setup call (e.g. its Albums/Artists toggle) -- rendering
// once at setup would leave every reopened panel showing stale or empty
// content after the first such rebuild.
function setupDropdowns(container, specs, signal) {
    const renderPanel = key => {
        const spec = specs[key];
        const panel = container.querySelector(`[data-dropdown-panel="${key}"]`);
        if (panel) {
            panel.innerHTML = _dropdownPanelContentHtml(spec.label, _resolveDropdownOptions(spec.options), spec.getValue());
            lucide.createIcons({ el: panel });
        }
    };

    container.addEventListener('click', e => {
        const trigger = e.target.closest('[data-dropdown-trigger]');
        if (trigger) {
            e.stopPropagation();
            const key = trigger.dataset.dropdownTrigger;
            if (!specs[key]) return;
            const panel = container.querySelector(`[data-dropdown-panel="${key}"]`);
            const wasOpen = panel && !panel.hidden;
            container.querySelectorAll('[data-dropdown-panel]').forEach(p => { p.hidden = true; });
            if (panel && !wasOpen) {
                renderPanel(key);
                panel.hidden = false;
                _positionDropdownPanel(trigger, panel);
                _updateDropdownFade(panel);
            }
            return;
        }
        const pick = e.target.closest('[data-dropdown-pick]');
        if (pick) {
            e.stopPropagation();
            const panel = pick.closest('[data-dropdown-panel]');
            const key = panel?.dataset.dropdownPanel;
            const spec = key && specs[key];
            if (!spec) return;
            const value = pick.dataset.dropdownPick;
            // onPick first -- it's expected to update whatever getValue()
            // reads synchronously, so the re-render right after reflects the
            // new pick instead of re-highlighting the stale one.
            spec.onPick(value);
            const valueEl = container.querySelector(`[data-dropdown-value="${key}"]`);
            if (valueEl) {
                const opt = _resolveDropdownOptions(spec.options).find(o => String(o.value) === value);
                valueEl.textContent = opt ? opt.label : '';
            }
            panel.hidden = true;
        }
    }, { signal });
    container.addEventListener('scroll', e => {
        const scroll = e.target.closest?.('.toolbar-dropdown-panel-scroll');
        if (scroll) _updateDropdownFade(scroll.closest('.toolbar-dropdown-panel'));
    }, { capture: true, signal });
    document.addEventListener('click', () => {
        container.querySelectorAll('[data-dropdown-panel]').forEach(p => { p.hidden = true; });
    }, { signal });
}

function createWideCard({ href, imageUrl, name, meta, totalListens, totalMinutes,
                          rounded = false, cert = null, viaArtist = null }) {
    const card = document.createElement('a');
    card.className = 'release-card';
    card.href = href;

    const imgSrc = imageUrl || getFallbackImageUrl();
    const metaParts = meta ? meta.split(' · ') : [];
    const metaHtml = metaParts.map((p, i) =>
        `<span class="${i === 0 ? 'release-year' : 'release-type-label'}">${p}</span>`
    ).join('');

    const certLabels = { gold: '250+ plays', platinum: '500+ plays', diamond: '1,000+ plays' };
    const certDot = cert
        ? `<span class="release-cert-dot release-cert-dot-${cert}" title="${certLabels[cert]}"></span>`
        : '';

    const statsHtml = totalListens != null ? `
        <div class="release-stats">
            <span class="stat-item">
                <i data-lucide="headphones" style="width: 13px; height: 13px;"></i>
                ${formatNumber(totalListens)}
            </span>
            <span class="stat-item">
                <i data-lucide="clock" style="width: 13px; height: 13px;"></i>
                ${formatNumber(totalMinutes)} min
            </span>
        </div>
    ` : '';

    const viaHtml = viaArtist ? `<span class="release-via-artist">${escapeHtml(viaArtist)}</span>` : '';

    card.innerHTML = `
        <div class="release-card-thumb${rounded ? ' rounded' : ''}" style="background-image: url('${cssUrl(imgSrc)}')">${certDot}</div>
        <div class="release-card-body">
            <div class="release-name">${escapeHtml(name)}</div>
            ${statsHtml}
            ${metaHtml || viaHtml ? `<div class="release-meta">${metaHtml}${viaHtml}</div>` : ''}
        </div>
    `;
    return card;
}

function createImageCard({ href, imageUrl, title = null, subtitle = null,
                            totalListens = null, totalMinutes = null,
                            collageLabel = null, extraClass = '' }) {
    const card = document.createElement('a');
    card.className = extraClass ? `image-card ${extraClass}` : 'image-card';
    card.href = href;

    const imgHtml = `<div class="image-card-img" style="background-image: url('${cssUrl(imageUrl || getFallbackImageUrl())}')"></div>`;

    if (collageLabel != null) {
        card.innerHTML = `${imgHtml}<div class="image-card-collage-label">${escapeHtml(collageLabel)}</div>`;
        return card;
    }
    if (title == null) {
        card.innerHTML = imgHtml;
        return card;
    }

    const statsHtml = totalListens != null ? `
        <div class="image-card-stats">
            <span class="stat-item"><i data-lucide="headphones" style="width:14px;height:14px;"></i>${formatNumber(totalListens)}</span>
            <span class="stat-item"><i data-lucide="clock" style="width:14px;height:14px;"></i>${formatNumber(totalMinutes)} min</span>
        </div>` : '';

    card.innerHTML = `
        ${imgHtml}
        <div class="image-card-overlay">
            <div class="image-card-name">${escapeHtml(title)}</div>
            ${subtitle ? `<div class="image-card-artist">${escapeHtml(subtitle)}</div>` : ''}
            ${statsHtml}
        </div>`;
    return card;
}
