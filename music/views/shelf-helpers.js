// Shared "shelf of album cards" rendering used by Recommendations and Trends.
// Uses <div role="link"> so a nested <a> for the Spotify icon is valid HTML.

function shelfCard({ id, title, artist, art, year, spotifyId }) {
    const img = art
        ? `<div class="disc-card-img" style="background-image:url('${cssUrl(art)}')"></div>`
        : `<div class="disc-card-img" style="background:var(--bg-tertiary)"></div>`;
    const sub = [artist, year].filter(Boolean).join(' · ');
    const streaming = SHOW_STREAMING_LINKS && spotifyId
        ? `<a class="disc-card-streaming" href="https://open.spotify.com/album/${spotifyId}"
              target="_blank" rel="noopener" title="Open on Spotify">
              <span class="disc-card-streaming-icon"></span>
           </a>`
        : '';
    // id is DB-derived; carry it in a data attribute and handle activation
    // with a delegated listener rather than interpolating it into inline JS.
    return `<div class="disc-card" role="link" tabindex="0" data-release-id="${escapeHtml(id)}">
        ${img}
        <div class="disc-card-meta">
            <div class="disc-card-info">
                <div class="disc-card-title">${escapeHtml(title || '')}</div>
                <div class="disc-card-sub">${escapeHtml(sub)}</div>
            </div>
            ${streaming}
        </div></div>`;
}

// Hidden if fewer than 2 results.
function shelfSection(title, desc, cards) {
    if (cards.length < 2) return '';
    return `<section class="rec-shelf">
        <div class="rec-shelf-header">
            <h2>${escapeHtml(title)}</h2>
            <p class="rec-desc">${escapeHtml(desc)}</p>
        </div>
        <ul class="disc-grid">${cards.map(c => `<li>${shelfCard(c)}</li>`).join('')}</ul>
    </section>`;
}

function shelfOnActivate(e) {
    if (e.type === 'keydown' && e.key !== 'Enter' && e.key !== ' ') return;
    const card = e.target.closest('.disc-card[data-release-id]');
    if (!card) return;
    // Let the nested Spotify anchor win.
    if (e.target.closest('a')) return;
    e.preventDefault();
    navigate({ view: 'release', id: card.dataset.releaseId });
}

// Shared mount/unmount scaffolding for a shelf-of-cards page (Recommendations,
// Trends): header + title + subtitle placeholder + shelves container, with
// click/keydown delegated to shelfOnActivate. Returns the shelves container
// element and an AbortController the caller must abort in its own unmount().
function shelfPageMount(container, { title, shelvesId, subtitleId }) {
    setPageTitle(title);
    container.innerHTML = `
        <header class="rec-header">
            <h1>${escapeHtml(title)}</h1>
            <p class="subtitle" id="${subtitleId}"></p>
        </header>
        <div id="${shelvesId}" class="rec-shelves"></div>
    `;
    const ac = new AbortController();
    const shelvesEl = document.getElementById(shelvesId);
    shelvesEl.addEventListener('click', shelfOnActivate, { signal: ac.signal });
    shelvesEl.addEventListener('keydown', shelfOnActivate, { signal: ac.signal });
    return { shelvesEl, ac };
}

// Renders `shelves` ([title, desc, cards][]) into shelvesEl and updates the
// subtitle to "N <singular|plural>" based on how many shelves actually
// rendered (shelfSection hides shelves with fewer than 2 cards).
function renderShelfPage(shelvesEl, subtitleId, shelves, singular, plural = `${singular}s`) {
    shelvesEl.innerHTML = shelves.map(([title, desc, cards]) => shelfSection(title, desc, cards)).join('');
    const rendered = shelves.filter(([, , cards]) => cards.length >= 2).length;
    const subtitleEl = document.getElementById(subtitleId);
    if (subtitleEl) subtitleEl.textContent = `${rendered} ${rendered === 1 ? singular : plural}`;
}
