// Shared "shelf of album cards" rendering used by Recommendations and Trends.
// .disc-card-link is a stretched real <a> (absolute, inset:0) covering the
// whole card -- gives it a real href so right-click/copy-link/drag-out work,
// which a click-handled <div role="link"> never supports. The Spotify icon
// sits on top via z-index (see .disc-card-streaming in styles.css) since a
// nested <a> inside <a> is invalid HTML and gets mis-parsed by browsers.

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
    return `<div class="disc-card" data-release-id="${escapeHtml(id)}">
        <a class="disc-card-link" href="${releaseHref(id)}" aria-label="${escapeHtml(title || '')}"></a>
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

// Renders `shelves` ([title, desc, cards][]) into shelvesEl and updates the
// subtitle to "N <singular|plural>" based on how many shelves actually
// rendered (shelfSection hides shelves with fewer than 2 cards).
function renderShelfPage(shelvesEl, subtitleId, shelves, singular, plural = `${singular}s`) {
    shelvesEl.innerHTML = shelves.map(([title, desc, cards]) => shelfSection(title, desc, cards)).join('');
    const rendered = shelves.filter(([, , cards]) => cards.length >= 2).length;
    const subtitleEl = document.getElementById(subtitleId);
    if (subtitleEl) subtitleEl.textContent = `${rendered} ${rendered === 1 ? singular : plural}`;
}
