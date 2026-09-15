// Shared "shelf of album cards" rendering used by Recommendations and Trends.
// .disc-card is a real <a> (matching every other disc-card in the app --
// home.js/artist.js/browse.js) so right-click/copy-link/drag-out work. That
// rules out nesting a second <a> for the Spotify icon (invalid HTML, and
// even worked around it still made browsers fall back to a bare-hostname
// drag preview instead of the album/artist text, since the anchor's own
// rendered text is what the drag ghost is built from). The icon is instead a
// role="link" span, activated via shelfStreamingOnActivate — it must call
// stopPropagation, not just preventDefault, or the click still bubbles to
// app.js's document-level `a[href]` interceptor and navigates the outer card
// link instead of opening Spotify.

function shelfCard({ id, title, artist, art, year, spotifyId }) {
    const img = art
        ? `<div class="disc-card-img" style="background-image:url('${cssUrl(art)}')"></div>`
        : `<div class="disc-card-img" style="background:var(--bg-tertiary)"></div>`;
    const sub = [artist, year].filter(Boolean).join(' · ');
    const tooltip = [title, artist ? `by ${artist}` : null].filter(Boolean).join(' ');
    const streaming = SHOW_STREAMING_LINKS && spotifyId
        ? `<span class="disc-card-streaming" role="link" tabindex="0" data-spotify-id="${escapeHtml(spotifyId)}" title="Open on Spotify">
              <span class="disc-card-streaming-icon"></span>
           </span>`
        : '';
    return `<a class="disc-card" href="${releaseHref(id)}" title="${escapeHtml(tooltip)}">
        ${img}
        <div class="disc-card-meta">
            <div class="disc-card-info">
                <div class="disc-card-title">${escapeHtml(title || '')}</div>
                <div class="disc-card-sub">${escapeHtml(sub)}</div>
            </div>
            ${streaming}
        </div></a>`;
}

function shelfStreamingOnActivate(e) {
    if (e.type === 'keydown' && e.key !== 'Enter' && e.key !== ' ') return;
    const el = e.target.closest('.disc-card-streaming[data-spotify-id]');
    if (!el) return;
    e.preventDefault();
    e.stopPropagation();
    window.open(`https://open.spotify.com/album/${el.dataset.spotifyId}`, '_blank', 'noopener');
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
