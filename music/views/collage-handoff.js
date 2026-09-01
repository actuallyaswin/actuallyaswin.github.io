// In-memory handoff for the Collage page. This SPA never reloads between
// navigate() calls (one HTML page, client-side routing), so a source view
// (Top, Browse, a genre page, ...) can stash its current result set here
// right before navigating to ?view=collage, and the Collage page reads it
// back out — no shared query/entity knowledge needed on either side, just a
// plain array of card-shaped objects ({id, href, imageUrl, label, ...}).
// Lost on a hard refresh (module state resets); collage.js shows a friendly
// "go back and pick something" empty state in that case.
let _collagePayload = null;

function setCollagePayload(payload) {
    _collagePayload = payload;
}

function getCollagePayload() {
    return _collagePayload;
}
