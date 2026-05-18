// Shared UI constants — loaded before all views.

// Number of album cards shown per shelf on desktop and mobile.
// The CSS rule `.rec-shelf .disc-card:nth-child(n + MOBILE+1)` must match SHELF_MOBILE.
const SHELF_DESKTOP = 8;
const SHELF_MOBILE  = 6;

// When true, a small Spotify icon appears on hover on recommendation cards
// linking directly to that release on Spotify (where available).
const SHOW_STREAMING_LINKS = true;

// When true, the History view shows unmatched scrobbles (raw track/artist text)
// alongside matched listens. Toggle to false to show only matched listens.
const HISTORY_SHOW_UNMATCHED = true;
