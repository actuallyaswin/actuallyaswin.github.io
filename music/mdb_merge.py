"""
mdb_merge — Multi-source release metadata harmonization.

Accepts data from any combination of providers (MusicBrainz, Spotify, Beatport,
Apple Music / iTunes, Bandcamp, and future sources) and produces canonical
MDBRelease + list[MDBTrack] entities for DB insertion.
Inspired by Harmony (https://github.com/nicowillis/harmony) but targeted at
our SQLite schema rather than MusicBrainz seeding.

Provider-specific source data accepted via the open `sources` dict:
  'mb'  — MusicBrainzRelease object OR raw dict from _mb_get()
  'sp'  — Spotify album dict from SpotifyClient.get_album() (with _all_tracks)
  'bp'  — BeatportRelease object with .tracks and ._data
  'am'  — ItunesRelease object (Apple Music / iTunes Lookup API)
  'bc'  — BandcampRelease object (HTML scrape)

Usage:
    merge = ReleaseMerge({'mb': mb_obj, 'sp': sp_dict, 'bp': bp_obj}, sp_full=full_map)
    mdb_r = merge.release()    # MDBRelease
    for t in merge.tracks():   # list[MDBTrack]
        ...

GTIN auto-discovery:
    ids = resolve_by_gtin('00602537700417', skip=frozenset({'mb'}))
    # → {'sp': '0QFMqnSP2kBMTrbfNkj3SB', 'am': '1234567890', ...}

DB persistence:
    with managed_db(DB_PATH) as conn:
        cur = conn.cursor()
        artist_id, _ = upsert_artist_mb(cur, mb_primary_artist)
        release_id, created = upsert_release_mdb(cur, merge.release(), artist_id)
        upsert_tracks_mdb(cur, release_id, merge.tracks())
        conn.commit()
"""

from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any

from mdb_strings import (
    normalize_text,
    _date_prec,
    _should_update_date,
    beatport_is_catalog_addition,
    normalize_upc,
)
from mdb_ops import (
    new_ulid,
    slugify,
    unique_slug,
    EL_RELEASE,
    EL_SVC_BEATPORT,
    EL_SVC_BANDCAMP,
    EL_SVC_DEEZER,
)


# -- Supporting types ----------------------------------------------------------

@dataclass
class MDBLabel:
    name: str
    catalog_number: str | None = None  # From MB label-info or Beatport (indie labels only)
    mbid: str | None = None            # From MB label MBID


@dataclass
class MDBArtistCredit:
    name: str
    credited_name: str | None = None   # As credited on this release (may differ from canonical)
    join_phrase: str = ''              # " & ", " ft. ", "" for last in list
    role: str = 'main'                 # 'main' | 'featured' | 'remixer'
    mbid: str | None = None
    spotify_id: str | None = None
    beatport_id: str | None = None     # Beatport numeric artist ID (as string)


@dataclass
class MDBTrack:
    isrc: str | None
    track_number: int
    disc_number: int = 1
    title: str = ''                    # Base title, ETI-free (e.g. "You & Me")
    mix_name: str | None = None        # Version label ("Flume Remix", "Original Mix")
    duration_ms: int | None = None     # Spotify (precise ms) > MB (rounded) > Beatport
    is_explicit: bool = False
    mbid: str | None = None            # MusicBrainz recording ID
    spotify_id: str | None = None
    bpm: int | None = None             # Beatport only
    musical_key: str | None = None     # Beatport only ("F Major", "D# Minor")
    key_camelot: str | None = None     # Camelot Wheel notation ("8A", "9B") — Beatport only
    beatport_genre: str | None = None  # Beatport per-track genre ("House", "Electronica")
    beatport_sub_genre: str | None = None
    beatport_track_id: int | None = None  # Beatport numeric track ID
    spotify_popularity: int | None = None
    artists: list = field(default_factory=list)    # list[MDBArtistCredit]
    sources: set = field(default_factory=set)      # {'mb', 'sp', 'bp', ...}
    source_map: dict = field(default_factory=dict) # field → source


@dataclass
class MDBRelease:
    title: str
    primary_artist: MDBArtistCredit | None    # → releases.primary_artist_id
    release_date: str | None                  # YYYY-MM-DD | YYYY-MM | YYYY
    date_source: str                          # 'majority' | 'musicbrainz' | 'spotify' | ...
    primary_type: str | None                  # 'album' | 'ep' | 'single' | ...
    type_secondary: str | None                # 'remix' | 'live' | 'soundtrack' | ...
    label: MDBLabel | None
    upc: str | None                           # Canonical form (longest available)
    release_group_mbid: str | None
    mbid: str | None
    spotify_id: str | None
    beatport_id: int | None
    apple_music_id: str | None
    album_art_url: str | None
    album_art_source: str | None              # 'beatport' | 'coverartarchive' | 'spotify'
    spotify_popularity: int | None
    total_tracks: int | None
    tracks: list                              # list[MDBTrack]
    conflicts: list                           # list[str] — human-readable conflict log
    source_map: dict                          # release-level field → source


# -- ReleaseMerge class --------------------------------------------------------

class ReleaseMerge:
    """
    Harmonizes metadata from multiple providers into MDBRelease + list[MDBTrack].

    Provider quality preferences (per-field):
      album_art_url : am = bc > bp > sp > caa   (Apple/Bandcamp 3000px; BP 1400px; SP 640px)
      duration_ms   : sp = am = bc > mb > dz     (Spotify/Apple/Bandcamp precise; MB rounded)
      release_date  : majority (2-of-3) > mb > sp = bp  (BP discounted for catalog additions)
      primary_type  : mb > sp = am = dz           (MB release-group is authoritative)
      type_secondary: mb only                     (no equivalent in other sources)
      label.name    : sp > mb > bp > bc           (Spotify most recognized; BP adds distributor noise)
      upc           : longest-form wins            (13-digit EAN-13 > 12-digit UPC-A)
      isrc          : bp + sp agreement > mb       (BP+SP ground truth; MB can have wrong ISRCs)
      bpm / key     : bp only                      (no equivalent without separate API call)
    """

    SUPPORTED = frozenset({'mb', 'sp', 'bp', 'bc', 'am', 'dz'})

    def __init__(self, sources: dict[str, Any], sp_full: dict | None = None):
        """
        Args:
            sources:  {'mb': MusicBrainzRelease or dict, 'sp': spotify_album_dict,
                       'bp': BeatportRelease, ...}
            sp_full:  {track_id: full_track_dict} from SpotifyClient.get_tracks_batch()
        """
        if not sources:
            raise ValueError('At least one source required')
        self._sources  = sources
        self._sp_full  = sp_full or {}
        self._cached: MDBRelease | None = None

    # -- Source accessors -------------------------------------------------------

    @property
    def _mb(self):
        return self._sources.get('mb')

    @property
    def _sp(self):
        return self._sources.get('sp')

    @property
    def _bp(self):
        return self._sources.get('bp')

    def _mb_data(self) -> dict:
        mb = self._mb
        if mb is None:
            return {}
        return mb._data if hasattr(mb, '_data') else mb

    def _mb_tracks(self) -> list:
        mb = self._mb
        if mb is None:
            return []
        if hasattr(mb, 'tracks'):
            return mb.tracks
        # Raw _mb_get dict — build track list from media
        tracks = []
        for medium in (mb.get('media') or []):
            disc_num = medium.get('position') or 1
            for t in (medium.get('tracks') or []):
                rec = t.get('recording') or {}
                tracks.append({
                    'name':             rec.get('title') or t.get('title', ''),
                    'duration_ms':      rec.get('length') or t.get('length'),
                    '_mb_recording_id': rec.get('id'),
                    '_isrcs':           list(rec.get('isrcs') or []),
                    '_disc_number':     disc_num,
                    '_track_number':    t.get('position'),
                    '_artist_credit':   rec.get('artist-credit') or t.get('artist-credit') or [],
                })
        return tracks

    def _sp_data(self) -> dict:
        return self._sp or {}

    def _bp_data(self) -> dict:
        bp = self._bp
        return bp._data if (bp is not None and hasattr(bp, '_data')) else {}

    def _am_data(self) -> dict:
        am = self._sources.get('am')
        if am is None:
            return {}
        return am._data if hasattr(am, '_data') else (am if isinstance(am, dict) else {})

    def _bc_data(self) -> dict:
        bc = self._sources.get('bc')
        if bc is None:
            return {}
        return bc._data if hasattr(bc, '_data') else (bc if isinstance(bc, dict) else {})

    def _dz_data(self) -> dict:
        dz = self._sources.get('dz')
        if dz is None:
            return {}
        return dz._data if hasattr(dz, '_data') else (dz if isinstance(dz, dict) else {})

    def _bp_tracks(self) -> list:
        bp = self._bp
        if bp is None:
            return []
        return bp.tracks if hasattr(bp, 'tracks') else []

    # -- Public API -------------------------------------------------------------

    def release(self) -> MDBRelease:
        if self._cached is None:
            self._cached = self._build_release()
        return self._cached

    def tracks(self) -> list:
        return self.release().tracks

    # -- Release-level merge ---------------------------------------------------

    def _build_release(self) -> MDBRelease:
        mb   = self._mb_data()
        sp   = self._sp_data()
        bp   = self._bp_data()
        am   = self._am_data()
        bc   = self._bc_data()
        dz   = self._dz_data()
        conflicts: list[str] = []
        source_map: dict[str, str] = {}

        # Title: MB > Spotify > Beatport > Apple Music > Bandcamp
        title = (mb.get('title') or sp.get('name') or bp.get('name')
                 or am.get('collectionName') or bc.get('title') or '')
        if mb.get('title'):
            source_map['title'] = 'mb'
        elif sp.get('name'):
            source_map['title'] = 'sp'
        elif bp.get('name'):
            source_map['title'] = 'bp'
        elif am.get('collectionName'):
            source_map['title'] = 'am'
        else:
            source_map['title'] = 'bc'

        # Primary type — MB release-group is authoritative; never use BP inferred type
        primary_type = None
        rg = mb.get('release-group') or {}
        if rg.get('primary-type'):
            primary_type = rg['primary-type'].lower()
            source_map['primary_type'] = 'mb'
        elif sp.get('album_type'):
            primary_type = sp['album_type'].lower()
            source_map['primary_type'] = 'sp'

        # Type secondary — only MB has this authoritatively
        type_secondary = None
        _known_secondary = {'compilation', 'soundtrack', 'live', 'remix', 'dj-mix',
                            'mixtape', 'demo', 'spokenword', 'interview', 'audiobook',
                            'audio drama', 'field recording'}
        rg_secondary = [s.lower() for s in (rg.get('secondary-types') or [])]
        type_secondary = next((s for s in rg_secondary if s in _known_secondary), None)
        if type_secondary:
            source_map['type_secondary'] = 'mb'

        # UPC: include Apple Music / Bandcamp / Deezer UPC if available
        upc = self._merge_upc(mb, sp, bp, am, bc, dz, conflicts, source_map)

        # Release date: include Apple Music, Bandcamp, and Deezer dates
        release_date, date_source = self._merge_date(mb, sp, bp, am, bc, dz, conflicts, source_map)

        # Label: Spotify > MB > Beatport > Deezer > Bandcamp
        label = self._merge_label(mb, sp, bp, dz, source_map)

        # Platform IDs
        mbid               = mb.get('id')
        spotify_id         = sp.get('id')
        beatport_id_raw    = bp.get('id')
        beatport_id        = int(beatport_id_raw) if beatport_id_raw else None
        release_group_mbid = rg.get('id')
        # Apple Music: ItunesRelease exposes .itunes_id; dict fallback via 'collectionId'
        am_obj = self._sources.get('am')
        apple_music_id = (
            am_obj.itunes_id if hasattr(am_obj, 'itunes_id') else
            (str(am.get('collectionId')) if am.get('collectionId') else None)
        )

        # Cover art: Apple Music (3000px) > Bandcamp (3000px) > Beatport (1400px) > Deezer (1000px) > Spotify
        album_art_url, album_art_source = self._merge_art(am, bc, bp, dz, sp, source_map)

        # Popularity
        spotify_popularity = sp.get('popularity')

        # Total tracks — max(MB, SP); BP/AM/BC excluded when they have gaps
        total_tracks = self._merge_total_tracks(mb, sp, bp, conflicts)

        # Tracks
        tracks = self._build_tracks(conflicts)

        # Primary artist
        primary_artist = self._merge_primary_artist(mb, sp)

        return MDBRelease(
            title=title,
            primary_artist=primary_artist,
            release_date=release_date,
            date_source=date_source,
            primary_type=primary_type,
            type_secondary=type_secondary,
            label=label,
            upc=upc,
            release_group_mbid=release_group_mbid,
            mbid=mbid,
            spotify_id=spotify_id,
            beatport_id=beatport_id,
            apple_music_id=apple_music_id,
            album_art_url=album_art_url,
            album_art_source=album_art_source,
            spotify_popularity=spotify_popularity,
            total_tracks=total_tracks,
            tracks=tracks,
            conflicts=conflicts,
            source_map=source_map,
        )

    def _merge_upc(self, mb, sp, bp, am, bc, dz, conflicts, source_map):
        upcs: dict[str, str] = {}
        if mb.get('barcode'):
            n = normalize_upc(mb['barcode'])
            if n: upcs['mb'] = n
        upc_sp = (sp.get('external_ids') or {}).get('upc')
        if upc_sp:
            n = normalize_upc(upc_sp)
            if n: upcs['sp'] = n
        if bp.get('upc'):
            n = normalize_upc(str(bp['upc']))
            if n: upcs['bp'] = n
        if bc.get('upc'):
            n = normalize_upc(str(bc['upc']))
            if n: upcs['bc'] = n
        # Deezer UPC
        dz_obj = self._sources.get('dz')
        dz_upc = dz_obj.upc if hasattr(dz_obj, 'upc') else dz.get('upc')
        if dz_upc:
            n = normalize_upc(str(dz_upc))
            if n: upcs['dz'] = n
        if not upcs:
            return None
        best = max(upcs.values(), key=len)
        unique = set(upcs.values())
        if len(unique) > 1:
            conflicts.append(f"upc: sources disagree — {dict(upcs)}")
        source_map['upc'] = next(k for k, v in upcs.items() if v == best)
        return best

    def _merge_date(self, mb, sp, bp, am, bc, dz, conflicts, source_map):
        candidates: dict[str, list[str]] = {}

        d_mb = (mb.get('date') or '').strip()
        if d_mb and _date_prec(d_mb) > 0:
            candidates.setdefault(d_mb, []).append('mb')

        raw_sp = sp.get('release_date', '')
        prec_sp = sp.get('release_date_precision', '')
        if raw_sp:
            d_sp = raw_sp[:7] if prec_sp == 'month' else (raw_sp[:4] if prec_sp == 'year' else raw_sp)
            if _date_prec(d_sp) > 0:
                candidates.setdefault(d_sp, []).append('sp')

        if bp:
            if beatport_is_catalog_addition(bp):
                pub = bp.get('publish_date', '')
                enc = (bp.get('encoded_date') or '')[:10]
                conflicts.append(
                    f"release_date: BP discounted (encoded={enc}, published={pub}; catalog addition)"
                )
            else:
                d_bp = (bp.get('new_release_date') or bp.get('publish_date') or '').strip()
                if d_bp and _date_prec(d_bp) > 0:
                    candidates.setdefault(d_bp, []).append('bp')

        # Apple Music date (ISO-8601 with T — strip to YYYY-MM-DD)
        am_obj = self._sources.get('am')
        d_am = am_obj.date if hasattr(am_obj, 'date') else (am.get('releaseDate') or '')
        d_am = d_am[:10] if d_am else ''  # trim "2013-06-11T07:00:00Z" → "2013-06-11"
        if d_am and _date_prec(d_am) > 0:
            candidates.setdefault(d_am, []).append('am')

        # Bandcamp date
        bc_obj = self._sources.get('bc')
        d_bc = bc_obj.date if hasattr(bc_obj, 'date') else (bc.get('release_date') or '')
        if d_bc and _date_prec(d_bc) > 0:
            candidates.setdefault(d_bc, []).append('bc')

        # Deezer date (YYYY-MM-DD from API)
        dz_obj = self._sources.get('dz')
        d_dz = dz_obj.date if hasattr(dz_obj, 'date') else (dz.get('release_date') or '')
        if d_dz and _date_prec(d_dz) > 0:
            candidates.setdefault(d_dz, []).append('dz')

        # 2-of-3 majority
        for date_str, sources in candidates.items():
            if len(sources) >= 2:
                if len(candidates) > 1:
                    conflicts.append(
                        f"release_date: majority={date_str} ({'+'.join(sources)}) vs others"
                    )
                source_map['release_date'] = 'majority'
                return date_str, 'majority'

        # Fallback: source priority mb > sp > bp = dz
        for src in ('mb', 'sp', 'bp', 'dz', 'bc', 'am'):
            for date_str, sources in candidates.items():
                if src in sources:
                    source_map['release_date'] = src
                    return date_str, src

        return None, 'none'

    def _merge_label(self, mb, sp, bp, dz, source_map):
        # Spotify preferred (most widely recognized label name format)
        if sp.get('label'):
            source_map['label'] = 'sp'
            return MDBLabel(name=sp['label'])
        # MB: use first non-empty label-info entry
        for info in (mb.get('label-info') or []):
            lbl_name = (info.get('label') or {}).get('name', '')
            cat = info.get('catalog-number') or None
            lbl_mbid = (info.get('label') or {}).get('id') or None
            if lbl_name:
                source_map['label'] = 'mb'
                return MDBLabel(name=lbl_name, catalog_number=cat, mbid=lbl_mbid)
        # Deezer label (before Beatport — cleaner format, no distributor noise)
        dz_obj = self._sources.get('dz')
        dz_label = dz_obj.label if hasattr(dz_obj, 'label') else dz.get('label', '')
        if dz_label:
            source_map['label'] = 'dz'
            return MDBLabel(name=dz_label)
        # Beatport: strip distributors stored in parens
        bp_lbl = (bp.get('label') or {}).get('name', '')
        if bp_lbl:
            cat_raw = bp.get('catalog_number')
            cat = None if (cat_raw and re.fullmatch(r'\d+', str(cat_raw))) else (cat_raw or None)
            source_map['label'] = 'bp'
            return MDBLabel(name=bp_lbl, catalog_number=cat)
        return None

    def _merge_art(self, am, bc, bp, dz, sp, source_map):
        """Priority: Apple Music (3000px) > Bandcamp (3000px) > Beatport (1400px) > Deezer (1000px) > Spotify."""
        am_obj = self._sources.get('am')
        if am_obj is not None:
            am_url = am_obj.image_url if hasattr(am_obj, 'image_url') else None
            if am_url:
                source_map['album_art_url'] = 'am'
                return am_url, 'apple_music'
        bc_obj = self._sources.get('bc')
        if bc_obj is not None:
            bc_url = bc_obj.image_url if hasattr(bc_obj, 'image_url') else None
            if bc_url:
                source_map['album_art_url'] = 'bc'
                return bc_url, 'bandcamp'
        if bp:
            img = bp.get('image') or {}
            dyn = img.get('dynamic_uri', '')
            if dyn:
                source_map['album_art_url'] = 'bp'
                return dyn.replace('{w}x{h}', '1400x1400'), 'beatport'
            uri = img.get('uri', '')
            if uri:
                source_map['album_art_url'] = 'bp'
                return uri, 'beatport'
        dz_obj = self._sources.get('dz')
        if dz_obj is not None:
            dz_url = dz_obj.image_url if hasattr(dz_obj, 'image_url') else None
            if dz_url:
                source_map['album_art_url'] = 'dz'
                return dz_url, 'deezer'
        if sp:
            images = sp.get('images') or []
            if images:
                best = max(images, key=lambda i: (i.get('width') or 0) * (i.get('height') or 0),
                           default=None)
                if best:
                    source_map['album_art_url'] = 'sp'
                    return best['url'], 'spotify'
        return None, None

    def _merge_total_tracks(self, mb, sp, bp, conflicts):
        mb_count = sum(len(m.get('tracks') or []) for m in (mb.get('media') or [])) or None
        sp_count = sp.get('total_tracks') or None
        bp_count = (bp.get('track_count') or len(self._bp_tracks())) or None

        counts = [c for c in [mb_count, sp_count] if c is not None]
        if len(set(counts)) > 1:
            conflicts.append(f"track_count: MB={mb_count}, SP={sp_count}")

        if bp_count is not None:
            best = max(filter(None, [mb_count, sp_count]), default=bp_count)
            if bp_count < best:
                conflicts.append(
                    f"track_count: BP has {bp_count} tracks, MB/SP have {best} — BP missing tracks"
                )
        return max(filter(None, [mb_count, sp_count, bp_count]), default=None)

    def _merge_primary_artist(self, mb, sp):
        for credit in (mb.get('artist-credit') or []):
            if isinstance(credit, dict) and 'artist' in credit:
                a = credit['artist']
                credited = credit.get('name')
                return MDBArtistCredit(
                    name=a.get('name', ''),
                    credited_name=credited if (credited and credited != a.get('name')) else None,
                    mbid=a.get('id'),
                    role='main',
                )
        for a in (sp.get('artists') or []):
            return MDBArtistCredit(name=a.get('name', ''), spotify_id=a.get('id'), role='main')
        return None

    # -- Track merge -----------------------------------------------------------

    def _build_tracks(self, conflicts) -> list[MDBTrack]:
        mb_tracks = self._mb_tracks()
        sp_tracks = self._sp_data().get('_all_tracks') or []
        bp_tracks = self._bp_tracks()

        # Build ISRC index per source
        mb_by_isrc: dict = {}
        for t in mb_tracks:
            for isrc in (t.get('_isrcs') or []):
                if isrc:
                    mb_by_isrc[isrc] = t

        sp_by_isrc: dict = {}  # isrc → (simple_track, full_track)
        for t in sp_tracks:
            full = self._sp_full.get(t.get('id'), t)
            isrc = (full.get('external_ids') or {}).get('isrc', '')
            if isrc:
                sp_by_isrc[isrc] = (t, full)

        bp_by_isrc: dict = {}
        for t in bp_tracks:
            for isrc in (t.get('_isrcs') or []):
                if isrc:
                    bp_by_isrc[isrc] = t

        dz_obj = self._sources.get('dz')
        dz_tracks = dz_obj.tracks if (dz_obj is not None and hasattr(dz_obj, 'tracks')) else []
        dz_by_isrc: dict = {}
        for t in dz_tracks:
            for isrc in (t.get('_isrcs') or []):
                if isrc:
                    dz_by_isrc[isrc] = t

        # Ordering: preferred source for track_number / disc_number
        mb_order: dict = {}
        for t in mb_tracks:
            isrcs = t.get('_isrcs') or []
            if isrcs:
                mb_order[isrcs[0]] = (t.get('_track_number'), t.get('_disc_number', 1))

        sp_order: dict = {}
        for t in sp_tracks:
            full = self._sp_full.get(t.get('id'), t)
            isrc = (full.get('external_ids') or {}).get('isrc', '')
            if isrc:
                sp_order[isrc] = (t.get('track_number'), t.get('disc_number', 1))

        all_isrcs = (set(mb_by_isrc) | set(sp_by_isrc) | set(bp_by_isrc) | set(dz_by_isrc)) - {'', None}

        # Fallback for releases where no source has ISRCs (e.g. self-released mixtapes
        # on MusicBrainz).  Build tracks positionally from MB, supplemented by Spotify
        # duration data matched on normalised title.
        if not all_isrcs:
            merged = self._build_tracks_positional(
                mb_tracks, sp_tracks, bp_tracks, conflicts
            )
            merged.sort(key=lambda t: (t.disc_number, t.track_number))
            return merged

        merged: list[MDBTrack] = []
        for isrc in all_isrcs:
            mb_t = mb_by_isrc.get(isrc)
            sp_pair = sp_by_isrc.get(isrc)
            sp_t = sp_pair[0] if sp_pair else None
            sp_full_t = sp_pair[1] if sp_pair else None
            bp_t = bp_by_isrc.get(isrc)
            dz_t = dz_by_isrc.get(isrc)

            # ISRC cross-validation
            if mb_t and sp_full_t:
                mb_isrcs = set(mb_t.get('_isrcs') or [])
                sp_isrc = (sp_full_t.get('external_ids') or {}).get('isrc', '')
                if sp_isrc and mb_isrcs and sp_isrc not in mb_isrcs:
                    conflicts.append(
                        f"isrc conflict: MB={sorted(mb_isrcs)} vs SP/BP={sp_isrc} — SP+BP used"
                    )

            # Track number / disc
            if isrc in mb_order:
                track_number, disc_number = mb_order[isrc]
            elif isrc in sp_order:
                track_number, disc_number = sp_order[isrc]
            else:
                track_number = (bp_t or {}).get('_track_number', 0) if bp_t else 0
                disc_number = 1

            # Title: always store the full displayable title (base + ETI in parens).
            # mix_name is populated from Beatport as an advisory annotation for
            # styled rendering — it is never load-bearing; title is always complete.
            if bp_t:
                mix_name = bp_t.get('_mix_name') or None
                # full_name from Beatport already contains "(mix_name)" when present
                title = bp_t.get('name', '')
            elif mb_t:
                title = mb_t.get('name', '')
                mix_name = None
            elif sp_t:
                title = (sp_t or {}).get('name', '')
                mix_name = None
            else:
                title = ''
                mix_name = None

            # Duration: Spotify (precise ms) > MB (rounded) > Beatport > Deezer (seconds precision)
            if sp_full_t and sp_full_t.get('duration_ms'):
                duration_ms = sp_full_t['duration_ms']
            elif mb_t and mb_t.get('duration_ms'):
                duration_ms = mb_t['duration_ms']
            elif bp_t and bp_t.get('duration_ms'):
                duration_ms = bp_t['duration_ms']
            elif dz_t and dz_t.get('duration_ms'):
                duration_ms = dz_t['duration_ms']
            else:
                duration_ms = None

            is_explicit   = bool((sp_full_t or {}).get('explicit', False))
            mbid          = (mb_t or {}).get('_mb_recording_id')
            spotify_id    = (sp_t or {}).get('id')
            bpm           = (bp_t or {}).get('_bpm') if bp_t else None
            musical_key   = (bp_t or {}).get('_key') if bp_t else None
            key_camelot   = (bp_t or {}).get('_key_camelot') if bp_t else None
            bp_genre      = (bp_t or {}).get('_genre') or None if bp_t else None
            bp_sub_genre  = (bp_t or {}).get('_sub_genre') or None if bp_t else None
            bp_track_id   = (bp_t or {}).get('_track_id') if bp_t else None
            sp_pop        = (sp_full_t or {}).get('popularity') if sp_full_t else None

            artists = self._merge_track_artists(mb_t, sp_t, bp_t)

            sources: set[str] = set()
            if mb_t:
                sources.add('mb')
            if sp_t:
                sources.add('sp')
            if bp_t:
                sources.add('bp')
            if dz_t:
                sources.add('dz')

            track_source_map = {
                'title':       'bp' if bp_t else ('mb' if mb_t else 'sp'),
                'duration_ms': 'sp' if (sp_full_t and sp_full_t.get('duration_ms'))
                               else ('mb' if (mb_t and mb_t.get('duration_ms')) else 'bp'),
            }
            if bpm:
                track_source_map['bpm'] = 'bp'
            if musical_key:
                track_source_map['musical_key'] = 'bp'

            merged.append(MDBTrack(
                isrc=isrc,
                track_number=track_number or 0,
                disc_number=disc_number or 1,
                title=title,
                mix_name=mix_name,
                duration_ms=duration_ms,
                is_explicit=is_explicit,
                mbid=mbid,
                spotify_id=spotify_id,
                bpm=bpm,
                musical_key=musical_key,
                key_camelot=key_camelot,
                beatport_genre=bp_genre,
                beatport_sub_genre=bp_sub_genre,
                beatport_track_id=int(bp_track_id) if bp_track_id else None,
                spotify_popularity=sp_pop,
                artists=artists,
                sources=sources,
                source_map=track_source_map,
            ))

        merged.sort(key=lambda t: (t.disc_number, t.track_number))
        return merged

    def _build_tracks_positional(
        self, mb_tracks: list, sp_tracks: list, bp_tracks: list, conflicts: list
    ) -> list[MDBTrack]:
        """Fallback when no source has ISRCs — build tracks by position from MB.

        Matches Spotify tracks by normalised title to fill in duration_ms and
        spotify_id; Beatport by position for BPM/key.  Used for releases like
        self-released mixtapes that lack ISRC registration.
        """
        # Spotify title lookup (normalised) for duration/id supplementation
        sp_by_title: dict = {}
        for t in sp_tracks:
            full = self._sp_full.get(t.get('id'), t)
            key = normalize_text(t.get('name', ''))
            if key:
                sp_by_title[key] = (t, full)

        # Beatport by position
        bp_by_pos: dict = {}
        for t in bp_tracks:
            bp_by_pos[(t.get('_disc_number', 1), t.get('_track_number', 0))] = t

        tracks: list[MDBTrack] = []
        for mb_t in sorted(mb_tracks, key=lambda t: (t.get('_disc_number', 1), t.get('_track_number', 0))):
            title     = mb_t.get('name', '')
            mix_name  = None
            track_num = mb_t.get('_track_number', 0)
            disc_num  = mb_t.get('_disc_number', 1)
            mbid      = mb_t.get('_mb_recording_id')

            # Try to supplement from Spotify by title match
            sp_pair  = sp_by_title.get(normalize_text(title))
            sp_t     = sp_pair[0] if sp_pair else None
            sp_full_t = sp_pair[1] if sp_pair else None

            bp_t     = bp_by_pos.get((disc_num, track_num))

            duration_ms = (
                (sp_full_t.get('duration_ms') if sp_full_t else None)
                or mb_t.get('duration_ms')
                or (bp_t.get('duration_ms') if bp_t else None)
            )
            spotify_id  = sp_t.get('id') if sp_t else None
            bpm         = bp_t.get('_bpm') if bp_t else None
            musical_key = bp_t.get('_key') if bp_t else None
            key_camelot = bp_t.get('_key_camelot') if bp_t else None
            is_explicit = bool((sp_full_t or {}).get('explicit', False))

            sources: set = {'mb'}
            if sp_t:
                sources.add('sp')
            if bp_t:
                sources.add('bp')

            artists = self._merge_track_artists(mb_t, sp_t, bp_t)

            tracks.append(MDBTrack(
                isrc=None,  # no ISRCs available
                track_number=track_num,
                disc_number=disc_num,
                title=title,
                mix_name=mix_name,
                duration_ms=duration_ms,
                is_explicit=is_explicit,
                mbid=mbid,
                spotify_id=spotify_id,
                bpm=bpm,
                musical_key=musical_key,
                key_camelot=key_camelot,
                beatport_genre=bp_t.get('_genre') if bp_t else None,
                beatport_sub_genre=bp_t.get('_sub_genre') if bp_t else None,
                beatport_track_id=bp_t.get('_track_id') if bp_t else None,
                spotify_popularity=(sp_full_t or {}).get('popularity') if sp_full_t else None,
                artists=artists,
                sources=sources,
                source_map={'title': 'mb', 'duration_ms': 'sp' if sp_full_t else 'mb'},
            ))
        return tracks

    def _merge_track_artists(self, mb_t, sp_t, bp_t) -> list[MDBArtistCredit]:
        """Merge artist credits from all sources, unifying by normalized name."""
        by_norm: dict[str, MDBArtistCredit] = {}

        # MB: most authoritative (has MBIDs)
        if mb_t:
            for j, credit in enumerate(mb_t.get('_artist_credit') or []):
                if not isinstance(credit, dict) or 'artist' not in credit:
                    continue
                a = credit['artist']
                name = a.get('name', '')
                key = normalize_text(name)
                if key:
                    credited = credit.get('name')
                    by_norm[key] = MDBArtistCredit(
                        name=name,
                        credited_name=credited if (credited and credited != name) else None,
                        join_phrase=credit.get('joinphrase', ''),
                        role='main' if j == 0 else 'featured',
                        mbid=a.get('id'),
                    )

        # Spotify: add spotify_id to existing entries, or create new
        if sp_t:
            for j, a in enumerate((sp_t.get('artists') or [])):
                name = a.get('name', '')
                key = normalize_text(name)
                if key in by_norm:
                    by_norm[key].spotify_id = a.get('id')
                else:
                    by_norm[key] = MDBArtistCredit(
                        name=name,
                        role='main' if j == 0 else 'featured',
                        spotify_id=a.get('id'),
                    )

        # Beatport remixers
        if bp_t:
            for r in (bp_t.get('_remixers') or []):
                if not isinstance(r, dict) or 'artist' not in r:
                    continue
                a = r['artist']
                name = a.get('name', '')
                key = normalize_text(name)
                if key in by_norm:
                    by_norm[key].beatport_id = str(a.get('id', ''))
                    by_norm[key].role = 'remixer'
                else:
                    by_norm[key] = MDBArtistCredit(
                        name=name,
                        role='remixer',
                        beatport_id=str(a.get('id', '')),
                    )

        return list(by_norm.values())


# -- GTIN auto-discovery -------------------------------------------------------

def resolve_by_gtin(upc: str, skip: frozenset = frozenset()) -> dict[str, str]:
    """Given a UPC/GTIN, return {provider_key: id} for all discoverable sources.

    Uses the GTIN broadcast approach from Harmony: one UPC queries Spotify,
    MusicBrainz, iTunes, etc. simultaneously.  Bandcamp does not support GTIN
    lookups and is never auto-discovered.

    Args:
        upc:   UPC/GTIN string (any format; normalized internally)
        skip:  Provider keys to skip (e.g. frozenset({'sp'}) if you already have Spotify)

    Returns:
        Dict with subset of {'sp': spotify_album_id, 'mb': mbid, 'am': itunes_id}
    """
    normalized = normalize_upc(upc)
    if not normalized:
        return {}

    result: dict[str, str] = {}

    if 'sp' not in skip:
        try:
            import os
            from mdb_apis import SpotifyClient
            from mdb_ops import load_dotenv
            load_dotenv()
            cid = os.environ.get('SPOTIFY_CLIENT_ID', '')
            csc = os.environ.get('SPOTIFY_CLIENT_SECRET', '')
            if cid and csc:
                client = SpotifyClient(cid, csc)
                data = client.get(f'/search?q=upc:{normalized}&type=album&limit=1')
                items = (data.get('albums') or {}).get('items') or []
                if items:
                    result['sp'] = items[0]['id']
        except Exception:
            pass

    if 'mb' not in skip:
        try:
            from mdb_apis import _mb_get
            data = _mb_get('/release', {'query': f'barcode:{normalized}', 'limit': 1})
            releases = data.get('releases') or []
            if releases:
                result['mb'] = releases[0]['id']
        except Exception:
            pass

    if 'am' not in skip:
        try:
            import json
            import urllib.request
            req = urllib.request.Request(
                f'https://itunes.apple.com/lookup?upc={normalized}&entity=album&limit=1',
                headers={'User-Agent': 'mdb/1.0'},
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                data = json.loads(r.read())
            for item in (data.get('results') or []):
                if item.get('wrapperType') == 'collection':
                    result['am'] = str(item['collectionId'])
                    break
        except Exception:
            pass

    return result


# -- DB persistence helpers ----------------------------------------------------

def _resolve_artist_credit(cur: sqlite3.Cursor, credit: MDBArtistCredit) -> str | None:
    """Find or create an artist from an MDBArtistCredit. Returns the DB artist id."""
    now = int(time.time())

    if credit.mbid:
        row = cur.execute('SELECT id FROM artists WHERE mbid = ?', (credit.mbid,)).fetchone()
        if row:
            return row[0]

    if credit.spotify_id:
        row = cur.execute('SELECT id FROM artists WHERE spotify_id = ?',
                          (credit.spotify_id,)).fetchone()
        if row:
            return row[0]

    if credit.name:
        row = cur.execute('SELECT id FROM artists WHERE lower(name) = lower(?)',
                          (credit.name,)).fetchone()
        if row:
            aid = row[0]
            if credit.mbid:
                try:
                    cur.execute('UPDATE artists SET mbid = ?, updated_at = ? WHERE id = ?',
                                (credit.mbid, now, aid))
                except sqlite3.IntegrityError:
                    pass
            if credit.spotify_id:
                try:
                    cur.execute('UPDATE artists SET spotify_id = ?, updated_at = ? WHERE id = ?',
                                (credit.spotify_id, now, aid))
                except sqlite3.IntegrityError:
                    pass
            return aid

    if not credit.name:
        return None

    base = slugify(credit.name)
    existing = {r[0] for r in cur.execute(
        'SELECT slug FROM artists WHERE slug IS NOT NULL').fetchall()}
    slug = unique_slug(base, existing)
    aid = new_ulid()
    cur.execute(
        'INSERT INTO artists (id, slug, name, mbid, spotify_id, created_at, updated_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?)',
        (aid, slug, credit.name, credit.mbid or None, credit.spotify_id or None, now, now),
    )
    return aid


def upsert_release_mdb(cur: sqlite3.Cursor,
                       release: MDBRelease,
                       primary_artist_id: str) -> tuple[str, bool]:
    """Insert or update a release from an MDBRelease. Returns (release_id, created).

    Respects existing manual/wikipedia dates via _should_update_date().
    Stores Beatport ID in external_links when present.
    """
    now = int(time.time())

    # Find existing record: spotify_id → mbid → beatport_id → apple_music_id in external_links
    row = None
    if release.spotify_id:
        row = cur.execute(
            'SELECT id, release_date, date_source, album_art_url FROM releases'
            ' WHERE spotify_id = ?', (release.spotify_id,)
        ).fetchone()
    if not row and release.mbid:
        row = cur.execute(
            'SELECT id, release_date, date_source, album_art_url FROM releases WHERE mbid = ?',
            (release.mbid,)
        ).fetchone()
    if not row and release.beatport_id:
        link_row = cur.execute(
            'SELECT entity_id FROM external_links'
            ' WHERE entity_type = ? AND service = ? AND link_value = ?',
            (EL_RELEASE, EL_SVC_BEATPORT, str(release.beatport_id))
        ).fetchone()
        if link_row:
            row = cur.execute(
                'SELECT id, release_date, date_source, album_art_url FROM releases WHERE id = ?',
                (link_row[0],)
            ).fetchone()
    if not row and release.apple_music_id:
        row = cur.execute(
            'SELECT id, release_date, date_source, album_art_url FROM releases'
            ' WHERE apple_music_id = ?', (release.apple_music_id,)
        ).fetchone()

    label_str = release.label.name if release.label else None
    rel_year  = int(release.release_date[:4]) if (release.release_date
                                                   and release.release_date[:4].isdigit()) else None

    if row:
        release_id  = row['id']
        update_date = _should_update_date(
            row['release_date'], row['date_source'],
            release.release_date, release.date_source,
        )
        date_fields = ', release_date=?, release_year=?, date_source=?' if update_date else ''
        date_vals   = (release.release_date, rel_year, release.date_source) if update_date else ()
        cur.execute(
            f'UPDATE releases SET title=?, primary_artist_id=?, type=?,'
            f' type_secondary=COALESCE(type_secondary, ?){date_fields},'
            f' label=?, mbid=COALESCE(mbid, ?), release_group_mbid=COALESCE(release_group_mbid, ?),'
            f' apple_music_id=COALESCE(apple_music_id, ?),'
            f' total_tracks=?, spotify_popularity=?, updated_at=? WHERE id=?',
            (release.title, primary_artist_id, release.primary_type,
             release.type_secondary, *date_vals,
             label_str, release.mbid, release.release_group_mbid,
             release.apple_music_id,
             release.total_tracks, release.spotify_popularity, now, release_id),
        )
        if release.album_art_url and not row['album_art_url']:
            cur.execute(
                'UPDATE releases SET album_art_url=?, album_art_source=? WHERE id=?',
                (release.album_art_url, release.album_art_source, release_id),
            )
        created = False
    else:
        release_id = new_ulid()
        base = slugify(release.title)
        existing = {r[0] for r in cur.execute(
            'SELECT slug FROM releases WHERE primary_artist_id IS ?',
            (primary_artist_id,)
        ).fetchall()}
        slug = unique_slug(base, existing)
        cur.execute(
            'INSERT INTO releases (id, slug, title, primary_artist_id, type, type_secondary,'
            ' release_date, release_year, date_source, label, spotify_id, mbid,'
            ' release_group_mbid, apple_music_id, album_art_url, album_art_source,'
            ' total_tracks, spotify_popularity, created_at, updated_at)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (release_id, slug, release.title, primary_artist_id,
             release.primary_type, release.type_secondary,
             release.release_date, rel_year, release.date_source or None,
             label_str, release.spotify_id, release.mbid,
             release.release_group_mbid, release.apple_music_id,
             release.album_art_url, release.album_art_source,
             release.total_tracks, release.spotify_popularity,
             now, now),
        )
        created = True

    if release.beatport_id:
        cur.execute(
            'INSERT INTO external_links (entity_type, entity_id, service, link_value)'
            ' VALUES (?, ?, ?, ?)'
            ' ON CONFLICT(entity_type, entity_id, service)'
            ' DO UPDATE SET link_value = excluded.link_value',
            (EL_RELEASE, release_id, EL_SVC_BEATPORT, str(release.beatport_id)),
        )

    # Store Bandcamp URL in external_links if we have a BC source
    bc_obj = None
    # Check the caller's source registry isn't available here — rely on a sentinel field
    # The Bandcamp URL is stored via _store_external_links in mdb.py after upsert

    return release_id, created


def upsert_tracks_mdb(cur: sqlite3.Cursor,
                      release_id: str,
                      tracks: list[MDBTrack]) -> tuple[int, int]:
    """Insert or update tracks from a list of MDBTrack objects.

    Resolves/creates artist records and inserts track_artists (main/featured/remixer).
    Returns (created_count, updated_count).
    """
    created = updated = 0
    now = int(time.time())

    for t in tracks:
        # Locate existing track: spotify_id → mbid → isrc
        row = None
        if t.spotify_id:
            row = cur.execute(
                'SELECT id, title FROM tracks WHERE spotify_id = ?', (t.spotify_id,)
            ).fetchone()
        if not row and t.mbid:
            row = cur.execute(
                'SELECT id, title FROM tracks WHERE mbid = ?', (t.mbid,)
            ).fetchone()
        if not row and t.isrc:
            row = cur.execute(
                'SELECT id, title FROM tracks WHERE isrc = ?', (t.isrc,)
            ).fetchone()

        def _do_update(track_id, include_ids=True):
            if include_ids:
                try:
                    cur.execute(
                        'UPDATE tracks SET release_id=?, title=?, track_number=?, disc_number=?,'
                        ' duration_ms=?, is_explicit=?, isrc=?, mbid=?, spotify_id=?,'
                        ' mix_name=?, musical_key=?, beatport_genre=?, beatport_sub_genre=?,'
                        ' tempo_bpm=?, spotify_popularity=?, updated_at=?'
                        ' WHERE id=?',
                        (release_id, t.title, t.track_number, t.disc_number,
                         t.duration_ms, 1 if t.is_explicit else 0, t.isrc, t.mbid, t.spotify_id,
                         t.mix_name, t.musical_key, t.beatport_genre, t.beatport_sub_genre,
                         t.bpm, t.spotify_popularity, now, track_id),
                    )
                    return True
                except sqlite3.IntegrityError:
                    pass
            cur.execute(
                'UPDATE tracks SET release_id=?, title=?, track_number=?, disc_number=?,'
                ' duration_ms=?, is_explicit=?, isrc=?,'
                ' mix_name=?, musical_key=?, beatport_genre=?, beatport_sub_genre=?,'
                ' tempo_bpm=?, spotify_popularity=?, updated_at=?'
                ' WHERE id=?',
                (release_id, t.title, t.track_number, t.disc_number,
                 t.duration_ms, 1 if t.is_explicit else 0, t.isrc,
                 t.mix_name, t.musical_key, t.beatport_genre, t.beatport_sub_genre,
                 t.bpm, t.spotify_popularity, now, track_id),
            )
            return True

        if row:
            track_id = row[0]
            _do_update(track_id)
            updated += 1
        else:
            track_id = new_ulid()
            try:
                cur.execute(
                    'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                    ' duration_ms, is_explicit, spotify_id, mbid, isrc,'
                    ' mix_name, musical_key, beatport_genre, beatport_sub_genre,'
                    ' tempo_bpm, spotify_popularity, created_at, updated_at)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (track_id, t.title, release_id, t.track_number, t.disc_number,
                     t.duration_ms, 1 if t.is_explicit else 0,
                     t.spotify_id, t.mbid, t.isrc,
                     t.mix_name, t.musical_key, t.beatport_genre, t.beatport_sub_genre,
                     t.bpm, t.spotify_popularity, now, now),
                )
            except sqlite3.IntegrityError:
                cur.execute(
                    'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                    ' duration_ms, is_explicit, isrc,'
                    ' mix_name, musical_key, beatport_genre, beatport_sub_genre,'
                    ' tempo_bpm, spotify_popularity, created_at, updated_at)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (track_id, t.title, release_id, t.track_number, t.disc_number,
                     t.duration_ms, 1 if t.is_explicit else 0, t.isrc,
                     t.mix_name, t.musical_key, t.beatport_genre, t.beatport_sub_genre,
                     t.bpm, t.spotify_popularity, now, now),
                )
            created += 1

        # Track artists (replace all, then re-insert)
        cur.execute('DELETE FROM track_artists WHERE track_id = ?', (track_id,))
        for credit in t.artists:
            artist_id = _resolve_artist_credit(cur, credit)
            if artist_id:
                try:
                    cur.execute(
                        'INSERT INTO track_artists (track_id, artist_id, role)'
                        ' VALUES (?, ?, ?)',
                        (track_id, artist_id, credit.role),
                    )
                except sqlite3.IntegrityError:
                    pass

    return created, updated
