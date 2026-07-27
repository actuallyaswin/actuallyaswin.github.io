# MusicBrainz Style Conventions

Sources:
- https://musicbrainz.org/doc/Style/Titles
- https://musicbrainz.org/doc/Style/Artist_Credits
- https://musicbrainz.org/doc/Style/Language/English
- https://musicbrainz.org/doc/Guess_Case

---

## Titles

When entering a new release into MusicBrainz, all **titles** are normalized according to these guidelines.

### Capitalization

Album and song titles are often rendered in all-uppercase on packaging (e.g. "SONGS OF LOVE AND HATE" on the cover of *Songs of Love and Hate*). MusicBrainz treats this as a typesetting decision, not an intrinsic feature of the work — titles are capitalized per language-specific rules instead of copied verbatim.

**Extra title information (ETI)**: additional information on a release/track title that is *not* part of the main title, but distinguishes it from other releases/tracks with the same main title (versions, remix names, live-recording info). Entered after the main title, preceded by a single space, wrapped in parentheses `()`. **Featured artists are never entered this way** — they belong in the artist credit (see below).

Titles/subtitles of mixes or versions follow language-specific style rules; all other ETI elements are lowercase except words that would normally be capitalized in that language. Examples:

- *Situations Like These (album version)*
- *Bear Witness (Automator's 2 Turntables and a Razorblade re-edit)*

If a language uses title-case formatting (like English), ETI capitalization follows:

1. No names or title elements in the ETI → all lowercase.
2. ETI contains a distinct title → use the language's title-case rules.
3. Combination of title + descriptive elements → lowercase only the descriptive part (words like *mix, remix, live, remaster, edit*).

Examples: *Never Ending Story (power club vocal mix)*, *The Age of Love (Watch Out for Stella club mix)*.

Additional information that's neither part of the title nor there to disambiguate should be removed outright: `Song (bonus track)` → `Song`; `Song (The Beatles cover)` → `Song` (plus a "recording of" relationship to the original work, tagged with the "cover" attribute).

### Subtitles

A colon (`:`) separates a subtitle from the primary title, unless the release itself used different punctuation (`?`, `!`, en dash), in which case use that instead. E.g. *Biography: The Greatest Hits*; *Who Cares a Lot? Greatest Hits* (already has a `?`).

### Multiple or split titles

When a release combines multiple earlier releases, a track contains two+ songs, or a split release has different titles per artist, format as `Title 1 / Title 2` (space-slash-space, both complete titles). Same convention applies to artist credits for multi-song/split entities: `Artist 1 / Artist 2`. Only applies to entities with genuinely multiple titles — not single titles that happen to contain a slash (e.g. *White Light/White Heat* stays as-is).

### Series numbering

Words like "volume"/"vol." or "part"/"pt." indicating position in a series are separated from the title by a comma and single space, unless the release itself used different punctuation. E.g. *Orchestral Songs, Volume 1*; *Sonates, vol. 3* (lowercase "vol." — French); *The Best Smooth Jazz… Ever! Vol. 4* (separated by "!" on the release).

### Format designations

Include a format/medium/packaging designation (EP, 7", CD, LP, single) in the title *only if it's explicitly part of the printed title*. Don't add it if it's just metadata about the release type. E.g. *Flatline EP* keeps "EP" (it's part of the title); *Broken* doesn't get "EP" appended even if it is one.

### Performers in titles

Release/release-group titles shouldn't generally include the performer's name unless it's clearly part of the title (the performer refers to it that way, or the title reads as "unfinished" without it). E.g. *The Best of Tangerine Dream* (not just "The Best Of"); *Her Majesty the Decemberists* (not just "Her Majesty," per the artist's own usage). Artist intent overrides the general rule when documented.

### Technical limitations

Titles over 1,024 characters (2,704 bytes) aren't supported. If truncating, prefer the precomposed ellipsis character `…` (U+2026) after the last full word that fits, and record the unabridged title as an annotation.

---

## Artist Credits

Artist credits generally follow the actual credit printed on the release/track, including join phrases (e.g. "&", "with the", "feat.").

- *The Fat of the Land* is credited to "Prodigy," not the main artist name "The Prodigy."
- *Cendre* is credited to "Fennesz + Sakamoto" (as printed, including using "Sakamoto" in Latin script rather than the artist's full main name).

Join phrases are capitalized as normally written in their language (join phrases are not considered part of a title). E.g. "with the," not "With the."

### Featured artists

**Always entered in the artist credit, never in the title.** Enter the credit as printed, omitting separators (parentheses) meant to set it apart from the track title. `"Artist 1 - Song Name (featuring Artist 2)"` on a tracklist → title `"Song Name"`, artist credit `"Artist 1 featuring Artist 2"`.

- *Umbrella* → "Rihanna feat. Jay-Z"
- *Game Over* → "Tinchy Stryder featuring Giggs, Professor Green, Tinie Tempah, Devlin, Example, Chipmunk" (all commas, matching the printed tracklist)

### Definite articles

If an artist name doesn't itself start with a definite article, but is credited with one, include the article in the credit — unless it reads as part of a join phrase ("and the," "with the," "y los"), in which case it belongs to the join phrase instead.

### Multiple artists / splits

Same `Artist 1 / Artist 2` convention as multi-titles above, for tracks containing multiple songs by different artists and for split releases/release-groups.

### Various Artists releases

A compilation cover listing a few well-known contributors ("X, Y, Z and many others") is promotional, not a real credit — still credited to "Various Artists."

### Mixes and compilations

DJ-mix compilations are credited to the named mixer (rather than "Various Artists") whenever the mixer is officially credited anywhere on the release. Same logic for unmixed compilations that credit a compiler on the cover.

### Field recordings

Credited to the principal compiler/field recordist when known; `[no artist]` (with an appropriate special-purpose-artist subset credit) when no artist information exists at all.

### No printed join phrase

If a release lists collaborating artists with no join phrase, just separated by whitespace, use the defaults: ampersand between the last two, comma between the rest.

---

## Language: English

All words in a title are capitalized (first letter up, rest lowercase) except:

1. **Always capitalize the first and last word of a title.** Applies even to words that would otherwise be lowercase. If a title is broken by major punctuation (colon, `?`, `!`, em dash, quotes), capitalize each piece as if it were its own title — so also the first/last word of each section.
2. Between the first and last word, **capitalize all words except**:
   - Articles: *a, an, the*
   - Coordinate conjunctions: *and, but, or, nor*
   - Short prepositions (≤3 letters): *as, at, by, for, in, of, on, to, but, cum, mid, off, per, qua, re, up, via* — except when used as adverbs or as an inseparable part of a verb (e.g. "Plug **In** Baby," "Shine **On** You Crazy Diamond").
   - "to" when forming an infinitive.
3. In hyphenated compounds, capitalize each part as if it were a separate word.
4. Capitalize contractions/slang consistent with the above where it clearly applies (don't capitalize `o'` for "of," `'n'`/`n'` for "and").

### Parts of titles inside parentheses

Mostly capitalized as if the parentheses weren't there:

- "Have You Ever Been (to Electric Ladyland)"
- "(I Don't Want to Go to) Chelsea"

**Exceptions** — when the parenthetical reads as optional or the sentence genuinely continues/restarts after it, capitalize as a new sentence:

- "(Don't Fear) The Reaper" — "(Don't Fear)" is optional, so "The" caps as if starting the sentence.
- "1983… (A Merman I Should Turn to Be)" — anything after "…" is a new sentence, so "A" is capitalized.
- "Ramp! (The Logical Song)" — everything after "!" is a new sentence too.
- "Fly Me to the Moon (In Other Words)" — two unrelated parts occurring at different points in the lyrics, not a continued sentence.

### Special case

Both parts of "O'Clock" are capitalized (e.g. "Nine O'Clock").

---

## Guess Case (MusicBrainz's auto-capitalization tool)

Guess Case adjusts title capitalization to be closer to the language guidelines above. Imperfect, but saves time; results still need human review.

**Modes**: English (follows the English guidelines above), Sentence (capitalizes only the first word of a sentence — closer to most non-English languages, but still needs a manual check for proper nouns), French (like Sentence, but pads `;:!?` and guillemets `«text»` with spaces), Turkish (like English, but with Turkish-specific lowercase word list and correct handling of dotted/dotless i).

**Options**: "Keep all-uppercase words uppercased" (on by default — leaves intentionally-uppercase words like "Absolute ABBA" alone; turn off to fix an all-caps tracklist). "Uppercase Roman numerals" (off by default is often safer — words like "mix," "mic," or "mi" can look like Roman numerals and get mangled if this is on without a specific reason).
