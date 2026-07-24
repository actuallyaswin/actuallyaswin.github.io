# Writing and publishing blog posts

This is a Jekyll blog living inside the `actuallyaswin.github.io` GitHub Pages repo. Posts are Markdown files in `_posts/`; pushing to `main` builds and deploys automatically.

## Creating a new post

1. Add a file to `_posts/` named `YYYY-MM-DD-slug.md`. The date/slug in the filename control ordering only — the live URL is always `/blog/slug/` (no date segments), via the `permalink: /blog/:title/` setting in `_config.yml`. Pick a slug that reads well as a URL.
2. Add front matter:

   ```yaml
   ---
   title: "Post Title"
   date: 2026-07-09
   tags: [computing, machinelearning]
   link: "https://example.com"   # optional — only for link-posts (see below)
   ---
   ```

   - `tags` must come from the fixed list in `_data/tags.yml`. Add a tag there first if you need a new one, plus a matching stub file in `blog/tag/<name>.html` (copy an existing one — 4 lines, no logic).
   - `link` is optional. If set, the post's title becomes an external link with a "→" glyph (a Daring-Fireball-style "link post") — use this for short commentary on someone else's writing rather than a full essay.
3. Write the body in Markdown. See the feature table below for what's supported.
4. Preview locally (see below) before pushing.
5. Commit and push to `main`. That's the entire publish step — GitHub Actions runs `bundle exec jekyll build` and deploys automatically; there is no separate manual deploy command.

## Local preview

```bash
bundle install          # first time only, or after Gemfile changes
bundle exec jekyll serve --port 4000
```

Then open `http://localhost:4000`. `jekyll serve` watches for file changes and rebuilds automatically — just save and refresh.

To do a one-off build without serving (e.g. to sanity-check output before committing):

```bash
bundle exec jekyll build
```

Output lands in `_site/`, which is gitignored and never committed — GitHub Actions rebuilds it fresh from source on every push.

## Supported Markdown features

| Feature | Syntax | Notes |
|---|---|---|
| Inline math | `` $$E = mc^2$$ `` within a sentence | Renders inline if the `$$...$$` sits within running text on the same line as other prose. |
| Display math | `` $$ `` on its own line, equation, `` $$ `` on its own line | Renders as a centered block if the `$$...$$` is alone in its own paragraph. Renders at build time via KaTeX — no client-side JS cost. |
| Code blocks | `` ```python `` / `` ```bash `` / `` ```javascript `` fenced blocks | Syntax-highlighted at build time via Rouge. Any Rouge-supported language works, but only Python/Bash/JavaScript are guaranteed to have been visually checked against both themes. |
| Inline code | `` `code` `` | Small bordered/background treatment, no syntax highlighting. |
| Diagrams | `` ```mermaid `` fenced block, standard Mermaid syntax (`graph LR`, `graph TD`, etc.) | Rendered client-side in the browser via Mermaid.js (loaded from CDN). Automatically matches the page's active light/dark theme. |
| Plain image | `![alt text](url)` | No caption, no border/frame — just the image, centered, sized to fit the content column. |
| Captioned figure | `{% include figure.html src="url" alt="alt text" caption="Caption text" %}` | Renders `<figure><img><figcaption>`. Omit `caption` to render the image with no caption (equivalent to the plain form, just via the include). |
| Tag pills | Set via post front matter `tags:`, not written inline in the body | Multiple tags per post are allowed. |

## Where things live

| Path | Purpose |
|---|---|
| `_posts/` | Post source files |
| `_data/tags.yml` | The fixed tag taxonomy — the only valid values for a post's `tags:` |
| `blog/tag/*.html` | One tiny stub file per tag (drives `/blog/tag/<name>/`) |
| `_layouts/post.html` | Single-post page template |
| `_layouts/blog-index.html` | `/blog/` index template |
| `_includes/figure.html` | Captioned-figure include used above |
| `css/blog.css` | Post/product-card layout, code block + Mermaid colors (dark/light) |
| `css/katex/` | Vendored KaTeX CSS/fonts (no CDN dependency for math) |
