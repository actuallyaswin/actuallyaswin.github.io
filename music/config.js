const DB_CONFIG = {
    filename: 'master.sqlite',
    cacheKey: 'v2',  // bump to force cache bust on all clients

    async fetchDatabase() { return this._loadWithCache(); },

    _idbOpen() {
        return new Promise((resolve, reject) => {
            const req = indexedDB.open('music-db', 1);
            req.onupgradeneeded = e => e.target.result.createObjectStore('cache');
            req.onsuccess = e => resolve(e.target.result);
            req.onerror = () => reject(req.error);
        });
    },

    async _idbGet(key) {
        try {
            const db = await this._idbOpen();
            return new Promise((resolve, reject) => {
                const tx = db.transaction('cache', 'readonly');
                const req = tx.objectStore('cache').get(key);
                req.onsuccess = () => resolve(req.result ?? null);
                req.onerror = () => reject(req.error);
            });
        } catch { return null; }
    },

    async _idbSet(key, value) {
        try {
            const db = await this._idbOpen();
            const tx = db.transaction('cache', 'readwrite');
            tx.objectStore('cache').put(value, key);
        } catch { /* fire-and-forget */ }
    },

    async _loadWithCache() {
        // Use Last-Modified / ETag as a server-side version fingerprint
        let serverVersion = null;
        try {
            const head = await fetch(this.filename, { method: 'HEAD' });
            serverVersion = head.headers.get('last-modified') || head.headers.get('etag');
        } catch { /* ignore — no cache validation, always re-fetch */ }

        const cacheTag = serverVersion ? `${serverVersion}:${this.cacheKey}` : null;

        if (cacheTag) {
            const [cachedBuf, cachedTag] = await Promise.all([
                this._idbGet(`${this.filename}:buf`),
                this._idbGet(`${this.filename}:tag`),
            ]);
            if (cachedBuf && cachedTag === cacheTag) {
                console.log('[db] IndexedDB cache hit');
                return cachedBuf;
            }
        }

        console.time('[db] fetch');
        const buf = await this._fetch();
        console.timeEnd('[db] fetch');

        if (cacheTag) {
            this._idbSet(`${this.filename}:buf`, buf);   // fire-and-forget
            this._idbSet(`${this.filename}:tag`, cacheTag);
        }
        return buf;
    },

    async _fetch() {
        // Try gzip-compressed version first
        try {
            const resp = await fetch(this.filename + '.gz');
            if (resp.ok && typeof DecompressionStream !== 'undefined') {
                console.time('[db] decompress');
                const buf = await new Response(
                    resp.body.pipeThrough(new DecompressionStream('gzip'))
                ).arrayBuffer();
                console.timeEnd('[db] decompress');
                return buf;
            }
        } catch { /* .gz not found or DecompressionStream unsupported — fall through */ }

        const resp = await fetch(this.filename);
        if (!resp.ok) throw new Error(`Failed to load database: ${resp.statusText}`);
        return resp.arrayBuffer();
    },
};
