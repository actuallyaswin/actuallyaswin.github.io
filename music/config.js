// Database configuration
const DB_CONFIG = {
    // Database version - update this when you create a new release
    version: 'v1.0',

    // GitHub repository info
    repo: 'actuallyaswin/actuallyaswin.github.io',

    // Database filenames
    filename: 'listening_history.sqlite',
    overridesFilename: 'listening_history_overrides.sqlite',

    // Local paths (for testing)
    get localPath() {
        return this.filename;
    },
    get overridesLocalPath() {
        return this.overridesFilename;
    },

    // CDN URLs (for production)
    get cdnUrl() {
        return `https://github.com/${this.repo}/releases/download/music-db-${this.version}/${this.filename}`;
    },
    get overridesCdnUrl() {
        return `https://github.com/${this.repo}/releases/download/music-db-${this.version}/${this.overridesFilename}`;
    },

    // Fetch main database with local fallback
    async fetchDatabase() {
        // Try local file first (for development)
        try {
            const localResponse = await fetch(this.localPath);
            if (localResponse.ok) {
                console.log('Loading database from local file');
                return await localResponse.arrayBuffer();
            }
        } catch (e) {
            // Local fetch failed, will try CDN
        }

        // Fall back to CDN
        console.log('Loading database from CDN');
        const cdnResponse = await fetch(this.cdnUrl);
        if (!cdnResponse.ok) {
            throw new Error(`Failed to load database: ${cdnResponse.statusText}`);
        }
        return await cdnResponse.arrayBuffer();
    },

    // Fetch overrides database (optional, may not exist)
    async fetchOverridesDatabase() {
        // Try local file first (for development)
        try {
            const localResponse = await fetch(this.overridesLocalPath);
            if (localResponse.ok) {
                console.log('Loading overrides database from local file');
                return await localResponse.arrayBuffer();
            }
        } catch (e) {
            // Local fetch failed, will try CDN
        }

        // Try CDN
        try {
            console.log('Loading overrides database from CDN');
            const cdnResponse = await fetch(this.overridesCdnUrl);
            if (cdnResponse.ok) {
                return await cdnResponse.arrayBuffer();
            }
        } catch (e) {
            // Overrides don't exist, that's ok
        }

        console.log('No overrides database found (this is OK)');
        return null;
    }
};
