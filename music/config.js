const DB_CONFIG = {
    async fetchDatabase() {
        const gz = await fetch('master.sqlite.gz');
        if (!gz.ok) throw new Error(`Failed to load database: ${gz.statusText}`);
        return new Response(
            gz.body.pipeThrough(new DecompressionStream('gzip'))
        ).arrayBuffer();
    },
};
