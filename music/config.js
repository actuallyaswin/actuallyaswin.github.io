const DB_CONFIG = {
    // The gzipped DB is ~18 MB (≈47 MB decompressed), which is ~14 s on good
    // 4G. The fetch stream already carries the byte counts needed for a
    // progress indicator, so surface them rather than discarding them —
    // otherwise the user stares at a static string for the whole download.
    async fetchDatabase(onProgress) {
        const gz = await fetch('master_prod.sqlite.gz');
        if (!gz.ok) throw new Error(`Failed to load database: ${gz.status} ${gz.statusText}`);

        let stream = gz.body;

        if (typeof onProgress === 'function' && stream) {
            // Content-Length is the *compressed* size; approximate when the
            // header is missing (e.g. chunked transfer) so the bar still moves.
            const total = Number(gz.headers.get('content-length')) || 18_000_000;
            let loaded = 0;
            stream = stream.pipeThrough(new TransformStream({
                transform(chunk, controller) {
                    loaded += chunk.byteLength;
                    onProgress(Math.min(loaded / total, 1), loaded, total);
                    controller.enqueue(chunk);
                },
            }));
        }

        return new Response(
            stream.pipeThrough(new DecompressionStream('gzip'))
        ).arrayBuffer();
    },
};
