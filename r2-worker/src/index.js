export default {
	async scheduled(controller, env, ctx) {
		const BAZAAR_URL = "https://api.hypixel.net/v2/skyblock/bazaar";
		const res = await fetch(BAZAAR_URL);
		const compressedStream = res.body.pipeThrough(new CompressionStream("gzip"));
		const responseBuffer = await new Response(compressedStream).arrayBuffer();
		const fileName = `bazaar/${Date.now()}.json.gz`;

		await env.BUCKET.put(fileName, responseBuffer, {
			httpMetadata: { contentType: "application/gzip" }
		});
	}
};
