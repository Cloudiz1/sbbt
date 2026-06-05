function getLastPrice(product, field) {
	let recentOrder = product[field][0];
	if (!recentOrder) return -1;
	return recentOrder["pricePerUnit"];
}

export default {
	async scheduled(controller, env, ctx) {
		const BAZAAR_URL = "https://api.hypixel.net/v2/skyblock/bazaar";
		const res = await fetch(BAZAAR_URL);
		const body = await res.json();

		if (!body || !body["products"]) {
			console.error("Invalid response from Hypixel API");
			return;
		}

		const products = Object.values(body["products"]).map((product) => {
			let quickStatus = product["quick_status"];
			return {
				productId: quickStatus["productId"],
				instaSellPrice: getLastPrice(product, "sell_summary"),
				instaSellVolume: quickStatus["sellVolume"],
				instaSellMovingWeek: quickStatus["sellMovingWeek"],
				instaBuyPrice: getLastPrice(product, "buy_summary"),
				instaBuyVolume: quickStatus["buyVolume"],
				instaBuyMovingWeek: quickStatus["buyMovingWeek"],
			};
		});

		const jsonlString = products.map(item => JSON.stringify(item)).join("\n") + "\n";
		const encoder = new TextEncoder();
		const uint8Array = encoder.encode(jsonlString);

		const compressedStream = new Response(uint8Array).body.pipeThrough(
			new CompressionStream("gzip")
		);

		const responseBuffer = await new Response(compressedStream).arrayBuffer();
		const fileName = `bazaar/${Date.now()}.jsonl.gz`;

		await env.BUCKET.put(fileName, responseBuffer, {
			httpMetadata: { contentType: "application/gzip" }
		});
	}
};
