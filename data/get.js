require('dotenv').config();
const aws = require("@aws-sdk/client-s3");
const fs = require("node:fs");
const zlib = require("node:zlib");

const OUTPUT_FILE = "bazaar.json";
const CONCURRENCY_LIMIT = 20;

const r2 = new aws.S3Client({
	region: "auto",
	endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
	credentials: {
		accessKeyId: process.env.R2_ACCESS_KEY,
		secretAccessKey: process.env.R2_SECRET
	}
});

async function readBucket() {
	const paginator = aws.paginateListObjectsV2(
		{ client: r2, pageSize: 1000 },
		{ Bucket: "sbbt", Prefix: "bazaar/" }
	);

	console.log("grabbing keys...");
	const keys = [];

	for await (const page of paginator) {
		if (page.Contents) {
			for (const obj of page.Contents) {
				keys.push(obj.Key);
			}
		}
	}

	if (keys.length == 0) {
		console.log("no keys found");
		return;
	}

	console.log(`found ${keys.length} files. starting download`);
	const writeStream = fs.createWriteStream(OUTPUT_FILE, { encoding: "utf-8" });
	writeStream.write("[\n");

	let index = 0;
	let successfulWrites = 0;
	let fileWriteLock = Promise.resolve();

	async function worker() {
		while (index < keys.length) {
			const currentIndex = index++;
			const key = keys[currentIndex];
			if (!key) break;

			try {
				const jsonObject = await downloadAndUnzip(key);
				const jsonString = JSON.stringify(jsonObject, null, 2);

				fileWriteLock = fileWriteLock.then(() => {
					return new Promise((resolveWrite) => {
						const prefix = successfulWrites > 0 ? ",\n" : "";
						const indentedString = jsonString.replace(/^/gm, "  ");

						writeStream.write(prefix + indentedString, () => {
							successfulWrites++;
							resolveWrite();
						});
					})
				})

				await fileWriteLock;
				if (currentIndex % 10 === 0 || currentIndex === keys.length - 1) {
					console.log(`Progress: ${currentIndex + 1}/${keys.length}`);
				}
			} catch (err) {
				console.log(`Error processing ${key}`, err.message);
			}
		}
	}

	const workers = Array(CONCURRENCY_LIMIT).fill(null).map(worker);
	await Promise.all(workers);
	await fileWriteLock;

	writeStream.write("\n]");
	writeStream.end();
	console.log("Finished!");
}

async function downloadAndUnzip(key) {
	const command = new aws.GetObjectCommand({
		Bucket: "sbbt",
		Key: key,
	});

	const response = await r2.send(command);
	const stream = response.Body;

	return new Promise((resolve, reject) => {
		const chunks = [];
		stream
			.pipe(zlib.createGunzip())
			.on("data", (chunk) => chunks.push(chunk))
			.on("end", () => {
				try {
					const buffer = Buffer.concat(chunks);
					const parsedJson = JSON.parse(buffer.toString("utf-8"));
					resolve(parsedJson);
				} catch (err) {
					reject(new Error(`failed to resolve JSON ${err}`, err.message));
				}
			})
			.on("error", reject);
	});
}

readBucket();
