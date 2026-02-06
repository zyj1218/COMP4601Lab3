// crawler.js
const axios = require("axios");
const cheerio = require("cheerio");
const crypto = require("crypto");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const DATASETS = {
  tinyfruits: "https://people.scs.carleton.ca/~avamckenney/tinyfruits/N-0.html",
  fruits100: "https://people.scs.carleton.ca/~avamckenney/fruits100/N-0.html",
  fruitsA: "https://people.scs.carleton.ca/~avamckenney/fruitsA/N-0.html",
  // 调试用（可选）：
  // fruitgraph: "https://people.scs.carleton.ca/~avamckenney/fruitgraph/N-0.html",
};

function normalize(url) {
  const u = new URL(url);
  u.hash = ""; // 去掉 #fragment，避免同一页被当成不同 URL
  return u.href;
}

function pageIdFromOrigUrl(origUrl) {
  return crypto.createHash("sha1").update(origUrl).digest("hex");
}

async function fetchHtml(url) {
  const res = await axios.get(url, {
    timeout: 15000,
    headers: { "User-Agent": "COMP4601-Lab3-Crawler" },
    validateStatus: (s) => s >= 200 && s < 400,
  });
  return res.data;
}

async function crawlDataset(datasetName, opts = {}) {
  const seed = DATASETS[datasetName];
  if (!seed) throw new Error(`Unknown dataset: ${datasetName}`);

  const maxPages = Number.isFinite(opts.maxPages) ? opts.maxPages : Infinity;

  const visited = new Set();
  const queue = [seed];

  // 图结构：origUrl -> { outgoing:Set, incoming:Set }
  const pages = new Map();

  const ensure = (origUrl) => {
    if (!pages.has(origUrl)) {
      pages.set(origUrl, { outgoing: new Set(), incoming: new Set() });
    }
  };

  while (queue.length > 0 && visited.size < maxPages) {
    const url = normalize(queue.shift());
    if (visited.has(url)) continue;

    visited.add(url);
    ensure(url);

    console.log(`[${datasetName}] Visiting: ${url}`);

    let html = "";
    try {
      html = await fetchHtml(url);
    } catch (e) {
      console.warn(`[${datasetName}]  Fetch failed: ${url}`);
      continue;
    }

    const $ = cheerio.load(html);

    $("a[href]").each((_, a) => {
      const href = $(a).attr("href");
      if (!href) return;

      let abs;
      try {
        abs = normalize(new URL(href, url).href);
      } catch {
        return;
      }

      // 建边：url -> abs
      ensure(abs);
      pages.get(url).outgoing.add(abs);
      pages.get(abs).incoming.add(url);

      if (!visited.has(abs)) queue.push(abs);
    });
  }

  console.log(`[${datasetName}] Done. Visited ${visited.size} pages.`);
  return pages;
}

async function saveToMongo(datasetName, pagesMap) {
  const uri = process.env.MONGO_URI;
  const dbName = process.env.MONGO_DB || "comp4601_lab3";
  if (!uri) throw new Error("Missing MONGO_URI in .env");

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);
  const pagesCol = db.collection("pages");

  // 清除这个 dataset 的旧数据（允许重复跑）
  await pagesCol.deleteMany({ dataset: datasetName });

  const docs = [];
  for (const [origUrl, info] of pagesMap.entries()) {
    const pageId = pageIdFromOrigUrl(origUrl);
    const outgoing = [...info.outgoing];
    const incoming = [...info.incoming];

    docs.push({
      dataset: datasetName,
      pageId,
      url: `/${datasetName}/page/${pageId}`, // 你 server 上的 URL
      origUrl,                               // 被爬的真实 URL
      outgoing,
      incoming,
      incomingCount: incoming.length,
      createdAt: new Date(),
    });
  }

  if (docs.length > 0) {
    await pagesCol.insertMany(docs);
  }

  // 索引：查 top10 和查单页更快
  await pagesCol.createIndex({ dataset: 1, incomingCount: -1 });
  await pagesCol.createIndex({ dataset: 1, pageId: 1 }, { unique: true });

  await client.close();
  console.log(`[${datasetName}] Saved ${docs.length} pages to MongoDB.`);
}

async function main() {
  const arg = process.argv[2] || "tinyfruits";
  const maxPagesArg = process.argv[3] ? Number(process.argv[3]) : Infinity;

  const targets = arg === "all" ? ["tinyfruits", "fruits100", "fruitsA"] : [arg];

  for (const ds of targets) {
    const pages = await crawlDataset(ds, { maxPages: maxPagesArg });
    await saveToMongo(ds, pages);
  }

  console.log("All done.");
}

main().catch((e) => {
  console.error("Crawler error:", e);
  process.exit(1);
});
