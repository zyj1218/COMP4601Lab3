// server.js
const express = require("express");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
//名字放这里
const SERVER_NAME = process.env.SERVER_NAME || "PUT_YOUR_SERVER_NAME_HERE";
const MONGO_URI = process.env.MONGO_URI;
const MONGO_DB = process.env.MONGO_DB || "comp4601_lab3";

let client;
let pagesCol;

async function connectDb() {
  if (!MONGO_URI) throw new Error("Missing MONGO_URI in .env");
  client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db(MONGO_DB);
  pagesCol = db.collection("pages");
  console.log("Connected to MongoDB.");
}

// INFO test (grader): must return {"name":"yourServerName"}
app.get("/info", (req, res) => {
  res.json({ name: SERVER_NAME });
});

// Lab3: GET /:datasetName/popular
// Return top 10 pages by incomingCount
app.get("/:datasetName/popular", async (req, res) => {
  try {
    const { datasetName } = req.params;

    const top10 = await pagesCol
      .find({ dataset: datasetName })
      .sort({ incomingCount: -1 })
      .limit(10)
      .project({ _id: 0, url: 1, origUrl: 1 })
      .toArray();

    res.json({ result: top10 });
  } catch (e) {
    console.error("popular error:", e);
    res.status(500).json({ error: "server error" });
  }
});

// Lab3: GET request to one of the "url" values returned by /popular
// We designed it as /:datasetName/page/:pageId
app.get("/:datasetName/page/:pageId", async (req, res) => {
  try {
    const { datasetName, pageId } = req.params;

    const doc = await pagesCol.findOne(
      { dataset: datasetName, pageId },
      { projection: { _id: 0, origUrl: 1, incoming: 1 } }
    );

    if (!doc) return res.status(404).json({ error: "not found" });

    res.json({
      webUrl: doc.origUrl,
      incomingLinks: doc.incoming || [],
    });
  } catch (e) {
    console.error("page error:", e);
    res.status(500).json({ error: "server error" });
  }
});

(async () => {
  await connectDb();

  app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
  });
})().catch((e) => {
  console.error("Startup error:", e);
  process.exit(1);
});
