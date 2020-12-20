import {
  deserializeFeed,
  FeedType,
  JsonFeed,
} from "https://deno.land/x/rss/mod.ts";
import { createHash } from "https://deno.land/std/hash/mod.ts";
import { readLines } from "https://deno.land/std/io/bufio.ts";
import { KV } from "../lib/kv.ts";
import { ensureDir } from "https://deno.land/std/fs/mod.ts";

const home = Deno.env.get("HOME");
const cacheDir = `${home}/.cache/jsonfeed`;
const allItemsFile = `${cacheDir}/allitems.ndjson`;
await ensureDir(cacheDir);

const latestUpdatedKv = new KV(`${cacheDir}/last-updated.kv`);
await latestUpdatedKv.load(true);

function toCanonicalUrl(url: string): string {
  return url.replace("http://", "").replace("https://", "");
}

function chunkArray(
  myArray: Array<string>,
  chunk_size: number,
): Array<Array<string>> {
  const results = [];

  while (myArray.length) {
    results.push(myArray.splice(0, chunk_size));
  }

  return results;
}

interface FetchFeedOptions {
  etag?: string;
  lastModified?: string;
}

function fetchBuilder(force: boolean = false) {
  return async (url: string): Promise<JsonFeed | undefined> => {
    const canonicalUrl = toCanonicalUrl(url);
    const now = new Date().toString();
    let headers = {};
    if (!force) {
      headers = {
        ...headers,
        "ETag": latestUpdatedKv.get(canonicalUrl + "-etag"),
        "If-Modified-Since": latestUpdatedKv.get(
          canonicalUrl + "-lastModified",
        ),
      };
    }
    try {
      const response = await fetch(url, { headers });
      const contentType = response.headers.get("content-type");
      const lastModified: string = response.headers.get("last-modified") || now;
      const eTag: string = response.headers.get("etag") || "";
      if (response.status < 200 || response.status >= 400) {
        throw new Error("Wrong Status code: " + response.status);
      }
      if (response.status === 304) {
        console.info(url, 304, "Not Modified");
        return undefined;
      }

      await latestUpdatedKv.put(canonicalUrl + "-etag", eTag);
      await latestUpdatedKv.put(canonicalUrl + "-lastModified", lastModified);
      const content = await response.text();
      if (contentType && contentType.includes("json")) {
        let feed: JsonFeed = JSON.parse(content);
        await saveFeed(canonicalUrl, feed);
        return feed;
      } else {
        const [_, feed] = await deserializeFeed(
          content,
          { outputJsonFeed: true },
        ) as [FeedType, JsonFeed];
        await saveFeed(canonicalUrl, feed);
        return feed;
      }
    } catch (e) {
      console.error(`Error fetching ${url}.`, e);
    }
  };
}

async function saveFeed(url: string, feed: JsonFeed) {
  const file = await Deno.open(
    allItemsFile,
    { write: true, create: true, truncate: false, append: true },
  );
  let encoder = new TextEncoder();
  for (const item of feed.items) {
    await file.write(encoder.encode(JSON.stringify(item) + "\n"));
  }
}

async function fetchParallel(
  items: Array<string>,
  n: number,
  force: boolean = false,
): Promise<Array<JsonFeed>> {
  const chunks = chunkArray(items, n);
  let res: Array<JsonFeed> = [];
  for (let chunk of chunks) {
    let promises = chunk.map(fetchBuilder(force));
    let values = await Promise.all(promises);
    // We remove undefined values
    let filtered: Array<JsonFeed> = values.filter((v) =>
      v !== undefined
    ) as Array<JsonFeed>;
    res = [...res, ...filtered];
  }
  return res;
}

const forceUpdate = false;
async function main() {
  let urls = [];
  for await (const line of readLines(Deno.stdin)) {
    urls.push(line);
  }
  let feed = await fetchParallel(urls, 10, forceUpdate);
  console.log(feed);
}
if (import.meta.main) {
  await main();
}
