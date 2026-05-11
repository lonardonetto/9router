import { ensureDirs, DATA_FILE } from "./paths.js";

// Use global to survive Next.js dev hot-reload (module state resets on reload)
if (!global._dbAdapter) global._dbAdapter = { instance: null, initPromise: null, logged: false };
const state = global._dbAdapter;
const isProductionBuild = process.env.NEXT_PHASE === "phase-production-build";

async function tryUpstashSqlite() {
  if (isProductionBuild) return null;
  try {
    const { createUpstashSqliteAdapter, hasUpstashRedisConfig } = await import("./adapters/upstashSqliteAdapter.js");
    if (!hasUpstashRedisConfig()) return null;
    return await createUpstashSqliteAdapter();
  } catch (e) {
    console.warn(`[DB] upstash-sql.js unavailable: ${e.message}`);
    return null;
  }
}

async function trySupabaseSqlite() {
  if (isProductionBuild) return null;
  try {
    const { createSupabaseSqliteAdapter, hasSupabaseConfig } = await import("./adapters/supabaseSqliteAdapter.js");
    if (!hasSupabaseConfig()) return null;
    return await createSupabaseSqliteAdapter();
  } catch (e) {
    console.warn(`[DB] supabase-sql.js unavailable: ${e.message}`);
    return null;
  }
}

async function tryBunSqlite() {
  // Bun runtime only — built-in, no install needed
  if (!process.versions.bun) return null;
  try {
    const { createBunSqliteAdapter } = await import("./adapters/bunSqliteAdapter.js");
    return await createBunSqliteAdapter(DATA_FILE);
  } catch (e) {
    console.warn(`[DB] bun:sqlite unavailable: ${e.message}`);
    return null;
  }
}

async function tryBetterSqlite() {
  // Skip on Bun — better-sqlite3 native bindings unsupported
  if (process.versions.bun) return null;
  try {
    const { createBetterSqliteAdapter } = await import("./adapters/betterSqliteAdapter.js");
    return createBetterSqliteAdapter(DATA_FILE);
  } catch (e) {
    console.warn(`[DB] better-sqlite3 unavailable: ${e.message}`);
    return null;
  }
}

async function tryNodeSqlite() {
  // Built-in since Node 22.5.0 — no install needed. Skip under Bun (no node:sqlite).
  if (process.versions.bun) return null;
  const [maj, min] = process.versions.node.split(".").map(Number);
  if (maj < 22 || (maj === 22 && min < 5)) return null;
  try {
    const { createNodeSqliteAdapter } = await import("./adapters/nodeSqliteAdapter.js");
    return await createNodeSqliteAdapter(DATA_FILE);
  } catch (e) {
    console.warn(`[DB] node:sqlite unavailable: ${e.message}`);
    return null;
  }
}

async function trySqlJs() {
  try {
    const { createSqlJsAdapter } = await import("./adapters/sqljsAdapter.js");
    return await createSqlJsAdapter(DATA_FILE);
  } catch (e) {
    console.warn(`[DB] sql.js unavailable: ${e.message}`);
    return null;
  }
}

async function initAdapter() {
  ensureDirs();
  let adapter = await trySupabaseSqlite();
  if (!adapter) adapter = await tryUpstashSqlite();
  if (process.env.VERCEL && !adapter && !isProductionBuild) {
    console.warn(
      "[DB] Running on Vercel without persistent storage. /tmp is ephemeral; set Supabase or Upstash env vars to persist connections."
    );
  }
  // Order per runtime:
  //   Bun:  bun:sqlite → sql.js
  //   Node: better-sqlite3 → node:sqlite (≥22.5) → sql.js
  if (!adapter) adapter = await tryBunSqlite();
  if (!adapter) adapter = await tryBetterSqlite();
  if (!adapter) adapter = await tryNodeSqlite();
  if (!adapter) adapter = await trySqlJs();
  if (!adapter) throw new Error("[DB] No SQLite driver available (bun/better/node/sql.js all failed)");

  if (!state.logged) {
    console.log(`[DB] Driver: ${adapter.driver} | storage: ${adapter.storage || DATA_FILE}`);
    state.logged = true;
  }

  const { runMigrationOnce } = await import("./migrate.js");
  await runMigrationOnce(adapter);
  await adapter.flush?.();
  return adapter;
}

export async function getAdapter() {
  if (state.instance) {
    await state.instance.refresh?.();
    return state.instance;
  }
  if (!state.initPromise) state.initPromise = initAdapter().then((a) => { state.instance = a; return a; });
  const adapter = await state.initPromise;
  await adapter.refresh?.();
  return adapter;
}

export function getAdapterSync() {
  if (!state.instance) throw new Error("[DB] adapter not initialized — await getAdapter() first");
  return state.instance;
}
