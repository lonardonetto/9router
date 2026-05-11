import initSqlJs from "sql.js";
import { PRAGMA_SQL } from "../schema.js";

const DEFAULT_KEY = "9router:sqlite";

let SQL = null;

async function loadSql() {
  if (SQL) return SQL;
  SQL = await initSqlJs();
  return SQL;
}

export function getUpstashRedisConfig() {
  const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || "";
  const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || "";
  const key = process.env.NINE_ROUTER_DB_REDIS_KEY || process.env.DB_REDIS_KEY || DEFAULT_KEY;
  return { url: url.replace(/\/+$/, ""), token, key, versionKey: `${key}:version` };
}

export function hasUpstashRedisConfig() {
  const { url, token } = getUpstashRedisConfig();
  return Boolean(url && token);
}

async function redisCommand(config, command) {
  const response = await fetch(config.url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(command),
    cache: "no-store",
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.error) {
    throw new Error(payload.error || `Upstash Redis request failed (${response.status})`);
  }
  return payload.result;
}

async function readRemoteDatabase(config) {
  const [value, version] = await Promise.all([
    redisCommand(config, ["GET", config.key]),
    redisCommand(config, ["GET", config.versionKey]),
  ]);
  if (!value) return null;
  if (typeof value !== "string") {
    throw new Error(`Unexpected Redis value for ${config.key}`);
  }
  return {
    data: Buffer.from(value, "base64"),
    version: Number(version || 0),
  };
}

async function writeRemoteDatabase(config, data) {
  const value = Buffer.from(data).toString("base64");
  await redisCommand(config, ["SET", config.key, value]);
  return Number(await redisCommand(config, ["INCR", config.versionKey]) || 0);
}

export async function createUpstashSqliteAdapter(config = getUpstashRedisConfig()) {
  if (!config.url || !config.token) {
    throw new Error("Missing KV_REST_API_URL/KV_REST_API_TOKEN or UPSTASH_REDIS_REST_URL/UPSTASH_REDIS_REST_TOKEN");
  }

  const SQLLib = await loadSql();
  const remote = await readRemoteDatabase(config);
  let remoteVersion = remote?.version || 0;
  let db = new SQLLib.Database(remote?.data || null);
  db.exec(PRAGMA_SQL);

  let dirtyVersion = 0;
  let persistedVersion = 0;
  let flushPromise = null;

  function markDirty() {
    dirtyVersion += 1;
  }

  async function flush() {
    if (flushPromise) await flushPromise;
    while (persistedVersion < dirtyVersion) {
      const version = dirtyVersion;
      const data = db.export();
      flushPromise = writeRemoteDatabase(config, data)
        .then((nextRemoteVersion) => {
          persistedVersion = Math.max(persistedVersion, version);
          remoteVersion = Math.max(remoteVersion, nextRemoteVersion);
        })
        .finally(() => {
          flushPromise = null;
        });
      await flushPromise;
    }
  }

  async function refresh() {
    if (persistedVersion < dirtyVersion || flushPromise) await flush();

    const latestVersion = Number(await redisCommand(config, ["GET", config.versionKey]) || 0);
    if (!latestVersion || latestVersion <= remoteVersion) return;

    const latest = await readRemoteDatabase(config);
    if (!latest?.data) return;

    const nextDb = new SQLLib.Database(latest.data);
    nextDb.exec(PRAGMA_SQL);
    try { db.close(); } catch {}
    db = nextDb;
    remoteVersion = latest.version || latestVersion;
    dirtyVersion = 0;
    persistedVersion = 0;
  }

  function paramsObj(params) {
    if (!params || (Array.isArray(params) && params.length === 0)) return undefined;
    return params;
  }

  function run(sql, params = []) {
    const stmt = db.prepare(sql);
    try {
      stmt.bind(paramsObj(params));
      stmt.step();
      const changes = db.getRowsModified();
      const lastInsertRowid = db.exec("SELECT last_insert_rowid() as id")[0]?.values?.[0]?.[0] ?? null;
      markDirty();
      return { changes, lastInsertRowid };
    } finally {
      stmt.free();
    }
  }

  function get(sql, params = []) {
    const stmt = db.prepare(sql);
    try {
      stmt.bind(paramsObj(params));
      if (stmt.step()) return stmt.getAsObject();
      return undefined;
    } finally {
      stmt.free();
    }
  }

  function all(sql, params = []) {
    const stmt = db.prepare(sql);
    try {
      stmt.bind(paramsObj(params));
      const rows = [];
      while (stmt.step()) rows.push(stmt.getAsObject());
      return rows;
    } finally {
      stmt.free();
    }
  }

  function exec(sql) {
    db.exec(sql);
    markDirty();
  }

  function transaction(fn) {
    const sp = `sp_${Math.random().toString(36).slice(2)}`;
    db.exec(`SAVEPOINT ${sp}`);
    try {
      const result = fn();
      db.exec(`RELEASE ${sp}`);
      markDirty();
      return result;
    } catch (e) {
      try { db.exec(`ROLLBACK TO ${sp}`); db.exec(`RELEASE ${sp}`); } catch {}
      markDirty();
      throw e;
    }
  }

  function close() {
    flush().catch((e) => console.error("[upstash-sqljs] save failed:", e));
    db.close();
  }

  const flushOnExit = () => {
    if (persistedVersion < dirtyVersion) {
      flush().catch((e) => console.error("[upstash-sqljs] save failed:", e));
    }
  };
  process.on("beforeExit", flushOnExit);
  process.on("SIGINT", flushOnExit);
  process.on("SIGTERM", flushOnExit);

  return {
    driver: "upstash-sql.js",
    storage: `redis:${config.key}`,
    run,
    get,
    all,
    exec,
    transaction,
    flush,
    refresh,
    close,
    get raw() { return db; },
  };
}
