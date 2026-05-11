import initSqlJs from "sql.js";
import { PRAGMA_SQL } from "../schema.js";

const DEFAULT_TABLE = "nine_router_storage";
const DEFAULT_KEY = "9router:sqlite";

let SQL = null;

async function loadSql() {
  if (SQL) return SQL;
  SQL = await initSqlJs();
  return SQL;
}

function assertSafeIdentifier(name, label) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`${label} must be a simple SQL identifier`);
  }
}

export function getSupabaseConfig() {
  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const secret =
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    process.env.SUPABASE_SECRET_KEY ||
    process.env.SUPABASE_ANON_KEY ||
    "";
  const table = process.env.NINE_ROUTER_SUPABASE_TABLE || DEFAULT_TABLE;
  const key = process.env.NINE_ROUTER_DB_SUPABASE_KEY || DEFAULT_KEY;
  return { url: url.replace(/\/+$/, ""), secret, table, key };
}

export function hasSupabaseConfig() {
  const { url, secret } = getSupabaseConfig();
  return Boolean(url && secret);
}

function makeHeaders(config, prefer = "") {
  const headers = {
    apikey: config.secret,
    "Content-Type": "application/json",
  };
  if (config.secret.startsWith("eyJ")) headers.Authorization = `Bearer ${config.secret}`;
  if (prefer) headers.Prefer = prefer;
  return headers;
}

async function supabaseFetch(config, path, options = {}) {
  assertSafeIdentifier(config.table, "NINE_ROUTER_SUPABASE_TABLE");

  const response = await fetch(`${config.url}/rest/v1/${path}`, {
    ...options,
    cache: "no-store",
    headers: {
      ...makeHeaders(config, options.prefer),
      ...(options.headers || {}),
    },
  });

  const text = await response.text();
  const payload = text ? JSON.parse(text) : null;
  if (!response.ok) {
    const message = payload?.message || payload?.error || `Supabase request failed (${response.status})`;
    throw new Error(message);
  }
  return payload;
}

async function readRemoteDatabase(config) {
  const rows = await supabaseFetch(
    config,
    `${config.table}?key=eq.${encodeURIComponent(config.key)}&select=value,version&limit=1`,
    { method: "GET" }
  );
  const row = Array.isArray(rows) ? rows[0] : null;
  if (!row?.value) return null;
  return {
    data: Buffer.from(row.value, "base64"),
    version: Number(row.version || 0),
  };
}

async function readRemoteVersion(config) {
  const rows = await supabaseFetch(
    config,
    `${config.table}?key=eq.${encodeURIComponent(config.key)}&select=version&limit=1`,
    { method: "GET" }
  );
  const row = Array.isArray(rows) ? rows[0] : null;
  return Number(row?.version || 0);
}

async function writeRemoteDatabase(config, data) {
  const version = Date.now();
  const rows = await supabaseFetch(config, `${config.table}?on_conflict=key`, {
    method: "POST",
    prefer: "resolution=merge-duplicates,return=representation",
    body: JSON.stringify({
      key: config.key,
      value: Buffer.from(data).toString("base64"),
      version,
      updated_at: new Date().toISOString(),
    }),
  });
  const row = Array.isArray(rows) ? rows[0] : rows;
  return Number(row?.version || version);
}

export async function createSupabaseSqliteAdapter(config = getSupabaseConfig()) {
  if (!config.url || !config.secret) {
    throw new Error("Missing SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SECRET_KEY");
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

    const latestVersion = await readRemoteVersion(config);
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
    flush().catch((e) => console.error("[supabase-sqljs] save failed:", e));
    db.close();
  }

  const flushOnExit = () => {
    if (persistedVersion < dirtyVersion) {
      flush().catch((e) => console.error("[supabase-sqljs] save failed:", e));
    }
  };
  process.on("beforeExit", flushOnExit);
  process.on("SIGINT", flushOnExit);
  process.on("SIGTERM", flushOnExit);

  return {
    driver: "supabase-sql.js",
    storage: `supabase:${config.table}/${config.key}`,
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
