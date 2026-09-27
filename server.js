import express from "express";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import pg from "pg";
const { Pool } = pg;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

// ============================================================
// CONFIG
// ============================================================

const PORT = process.env.PORT || 10000;
const APP_MODE = String(process.env.APP_MODE || "crypto").trim().toLowerCase();
if (!["crypto", "markets"].includes(APP_MODE)) {
  throw new Error("APP_MODE must be either crypto or markets");
}
const SERVICE_ACCESS_TYPE = APP_MODE === "crypto" ? "CRYPTO" : "MARKETS";
const SERVICE_MARKETS = APP_MODE === "crypto"
  ? ["futures", "spot"]
  : ["stocks", "gold", "silver", "oil", "forex"];

const configuredOkxBase =
  String(process.env.OKX_BASE_URL || "").trim();

const OKX_PROXY =
  configuredOkxBase &&
  !configuredOkxBase.includes("workers.dev")
    ? configuredOkxBase.replace(/\/$/, "")
    : "https://www.okx.com";

const ADMIN_PASSWORD =
  process.env.ADMIN_PASSWORD || "CHANGE_THIS_ADMIN_PASSWORD";

const EXTERNAL_ASSET_UNIVERSES = {
  stocks: [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","AVGO"
  ],
  gold: ["GC=F"],
  silver: ["SI=F"],
  oil: ["CL=F"],
  forex: [
    "EURUSD=X","GBPUSD=X","USDJPY=X","USDCHF=X","AUDUSD=X","USDCAD=X"
  ]
};

const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 20);
const SPOT_MAX_SYMBOLS = Number(
  process.env.SPOT_MAX_SYMBOLS || MAX_SYMBOLS
);

const SCAN_INTERVAL_MS = Number(
  process.env.SCAN_INTERVAL_MS || 60000
);

// ============================================================
// IN-MEMORY DATA
// ============================================================
//
// Recommendation caches are deliberately service-local. Shared business data
// (codes, devices, payments and settings) lives in PostgreSQL below.
// ============================================================

const users = new Map();

const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, max: 5 })
  : null;

let dbReady = false;

async function initPersistence() {
  if (!pool) {
    console.log("DATABASE_URL not set: persistent storage is disabled");
    return;
  }

  try {
    // The database may already contain a sharks_state table created by an
    // earlier version of the app. CREATE TABLE IF NOT EXISTS does not change
    // an existing table, so older schemas can otherwise cause:
    //   column "key" does not exist
    // Migrate the existing table in-place without dropping user data.
    const tableCheck = await pool.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'sharks_state'
      ORDER BY ordinal_position
    `);

    if (tableCheck.rows.length === 0) {
      await pool.query(`
        CREATE TABLE sharks_state (
          key TEXT PRIMARY KEY,
          value JSONB NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      console.log("Created PostgreSQL table sharks_state");
    } else {
      const columns = new Map(
        tableCheck.rows.map(row => [row.column_name, row.data_type])
      );

      if (!columns.has("key")) {
        await pool.query(`ALTER TABLE sharks_state ADD COLUMN key TEXT`);
        console.log("PostgreSQL migration: added sharks_state.key");

        // Recover keys only from explicit legacy state-key column names.
        // Do not guess from generic columns such as id/name because those can
        // contain unrelated application data.
        const legacyKey = ["state_key", "key_name"]
          .find(name => columns.has(name));
        if (legacyKey) {
          await pool.query(
            `UPDATE sharks_state SET key = "${legacyKey}"::text WHERE key IS NULL`
          );
          console.log(`PostgreSQL migration: copied ${legacyKey} -> key`);
        }
      }

      if (!columns.has("value")) {
        await pool.query(`ALTER TABLE sharks_state ADD COLUMN value JSONB`);
        console.log("PostgreSQL migration: added sharks_state.value");

        const legacyValue = ["data", "json_data", "state_data"]
          .find(name => columns.has(name));
        if (legacyValue) {
          await pool.query(
            `UPDATE sharks_state SET value = CASE
               WHEN pg_typeof("${legacyValue}")::text = 'jsonb' THEN "${legacyValue}"::jsonb
               ELSE NULL
             END
             WHERE value IS NULL`
          ).catch(() => undefined);
          // The guarded update above intentionally avoids destructive casts.
          // If the legacy column is JSON/JSONB, copy it directly below.
          const legacyType = columns.get(legacyValue);
          if (legacyType === "jsonb" || legacyType === "json") {
            await pool.query(
              `UPDATE sharks_state SET value = "${legacyValue}"::jsonb WHERE value IS NULL`
            );
            console.log(`PostgreSQL migration: copied ${legacyValue} -> value`);
          }
        }
      }

      if (!columns.has("updated_at")) {
        await pool.query(`
          ALTER TABLE sharks_state
          ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        `);
        console.log("PostgreSQL migration: added sharks_state.updated_at");
      }
    }

    // Ensure ON CONFLICT(key) works even when the old table did not use key
    // as its primary key. NULL legacy rows are harmless and are ignored when
    // loading state below.
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS sharks_state_key_uq
      ON sharks_state(key)
    `);

    const result = await pool.query(`
      SELECT key, value
      FROM sharks_state
      WHERE key IS NOT NULL AND value IS NOT NULL
    `);

    for (const row of result.rows) {
      if (row.key === "accessCodes") {
        restoreMap(accessCodes, row.value);
        for (const item of accessCodes.values()) item.accessType = normalizeAccessType(item.accessType, "CRYPTO");
      }
      if (row.key === "paymentRequests") restoreMap(paymentRequests, row.value);
      if (row.key === "users") restoreMap(users, row.value);
      if (row.key === "settings") Object.assign(defaultSettings, row.value || {});
    }

    // A transaction-scoped advisory lock makes this migration safe when both
    // Render services start at the same time. Legacy JSONB is read once and
    // copied into row-level tables; it is never written again.
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(853148021)");
      await client.query(`CREATE TABLE IF NOT EXISTS sharks_access_codes (
        code TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', duration_days INTEGER NOT NULL,
        max_devices INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'unused',
        access_type TEXT NOT NULL DEFAULT 'CRYPTO' CHECK (access_type IN ('CRYPTO','MARKETS')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), activated_at TIMESTAMPTZ, expires_at TIMESTAMPTZ,
        created_by_admin BOOLEAN NOT NULL DEFAULT TRUE
      )`);
      await client.query(`CREATE TABLE IF NOT EXISTS sharks_code_devices (
        code TEXT NOT NULL REFERENCES sharks_access_codes(code) ON DELETE CASCADE,
        device_id TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (code, device_id)
      )`);
      await client.query(`CREATE TABLE IF NOT EXISTS sharks_payment_requests (
        id TEXT PRIMARY KEY, plan TEXT NOT NULL, days INTEGER NOT NULL, method TEXT NOT NULL,
        customer_name TEXT NOT NULL DEFAULT '', contact TEXT NOT NULL DEFAULT '', transaction_id TEXT NOT NULL DEFAULT '',
        note TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', access_type TEXT NOT NULL DEFAULT 'CRYPTO'
          CHECK (access_type IN ('CRYPTO','MARKETS')), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reviewed_at TIMESTAMPTZ, access_code TEXT REFERENCES sharks_access_codes(code)
      )`);
      await client.query(`CREATE TABLE IF NOT EXISTS sharks_settings (
        id SMALLINT PRIMARY KEY CHECK (id = 1), value JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);

      // Existing subscriptions are crypto-only by default. This is the safe
      // backwards-compatible choice: a legacy code never receives newly-added
      // Markets access without an administrator issuing a Markets code.
      for (const item of accessCodes.values()) {
        await client.query(`INSERT INTO sharks_access_codes
          (code,name,duration_days,max_devices,status,access_type,created_at,activated_at,expires_at,created_by_admin)
          VALUES ($1,$2,$3,$4,$5,'CRYPTO',to_timestamp($6/1000.0),CASE WHEN $7::bigint IS NULL THEN NULL ELSE to_timestamp($7/1000.0) END,CASE WHEN $8::bigint IS NULL THEN NULL ELSE to_timestamp($8/1000.0) END,$9)
          ON CONFLICT (code) DO NOTHING`, [item.code, item.name || "", item.durationDays || 7, item.maxDevices || 1, item.status || "unused", item.createdAt || now(), item.activatedAt, item.expiresAt, item.createdByAdmin !== false]);
        for (const deviceId of item.devices || []) await client.query(
          "INSERT INTO sharks_code_devices(code,device_id) VALUES($1,$2) ON CONFLICT DO NOTHING", [item.code, String(deviceId)]);
      }
      for (const payment of paymentRequests.values()) await client.query(`INSERT INTO sharks_payment_requests
        (id,plan,days,method,customer_name,contact,transaction_id,note,status,access_type,created_at,reviewed_at,access_code)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'CRYPTO',to_timestamp($10/1000.0),CASE WHEN $11::bigint IS NULL THEN NULL ELSE to_timestamp($11/1000.0) END,$12)
        ON CONFLICT (id) DO NOTHING`, [payment.id,payment.plan,payment.days,payment.method,payment.customerName || "",payment.contact || "",payment.transactionId || "",payment.note || "",payment.status || "pending",payment.createdAt || now(),payment.reviewedAt,payment.accessCode]);
      await client.query("INSERT INTO sharks_settings(id,value) VALUES(1,$1) ON CONFLICT (id) DO NOTHING", [defaultSettings]);
      const settings = await client.query("SELECT value FROM sharks_settings WHERE id=1");
      if (settings.rows[0]?.value) Object.assign(defaultSettings, settings.rows[0].value);
      await client.query("COMMIT");
    } catch (migrationError) {
      await client.query("ROLLBACK");
      throw migrationError;
    } finally { client.release(); }

    dbReady = true;
    console.log("Persistent PostgreSQL storage: READY");
    console.log(`Persistent access codes loaded: ${accessCodes.size}`);
    console.log(`Persistent payment requests loaded: ${paymentRequests.size}`);
  } catch (error) {
    dbReady = false;
    console.error("PostgreSQL init failed:", error.message);
    // Keep the app running. A database migration problem must not bring down
    // market scans or make the whole website unavailable.
  }
}

function restoreMap(map, value) {
  map.clear();
  if (value && typeof value === "object") {
    for (const [k, v] of Object.entries(value)) map.set(k, v);
  }
}

function mapObject(map) {
  return Object.fromEntries(map.entries());
}

async function persistState() {
  if (!pool || !dbReady) return;
  // Settings are a single administrator-owned document. Codes, devices and
  // payments intentionally never pass through this aggregate write.
  await pool.query(`INSERT INTO sharks_settings(id,value,updated_at) VALUES(1,$1,NOW())
    ON CONFLICT(id) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()`, [defaultSettings]);
}

let persistTimer = null;
function schedulePersist() {
  if (!pool) return;
  clearTimeout(persistTimer);
  persistTimer = setTimeout(() => { persistState().catch(e => console.error("Persistence error:", e.message)); }, 150);
}
const accessCodes = new Map();
const paymentRequests = new Map();
const adminSessions = new Map();

let cachedRecommendations = [];
let cachedSpotRecommendations = [];
let cachedStocksRecommendations = [];
let cachedGoldRecommendations = [];
let cachedForexRecommendations = [];
let cachedSilverRecommendations = [];
let cachedOilRecommendations = [];

let lastScanAt = null;
let lastSpotScanAt = null;
let lastStocksScanAt = null;
let lastGoldScanAt = null;
let lastForexScanAt = null;
let lastSilverScanAt = null;
let lastOilScanAt = null;

let scanning = false;
let spotScanning = false;
let stocksScanning = false;
let goldScanning = false;
let forexScanning = false;
let silverScanning = false;
let oilScanning = false;

// ============================================================
// DEFAULT SETTINGS
// ============================================================

const defaultSettings = {
  capital: 1000,
  riskPercent: 1,
  leverage: 5,
  maxPositions: 8,
  minScore: 65,

  tp1Percent: 0.8,
  tp2Percent: 1.5,
  tp3Percent: 2.5,
  tp4Percent: 4,
  tp5Percent: 6,

  runnerPercent: 20,

  scanIntervalSeconds: 60,

  timeframes: ["5m", "15m", "1h", "4h"],

  paymentMethods: [
    {
      id: "binance-pay",
      name: "Binance Pay",
      enabled: true,
      instructions:
        "أرسل الدفع عبر Binance Pay ثم أرسل رقم العملية أو صورة إثبات الدفع.",
      details: "",
      qrCode: ""
    },
    {
      id: "usdt",
      name: "USDT",
      enabled: true,
      instructions:
        "أرسل USDT إلى العنوان الموضح أدناه، وتأكد من اختيار الشبكة الصحيحة.",
      details: "",
      qrCode: ""
    },
    {
      id: "cliq",
      name: "CliQ",
      enabled: false,
      instructions:
        "حوّل المبلغ إلى CliQ ثم أرسل رقم العملية.",
      details: "",
      qrCode: ""
    },
    {
      id: "bank",
      name: "تحويل بنكي",
      enabled: false,
      instructions:
        "حوّل المبلغ إلى الحساب البنكي ثم أرسل إثبات التحويل.",
      details: "",
      qrCode: ""
    }
  ]
};

// ============================================================
// UTILS
// ============================================================

function now() {
  return Date.now();
}

function randomString(length = 8) {
  return crypto
    .randomBytes(Math.ceil(length / 2))
    .toString("hex")
    .slice(0, length)
    .toUpperCase();
}

function generateAccessCode() {
  return `SH-${randomString(5)}-${randomString(5)}`;
}

function generateId(prefix = "ID") {
  return `${prefix}-${Date.now()}-${randomString(5)}`;
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function hashPassword(value) {
  return crypto
    .createHash("sha256")
    .update(String(value))
    .digest("hex");
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeSymbol(symbol) {
  return String(symbol || "").toUpperCase().trim();
}

// ============================================================
// ADMIN AUTH
// ============================================================

function createAdminSession() {
  const token = crypto.randomBytes(32).toString("hex");

  adminSessions.set(token, {
    createdAt: now(),
    expiresAt: now() + 24 * 60 * 60 * 1000
  });

  return token;
}

function requireAdmin(req, res, next) {
  const token = req.headers["x-admin-token"];

  if (!token) {
    return res.status(401).json({
      ok: false,
      error: "ADMIN_AUTH_REQUIRED"
    });
  }

  const session = adminSessions.get(token);

  if (!session || session.expiresAt < now()) {
    adminSessions.delete(token);

    return res.status(401).json({
      ok: false,
      error: "ADMIN_SESSION_EXPIRED"
    });
  }

  next();
}

// ============================================================
// ACCESS CODE SYSTEM
// ============================================================

function normalizeAccessType(value, fallback = SERVICE_ACCESS_TYPE) {
  const type = String(value || fallback).trim().toUpperCase();
  return ["CRYPTO", "MARKETS"].includes(type) ? type : fallback;
}

function rowToCode(row, devices = []) {
  return {
    code: row.code, name: row.name, durationDays: Number(row.duration_days),
    maxDevices: Number(row.max_devices), status: row.status, accessType: row.access_type,
    createdAt: new Date(row.created_at).getTime(),
    activatedAt: row.activated_at ? new Date(row.activated_at).getTime() : null,
    expiresAt: row.expires_at ? new Date(row.expires_at).getTime() : null,
    devices, createdByAdmin: row.created_by_admin
  };
}

async function getDbCode(code, client = pool, lock = false) {
  const result = await client.query(`SELECT * FROM sharks_access_codes WHERE code=$1${lock ? " FOR UPDATE" : ""}`, [code]);
  if (!result.rows[0]) return null;
  const devices = await client.query("SELECT device_id FROM sharks_code_devices WHERE code=$1 ORDER BY created_at", [code]);
  return rowToCode(result.rows[0], devices.rows.map(row => row.device_id));
}

async function createAccessCode({
  days,
  name = "",
  maxDevices = 1,
  accessType = SERVICE_ACCESS_TYPE
}) {
  const durationDays = clamp(
    Math.floor(safeNumber(days, 7)),
    1,
    3650
  );

  let code = generateAccessCode();

  const item = {
    code,
    name,
    durationDays,

    maxDevices: clamp(
      Math.floor(safeNumber(maxDevices, 1)),
      1,
      20
    ),

    status: "unused",

    createdAt: now(),
    activatedAt: null,
    expiresAt: null,

    devices: [],

    createdByAdmin: true,
    accessType: normalizeAccessType(accessType)
  };
  if (!pool || !dbReady) {
    while (accessCodes.has(code)) code = generateAccessCode();
    item.code = code; accessCodes.set(code, item); return item;
  }
  for (let attempt = 0; attempt < 5; attempt++) {
    item.code = code;
    try {
      await pool.query(`INSERT INTO sharks_access_codes
        (code,name,duration_days,max_devices,status,access_type,created_at,created_by_admin)
        VALUES($1,$2,$3,$4,$5,$6,NOW(),TRUE)`, [code,item.name,item.durationDays,item.maxDevices,item.status,item.accessType]);
      return getDbCode(code);
    } catch (error) {
      if (error.code !== "23505") throw error;
      code = generateAccessCode();
    }
  }
  throw new Error("Could not generate a unique access code");
}

function isCodeActive(item) {
  if (!item) return false;
  if (item.status === "revoked") return false;
  if (!item.expiresAt) return false;

  return item.expiresAt > now();
}

async function activateCode(code, deviceId = "") {
  if (pool && dbReady) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const item = await getDbCode(code, client, true);
      if (!item) { await client.query("ROLLBACK"); return { ok:false, error:"INVALID_CODE" }; }
      if (item.accessType !== SERVICE_ACCESS_TYPE) { await client.query("ROLLBACK"); return { ok:false, error:"ACCESS_TYPE_MISMATCH" }; }
      if (item.status === "revoked") { await client.query("ROLLBACK"); return { ok:false, error:"CODE_REVOKED" }; }
      if (item.expiresAt && item.expiresAt <= now()) {
        await client.query("UPDATE sharks_access_codes SET status='expired' WHERE code=$1", [code]);
        await client.query("COMMIT"); return { ok:false, error:"CODE_EXPIRED" };
      }
      if (!item.activatedAt) {
        item.activatedAt = now(); item.expiresAt = item.activatedAt + item.durationDays * 86400000; item.status = "active";
        await client.query("UPDATE sharks_access_codes SET activated_at=NOW(), expires_at=NOW() + ($2 * INTERVAL '1 day'), status='active' WHERE code=$1", [code,item.durationDays]);
      }
      if (deviceId && !item.devices.includes(deviceId)) {
        if (item.devices.length >= item.maxDevices) { await client.query("ROLLBACK"); return { ok:false, error:"DEVICE_LIMIT_REACHED" }; }
        await client.query("INSERT INTO sharks_code_devices(code,device_id) VALUES($1,$2) ON CONFLICT DO NOTHING", [code,deviceId]);
      }
      await client.query("COMMIT");
      return { ok:true, code:item.code, accessType:item.accessType, status:item.status, activatedAt:item.activatedAt, expiresAt:item.expiresAt, remainingMs:Math.max(0,item.expiresAt-now()) };
    } catch (error) { await client.query("ROLLBACK"); throw error; } finally { client.release(); }
  }
  const item = accessCodes.get(code);

  if (!item) {
    return {
      ok: false,
      error: "INVALID_CODE"
    };
  }

  if (item.status === "revoked") {
    return {
      ok: false,
      error: "CODE_REVOKED"
    };
  }

  if (item.expiresAt && item.expiresAt <= now()) {
    item.status = "expired";
    schedulePersist();

    return {
      ok: false,
      error: "CODE_EXPIRED"
    };
  }

  if (!item.activatedAt) {
    item.activatedAt = now();

    item.expiresAt =
      item.activatedAt +
      item.durationDays * 24 * 60 * 60 * 1000;

    item.status = "active";
  }

  if (deviceId) {
    if (!item.devices.includes(deviceId)) {
      if (item.devices.length >= item.maxDevices) {
        return {
          ok: false,
          error: "DEVICE_LIMIT_REACHED"
        };
      }

      item.devices.push(deviceId);
      schedulePersist();
    }
  }

  if (item.accessType && item.accessType !== SERVICE_ACCESS_TYPE) return { ok:false, error:"ACCESS_TYPE_MISMATCH" };
  return {
    ok: true,
    code: item.code,
    status: item.status,
    activatedAt: item.activatedAt,
    expiresAt: item.expiresAt,
    remainingMs: Math.max(0, item.expiresAt - now())
  };
}

// ============================================================
// CUSTOMER LOGIN
// ============================================================

app.post("/api/access/login", async (req, res) => {
  const code = String(req.body.code || "")
    .trim()
    .toUpperCase();

  const deviceId = String(req.body.deviceId || "").trim();

  if (!code) {
    return res.status(400).json({
      ok: false,
      error: "CODE_REQUIRED"
    });
  }

  const result = await activateCode(code, deviceId);

  if (!result.ok) {
    return res.status(403).json(result);
  }

  const sessionToken = crypto.randomBytes(32).toString("hex");

  users.set(sessionToken, {
    code,
    deviceId,
    createdAt: now(),
    expiresAt: result.expiresAt
  });
  schedulePersist();

  res.json({
    ok: true,
    sessionToken,
    expiresAt: result.expiresAt,
    remainingMs: result.remainingMs
  });
});

// ============================================================
// CUSTOMER SESSION
// ============================================================

async function requireCustomer(req, res, next) {
  const token = req.headers["x-user-token"];

  if (!token) {
    return res.status(401).json({
      ok: false,
      error: "LOGIN_REQUIRED"
    });
  }

  const session = users.get(token);

  if (!session) {
    return res.status(401).json({
      ok: false,
      error: "INVALID_SESSION"
    });
  }

  const code = pool && dbReady ? await getDbCode(session.code) : accessCodes.get(session.code);

  if (!code || !isCodeActive(code) || code.accessType !== SERVICE_ACCESS_TYPE) {
    users.delete(token);
    schedulePersist();

    return res.status(403).json({
      ok: false,
      error: "SUBSCRIPTION_EXPIRED"
    });
  }

  if (session.expiresAt <= now()) {
    users.delete(token);
    schedulePersist();

    return res.status(403).json({
      ok: false,
      error: "SUBSCRIPTION_EXPIRED"
    });
  }

  req.accessCode = code;
  next();
}

// ============================================================
// ADMIN LOGIN
// ============================================================

app.post("/api/admin/login", (req, res) => {
  const password = String(req.body.password || "");

  if (hashPassword(password) !== hashPassword(ADMIN_PASSWORD)) {
    return res.status(401).json({
      ok: false,
      error: "INVALID_ADMIN_PASSWORD"
    });
  }

  const token = createAdminSession();

  res.json({
    ok: true,
    token
  });
});

// ============================================================
// ADMIN - CREATE ACCESS CODE
// ============================================================

app.post("/api/admin/codes", requireAdmin, async (req, res) => {
  const days = safeNumber(req.body.days, 7);
  const name = String(req.body.name || "");
  const maxDevices = safeNumber(req.body.maxDevices, 1);

  const item = await createAccessCode({
    days,
    name,
    maxDevices,
    accessType: normalizeAccessType(req.body.accessType)
  });

  res.json({
    ok: true,
    code: item
  });
});

// ============================================================
// ADMIN - LIST CODES
// ============================================================

app.get("/api/admin/codes", requireAdmin, async (req, res) => {
  if (pool && dbReady) {
    const result = await pool.query(`SELECT c.*, COALESCE(array_agg(d.device_id) FILTER (WHERE d.device_id IS NOT NULL), '{}') devices
      FROM sharks_access_codes c LEFT JOIN sharks_code_devices d ON d.code=c.code GROUP BY c.code ORDER BY c.created_at DESC`);
    const list = result.rows.map(row => rowToCode(row, row.devices));
    return res.json({ ok:true, codes:list });
  }
  const list = Array.from(accessCodes.values())
    .map(item => {
      if (
        item.status === "active" &&
        item.expiresAt &&
        item.expiresAt <= now()
      ) {
        item.status = "expired";
      }

      return item;
    })
    .sort((a, b) => b.createdAt - a.createdAt);

  res.json({
    ok: true,
    codes: list
  });
});

// ============================================================
// ADMIN - REVOKE CODE
// ============================================================

app.post("/api/admin/codes/revoke", requireAdmin, async (req, res) => {
  const code = String(req.body.code || "")
    .trim()
    .toUpperCase();

  if (pool && dbReady) {
    const result = await pool.query("UPDATE sharks_access_codes SET status='revoked' WHERE code=$1 RETURNING *", [code]);
    if (!result.rows[0]) return res.status(404).json({ok:false,error:"CODE_NOT_FOUND"});
    return res.json({ok:true,code:rowToCode(result.rows[0])});
  }
  const item = accessCodes.get(code);

  if (!item) {
    return res.status(404).json({
      ok: false,
      error: "CODE_NOT_FOUND"
    });
  }

  item.status = "revoked";
  schedulePersist();

  res.json({
    ok: true,
    code: item
  });
});

// ============================================================
// ADMIN - EXTEND CODE
// ============================================================

app.post("/api/admin/codes/extend", requireAdmin, async (req, res) => {
  const code = String(req.body.code || "")
    .trim()
    .toUpperCase();

  const days = clamp(
    Math.floor(safeNumber(req.body.days, 7)),
    1,
    3650
  );

  if (pool && dbReady) {
    const result = await pool.query(`UPDATE sharks_access_codes
      SET expires_at=GREATEST(COALESCE(expires_at,NOW()),NOW()) + ($2 * INTERVAL '1 day'), status='active'
      WHERE code=$1 RETURNING *`, [code,days]);
    if (!result.rows[0]) return res.status(404).json({ok:false,error:"CODE_NOT_FOUND"});
    return res.json({ok:true,code:rowToCode(result.rows[0])});
  }
  const item = accessCodes.get(code);

  if (!item) {
    return res.status(404).json({
      ok: false,
      error: "CODE_NOT_FOUND"
    });
  }

  if (!item.expiresAt) {
    item.expiresAt =
      now() + days * 24 * 60 * 60 * 1000;
  } else {
    item.expiresAt +=
      days * 24 * 60 * 60 * 1000;
  }

  item.status = "active";
  schedulePersist();

  res.json({
    ok: true,
    code: item
  });
});

// ============================================================
// PAYMENT SYSTEM
// ============================================================

function rowToPayment(row) {
  return { id:row.id, plan:row.plan, days:Number(row.days), method:row.method,
    customerName:row.customer_name, contact:row.contact, transactionId:row.transaction_id,
    note:row.note, status:row.status, accessType:row.access_type,
    createdAt:new Date(row.created_at).getTime(), reviewedAt:row.reviewed_at ? new Date(row.reviewed_at).getTime() : null,
    accessCode:row.access_code };
}

app.get("/api/payment-methods", (req, res) => {
  res.json({
    ok: true,
    methods: defaultSettings.paymentMethods.filter(
      x => x.enabled
    )
  });
});

app.post("/api/payment/request", async (req, res) => {
  const plan = String(req.body.plan || "").trim();
  const requestedDays = clamp(
    Math.floor(safeNumber(req.body.days, safeNumber(plan, 0))),
    1,
    3650
  );
  const method = String(req.body.method || "").trim();

  const customerName = String(
    req.body.customerName || ""
  ).trim();

  const contact = String(
    req.body.contact || ""
  ).trim();

  const transactionId = String(
    req.body.transactionId || ""
  ).trim();

  const note = String(
    req.body.note || ""
  ).trim();

  if (!plan || !method) {
    return res.status(400).json({
      ok: false,
      error: "PLAN_AND_METHOD_REQUIRED"
    });
  }

  const id = generateId("PAY");

  const payment = {
    id,
    plan,
    days: requestedDays,
    method,
    customerName,
    contact,
    transactionId,
    note,
    status: "pending",
    createdAt: now(),
    reviewedAt: null,
    accessCode: null,
    accessType: SERVICE_ACCESS_TYPE
  };

  if (pool && dbReady) {
    const result = await pool.query(`INSERT INTO sharks_payment_requests
      (id,plan,days,method,customer_name,contact,transaction_id,note,status,access_type,created_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9,NOW()) RETURNING *`,
      [id,plan,requestedDays,method,customerName,contact,transactionId,note,SERVICE_ACCESS_TYPE]);
    return res.json({ok:true,payment:rowToPayment(result.rows[0])});
  }

  paymentRequests.set(id, payment);
  schedulePersist();

  res.json({
    ok: true,
    payment
  });
});

// ============================================================
// ADMIN - PAYMENT REQUESTS
// ============================================================

app.get(
  "/api/admin/payments",
  requireAdmin,
  async (req, res) => {
    if (pool && dbReady) {
      const result = await pool.query("SELECT * FROM sharks_payment_requests ORDER BY created_at DESC");
      return res.json({ok:true,payments:result.rows.map(rowToPayment)});
    }
    const list = Array.from(paymentRequests.values())
      .sort((a, b) => b.createdAt - a.createdAt);

    res.json({
      ok: true,
      payments: list
    });
  }
);

// ============================================================
// ADMIN - APPROVE PAYMENT
// ============================================================

app.post(
  "/api/admin/payments/approve",
  requireAdmin,
  async (req, res) => {
    const paymentId = String(
      req.body.paymentId || ""
    ).trim();

    if (pool && dbReady) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const locked = await client.query("SELECT * FROM sharks_payment_requests WHERE id=$1 FOR UPDATE", [paymentId]);
        if (!locked.rows[0]) { await client.query("ROLLBACK"); return res.status(404).json({ok:false,error:"PAYMENT_NOT_FOUND"}); }
        const current = rowToPayment(locked.rows[0]);
        if (current.status === "approved") { await client.query("COMMIT"); return res.json({ok:true,payment:current}); }
        const days = clamp(Math.floor(safeNumber(current.days, safeNumber(current.plan,7))),1,3650);
        let code = generateAccessCode();
        for (let attempts=0; attempts<5; attempts++) {
          try {
            const inserted = await client.query(`INSERT INTO sharks_access_codes
              (code,name,duration_days,max_devices,status,access_type,created_at,created_by_admin)
              VALUES($1,$2,$3,1,'unused',$4,NOW(),TRUE) RETURNING *`,
              [code,current.customerName || current.contact || "Customer",days,current.accessType]);
            await client.query("UPDATE sharks_payment_requests SET status='approved', reviewed_at=NOW(), access_code=$2 WHERE id=$1", [paymentId,code]);
            await client.query("COMMIT");
            return res.json({ok:true,payment:{...current,status:"approved",reviewedAt:now(),accessCode:code},code:rowToCode(inserted.rows[0])});
          } catch (error) { if (error.code !== "23505") throw error; code=generateAccessCode(); }
        }
        throw new Error("Could not generate payment access code");
      } catch (error) { await client.query("ROLLBACK"); throw error; } finally { client.release(); }
    }
    const payment =
      paymentRequests.get(paymentId);

    if (!payment) {
      return res.status(404).json({
        ok: false,
        error: "PAYMENT_NOT_FOUND"
      });
    }

    if (payment.status === "approved") {
      return res.json({
        ok: true,
        payment
      });
    }

    const days = clamp(
      Math.floor(
        safeNumber(payment.days, safeNumber(payment.plan, 7))
      ),
      1,
      3650
    );

    const code = await createAccessCode({
      days,
      name:
        payment.customerName ||
        payment.contact ||
        "Customer",
      maxDevices: 1,
      accessType: payment.accessType || SERVICE_ACCESS_TYPE
    });

    payment.status = "approved";
    payment.reviewedAt = now();
    payment.accessCode = code.code;
    schedulePersist();

    res.json({
      ok: true,
      payment,
      code
    });
  }
);

// ============================================================
// ADMIN - REJECT PAYMENT
// ============================================================

app.post(
  "/api/admin/payments/reject",
  requireAdmin,
  async (req, res) => {
    const paymentId = String(
      req.body.paymentId || ""
    ).trim();

    if (pool && dbReady) {
      const result = await pool.query("UPDATE sharks_payment_requests SET status='rejected', reviewed_at=NOW() WHERE id=$1 AND status <> 'approved' RETURNING *", [paymentId]);
      if (!result.rows[0]) return res.status(404).json({ok:false,error:"PAYMENT_NOT_FOUND"});
      return res.json({ok:true,payment:rowToPayment(result.rows[0])});
    }
    const payment =
      paymentRequests.get(paymentId);

    if (!payment) {
      return res.status(404).json({
        ok: false,
        error: "PAYMENT_NOT_FOUND"
      });
    }

    payment.status = "rejected";
    payment.reviewedAt = now();
    schedulePersist();

    res.json({
      ok: true,
      payment
    });
  }
);

// ============================================================
// ADMIN - PAYMENT METHOD SETTINGS
// ============================================================

app.get(
  "/api/admin/payment-methods",
  requireAdmin,
  (req, res) => {
    res.json({
      ok: true,
      methods: defaultSettings.paymentMethods
    });
  }
);

app.post(
  "/api/admin/payment-methods",
  requireAdmin,
  (req, res) => {
    const methods = Array.isArray(req.body.methods)
      ? req.body.methods
      : [];

    defaultSettings.paymentMethods =
      methods.map((m, index) => ({
        id: String(
          m.id || `method-${index + 1}`
        ),
        name: String(
          m.name || `طريقة ${index + 1}`
        ),
        enabled: Boolean(m.enabled),
        instructions: String(
          m.instructions || ""
        ),
        details: String(
          m.details || ""
        ),
        qrCode:
          typeof m.qrCode === "string" &&
          m.qrCode.startsWith("data:image/")
            ? m.qrCode.slice(0, 1500000)
            : ""
      }));

    schedulePersist();

    res.json({
      ok: true,
      methods:
        defaultSettings.paymentMethods
    });
  }
);

// ============================================================
// CUSTOMER SETTINGS
// ============================================================

app.get(
  "/api/config",
  (req, res) => {
    res.json({
      ok: true,
      appMode: APP_MODE,
      accessType: SERVICE_ACCESS_TYPE,
      markets: SERVICE_MARKETS,
      settings: {
        capital: defaultSettings.capital,
        riskPercent:
          defaultSettings.riskPercent,
        leverage: defaultSettings.leverage,
        maxPositions:
          defaultSettings.maxPositions,
        minScore:
          defaultSettings.minScore,

        tp1Percent:
          defaultSettings.tp1Percent,
        tp2Percent:
          defaultSettings.tp2Percent,
        tp3Percent:
          defaultSettings.tp3Percent,
        tp4Percent:
          defaultSettings.tp4Percent,
        tp5Percent:
          defaultSettings.tp5Percent,

        runnerPercent:
          defaultSettings.runnerPercent,

        scanIntervalSeconds:
          defaultSettings.scanIntervalSeconds,

        timeframes:
          defaultSettings.timeframes
      }
    });
  }
);

// ============================================================
// ADMIN SETTINGS
// ============================================================

app.get(
  "/api/admin/settings",
  requireAdmin,
  (req, res) => {
    res.json({
      ok: true,
      settings: defaultSettings
    });
  }
);

app.post(
  "/api/admin/settings",
  requireAdmin,
  (req, res) => {
    const body = req.body || {};

    defaultSettings.capital =
      clamp(
        safeNumber(
          body.capital,
          defaultSettings.capital
        ),
        1,
        100000000
      );

    defaultSettings.riskPercent =
      clamp(
        safeNumber(
          body.riskPercent,
          defaultSettings.riskPercent
        ),
        0.01,
        100
      );

    defaultSettings.leverage =
      clamp(
        Math.floor(
          safeNumber(
            body.leverage,
            defaultSettings.leverage
          )
        ),
        1,
        125
      );

    defaultSettings.maxPositions =
      clamp(
        Math.floor(
          safeNumber(
            body.maxPositions,
            defaultSettings.maxPositions
          )
        ),
        1,
        100
      );

    defaultSettings.minScore =
      clamp(
        safeNumber(
          body.minScore,
          defaultSettings.minScore
        ),
        0,
        100
      );

    defaultSettings.tp1Percent =
      clamp(
        safeNumber(
          body.tp1Percent,
          defaultSettings.tp1Percent
        ),
        0.01,
        100
      );

    defaultSettings.tp2Percent =
      clamp(
        safeNumber(
          body.tp2Percent,
          defaultSettings.tp2Percent
        ),
        0.01,
        200
      );

    defaultSettings.tp3Percent =
      clamp(
        safeNumber(
          body.tp3Percent,
          defaultSettings.tp3Percent
        ),
        0.01,
        300
      );

    defaultSettings.tp4Percent =
      clamp(
        safeNumber(
          body.tp4Percent,
          defaultSettings.tp4Percent
        ),
        0.01,
        500
      );

    defaultSettings.tp5Percent =
      clamp(
        safeNumber(
          body.tp5Percent,
          defaultSettings.tp5Percent
        ),
        0.01,
        1000
      );

    defaultSettings.runnerPercent =
      clamp(
        safeNumber(
          body.runnerPercent,
          defaultSettings.runnerPercent
        ),
        0,
        100
      );

    defaultSettings.scanIntervalSeconds =
      clamp(
        Math.floor(
          safeNumber(
            body.scanIntervalSeconds,
            defaultSettings.scanIntervalSeconds
          )
        ),
        30,
        3600
      );

    if (Array.isArray(body.timeframes)) {
      defaultSettings.timeframes =
        body.timeframes.filter(x =>
          ["5m", "15m", "1h", "4h"].includes(x)
        );
    }

    schedulePersist();

    res.json({
      ok: true,
      settings: defaultSettings
    });
  }
);

// ============================================================
// OKX HELPERS
// ============================================================

const OKX_BASE = OKX_PROXY;

const OKX_TIMEFRAME_MAP = {
  "5m": "5m",
  "15m": "15m",
  "1h": "1H",
  "4h": "4H"
};

// ============================================================
// OKX PUBLIC REST RATE LIMITER
// ============================================================
//
// OKX public market-data REST limits are IP based.
// The automatic scanner used to make hundreds of requests
// almost immediately (hundreds of candle requests per cycle),
// which caused HTTP 429 / code 50011.
//
// We deliberately serialize requests and keep ~750 ms between
// requests. Automatic scanning is limited to the top 4 symbols
// per market. Manual Search still analyzes the requested symbol.
// ============================================================

const OKX_REQUEST_GAP_MS = Number(
  process.env.OKX_REQUEST_GAP_MS || 400
);

let okxRequestChain = Promise.resolve();
let lastOkxRequestAt = 0;

function queueOkxRequest(task) {
  const run = okxRequestChain.then(async () => {
    const elapsed =
      Date.now() - lastOkxRequestAt;

    const wait =
      Math.max(
        0,
        OKX_REQUEST_GAP_MS - elapsed
      );

    if (wait > 0) {
      await sleep(wait);
    }

    lastOkxRequestAt = Date.now();

    return task();
  });

  // Keep the queue alive even if one request fails.
  okxRequestChain = run.catch(() => undefined);

  return run;
}

async function fetchOkxOnce(endpoint) {
  const url =
    `${OKX_BASE}${endpoint}`;

  const response = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "User-Agent":
        "Sharks-Recommendation/1.0"
    }
  });

  const text =
    await response.text();

  return {
    response,
    text
  };
}

async function okxFetch(endpoint) {

  let lastError = null;

  // A small retry is useful after a transient 429.
  // Backoff is intentionally slow so we do not amplify
  // rate-limit pressure.
  const retryDelays = [
    0,
    3000,
    8000
  ];

  for (
    let attempt = 0;
    attempt < retryDelays.length;
    attempt++
  ) {

    if (retryDelays[attempt] > 0) {
      await sleep(
        retryDelays[attempt]
      );
    }

    try {

      const result =
        await queueOkxRequest(
          () =>
            fetchOkxOnce(
              endpoint
            )
        );

      const response =
        result.response;

      const text =
        result.text;

      if (!response.ok) {

        lastError =
          new Error(
            `OKX HTTP ${response.status}: ${text.slice(0, 300)}`
          );

        // Retry only rate limiting.
        if (response.status === 429) {
          continue;
        }

        throw lastError;
      }

      let data;

      try {
        data =
          JSON.parse(text);
      } catch (_) {
        throw new Error(
          `OKX returned invalid JSON: ${text.slice(0, 300)}`
        );
      }

      if (
        data.code !== undefined &&
        String(data.code) !== "0"
      ) {

        lastError =
          new Error(
            `OKX API ${data.code}: ${data.msg || "Unknown error"}`
          );

        // OKX code 50011 = rate limit reached.
        if (
          String(data.code) === "50011"
        ) {
          continue;
        }

        throw lastError;
      }

      return data;

    } catch (error) {

      lastError = error;

      if (
        !String(error.message || "")
          .includes("429")
      ) {
        throw error;
      }
    }
  }

  throw lastError ||
    new Error(
      "OKX request failed after retries"
    );
}

function normalizeSearchSymbol(input, market) {
  let value = String(input || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "");

  if (!value) return "";

  if (market === "spot") {
    value = value.replace(/[-_\/]/g, "");

    if (value.endsWith("USDT")) {
      return `${value.slice(0, -4)}-USDT`;
    }

    return `${value}-USDT`;
  }

  value = value
    .replace(/[-_\/]/g, "")
    .replace(/SWAP$/i, "");

  if (value.endsWith("USDT")) {
    return `${value.slice(0, -4)}-USDT-SWAP`;
  }

  return `${value}-USDT-SWAP`;
}


// ============================================================
// OKX MARKET DATA ADAPTER
// ============================================================

function normalizeTicker(ticker, market) {
  const last = safeNumber(ticker.last);
  const open24 = safeNumber(
    ticker.sodUtc8 || ticker.open24h || ticker.open24
  );

  const change24h =
    open24 > 0
      ? ((last - open24) / open24) * 100
      : 0;

  return {
    symbol: ticker.instId,
    price: last,
    volume: safeNumber(
      ticker.volCcy24h || ticker.vol24h
    ),
    change24h,
    market
  };
}

function diversifyUniverse(all, limit, market) {
  const suffix = market === "spot" ? "-USDT" : "-USDT-SWAP";
  const must = ["BTC", "ETH", "SOL"].map(x => `${x}${suffix}`);
  const byId = new Map(all.map(x => [x.symbol, x]));
  const selected = [];
  for (const id of must) {
    const item = byId.get(id);
    if (item) selected.push(item);
  }
  for (const item of all) {
    if (selected.length >= limit) break;
    if (!selected.some(x => x.symbol === item.symbol)) selected.push(item);
  }
  return selected.slice(0, limit);
}

async function getFuturesUniverse() {
  const data = await okxFetch(
    "/api/v5/market/tickers?instType=SWAP"
  );

  const all = (data.data || [])
    .filter(x => String(x.instId || "").endsWith("-USDT-SWAP"))
    .map(x => normalizeTicker(x, "futures"))
    .filter(x => x.price > 0)
    .sort((a, b) => b.volume - a.volume);

  return diversifyUniverse(all, MAX_SYMBOLS, "futures");
}

async function getSpotUniverse() {
  const data = await okxFetch(
    "/api/v5/market/tickers?instType=SPOT"
  );

  const all = (data.data || [])
    .filter(x => String(x.instId || "").endsWith("-USDT"))
    .map(x => normalizeTicker(x, "spot"))
    .filter(x => x.price > 0)
    .sort((a, b) => b.volume - a.volume);

  return diversifyUniverse(all, SPOT_MAX_SYMBOLS, "spot");
}

async function getSpecificMarketSymbol(input, market) {
  const instId =
    normalizeSearchSymbol(input, market);

  if (!instId) {
    throw new Error("اكتب اسم العملة أولًا.");
  }

  const data = await okxFetch(
    `/api/v5/market/ticker?instId=${encodeURIComponent(instId)}`
  );

  const ticker = data.data?.[0];

  if (!ticker) {
    throw new Error(
      `العملة ${instId} غير موجودة على OKX في سوق ${market === "spot" ? "Spot" : "Futures"}.`
    );
  }

  return normalizeTicker(ticker, market);
}

function okxBar(interval) {
  const bar = OKX_TIMEFRAME_MAP[interval];

  if (!bar) {
    throw new Error(`Unsupported timeframe: ${interval}`);
  }

  return bar;
}

async function getFuturesKlines(symbol, interval, limit = 150) {
  const endpoint =
    `/api/v5/market/candles?instId=${encodeURIComponent(symbol)}` +
    `&bar=${encodeURIComponent(okxBar(interval))}` +
    `&limit=${Math.min(Math.max(limit, 50), 300)}`;

  const data = await okxFetch(endpoint);

  // OKX returns newest candle first. The analyzer expects oldest -> newest.
  return (data.data || [])
    .slice()
    .reverse();
}

async function getSpotKlines(symbol, interval, limit = 150) {
  const endpoint =
    `/api/v5/market/candles?instId=${encodeURIComponent(symbol)}` +
    `&bar=${encodeURIComponent(okxBar(interval))}` +
    `&limit=${Math.min(Math.max(limit, 50), 300)}`;

  const data = await okxFetch(endpoint);

  return (data.data || [])
    .slice()
    .reverse();
}

// ============================================================
// MULTI-SOURCE EXTERNAL MARKET DATA — STOCKS / GOLD / SILVER / OIL / FOREX
// ============================================================
// Yahoo is used when available, but it is NOT the only source.
// Stooq is used as an independent daily-data fallback/confirmation source.
// Optional provider keys can be added later without changing the API.

const YAHOO_BASES = [
  "https://query1.finance.yahoo.com",
  "https://query2.finance.yahoo.com"
];
const YAHOO_REQUEST_GAP_MS = Number(process.env.YAHOO_REQUEST_GAP_MS || 1800);
const STOOQ_BASE = "https://stooq.com/q/d/l/";
const STOOQ_REQUEST_GAP_MS = Number(process.env.STOOQ_REQUEST_GAP_MS || 1200);
let yahooRequestChain = Promise.resolve();
let stooqRequestChain = Promise.resolve();
let lastYahooRequestAt = 0;
let lastStooqRequestAt = 0;

function queueRateLimited(chainName, task, gapMs) {
  if (chainName === "yahoo") {
    const run = yahooRequestChain.then(async () => {
      const elapsed = Date.now() - lastYahooRequestAt;
      const wait = Math.max(0, gapMs - elapsed);
      if (wait > 0) await sleep(wait);
      lastYahooRequestAt = Date.now();
      return task();
    });
    yahooRequestChain = run.catch(() => undefined);
    return run;
  }

  const run = stooqRequestChain.then(async () => {
    const elapsed = Date.now() - lastStooqRequestAt;
    const wait = Math.max(0, gapMs - elapsed);
    if (wait > 0) await sleep(wait);
    lastStooqRequestAt = Date.now();
    return task();
  });
  stooqRequestChain = run.catch(() => undefined);
  return run;
}

const YAHOO_TIMEFRAME_MAP = {
  "5m": { interval: "5m", range: "30d" },
  "15m": { interval: "15m", range: "60d" },
  "1h": { interval: "60m", range: "730d" },
  "4h": { interval: "1h", range: "730d" }
};

function externalStooqSymbol(symbol, market) {
  const clean = String(symbol || "").trim().toLowerCase();
  if (market === "stocks") return `${clean.replace(/[^a-z0-9.]/g, "")}.us`;
  if (market === "gold") return "gc.f";
  if (market === "silver") return "si.f";
  if (market === "oil") return "cl.f";
  if (market === "forex") return clean.replace("=x", "");
  return clean;
}

async function yahooFetchChart(symbol, timeframe) {
  const cfg = YAHOO_TIMEFRAME_MAP[timeframe];
  if (!cfg) throw new Error(`Unsupported timeframe: ${timeframe}`);

  let lastError = null;
  for (const base of YAHOO_BASES) {
    const endpoint =
      `${base}/v8/finance/chart/${encodeURIComponent(symbol)}` +
      `?interval=${encodeURIComponent(cfg.interval)}` +
      `&range=${encodeURIComponent(cfg.range)}` +
      `&includePrePost=false&events=div%2Csplits`;

    try {
      const response = await queueRateLimited("yahoo", () =>
        fetch(endpoint, {
          headers: {
            Accept: "application/json",
            "User-Agent": "Mozilla/5.0 Sharks-Recommendation/2.0"
          }
        })
      );

      const text = await response.text();
      if (!response.ok) {
        lastError = new Error(`Yahoo HTTP ${response.status}: ${text.slice(0, 160)}`);
        continue;
      }

      const data = JSON.parse(text);
      const result = data?.chart?.result?.[0];
      if (!result) throw new Error(`لا توجد بيانات متاحة لـ ${symbol} على ${timeframe}.`);

      const ts = result.timestamp || [];
      const quote = result.indicators?.quote?.[0] || {};
      const candles = [];

      for (let i = 0; i < ts.length; i++) {
        const open = safeNumber(quote.open?.[i]);
        const high = safeNumber(quote.high?.[i]);
        const low = safeNumber(quote.low?.[i]);
        const close = safeNumber(quote.close?.[i]);
        const volume = safeNumber(quote.volume?.[i], 0);
        if (open > 0 && high > 0 && low > 0 && close > 0) {
          candles.push([ts[i], open, high, low, close, volume]);
        }
      }
      if (candles.length) return candles;
      lastError = new Error(`Yahoo returned no usable candles for ${symbol}.`);
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error(`تعذر جلب بيانات ${symbol} من Yahoo.`);
}

async function stooqFetchDaily(symbol, market) {
  const stooqSymbol = externalStooqSymbol(symbol, market);
  const endpoint = `${STOOQ_BASE}?s=${encodeURIComponent(stooqSymbol)}&d1=${formatStooqDate(Date.now() - 1000 * 60 * 60 * 24 * 120)}&d2=${formatStooqDate(Date.now())}&i=d`;

  const response = await queueRateLimited("stooq", () =>
    fetch(endpoint, {
      headers: {
        Accept: "text/csv,text/plain,*/*",
        "User-Agent": "Sharks-Recommendation/2.0"
      }
    })
  );

  const text = await response.text();
  if (!response.ok) throw new Error(`Stooq HTTP ${response.status}: ${text.slice(0, 160)}`);

  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 3 || !/^Date,Open,High,Low,Close/i.test(lines[0])) {
    throw new Error(`Stooq no data for ${stooqSymbol}`);
  }

  const candles = [];
  for (const line of lines.slice(1)) {
    const parts = line.split(",");
    if (parts.length < 6) continue;
    const [date, open, high, low, close, volume] = parts;
    const o = Number(open), h = Number(high), l = Number(low), c = Number(close);
    if (![o,h,l,c].every(Number.isFinite) || o <= 0 || h <= 0 || l <= 0 || c <= 0) continue;
    const ts = Math.floor(Date.parse(`${date}T00:00:00Z`) / 1000);
    candles.push([ts, o, h, l, c, Number(volume) || 0]);
  }
  return candles;
}

function formatStooqDate(timestamp) {
  const d = new Date(timestamp);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

async function getExternalCandles(symbol, market, timeframe) {
  try {
    return {
      candles: await yahooFetchChart(symbol, timeframe),
      source: "Yahoo"
    };
  } catch (yahooError) {
    try {
      // Stooq is daily, so use it as an independent fallback when Yahoo is unavailable.
      const daily = await stooqFetchDaily(symbol, market);
      if (daily.length >= 50) {
        return { candles: daily, source: "Stooq" };
      }
      throw new Error("Stooq returned insufficient daily data.");
    } catch (stooqError) {
      throw new Error(`Yahoo failed (${yahooError.message}); Stooq failed (${stooqError.message})`);
    }
  }
}

async function getExternalTicker(symbol, market) {
  try {
    const candles = await yahooFetchChart(symbol, "1h");
    const last = candles[candles.length - 1];
    const previous = candles.length > 24 ? candles[candles.length - 25][4] : candles[0][4];
    return {
      symbol,
      price: last[4],
      volume: last[5],
      change24h: previous > 0 ? ((last[4] - previous) / previous) * 100 : 0,
      market,
      source: "Yahoo"
    };
  } catch (yahooError) {
    const candles = await stooqFetchDaily(symbol, market);
    const last = candles[candles.length - 1];
    const previous = candles.length > 1 ? candles[candles.length - 2][4] : last[4];
    return {
      symbol,
      price: last[4],
      volume: last[5],
      change24h: previous > 0 ? ((last[4] - previous) / previous) * 100 : 0,
      market,
      source: "Stooq",
      fallbackFrom: yahooError.message
    };
  }
}

function normalizeExternalSearchSymbol(input, market) {
  let value = String(input || "").trim().toUpperCase().replace(/\s+/g, "");
  if (!value) return "";
  if (market === "gold") {
    if (["GOLD","XAU","XAUUSD","GC","GC=F"].includes(value)) return "GC=F";
    return value;
  }
  if (market === "silver") {
    if (["SILVER","XAG","XAGUSD","SI","SI=F"].includes(value)) return "SI=F";
    return value;
  }
  if (market === "oil") {
    if (["OIL","WTI","CL","CL=F"].includes(value)) return "CL=F";
    return value;
  }
  if (market === "forex") {
    value = value.replace(/[\/_-]/g, "");
    if (value.length === 6 && !value.endsWith("=X")) return `${value}=X`;
    return value.endsWith("=X") ? value : `${value}=X`;
  }
  return value.replace(/[\/_]/g, "");
}

async function analyzeExternalSymbol(input, market) {
  const symbol = normalizeExternalSearchSymbol(input, market);
  if (!symbol) throw new Error("اكتب الرمز أولًا.");

  const ticker = await getExternalTicker(symbol, market);
  const results = [];
  const sourcesUsed = new Set([ticker.source]);

  for (const timeframe of defaultSettings.timeframes) {
    try {
      const resultData = await getExternalCandles(symbol, market, timeframe);
      const result = calculateTimeframeAnalysis(resultData.candles, timeframe);
      if (result) {
        result.source = resultData.source;
        sourcesUsed.add(resultData.source);
        results.push(result);
      }
    } catch (error) {
      console.error("External", market, symbol, timeframe, error.message);
    }
  }

  if (!results.length) return null;

  let buyCount = 0, sellCount = 0, buyTotal = 0, sellTotal = 0;
  for (const r of results) {
    if (r.side === "LONG") { buyCount++; buyTotal += r.score; }
    else { sellCount++; sellTotal += r.score; }
  }

  const side = buyCount >= sellCount ? "BUY" : "SELL";
  const agreement = side === "BUY" ? buyCount : sellCount;
  const averageScore = side === "BUY" ? buyTotal / Math.max(buyCount, 1) : sellTotal / Math.max(sellCount, 1);
  if (agreement < Math.ceil(results.length * 0.5)) return null;

  const entry = ticker.price;
  const atrValue = results[0].atr > 0 ? results[0].atr : entry * 0.01;
  const stopDistance = Math.max(atrValue * 1.5, entry * 0.003);
  const p = [defaultSettings.tp1Percent, defaultSettings.tp2Percent, defaultSettings.tp3Percent, defaultSettings.tp4Percent, defaultSettings.tp5Percent];
  const sl = side === "BUY" ? Math.max(entry - stopDistance, 0) : entry + stopDistance;
  const tp = p.map(x => side === "BUY" ? entry * (1 + x / 100) : Math.max(entry * (1 - x / 100), 0));
  const riskMoney = defaultSettings.capital * (defaultSettings.riskPercent / 100);
  const stopPercent = entry > 0 ? Math.abs(entry - sl) / entry : 0;

  return {
    market, symbol, side, score: clamp(Math.round(averageScore), 0, 100), agreement,
    timeframes: results.map(r => r.interval),
    entry, sl, tp1: tp[0], tp2: tp[1], tp3: tp[2], tp4: tp[3], tp5: tp[4],
    runner: defaultSettings.runnerPercent, leverage: 1, leverageLabel: "بدون رافعة",
    capital: defaultSettings.capital, riskPercent: defaultSettings.riskPercent, riskMoney,
    positionNotional: stopPercent > 0 ? riskMoney / stopPercent : 0,
    volume: ticker.volume, change24h: ticker.change24h,
    dataSources: Array.from(sourcesUsed),
    dataSource: Array.from(sourcesUsed).join(" + "),
    reasons: results.flatMap(r => r.reasons).slice(0, 8),
    generatedAt: now()
  };
}

async function scanExternalMarket(market) {
  const state = { stocks: "stocks", gold: "gold", silver: "silver", oil: "oil", forex: "forex" }[market];
  if (!state) return;
  const isScanning = ({stocks: stocksScanning, gold: goldScanning, silver: silverScanning, oil: oilScanning, forex: forexScanning})[market];
  if (isScanning) return;
  if (market === "stocks") stocksScanning = true;
  if (market === "gold") goldScanning = true;
  if (market === "silver") silverScanning = true;
  if (market === "oil") oilScanning = true;
  if (market === "forex") forexScanning = true;

  try {
    const output = [];
    const universe = EXTERNAL_ASSET_UNIVERSES[market] || [];
    for (const symbol of universe) {
      try {
        const rec = await analyzeExternalSymbol(symbol, market);
        if (rec && rec.score >= defaultSettings.minScore) output.push(rec);
      } catch (error) {
        console.error(`${market} analyze error:`, symbol, error.message);
      }
    }
    output.sort((a, b) => b.score - a.score);
    if (market === "stocks") { cachedStocksRecommendations = output.slice(0, defaultSettings.maxPositions); lastStocksScanAt = now(); }
    if (market === "gold") { cachedGoldRecommendations = output.slice(0, defaultSettings.maxPositions); lastGoldScanAt = now(); }
    if (market === "silver") { cachedSilverRecommendations = output.slice(0, defaultSettings.maxPositions); lastSilverScanAt = now(); }
    if (market === "oil") { cachedOilRecommendations = output.slice(0, defaultSettings.maxPositions); lastOilScanAt = now(); }
    if (market === "forex") { cachedForexRecommendations = output.slice(0, defaultSettings.maxPositions); lastForexScanAt = now(); }
    console.log(`${market} multi-source scan complete: ${output.length} candidates`);
  } finally {
    if (market === "stocks") stocksScanning = false;
    if (market === "gold") goldScanning = false;
    if (market === "silver") silverScanning = false;
    if (market === "oil") oilScanning = false;
    if (market === "forex") forexScanning = false;
  }
}

// ============================================================
// TECHNICAL INDICATORS
// ============================================================

function ema(values, period) {
  const data = values
    .map(Number)
    .filter(Number.isFinite);

  if (data.length < period) {
    return data.length
      ? data[data.length - 1]
      : 0;
  }

  const multiplier = 2 / (period + 1);

  let value =
    data
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  for (let i = period; i < data.length; i++) {
    value =
      (data[i] - value) * multiplier +
      value;
  }

  return value;
}

function rsi(values, period = 14) {
  const closes = values
    .map(Number)
    .filter(Number.isFinite);

  if (closes.length <= period) {
    return 50;
  }

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const change =
      closes[i] - closes[i - 1];

    if (change >= 0) {
      gains += change;
    } else {
      losses -= change;
    }
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  for (
    let i = period + 1;
    i < closes.length;
    i++
  ) {
    const change =
      closes[i] - closes[i - 1];

    const gain =
      change > 0 ? change : 0;

    const loss =
      change < 0 ? -change : 0;

    avgGain =
      (avgGain * (period - 1) + gain) /
      period;

    avgLoss =
      (avgLoss * (period - 1) + loss) /
      period;
  }

  if (avgLoss === 0) {
    return 100;
  }

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

function atr(candles, period = 14) {
  if (
    !Array.isArray(candles) ||
    candles.length <= period
  ) {
    return 0;
  }

  const trueRanges = [];

  for (let i = 1; i < candles.length; i++) {
    const high = Number(candles[i][2]);
    const low = Number(candles[i][3]);
    const previousClose =
      Number(candles[i - 1][4]);

    if (
      !Number.isFinite(high) ||
      !Number.isFinite(low) ||
      !Number.isFinite(previousClose)
    ) {
      continue;
    }

    trueRanges.push(
      Math.max(
        high - low,
        Math.abs(high - previousClose),
        Math.abs(low - previousClose)
      )
    );
  }

  if (trueRanges.length < period) {
    return 0;
  }

  let value =
    trueRanges
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  for (
    let i = period;
    i < trueRanges.length;
    i++
  ) {
    value =
      (value * (period - 1) +
        trueRanges[i]) /
      period;
  }

  return value;
}

// ============================================================
// GENERIC TIMEFRAME ANALYSIS
// ============================================================

function calculateTimeframeAnalysis(
  candles,
  interval
) {
  if (
    !Array.isArray(candles) ||
    candles.length < 50
  ) {
    return null;
  }

  const closes =
    candles.map(x => Number(x[4]));

  const volumes =
    candles.map(x => Number(x[5]));

  const current =
    closes[closes.length - 1];

  const ema20 =
    ema(closes.slice(-80), 20);

  const ema50 =
    ema(closes.slice(-100), 50);

  const rsi14 =
    rsi(closes, 14);

  const atr14 =
    atr(candles, 14);

  const averageVolume =
    volumes
      .slice(-21, -1)
      .reduce(
        (a, b) => a + b,
        0
      ) / 20;

  const currentVolume =
    volumes[volumes.length - 1];

  const volumeRatio =
    averageVolume > 0
      ? currentVolume / averageVolume
      : 1;

  let longScore = 50;
  let shortScore = 50;

  const reasonsLong = [];
  const reasonsShort = [];

  if (current > ema20) {
    longScore += 10;
    reasonsLong.push(
      "السعر فوق EMA20"
    );
  } else {
    shortScore += 10;
    reasonsShort.push(
      "السعر تحت EMA20"
    );
  }

  if (ema20 > ema50) {
    longScore += 12;
    reasonsLong.push(
      "EMA20 فوق EMA50"
    );
  } else {
    shortScore += 12;
    reasonsShort.push(
      "EMA20 تحت EMA50"
    );
  }

  if (
    rsi14 >= 50 &&
    rsi14 <= 68
  ) {
    longScore += 12;
    reasonsLong.push(
      `RSI داعم للصعود ${rsi14.toFixed(1)}`
    );
  }

  if (
    rsi14 <= 50 &&
    rsi14 >= 32
  ) {
    shortScore += 12;
    reasonsShort.push(
      `RSI داعم للهبوط ${rsi14.toFixed(1)}`
    );
  }

  if (rsi14 > 72) {
    shortScore += 8;
    reasonsShort.push(
      "RSI مرتفع"
    );
  }

  if (rsi14 < 28) {
    longScore += 8;
    reasonsLong.push(
      "RSI منخفض"
    );
  }

  if (volumeRatio >= 1.5) {
    longScore += 8;
    shortScore += 8;

    reasonsLong.push(
      "ارتفاع واضح في الحجم"
    );

    reasonsShort.push(
      "ارتفاع واضح في الحجم"
    );
  }

  longScore =
    clamp(
      Math.round(longScore),
      0,
      100
    );

  shortScore =
    clamp(
      Math.round(shortScore),
      0,
      100
    );

  const side =
    longScore >= shortScore
      ? "LONG"
      : "SHORT";

  const strength =
    side === "LONG"
      ? longScore
      : shortScore;

  return {
    interval,
    side,
    score: strength,

    longScore,
    shortScore,

    price: current,

    ema20,
    ema50,
    rsi: rsi14,
    atr: atr14,

    volumeRatio,

    reasons:
      side === "LONG"
        ? reasonsLong
        : reasonsShort
  };
}

// ============================================================
// FUTURES TIMEFRAME ANALYSIS
// ============================================================

async function analyzeFuturesTimeframe(
  symbol,
  interval
) {
  const candles =
    await getFuturesKlines(
      symbol,
      interval,
      150
    );

  return calculateTimeframeAnalysis(
    candles,
    interval
  );
}

// ============================================================
// SPOT TIMEFRAME ANALYSIS
// ============================================================

async function analyzeSpotTimeframe(
  symbol,
  interval
) {
  const candles =
    await getSpotKlines(
      symbol,
      interval,
      150
    );

  return calculateTimeframeAnalysis(
    candles,
    interval
  );
}

// ============================================================
// BUILD FUTURES RECOMMENDATION
// ============================================================

async function analyzeFuturesSymbol(
  symbolData
) {
  const symbol =
    symbolData.symbol;

  const results = [];

  for (
    const timeframe of
    defaultSettings.timeframes
  ) {
    try {
      const result =
        await analyzeFuturesTimeframe(
          symbol,
          timeframe
        );

      if (result) {
        results.push(result);
      }
    } catch (error) {
      console.error(
        "Futures",
        symbol,
        timeframe,
        error.message
      );
    }
  }

  if (!results.length) {
    return null;
  }

  let longCount = 0;
  let shortCount = 0;

  let longTotal = 0;
  let shortTotal = 0;

  for (const r of results) {
    if (r.side === "LONG") {
      longCount++;
      longTotal += r.score;
    } else {
      shortCount++;
      shortTotal += r.score;
    }
  }

  const side =
    longCount >= shortCount
      ? "LONG"
      : "SHORT";

  const agreement =
    side === "LONG"
      ? longCount
      : shortCount;

  const averageScore =
    side === "LONG"
      ? longTotal /
        Math.max(longCount, 1)
      : shortTotal /
        Math.max(shortCount, 1);

  const score =
    clamp(
      Math.round(averageScore),
      0,
      100
    );

  if (
    agreement <
    Math.ceil(
      results.length * 0.75
    )
  ) {
    return null;
  }

  const entry =
    symbolData.price;

  const first =
    results[0];

  const atrValue =
    first.atr > 0
      ? first.atr
      : entry * 0.01;

  const stopDistance =
    Math.max(
      atrValue * 1.5,
      entry * 0.003
    );

  let sl;
  let tp1;
  let tp2;
  let tp3;
  let tp4;
  let tp5;

  const tpPercents = [
    defaultSettings.tp1Percent,
    defaultSettings.tp2Percent,
    defaultSettings.tp3Percent,
    defaultSettings.tp4Percent,
    defaultSettings.tp5Percent
  ];

  if (side === "LONG") {
    sl = entry - stopDistance;
    tp1 = entry * (1 + tpPercents[0] / 100);
    tp2 = entry * (1 + tpPercents[1] / 100);
    tp3 = entry * (1 + tpPercents[2] / 100);
    tp4 = entry * (1 + tpPercents[3] / 100);
    tp5 = entry * (1 + tpPercents[4] / 100);
  } else {
    sl = entry + stopDistance;
    tp1 = entry * (1 - tpPercents[0] / 100);
    tp2 = entry * (1 - tpPercents[1] / 100);
    tp3 = entry * (1 - tpPercents[2] / 100);
    tp4 = entry * (1 - tpPercents[3] / 100);
    tp5 = entry * (1 - tpPercents[4] / 100);
  }

  const riskMoney =
    defaultSettings.capital *
    (defaultSettings.riskPercent / 100);

  const stopPercent =
    Math.abs(entry - sl) / entry;

  const positionNotional =
    stopPercent > 0
      ? riskMoney / stopPercent
      : 0;

  const reasons =
    results
      .flatMap(r => r.reasons)
      .slice(0, 8);

  return {
    market: "futures",
    symbol,
    side,
    score,

    agreement,
    timeframes:
      results.map(r => r.interval),

    entry,
    sl,

    tp1,
    tp2,
    tp3,
    tp4,
    tp5,

    runner:
      defaultSettings.runnerPercent,

    leverage:
      defaultSettings.leverage,

    capital:
      defaultSettings.capital,

    riskPercent:
      defaultSettings.riskPercent,

    riskMoney,
    positionNotional,

    volume:
      symbolData.volume,

    change24h:
      symbolData.change24h,

    reasons,

    generatedAt: now()
  };
}

// ============================================================
// BUILD SPOT RECOMMENDATION
// ============================================================

async function analyzeSpotSymbol(
  symbolData
) {
  const symbol =
    symbolData.symbol;

  const results = [];

  for (
    const timeframe of
    defaultSettings.timeframes
  ) {
    try {
      const result =
        await analyzeSpotTimeframe(
          symbol,
          timeframe
        );

      if (result) {
        results.push(result);
      }
    } catch (error) {
      console.error(
        "Spot",
        symbol,
        timeframe,
        error.message
      );
    }
  }

  if (!results.length) {
    return null;
  }

  let longCount = 0;
  let shortCount = 0;

  let longTotal = 0;
  let shortTotal = 0;

  for (const r of results) {
    if (r.side === "LONG") {
      longCount++;
      longTotal += r.score;
    } else {
      shortCount++;
      shortTotal += r.score;
    }
  }

  const side =
    longCount >= shortCount
      ? "BUY"
      : "SELL";

  const agreement =
    side === "BUY"
      ? longCount
      : shortCount;

  const averageScore =
    side === "BUY"
      ? longTotal /
        Math.max(longCount, 1)
      : shortTotal /
        Math.max(shortCount, 1);

  const score =
    clamp(
      Math.round(averageScore),
      0,
      100
    );

  // Spot can still produce a signal when 2 of 4 timeframes agree.
  // This avoids an empty Spot screen when the market is mixed while
  // keeping the minimum technical score filter in the scanner.
  if (
    agreement <
    Math.ceil(
      results.length * 0.5
    )
  ) {
    return null;
  }

  const entry =
    symbolData.price;

  const first =
    results[0];

  const atrValue =
    first.atr > 0
      ? first.atr
      : entry * 0.01;

  // Spot uses wider volatility-based levels.
  const stopDistance =
    Math.max(
      atrValue * 1.5,
      entry * 0.003
    );

  let sl;
  let tp1;
  let tp2;
  let tp3;
  let tp4;
  let tp5;

  const tpPercents = [
    defaultSettings.tp1Percent,
    defaultSettings.tp2Percent,
    defaultSettings.tp3Percent,
    defaultSettings.tp4Percent,
    defaultSettings.tp5Percent
  ];

  if (side === "BUY") {
    sl = Math.max(entry - stopDistance, 0);
    tp1 = entry * (1 + tpPercents[0] / 100);
    tp2 = entry * (1 + tpPercents[1] / 100);
    tp3 = entry * (1 + tpPercents[2] / 100);
    tp4 = entry * (1 + tpPercents[3] / 100);
    tp5 = entry * (1 + tpPercents[4] / 100);
  } else {
    sl = entry + stopDistance;
    tp1 = Math.max(entry * (1 - tpPercents[0] / 100), 0);
    tp2 = Math.max(entry * (1 - tpPercents[1] / 100), 0);
    tp3 = Math.max(entry * (1 - tpPercents[2] / 100), 0);
    tp4 = Math.max(entry * (1 - tpPercents[3] / 100), 0);
    tp5 = Math.max(entry * (1 - tpPercents[4] / 100), 0);
  }

  const riskMoney =
    defaultSettings.capital *
    (defaultSettings.riskPercent / 100);

  const stopPercent =
    entry > 0
      ? Math.abs(entry - sl) / entry
      : 0;

  const positionNotional =
    stopPercent > 0
      ? riskMoney / stopPercent
      : 0;

  const reasons =
    results
      .flatMap(r => r.reasons)
      .slice(0, 8);

  return {
    market: "spot",
    symbol,
    side,
    score,

    agreement,
    timeframes:
      results.map(r => r.interval),

    entry,
    sl,

    tp1,
    tp2,
    tp3,
    tp4,
    tp5,

    runner:
      defaultSettings.runnerPercent,

    leverage: 1,
    leverageLabel: "بدون رافعة",

    capital:
      defaultSettings.capital,

    riskPercent:
      defaultSettings.riskPercent,

    riskMoney,
    positionNotional,

    volume:
      symbolData.volume,

    change24h:
      symbolData.change24h,

    reasons,

    generatedAt: now()
  };
}

// ============================================================
// SCAN FUTURES
// ============================================================

async function scanFuturesMarket() {
  if (scanning) {
    return;
  }

  scanning = true;

  try {
    console.log(
      `Starting OKX Futures scan... (${MAX_SYMBOLS} symbols, ultra-safe)`
    );

    const universe =
      await getFuturesUniverse();

    const output = [];

    for (const symbol of universe) {
      try {
        const rec =
          await analyzeFuturesSymbol(
            symbol
          );

        if (
          rec &&
          rec.score >=
            defaultSettings.minScore
        ) {
          output.push(rec);
        }
      } catch (error) {
        console.error(
          "Futures analyze error:",
          symbol.symbol,
          error.message
        );
      }
    }

    output.sort(
      (a, b) =>
        b.score - a.score
    );

    cachedRecommendations =
      output.slice(
        0,
        defaultSettings.maxPositions
      );

    lastScanAt = now();

    console.log(
      `Futures scan complete: ${cachedRecommendations.length} recommendations`
    );
  } catch (error) {
    console.error(
      "OKX FUTURES SCAN ERROR:",
      error.message
    );
  } finally {
    scanning = false;
  }
}

// ============================================================
// SCAN SPOT
// ============================================================

async function scanSpotMarket() {
  if (spotScanning) {
    return;
  }

  spotScanning = true;

  try {
    console.log(
      `Starting OKX Spot scan... (${SPOT_MAX_SYMBOLS} symbols, ultra-safe)`
    );

    const universe =
      await getSpotUniverse();

    const output = [];

    for (const symbol of universe) {
      try {
        const rec =
          await analyzeSpotSymbol(
            symbol
          );

        if (
          rec &&
          rec.score >=
            defaultSettings.minScore
        ) {
          output.push(rec);
        }
      } catch (error) {
        console.error(
          "Spot analyze error:",
          symbol.symbol,
          error.message
        );
      }
    }

    output.sort(
      (a, b) =>
        b.score - a.score
    );

    cachedSpotRecommendations =
      output.slice(
        0,
        defaultSettings.maxPositions
      );

    lastSpotScanAt = now();

    console.log(
      `Spot scan complete: ${cachedSpotRecommendations.length} recommendations`
    );
  } catch (error) {
    console.error(
      "OKX SPOT SCAN ERROR:",
      error.message
    );
  } finally {
    spotScanning = false;
  }
}

// ============================================================
// CUSTOMER RECOMMENDATIONS
// ============================================================

app.get(
  "/api/recommendations",
  requireCustomer,
  async (req, res) => {
    const market =
      String(
        req.query.market || "futures"
      )
        .trim()
        .toLowerCase();

    const requestedSymbol = String(req.query.symbol || "").trim();

    if (!SERVICE_MARKETS.includes(market)) return res.status(403).json({ok:false,error:"MARKET_NOT_AVAILABLE_IN_THIS_SERVICE"});

    if (["stocks", "gold", "silver", "oil", "forex"].includes(market)) {
      try {
        if (requestedSymbol) {
          const rec = await analyzeExternalSymbol(requestedSymbol, market);
          return res.json({ ok:true, market, search:requestedSymbol, recommendations:rec ? [rec] : [], lastScanAt: now() });
        }
        const map = { stocks:[cachedStocksRecommendations,lastStocksScanAt,stocksScanning], gold:[cachedGoldRecommendations,lastGoldScanAt,goldScanning], silver:[cachedSilverRecommendations,lastSilverScanAt,silverScanning], oil:[cachedOilRecommendations,lastOilScanAt,oilScanning], forex:[cachedForexRecommendations,lastForexScanAt,forexScanning] };
        const [items,last,scan] = map[market];
        return res.json({ ok:true, market, recommendations:items, lastScanAt:last, scanning:scan });
      } catch (error) {
        return res.status(400).json({ ok:false, error:error.message });
      }
    }

    if (requestedSymbol) {
      try {
        const symbolData = await getSpecificMarketSymbol(
          requestedSymbol,
          market === "spot" ? "spot" : "futures"
        );

        const rec =
          market === "spot"
            ? await analyzeSpotSymbol(symbolData)
            : await analyzeFuturesSymbol(symbolData);

        return res.json({
          ok: true,
          market,
          search: requestedSymbol,
          recommendations: rec ? [rec] : [],
          lastScanAt: now()
        });
      } catch (error) {
        return res.status(400).json({
          ok: false,
          error: error.message
        });
      }
    }

    if (market === "spot") {
      return res.json({
        ok: true,
        market: "spot",
        recommendations:
          cachedSpotRecommendations,
        lastScanAt: lastSpotScanAt,
        nextScanIn:
          Math.max(
            0,
            SCAN_INTERVAL_MS -
              (now() -
                (lastSpotScanAt || now()))
          )
      });
    }

    res.json({
      ok: true,
      market: "futures",
      recommendations:
        cachedRecommendations,
      lastScanAt,
      nextScanIn:
        Math.max(
          0,
          SCAN_INTERVAL_MS -
            (now() -
              (lastScanAt || now()))
        )
    });
  }
);

// ============================================================
// CUSTOMER MARKET STATUS
// ============================================================

app.get(
  "/api/markets",
  requireCustomer,
  (req, res) => {
    res.json({
      ok: true,
      markets: {
        futures: {
          enabled: APP_MODE === "crypto",
          enabled: true,
          scanning,
          lastScanAt,
          recommendations:
            cachedRecommendations.length
        },
        spot: { enabled: APP_MODE === "crypto", scanning:spotScanning, lastScanAt:lastSpotScanAt, recommendations:cachedSpotRecommendations.length },
        stocks: { enabled: APP_MODE === "markets", scanning:stocksScanning, lastScanAt:lastStocksScanAt, recommendations:cachedStocksRecommendations.length },
        gold: { enabled: APP_MODE === "markets", scanning:goldScanning, lastScanAt:lastGoldScanAt, recommendations:cachedGoldRecommendations.length },
        silver: { enabled: APP_MODE === "markets", scanning:silverScanning, lastScanAt:lastSilverScanAt, recommendations:cachedSilverRecommendations.length },
        oil: { enabled: APP_MODE === "markets", scanning:oilScanning, lastScanAt:lastOilScanAt, recommendations:cachedOilRecommendations.length },
        forex: { enabled: APP_MODE === "markets", scanning:forexScanning, lastScanAt:lastForexScanAt, recommendations:cachedForexRecommendations.length }
      }
    });
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service:
      `Sharks Recommendation (${APP_MODE})`,
    appMode: APP_MODE,
    accessType: SERVICE_ACCESS_TYPE,
    dataSource: APP_MODE === "crypto" ? "OKX" : "Yahoo Finance + Stooq fallback",
    proxy: OKX_PROXY,
    markets: {
      futures: {
        scanning,
        lastScanAt,
        recommendations:
          cachedRecommendations.length
      },
      spot: { scanning:spotScanning, lastScanAt:lastSpotScanAt, recommendations:cachedSpotRecommendations.length },
      stocks: { scanning:stocksScanning, lastScanAt:lastStocksScanAt, recommendations:cachedStocksRecommendations.length },
      gold: { scanning:goldScanning, lastScanAt:lastGoldScanAt, recommendations:cachedGoldRecommendations.length },
      silver: { scanning:silverScanning, lastScanAt:lastSilverScanAt, recommendations:cachedSilverRecommendations.length },
      oil: { scanning:oilScanning, lastScanAt:lastOilScanAt, recommendations:cachedOilRecommendations.length },
      forex: { scanning:forexScanning, lastScanAt:lastForexScanAt, recommendations:cachedForexRecommendations.length }
    }
  });
});

// ============================================================
// ADMIN STATS
// ============================================================

app.get(
  "/api/admin/stats",
  requireAdmin,
  async (req, res) => {
    if (pool && dbReady) {
      await pool.query("UPDATE sharks_access_codes SET status='expired' WHERE status='active' AND expires_at <= NOW()");
      const [codes, payments] = await Promise.all([
        pool.query(`SELECT COUNT(*)::int total,
          COUNT(*) FILTER (WHERE status='active')::int active,
          COUNT(*) FILTER (WHERE status='expired')::int expired,
          COUNT(*) FILTER (WHERE status='revoked')::int revoked FROM sharks_access_codes`),
        pool.query(`SELECT COUNT(*)::int total,
          COUNT(*) FILTER (WHERE status='pending')::int pending,
          COUNT(*) FILTER (WHERE status='approved')::int approved,
          COUNT(*) FILTER (WHERE status='rejected')::int rejected FROM sharks_payment_requests`)
      ]);
      return res.json({ok:true,codes:codes.rows[0],payments:payments.rows[0]});
    }
    let activeCodes = 0;
    let expiredCodes = 0;
    let revokedCodes = 0;

    for (const item of accessCodes.values()) {
      if (
        item.expiresAt &&
        item.expiresAt <= now() &&
        item.status === "active"
      ) {
        item.status = "expired";
      }

      if (item.status === "active") {
        activeCodes++;
      }

      if (item.status === "expired") {
        expiredCodes++;
      }

      if (item.status === "revoked") {
        revokedCodes++;
      }
    }

    const payments =
      Array.from(
        paymentRequests.values()
      );

    res.json({
      ok: true,

      codes: {
        total: accessCodes.size,
        active: activeCodes,
        expired: expiredCodes,
        revoked: revokedCodes
      },

      payments: {
        total: payments.length,
        pending:
          payments.filter(
            x => x.status === "pending"
          ).length,
        approved:
          payments.filter(
            x => x.status === "approved"
          ).length,
        rejected:
          payments.filter(
            x => x.status === "rejected"
          ).length
      }
    });
  }
);

// ============================================================
// FRONTEND FALLBACK
// ============================================================

app.use((req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

// ============================================================
// START
// ============================================================

app.listen(PORT, async () => {
  await initPersistence().catch(error => {
    console.error("PostgreSQL init failed:", error.message);
  });

  console.log(
    `Server running on port ${PORT}`
  );
  console.log(`APP_MODE: ${APP_MODE} (${SERVICE_ACCESS_TYPE})`);

  console.log(
    `OKX market-data base: ${OKX_PROXY}`
  );

  console.log(
    `MAX_SYMBOLS: ${MAX_SYMBOLS} (DYNAMIC TOP UNIVERSE)`
  );

  console.log(
    `SPOT_MAX_SYMBOLS: ${SPOT_MAX_SYMBOLS} (DYNAMIC TOP UNIVERSE)`
  );

  console.log(
    `OKX_REQUEST_GAP_MS: ${OKX_REQUEST_GAP_MS} ms | SCAN_INTERVAL_MS: ${SCAN_INTERVAL_MS} ms`
  );

  if (APP_MODE === "crypto") {
    // Both crypto scanners share the existing OKX limiter.
    scanFuturesMarket();
    setTimeout(scanSpotMarket, 45000);
    setInterval(scanFuturesMarket, SCAN_INTERVAL_MS);
    setInterval(scanSpotMarket, SCAN_INTERVAL_MS);
  } else {
    // Return cached results immediately while these low-frequency refreshes run
    // in the background. Oil is intentionally included as CL=F.
    const external = ["gold", "silver", "oil", "stocks", "forex"];
    external.forEach((market, index) => setTimeout(() => scanExternalMarket(market), 10000 + index * 20000));
    setInterval(() => scanExternalMarket("gold"), Math.max(SCAN_INTERVAL_MS, 240000));
    setInterval(() => scanExternalMarket("silver"), Math.max(SCAN_INTERVAL_MS, 240000));
    setInterval(() => scanExternalMarket("oil"), Math.max(SCAN_INTERVAL_MS, 240000));
    setInterval(() => scanExternalMarket("stocks"), Math.max(SCAN_INTERVAL_MS, 600000));
    setInterval(() => scanExternalMarket("forex"), Math.max(SCAN_INTERVAL_MS, 300000));
  }

});
