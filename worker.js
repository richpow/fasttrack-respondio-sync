import pg from "pg";

const { Pool } = pg;

/* ======================
   LOGGING & HELPERS
====================== */

function nowIso() {
  return new Date().toISOString();
}

function log(...args) {
  console.log(nowIso(), ...args);
}

function envRequired(name) {
  const v = process.env[name];
  if (!v) throw new Error("Missing env var: " + name);
  return String(v);
}

function envOptional(name, fallback) {
  const v = process.env[name];
  if (v === undefined || v === null) return fallback;
  return String(v);
}

function hasEnv(name) {
  return Boolean(process.env[name] && String(process.env[name]).trim().length > 0);
}

function s(v) {
  return typeof v === "string" ? v.trim() : "";
}

function sleepMs(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function uniq(arr) {
  return [...new Set(arr.filter(Boolean))];
}

function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

function extractInsideParens(v) {
  const txt = s(v);
  const open = txt.indexOf("(");
  const close = txt.lastIndexOf(")");
  if (open >= 0 && close > open) return txt.slice(open + 1, close).trim();
  return txt;
}

function emailLocalPart(v) {
  const txt = s(v);
  const at = txt.indexOf("@");
  return at > 0 ? txt.slice(0, at) : txt;
}

/* ======================
   FORMATTERS
====================== */

function formatNumber(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "0";
  return new Intl.NumberFormat("en-GB").format(Math.trunc(n));
}

function hoursDecimalToHhMm(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "0h 0m";
  const mins = Math.round(n * 60);
  return Math.floor(mins / 60) + "h " + (mins % 60) + "m";
}

function toDayMonth(v) {
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "";
  return d.getUTCDate() + " " + d.toLocaleString("en-GB", { month: "short", timeZone: "UTC" });
}

/* ======================
   RESPOND.IO HTTP
====================== */

function headers(token) {
  return {
    Authorization: "Bearer " + token,
    Accept: "application/json",
    "Content-Type": "application/json"
  };
}

async function call(method, url, token, body) {
  const res = await fetch(url, {
    method,
    headers: headers(token),
    body: body ? JSON.stringify(body) : undefined
  });
  return { ok: res.ok, status: res.status, text: await res.text() };
}

async function retryQueue(fn) {
  const max = 8;
  let attempt = 0;
  while (true) {
    attempt++;
    const r = await fn();
    if (r.ok || r.status !== 449 || attempt >= max) return r;
    log("HTTP 449 queue retry", "attempt", attempt);
    await sleepMs(2000 * attempt);
  }
}

function urlWithPhone(template, phone) {
  return template.replace("{identifier}", "phone:" + phone);
}

/* ======================
   DB
====================== */

const pool = new Pool({
  connectionString: envRequired("DATABASE_URL"),
  ssl: envOptional("DATABASE_SSL", "false") === "true" ? { rejectUnauthorized: false } : undefined
});

async function fetchUsers(limit) {
  const c = await pool.connect();
  try {
    const q = `
      SELECT
        user_id,
        phone_e164,
        tiktok_username,
        creator_id,
        agency_status,
        role_tag,
        group_raw,
        manager_raw,
        tier_tag,
        profile_pic_url,
        diamonds_mtd,
        valid_days_mtd,
        live_duration_mtd_hours,
        stats_as_of
      FROM v_respond_sync_users
      ORDER BY user_id
      LIMIT $1
    `;
    return (await c.query(q, [limit])).rows;
  } finally {
    c.release();
  }
}

/* ======================
   MAIN
====================== */

async function main() {
  log("BOOT worker start");

  const token = envRequired("RESPOND_IO_TOKEN");

  const rows = await fetchUsers(Number(envOptional("SYNC_LIMIT", "800")));

  for (const r of rows) {
    const phone = s(r.phone_e164);
    if (!phone) continue;

    try {
      if (r.agency_status === "left_agency") {
        await call(
          "DELETE",
          urlWithPhone(envRequired("RESPOND_IO_DELETE_CONTACT_URL"), phone),
          token
        );
        log("OK delete", phone);
        continue;
      }

      const custom_fields = [
        { name: "group", value: extractInsideParens(r.group_raw) },
        { name: "manager", value: emailLocalPart(r.manager_raw) },
        { name: "diamonds_mtd", value: formatNumber(r.diamonds_mtd) },
        { name: "valid_days_mtd", value: formatNumber(r.valid_days_mtd) },
        { name: "live_duration_mtd", value: hoursDecimalToHhMm(r.live_duration_mtd_hours) },
        { name: "stats_as_of", value: toDayMonth(r.stats_as_of) }
      ];

      const cu = await retryQueue(() =>
        call(
          "POST",
          urlWithPhone(envRequired("RESPOND_IO_CREATE_OR_UPDATE_URL"), phone),
          token,
          {
            firstName: r.tiktok_username || "creator",
            phone,
            profilePic: r.profile_pic_url,
            custom_fields
          }
        )
      );

      if (!cu.ok) throw new Error(cu.text);

      await retryQueue(() =>
        call(
          "DELETE",
          urlWithPhone(envRequired("RESPOND_IO_DELETE_TAGS_URL"), phone),
          token,
          ["role_creator", "Creator", "role_manager", "Manager"]
        )
      );

      if (r.role_tag) {
        await retryQueue(() =>
          call(
            "POST",
            urlWithPhone(envRequired("RESPOND_IO_ADD_TAGS_URL"), phone),
            token,
            [r.role_tag]
          )
        );
      }

      log("OK sync", phone);
      await sleepMs(600);
    } catch (e) {
      log("FAIL", phone, String(e));
    }
  }

  await pool.end();
  log("Worker completed");
}

main().catch((e) => {
  console.error(nowIso(), "FATAL", e.stack || e);
  process.exit(1);
});
