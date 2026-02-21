import pg from "pg";
import http from "http";
import { URL } from "url";

const { Pool } = pg;

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

function s(v) {
  return typeof v === "string" ? v.trim() : "";
}

function normalizeText(v) {
  const txt = s(v);
  if (!txt) return "";
  if (txt.toUpperCase() === "N/A") return "";
  return txt;
}

function sleepMs(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function uniq(arr) {
  const out = [];
  const seen = new Set();
  for (const item of arr) {
    const v = s(item);
    if (!v) continue;
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

function extractInsideParens(v) {
  const txt = s(v);
  if (!txt) return "";
  const open = txt.indexOf("(");
  const close = txt.lastIndexOf(")");
  if (open >= 0 && close > open) {
    const inside = txt.slice(open + 1, close).trim();
    return inside || txt;
  }
  return txt;
}

function emailLocalPart(v) {
  const txt = s(v);
  if (!txt) return "";
  const at = txt.indexOf("@");
  if (at > 0) {
    const left = txt.slice(0, at).trim();
    return left || txt;
  }
  return txt;
}

function formatNumber(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "0";
  return new Intl.NumberFormat("en-GB").format(Math.trunc(n));
}

function hoursDecimalToHhMm(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "0h 0m";
  const totalMinutes = Math.round(n * 60);
  const h = Math.floor(totalMinutes / 60);
  const m = totalMinutes % 60;
  return String(h) + "h " + String(m) + "m";
}

function ordinalSuffix(day) {
  const d = Number(day);
  if (!Number.isFinite(d)) return "th";
  const mod100 = d % 100;
  if (mod100 >= 11 && mod100 <= 13) return "th";
  const mod10 = d % 10;
  if (mod10 === 1) return "st";
  if (mod10 === 2) return "nd";
  if (mod10 === 3) return "rd";
  return "th";
}

function toDayMonth(v) {
  if (!v) return "";

  let d;
  if (v instanceof Date) {
    d = v;
  } else {
    const txt = typeof v === "string" ? v.trim() : "";
    if (!txt) return "";
    d = new Date(txt);
  }

  if (Number.isNaN(d.getTime())) return "";

  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const day = d.getUTCDate();
  const month = months[d.getUTCMonth()] || "";
  if (!month) return "";
  return String(day) + ordinalSuffix(day) + " " + month;
}

/**
 * Canonicalise the Respond identifier.
 * Always returns "+<digits>".
 * Examples:
 * "+4474 123 45678" -> "+447412345678"
 * "447412345678" -> "+447412345678"
 */
function normalizePhoneE164(v) {
  const raw = s(v);
  if (!raw) return "";
  const digits = raw.replace(/\D/g, "");
  if (!digits) return "";
  return "+" + digits;
}

function respondHeaders(token) {
  return {
    Accept: "application/json, application/xml, multipart/form-data",
    Authorization: "Bearer " + token,
    "Content-Type": "application/json"
  };
}

async function httpCall(method, url, token, body) {
  const res = await fetch(url, {
    method,
    headers: respondHeaders(token),
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await res.text();
  return { ok: res.ok, status: res.status, text };
}

async function withQueueRetry(fn) {
  const maxAttempts = Number(envOptional("RESPOND_IO_RETRY_MAX", "12"));
  const baseDelay = Number(envOptional("RESPOND_IO_RETRY_BASE_MS", "2000"));
  const maxDelay = Number(envOptional("RESPOND_IO_RETRY_MAX_MS", "30000"));

  let attempt = 0;
  while (true) {
    attempt += 1;

    const r = await fn();
    if (r.ok) return r;

    const isQueue = r.status === 449 && s(r.text).includes("in the queue");
    if (!isQueue) return r;
    if (attempt >= maxAttempts) return r;

    const delay = Math.min(maxDelay, baseDelay * Math.pow(2, attempt - 1));
    log("HTTP 449 queue retry", "delay_ms=" + delay, "attempt=" + attempt + "/" + maxAttempts);
    await sleepMs(delay);
  }
}

function urlWithPhone(template, phoneE164) {
  return template.replace("{identifier}", "phone:" + phoneE164);
}

async function respondCreateOrUpdate(token, phoneE164, firstName, profilePic, customFields) {
  const base = envRequired("RESPOND_IO_CREATE_OR_UPDATE_URL");
  const url = urlWithPhone(base, phoneE164);

  const body = {
    firstName,
    phone: phoneE164,
    custom_fields: customFields
  };

  if (s(profilePic)) body.profilePic = s(profilePic);

  return await withQueueRetry(() => httpCall("POST", url, token, body));
}

async function respondDeleteContact(token, phoneE164) {
  const base = envRequired("RESPOND_IO_DELETE_CONTACT_URL");
  const url = urlWithPhone(base, phoneE164);
  return await httpCall("DELETE", url, token, undefined);
}

async function respondAddTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_ADD_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  const payload = uniq(tags);
  if (payload.length === 0) return { ok: true, status: 200, text: "" };

  for (const part of chunk10(payload)) {
    const r = await withQueueRetry(() => httpCall("POST", url, token, part));
    if (!r.ok) return r;
  }
  return { ok: true, status: 200, text: "" };
}

async function respondDeleteTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_DELETE_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  const payload = uniq(tags);
  if (payload.length === 0) return { ok: true, status: 200, text: "" };

  for (const part of chunk10(payload)) {
    const r = await withQueueRetry(() => httpCall("DELETE", url, token, part));
    if (!r.ok) return r;
  }
  return { ok: true, status: 200, text: "" };
}

async function respondUpdateLifecycle(token, phoneE164, lifecycleName) {
  const base = envRequired("RESPOND_IO_UPDATE_LIFECYCLE_URL");
  const url = urlWithPhone(base, phoneE164);

  const body = { name: s(lifecycleName) ? s(lifecycleName) : "" };
  return await withQueueRetry(() => httpCall("POST", url, token, body));
}

function tierUniverse() {
  const canonical = [
    "Tier 1",
    "Tier 2",
    "Tier 3 (Mature)",
    "Tier 4",
    "Tier 5 (Pre top)",
    "Tier 6",
    "Tier 7",
    "Tier 8 (Top)",
    "Tier 9",
    "Tier 10"
  ];

  const fromEnv = uniq(
    s(envOptional("TIER_TAGS_CSV", ""))
      .split(",")
      .map((x) => s(x))
  );

  return uniq(canonical.concat(fromEnv));
}

function tierRankFromTierTag(tierTag) {
  const t = normalizeText(tierTag);

  if (t.startsWith("Tier 10")) return 10;
  if (t.startsWith("Tier 9")) return 9;
  if (t.startsWith("Tier 8")) return 8;
  if (t.startsWith("Tier 7")) return 7;
  if (t.startsWith("Tier 6")) return 6;
  if (t.startsWith("Tier 5")) return 5;
  if (t.startsWith("Tier 4")) return 4;
  if (t.startsWith("Tier 3")) return 3;
  if (t.startsWith("Tier 2")) return 2;
  return 1;
}

function tierRankFromDiamonds(d) {
  const n = Number(d);
  if (!Number.isFinite(n) || n <= 0) return 1;

  if (n >= 5000000) return 10;
  if (n >= 2500000) return 9;
  if (n >= 1600000) return 8;
  if (n >= 1000000) return 7;
  if (n >= 700000) return 6;
  if (n >= 500000) return 5;
  if (n >= 300000) return 4;
  if (n >= 200000) return 3;
  if (n >= 100000) return 2;
  return 1;
}

function tierStatusFromRanks(prevRank, currentMonthRank) {
  if (currentMonthRank > prevRank) return "Upgrading";
  if (currentMonthRank < prevRank) return "Downgrading";
  return "Retained";
}

const pool = new Pool({
  connectionString: envRequired("DATABASE_URL"),
  ssl: envOptional("DATABASE_SSL", "false") === "true" ? { rejectUnauthorized: false } : undefined
});

async function fetchRows(limit) {
  const client = await pool.connect();
  try {
    const q = `
      SELECT
        user_id,
        phone_e164,
        tiktok_username,
        real_first_name,
        agency_status,
        role_tag,
        group_raw,
        manager_raw,
        tier_tag,
        profile_pic_url,
        stats_as_of,
        diamonds_mtd,
        valid_days_mtd,
        live_duration_mtd_hours,
        lifecycle
      FROM v_respond_sync_users
      ORDER BY user_id
      LIMIT $1
    `;
    const res = await client.query(q, [limit]);
    return res.rows;
  } finally {
    client.release();
  }
}

function dedupeByPhone(rows) {
  const byPhone = new Map();

  for (const r of rows) {
    const phone = normalizePhoneE164(r.phone_e164);
    if (!phone) continue;

    const current = byPhone.get(phone);
    if (!current) byPhone.set(phone, { phone, rows: [r] });
    else current.rows.push(r);
  }

  const out = [];
  for (const entry of byPhone.values()) {
    const anyInAgency = entry.rows.some((x) => s(x.agency_status) === "in_agency");

    if (anyInAgency) {
      const best = entry.rows
        .filter((x) => s(x.agency_status) === "in_agency")
        .sort((a, b) => Number(b.user_id) - Number(a.user_id))[0];

      out.push({ action: "sync", row: best, phone: entry.phone });
    } else {
      const best = entry.rows.sort((a, b) => Number(b.user_id) - Number(a.user_id))[0];
      out.push({ action: "delete", row: best, phone: entry.phone });
    }
  }

  out.sort((a, b) => Number(a.row.user_id) - Number(b.row.user_id));
  return out;
}

async function runSyncOnce() {
  log("RUN start");

  const token = envRequired("RESPOND_IO_TOKEN");
  const limit = Number(envOptional("SYNC_LIMIT", "100000"));
  const paceMs = Number(envOptional("RESPOND_IO_PER_CONTACT_PACE_MS", "900"));

  const allTierTags = tierUniverse();

  const rows = await fetchRows(limit);
  const work = dedupeByPhone(rows);

  let ok = 0;
  let fail = 0;

  for (const item of work) {
    const r = item.row;
    const userId = r.user_id;
    const phone = item.phone;

    try {
      if (item.action === "delete") {
        const del = await respondDeleteContact(token, phone);
        const treatMissingOk = del.status === 400 || del.status === 404;

        if (!del.ok && !treatMissingOk) {
          throw new Error("Delete contact failed HTTP " + del.status + " " + del.text);
        }

        ok += 1;
        log("OK delete", phone);
        await sleepMs(paceMs);
        continue;
      }

      const tiktok = normalizeText(r.tiktok_username);
      const realFirst = normalizeText(r.real_first_name);
      const roleTag = normalizeText(r.role_tag);
      const tierTag = normalizeText(r.tier_tag) || "Tier 1";
      const lifecycle = normalizeText(r.lifecycle);

      const groupValue = extractInsideParens(normalizeText(r.group_raw));
      const managerValue = emailLocalPart(normalizeText(r.manager_raw));

      const diamondsMtdRaw = Number(r.diamonds_mtd);
      const diamondsMtd = formatNumber(r.diamonds_mtd);
      const validDaysMtd = formatNumber(r.valid_days_mtd);
      const liveDurationMtd = hoursDecimalToHhMm(r.live_duration_mtd_hours);
      const statsAsOf = toDayMonth(r.stats_as_of);

      const prevTierRank = tierRankFromTierTag(tierTag);
      const currentMonthTierRank = tierRankFromDiamonds(diamondsMtdRaw);
      const tierStatus = tierStatusFromRanks(prevTierRank, currentMonthTierRank);

      const firstName = tiktok ? tiktok : "user_" + String(userId);

      const customFields = [
        { name: "tiktok_username", value: tiktok || null },
        { name: "real_first_name", value: realFirst || null },
        { name: "group", value: groupValue || null },
        { name: "manager", value: managerValue || null },
        { name: "tier", value: tierTag || null },
        { name: "tier_status", value: tierStatus },
        { name: "diamonds_mtd", value: diamondsMtd },
        { name: "valid_days_mtd", value: validDaysMtd },
        { name: "live_duration_mtd", value: liveDurationMtd },
        { name: "stats_as_of", value: statsAsOf || null },
        { name: "agency_status", value: "in_agency" }
      ];

      const cu = await respondCreateOrUpdate(token, phone, firstName, s(r.profile_pic_url), customFields);
      if (!cu.ok) {
        throw new Error("Create or update failed HTTP " + cu.status + " " + cu.text);
      }

      const roleLegacy = ["role_creator", "role_manager"];
      const roleCanon = ["Creator", "Manager"];
      const roleDelete = uniq(roleLegacy.concat(roleCanon));

      const dr = await respondDeleteTags(token, phone, roleDelete);
      if (!dr.ok) {
        throw new Error("Delete role tags failed HTTP " + dr.status + " " + dr.text);
      }

      if (roleTag) {
        const ar = await respondAddTags(token, phone, [roleTag]);
        if (!ar.ok) {
          throw new Error("Add role tag failed HTTP " + ar.status + " " + ar.text);
        }
      }

      if (allTierTags.length > 0) {
        const dt = await respondDeleteTags(token, phone, allTierTags);
        if (!dt.ok) {
          throw new Error("Delete tier tags failed HTTP " + dt.status + " " + dt.text);
        }

        if (tierTag) {
          const at = await respondAddTags(token, phone, [tierTag]);
          if (!at.ok) {
            throw new Error("Add tier tag failed HTTP " + at.status + " " + at.text);
          }
        }
      }

      const lc = await respondUpdateLifecycle(token, phone, lifecycle);
      if (!lc.ok) {
        throw new Error("Update lifecycle failed HTTP " + lc.status + " " + lc.text);
      }

      ok += 1;
      log("OK sync", phone, "tier=" + tierTag, "tier_status=" + tierStatus, "lifecycle=" + (lifecycle || ""));
    } catch (e) {
      fail += 1;
      log("FAIL", "user_id=" + userId, "phone=" + phone, "err=" + String(e && e.message ? e.message : e));
    }

    await sleepMs(paceMs);
  }

  log("RUN summary", "phones=" + work.length, "ok=" + ok, "fail=" + fail);
  log("RUN end");

  return { phones: work.length, ok, fail };
}

let isRunning = false;
let lastRun = null;

async function runGuarded() {
  if (isRunning) return { status: "already_running", lastRun };
  isRunning = true;
  try {
    const startedAt = nowIso();
    const result = await runSyncOnce();
    lastRun = { startedAt, finishedAt: nowIso(), ...result };
    return { status: "started", lastRun };
  } finally {
    isRunning = false;
  }
}

function readBodyJson(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 1024 * 1024) {
        reject(new Error("Body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (e) {
        reject(new Error("Invalid JSON body"));
      }
    });
  });
}

function sendJson(res, statusCode, obj) {
  const payload = JSON.stringify(obj);
  res.statusCode = statusCode;
  res.setHeader("Content-Type", "application/json");
  res.setHeader("Content-Length", Buffer.byteLength(payload));
  res.end(payload);
}

async function handleRequest(req, res) {
  const urlObj = new URL(req.url || "/", "http://localhost");
  const path = urlObj.pathname || "/";
  const method = (req.method || "GET").toUpperCase();

  if (method === "GET" && path === "/health") {
    return sendJson(res, 200, { ok: true, running: isRunning, lastRun });
  }

  if (method === "POST" && path === "/run") {
    try {
      await readBodyJson(req);
      const out = await runGuarded();
      return sendJson(res, 200, out);
    } catch (e) {
      return sendJson(res, 500, { status: "error", error: String(e && e.message ? e.message : e) });
    }
  }

  return sendJson(res, 404, { ok: false, error: "Not found" });
}

async function main() {
  const port = Number(process.env.PORT || "8080");
  const server = http.createServer((req, res) => {
    handleRequest(req, res).catch((e) => {
      sendJson(res, 500, { status: "error", error: String(e && e.message ? e.message : e) });
    });
  });

  server.listen(port, () => log("Worker API listening", "port=" + port));
}

main().catch((e) => {
  console.error(nowIso(), "FATAL", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
