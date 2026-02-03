import pg from "pg";

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

function hasEnv(name) {
  return Boolean(process.env[name] && String(process.env[name]).trim().length > 0);
}

function isNonEmptyString(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function s(v) {
  return isNonEmptyString(v) ? v.trim() : "";
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function parseCsv(v) {
  const txt = s(v);
  if (!txt) return [];
  return uniq(txt.split(",").map((x) => s(x)).filter((x) => x.length > 0));
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

let shuttingDown = false;

process.on("SIGTERM", () => {
  shuttingDown = true;
  log("SIGTERM received");
});

process.on("SIGINT", () => {
  shuttingDown = true;
  log("SIGINT received");
});

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
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }

  return { ok: res.ok, status: res.status, text, json };
}

async function withQueueRetry(fn) {
  const maxAttempts = Number(envOptional("RESPOND_IO_RETRY_MAX", "8"));
  const baseDelay = Number(envOptional("RESPOND_IO_RETRY_BASE_MS", "2000"));
  const maxDelay = Number(envOptional("RESPOND_IO_RETRY_MAX_MS", "30000"));

  let attempt = 0;

  while (true) {
    attempt += 1;
    const r = await fn();
    if (r.ok) return r;

    const isQueue = r.status === 449 && isNonEmptyString(r.text) && r.text.includes("in the queue");
    if (!isQueue) return r;
    if (attempt >= maxAttempts) return r;

    const delay = Math.min(maxDelay, baseDelay * Math.pow(2, attempt - 1));
    log("HTTP 449 queue retry", "delay_ms=" + delay, "attempt=" + attempt + "/" + maxAttempts);
    await sleepMs(delay);

    if (shuttingDown) return r;
  }
}

function urlWithPhone(template, phoneE164) {
  const identifier = "phone:" + phoneE164;
  return template.replace("{identifier}", identifier);
}

async function respondCreateOrUpdate(token, phoneE164, firstName, profilePic, customFields) {
  const base = envRequired("RESPOND_IO_CREATE_OR_UPDATE_URL");
  const url = urlWithPhone(base, phoneE164);

  const body = {
    firstName: firstName,
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
  if (payload.length === 0) return { ok: true, status: 200, text: "", json: {} };

  for (const part of chunk10(payload)) {
    const r = await withQueueRetry(() => httpCall("POST", url, token, part));
    if (!r.ok) return r;
  }
  return { ok: true, status: 200, text: "", json: {} };
}

async function respondDeleteTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_DELETE_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  const payload = uniq(tags);
  if (payload.length === 0) return { ok: true, status: 200, text: "", json: {} };

  for (const part of chunk10(payload)) {
    const r = await withQueueRetry(() => httpCall("DELETE", url, token, part));
    if (!r.ok) return r;
  }
  return { ok: true, status: 200, text: "", json: {} };
}

async function fetchWork(pool, limit) {
  const client = await pool.connect();
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
        profile_pic_url
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

async function main() {
  log("BOOT worker.js reached");

  log(
    "ENV presence",
    "DATABASE_URL=" + hasEnv("DATABASE_URL"),
    "DATABASE_SSL=" + hasEnv("DATABASE_SSL"),
    "RESPOND_IO_TOKEN=" + hasEnv("RESPOND_IO_TOKEN"),
    "RESPOND_IO_CREATE_OR_UPDATE_URL=" + hasEnv("RESPOND_IO_CREATE_OR_UPDATE_URL"),
    "RESPOND_IO_ADD_TAGS_URL=" + hasEnv("RESPOND_IO_ADD_TAGS_URL"),
    "RESPOND_IO_DELETE_TAGS_URL=" + hasEnv("RESPOND_IO_DELETE_TAGS_URL"),
    "RESPOND_IO_DELETE_CONTACT_URL=" + hasEnv("RESPOND_IO_DELETE_CONTACT_URL")
  );

  const dbUrl = envRequired("DATABASE_URL");
  const dbSsl = envOptional("DATABASE_SSL", "false") === "true";

  const token = envRequired("RESPOND_IO_TOKEN");

  const limit = Number(envOptional("SYNC_LIMIT", "800"));
  const paceMs = Number(envOptional("RESPOND_IO_PER_CONTACT_PACE_MS", "600"));
  const tierUniverse = parseCsv(envOptional("TIER_TAGS_CSV", ""));

  const pool = new Pool({
    connectionString: dbUrl,
    ssl: dbSsl ? { rejectUnauthorized: false } : undefined
  });

  let total = 0;
  let ok = 0;
  let fail = 0;
  let synced = 0;
  let deleted = 0;

  log("Worker start", "limit=" + limit, "pace_ms=" + paceMs);

  const rows = await fetchWork(pool, limit);
  log("Fetched rows", "count=" + rows.length);

  for (const r of rows) {
    if (shuttingDown) break;

    total += 1;

    const userId = r.user_id;
    const phone = s(r.phone_e164);

    try {
      if (!phone) {
        ok += 1;
        continue;
      }

      const agencyStatus = s(r.agency_status);

      if (agencyStatus === "left_agency") {
        const del = await respondDeleteContact(token, phone);
        const treatMissingOk = del.status === 400 || del.status === 404;

        if (!del.ok && !treatMissingOk) {
          throw new Error("Delete contact failed HTTP " + del.status + " " + del.text);
        }

        deleted += 1;
        ok += 1;
        log("OK delete", "user_id=" + userId, "phone=" + phone);
        await sleepMs(paceMs);
        continue;
      }

      const tiktok = s(r.tiktok_username);
      const creatorId = s(r.creator_id);
      const roleTag = s(r.role_tag);
      const tierTag = s(r.tier_tag);

      const groupValue = extractInsideParens(s(r.group_raw));
      const managerValue = emailLocalPart(s(r.manager_raw));

      const firstName = tiktok ? tiktok : "user_" + String(userId);

      const customFields = [
        { name: "neon_user_id", value: String(userId) },
        { name: "creator_id", value: creatorId || null },
        { name: "tiktok_username", value: tiktok || null },
        { name: "agency_status", value: agencyStatus || null },
        { name: "Group", value: groupValue || null },
        { name: "Manager", value: managerValue || null }
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

      if (tierUniverse.length > 0) {
        const dt = await respondDeleteTags(token, phone, tierUniverse);
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

      synced += 1;
      ok += 1;

      log(
        "OK sync",
        "user_id=" + userId,
        "phone=" + phone,
        "group=" + groupValue,
        "manager=" + managerValue
      );
    } catch (e) {
      fail += 1;
      log("FAIL", "user_id=" + userId, "phone=" + phone, "err=" + String(e && e.message ? e.message : e));
    }

    await sleepMs(paceMs);
  }

  log(
    "Summary",
    "total=" + total,
    "ok=" + ok,
    "fail=" + fail,
    "synced=" + synced,
    "deleted=" + deleted
  );

  await pool.end();
  log("Worker completed");
}

main().catch((e) => {
  console.error(nowIso(), "FATAL", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
