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

function toDayMonth(v) {
  const txt = s(v);
  if (!txt) return "";
  const d = new Date(txt);
  if (Number.isNaN(d.getTime())) return "";
  const day = d.getUTCDate();
  const month = d.toLocaleString("en-GB", { month: "short", timeZone: "UTC" });
  return String(day) + " " + month;
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

const pool = new Pool({
  connectionString: envRequired("DATABASE_URL"),
  ssl: envOptional("DATABASE_SSL", "false") === "true" ? { rejectUnauthorized: false } : undefined
});

function tierUniverseHardcoded() {
  return [
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
}

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
        tier,
        profile_pic_url,
        stats_as_of,
        diamonds_mtd,
        valid_days_mtd,
        live_duration_mtd_hours
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
    const phone = s(r.phone_e164);
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

function buildUniversesFromRows(rows) {
  const groupTags = [];
  const managerTags = [];

  for (const r of rows) {
    const g = extractInsideParens(normalizeText(r.group_raw));
    const m = emailLocalPart(normalizeText(r.manager_raw));
    if (g) groupTags.push("Group " + g);
    if (m) managerTags.push("Manager " + m);
  }

  return {
    groupTagUniverse: uniq(groupTags),
    managerTagUniverse: uniq(managerTags),
    tierTagUniverse: tierUniverseHardcoded()
  };
}

async function main() {
  log("BOOT worker start");

  const token = envRequired("RESPOND_IO_TOKEN");
  const limit = Number(envOptional("SYNC_LIMIT", "100000"));
  const paceMs = Number(envOptional("RESPOND_IO_PER_CONTACT_PACE_MS", "900"));

  const rows = await fetchRows(limit);
  const work = dedupeByPhone(rows);

  const universes = buildUniversesFromRows(rows);

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

      const groupValue = extractInsideParens(normalizeText(r.group_raw));
      const managerValue = emailLocalPart(normalizeText(r.manager_raw));

      const diamondsMtd = formatNumber(r.diamonds_mtd);
      const validDaysMtd = formatNumber(r.valid_days_mtd);
      const liveDurationMtd = hoursDecimalToHhMm(r.live_duration_mtd_hours);
      const statsAsOf = toDayMonth(r.stats_as_of);

      const tierTag = normalizeText(r.tier_tag);
      const tierField = normalizeText(r.tier) || tierTag;

      const firstName = tiktok ? tiktok : "user_" + String(userId);

      const customFields = [
        { name: "tiktok_username", value: tiktok || null },
        { name: "real_first_name", value: realFirst || null },
        { name: "group", value: groupValue || null },
        { name: "manager", value: managerValue || null },
        { name: "tier", value: tierField || null },
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

      const dt = await respondDeleteTags(token, phone, universes.tierTagUniverse);
      if (!dt.ok) {
        throw new Error("Delete tier tags failed HTTP " + dt.status + " " + dt.text);
      }

      if (tierTag) {
        const at = await respondAddTags(token, phone, [tierTag]);
        if (!at.ok) {
          throw new Error("Add tier tag failed HTTP " + at.status + " " + at.text);
        }
      }

      const dg = await respondDeleteTags(token, phone, universes.groupTagUniverse);
      if (!dg.ok) {
        throw new Error("Delete group tags failed HTTP " + dg.status + " " + dg.text);
      }

      if (groupValue) {
        const ag = await respondAddTags(token, phone, ["Group " + groupValue]);
        if (!ag.ok) {
          throw new Error("Add group tag failed HTTP " + ag.status + " " + ag.text);
        }
      }

      const dm = await respondDeleteTags(token, phone, universes.managerTagUniverse);
      if (!dm.ok) {
        throw new Error("Delete manager tags failed HTTP " + dm.status + " " + dm.text);
      }

      if (managerValue) {
        const am = await respondAddTags(token, phone, ["Manager " + managerValue]);
        if (!am.ok) {
          throw new Error("Add manager tag failed HTTP " + am.status + " " + am.text);
        }
      }

      ok += 1;
      log("OK sync", phone);
    } catch (e) {
      fail += 1;
      log("FAIL", "user_id=" + userId, "phone=" + phone, "err=" + String(e && e.message ? e.message : e));
    }

    await sleepMs(paceMs);
  }

  log("Summary", "phones=" + work.length, "ok=" + ok, "fail=" + fail);
  await pool.end();
  log("Worker completed");
}

main().catch((e) => {
  console.error(nowIso(), "FATAL", e && e.stack ? e.stack : String(e));
  process.exit(1);
});
