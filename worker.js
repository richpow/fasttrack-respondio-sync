import pg from "pg";

const { Pool } = pg;

function env_required(name) {
  const v = process.env[name];
  if (!v) throw new Error("Missing env var: " + name);
  return String(v);
}

function env_optional(name, fallback_value) {
  const v = process.env[name];
  if (v === undefined || v === null) return fallback_value;
  return String(v);
}

function is_non_empty_string(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function s(v) {
  return is_non_empty_string(v) ? v.trim() : "";
}

function sleep_ms(ms) {
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

function split_csv(v) {
  const txt = s(v);
  if (!txt) return [];
  return uniq(txt.split(",").map((x) => s(x)).filter((x) => x.length > 0));
}

function chunk_10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

function extract_inside_parens(v) {
  const txt = s(v);
  if (!txt) return "";
  const open_i = txt.indexOf("(");
  const close_i = txt.lastIndexOf(")");
  if (open_i >= 0 && close_i > open_i) {
    const inside = txt.slice(open_i + 1, close_i).trim();
    return inside || txt;
  }
  return txt;
}

function email_local_part(v) {
  const txt = s(v);
  if (!txt) return "";
  const at_i = txt.indexOf("@");
  if (at_i > 0) {
    const left = txt.slice(0, at_i).trim();
    return left || txt;
  }
  return txt;
}

const dash = String.fromCharCode(45);
const accept_json = "application/json";
const accept_multi = "application/json, application/xml, multipart/form" + dash + "data";
const header_auth = "Authorization";
const header_ct = "Content" + dash + "Type";

let shutting_down = false;

process.on("SIGTERM", () => {
  shutting_down = true;
  console.log("SIGTERM received");
});

process.on("SIGINT", () => {
  shutting_down = true;
  console.log("SIGINT received");
});

function respond_headers(accept_value) {
  const h = {};
  h.Accept = accept_value || accept_json;
  h[header_auth] = "Bearer " + env_required("RESPOND_IO_TOKEN");
  h[header_ct] = accept_json;
  return h;
}

async function http_call(method, url, body, accept_value) {
  const res = await fetch(url, {
    method,
    headers: respond_headers(accept_value),
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

async function with_queue_retry(fn) {
  const max_attempts = Number(env_optional("RESPOND_IO_RETRY_MAX", "8"));
  const base_delay_ms = Number(env_optional("RESPOND_IO_RETRY_BASE_MS", "2000"));
  const max_delay_ms = Number(env_optional("RESPOND_IO_RETRY_MAX_MS", "30000"));

  let attempt = 0;

  while (true) {
    attempt += 1;

    const r = await fn();
    if (r.ok) return r;

    const is_queue =
      r.status === 449 &&
      is_non_empty_string(r.text) &&
      r.text.includes("in the queue");

    if (!is_queue) return r;
    if (attempt >= max_attempts) return r;

    const delay = Math.min(max_delay_ms, base_delay_ms * Math.pow(2, attempt - 1));
    console.log("HTTP 449 queue, retry in ms: " + delay + ", attempt " + attempt + " of " + max_attempts);
    await sleep_ms(delay);

    if (shutting_down) return r;
  }
}

function url_with_identifier(template_env, phone_e164) {
  const base = env_required(template_env);
  const identifier = "phone:" + phone_e164;
  return base.replace("{identifier}", identifier);
}

async function respond_create_or_update(phone_e164, first_name, profile_pic, custom_fields) {
  const url = url_with_identifier("RESPOND_IO_CREATE_OR_UPDATE_URL", phone_e164);

  const body = {
    firstName: first_name,
    phone: phone_e164,
    custom_fields: custom_fields
  };

  if (s(profile_pic)) body.profilePic = s(profile_pic);

  return await with_queue_retry(() => http_call("POST", url, body, accept_multi));
}

async function respond_delete_contact(phone_e164) {
  const url = url_with_identifier("RESPOND_IO_DELETE_CONTACT_URL", phone_e164);
  return await http_call("DELETE", url, undefined, accept_json);
}

async function respond_add_tags(phone_e164, tags) {
  const url = url_with_identifier("RESPOND_IO_ADD_TAGS_URL", phone_e164);
  const payload = uniq(tags);

  if (payload.length === 0) return { ok: true, status: 200, text: "", json: {} };

  for (const part of chunk_10(payload)) {
    const r = await with_queue_retry(() => http_call("POST", url, part, accept_multi));
    if (!r.ok) return r;
  }
  return { ok: true, status: 200, text: "", json: {} };
}

async function respond_delete_tags(phone_e164, tags) {
  const url = url_with_identifier("RESPOND_IO_DELETE_TAGS_URL", phone_e164);
  const payload = uniq(tags);

  if (payload.length === 0) return { ok: true, status: 200, text: "", json: {} };

  for (const part of chunk_10(payload)) {
    const r = await with_queue_retry(() => http_call("DELETE", url, part, accept_multi));
    if (!r.ok) return r;
  }
  return { ok: true, status: 200, text: "", json: {} };
}

const pool = new Pool({
  connectionString: env_required("DATABASE_URL"),
  ssl: env_optional("DATABASE_SSL", "false") === "true" ? { rejectUnauthorized: false } : undefined
});

async function fetch_rows(limit) {
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
  console.log("BOOT worker.js");
  console.log("Worker start");

  const limit = Number(env_optional("SYNC_LIMIT", "800"));
  const pace_ms = Number(env_optional("RESPOND_IO_PER_CONTACT_PACE_MS", "600"));
  const tier_universe = split_csv(env_optional("TIER_TAGS_CSV", ""));

  let total = 0;
  let ok = 0;
  let fail = 0;
  let synced = 0;
  let deleted = 0;

  const rows = await fetch_rows(limit);

  for (const r of rows) {
    if (shutting_down) break;

    total += 1;

    const user_id = r.user_id;
    const phone_e164 = s(r.phone_e164);

    try {
      if (!phone_e164) {
        ok += 1;
        continue;
      }

      const agency_status = s(r.agency_status);

      if (agency_status === "left_agency") {
        const del = await respond_delete_contact(phone_e164);

        const treat_missing_ok = del.status === 400 || del.status === 404;
        if (!del.ok && !treat_missing_ok) {
          throw new Error("Delete contact failed HTTP " + del.status + " " + del.text);
        }

        deleted += 1;
        ok += 1;
        console.log("OK delete user_id=" + user_id + " phone=" + phone_e164);
        await sleep_ms(pace_ms);
        continue;
      }

      const tiktok_username = s(r.tiktok_username);
      const creator_id = s(r.creator_id);

      const role_tag = s(r.role_tag);
      const tier_tag = s(r.tier_tag);

      const group_value = extract_inside_parens(s(r.group_raw));
      const manager_value = email_local_part(s(r.manager_raw));

      const first_name = tiktok_username ? tiktok_username : "user_" + String(user_id);

      const custom_fields = [
        { name: "neon_user_id", value: String(user_id) },
        { name: "creator_id", value: creator_id || null },
        { name: "tiktok_username", value: tiktok_username || null },
        { name: "agency_status", value: agency_status || null },
        { name: "Group", value: group_value || null },
        { name: "Manager", value: manager_value || null }
      ];

      const cu = await respond_create_or_update(
        phone_e164,
        first_name,
        s(r.profile_pic_url),
        custom_fields
      );

      if (!cu.ok) {
        throw new Error("Create or update failed HTTP " + cu.status + " " + cu.text);
      }

      const role_legacy = ["role_creator", "role_manager"];
      const role_canon = ["Creator", "Manager"];
      const role_delete = uniq(role_legacy.concat(role_canon));

      const dr = await respond_delete_tags(phone_e164, role_delete);
      if (!dr.ok) {
        throw new Error("Delete role tags failed HTTP " + dr.status + " " + dr.text);
      }

      if (role_tag) {
        const ar = await respond_add_tags(phone_e164, [role_tag]);
        if (!ar.ok) {
          throw new Error("Add role tag failed HTTP " + ar.status + " " + ar.text);
        }
      }

      if (tier_universe.length > 0) {
        const dt = await respond_delete_tags(phone_e164, tier_universe);
        if (!dt.ok) {
          throw new Error("Delete tier tags failed HTTP " + dt.status + " " + dt.text);
        }

        if (tier_tag) {
          const at = await respond_add_tags(phone_e164, [tier_tag]);
          if (!at.ok) {
            throw new Error("Add tier tag failed HTTP " + at.status + " " + at.text);
          }
        }
      }

      synced += 1;
      ok += 1;

      console.log(
        "OK sync user_id=" + user_id +
        " phone=" + phone_e164 +
        " group=" + group_value +
        " manager=" + manager_value
      );
    } catch (e) {
      fail += 1;
      console.log(
        "FAIL user_id=" + user_id +
        " phone=" + phone_e164 +
        " err=" + String(e && e.message ? e.message : e)
      );
    }

    await sleep_ms(pace_ms);
  }

  console.log(
    "Summary total=" + total +
    " ok=" + ok +
    " fail=" + fail +
    " synced=" + synced +
    " deleted=" + deleted
  );

  await pool.end();
  console.log("Worker completed");
}

main().catch(async (e) => {
  console.error("FATAL " + String(e && e.message ? e.message : e));
  try {
    await pool.end();
  } catch {
  }
  process.exitCode = 1;
});
