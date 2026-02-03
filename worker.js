import fetch from "node-fetch";
import pg from "pg";

const { Pool } = pg;

function env(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function envOptional(name) {
  const v = process.env[name];
  return v === undefined ? "" : String(v);
}

function isNonEmptyString(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function safeString(v) {
  return isNonEmptyString(v) ? v.trim() : "";
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function uniqueStrings(arr) {
  const out = [];
  const seen = new Set();
  for (const x of arr) {
    if (!isNonEmptyString(x)) continue;
    const v = x.trim();
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function parseCsvTags(v) {
  const s = safeString(v);
  if (!s) return [];
  return uniqueStrings(
    s.split(",").map((x) => x.trim()).filter((x) => x.length > 0)
  );
}

function extractBracketValue(s) {
  const v = safeString(s);
  if (!v) return "";

  const open = v.indexOf("(");
  const close = v.lastIndexOf(")");

  if (open >= 0 && close > open) {
    const inside = v.slice(open + 1, close).trim();
    return inside || v;
  }

  return v;
}

function emailLocalPart(s) {
  const v = safeString(s);
  if (!v) return "";

  const at = v.indexOf("@");
  if (at > 0) return v.slice(0, at).trim() || v;

  return v;
}

function normalizeGroupField(row) {
  const raw = safeString(row.group_value) || safeString(row.group_tag) || "";
  return extractBracketValue(raw);
}

function normalizeManagerField(row) {
  const raw = safeString(row.manager_value) || safeString(row.creator_network_manager_tag) || "";
  return emailLocalPart(raw);
}

let shuttingDown = false;

process.on("SIGTERM", () => {
  shuttingDown = true;
  console.log("SIGTERM received, will stop after current item");
});

process.on("SIGINT", () => {
  shuttingDown = true;
  console.log("SIGINT received, will stop after current item");
});

const pool = new Pool({
  connectionString: env("DATABASE_URL"),
  ssl: process.env.DATABASE_SSL === "true" ? { rejectUnauthorized: false } : undefined
});

function respondHeaders(acceptOverride) {
  return {
    Accept: acceptOverride || "application/json",
    Authorization: `Bearer ${env("RESPOND_IO_TOKEN")}`,
    "Content-Type": "application/json"
  };
}

async function http(method, url, body, acceptOverride) {
  const res = await fetch(url, {
    method,
    headers: respondHeaders(acceptOverride),
    body: body !== undefined ? JSON.stringify(body) : undefined
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
  const maxAttempts = Number(envOptional("RESPOND_IO_RETRY_MAX") || "8");
  const baseDelayMs = Number(envOptional("RESPOND_IO_RETRY_BASE_MS") || "2000");
  const maxDelayMs = Number(envOptional("RESPOND_IO_RETRY_MAX_MS") || "30000");

  let attempt = 0;
  while (true) {
    attempt += 1;
    const res = await fn();

    if (res.ok) return res;

    const isQueue =
      res.status === 449 &&
      isNonEmptyString(res.text) &&
      res.text.includes("in the queue");

    if (!isQueue) return res;
    if (attempt >= maxAttempts) return res;

    const delay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt - 1));
    console.log(`HTTP 449 queue, retrying in ${delay}ms (attempt ${attempt}/${maxAttempts})`);
    await sleep(delay);

    if (shuttingDown) return res;
  }
}

function urlWithPhone(templateEnvVar, phoneE164) {
  const base = env(templateEnvVar);
  const identifier = `phone:${phoneE164}`;
  return base.replace("{identifier}", identifier);
}

function buildContactBody({ phoneE164, firstName, profilePicUrl, customFields }) {
  const body = {
    firstName,
    phone: phoneE164,
    custom_fields: customFields
  };

  if (isNonEmptyString(profilePicUrl)) {
    body.profilePic = profilePicUrl.trim();
  }

  return body;
}

function contactIdFromJson(json) {
  if (!json) return 0;
  const v = json.contactId;
  if (typeof v === "number") return v;
  if (typeof v === "string" && v.trim() !== "" && !Number.isNaN(Number(v))) return Number(v);
  return 0;
}

async function createOrUpdateContact({ phoneE164, firstName, profilePicUrl, customFields }) {
  const url = urlWithPhone("RESPOND_IO_CREATE_OR_UPDATE_URL", phoneE164);
  const body = buildContactBody({ phoneE164, firstName, profilePicUrl, customFields });

  const res = await withQueueRetry(() =>
    http("POST", url, body, "application/json, application/xml, multipart/form-data")
  );

  return res;
}

async function addTags({ phoneE164, tags }) {
  const url = urlWithPhone("RESPOND_IO_ADD_TAGS_URL", phoneE164);
  const payload = uniqueStrings(tags);

  if (payload.length < 1) return { ok: true, status: 200, text: "", json: { contactId: 0 } };
  if (payload.length > 10) return { ok: false, status: 400, text: `Too many tags: ${payload.length}`, json: null };

  const res = await withQueueRetry(() =>
    http("POST", url, payload, "application/json, application/xml, multipart/form-data")
  );

  return res;
}

async function deleteTags({ phoneE164, tags }) {
  const url = urlWithPhone("RESPOND_IO_DELETE_TAGS_URL", phoneE164);
  const payload = uniqueStrings(tags);

  if (payload.length < 1) return { ok: true, status: 200, text: "", json: { contactId: 0 } };
  if (payload.length > 10) return { ok: false, status: 400, text: `Too many tags: ${payload.length}`, json: null };

  const res = await withQueueRetry(() =>
    http("DELETE", url, payload, "application/json, application/xml, multipart/form-data")
  );

  return res;
}

async function deleteContact({ phoneE164 }) {
  const url = urlWithPhone("RESPOND_IO_DELETE_CONTACT_URL", phoneE164);
  const res = await http("DELETE", url, undefined, "application/json");
  return res;
}

async function fetchWork(limit) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT
        v.user_id,
        v.phone_e164,
        v.tiktok_username,
        v.creator_id,
        v.agency_status,
        v.profile_pic_url,
        v.role_tag,
        v.group_tag,
        v.creator_network_manager_tag,
        v.tier_tag,
        v.group_value,
        v.manager_value,
        ss.last_role_tag,
        ss.last_group_tag,
        ss.last_creator_network_manager_tag,
        ss.last_tier_tag,
        ss.last_profile_pic_url,
        ss.last_first_name,
        ss.last_creator_id,
        ss.last_tiktok_username,
        ss.last_agency_status
      FROM v_respond_sync_users v
      LEFT JOIN respond_sync_state ss ON ss.user_id = v.user_id
      WHERE v.phone_e164 IS NOT NULL
        AND v.phone_e164 <> ''
      ORDER BY v.user_id
      LIMIT $1
      `,
      [limit]
    );
    return rows;
  } finally {
    client.release();
  }
}

async function upsertRespondContacts(userId, phoneE164, respondContactId) {
  const client = await pool.connect();
  try {
    await client.query(
      `
      INSERT INTO respond_contacts (user_id, respond_contact_id, phone_e164, created_at, updated_at)
      VALUES ($1, $2, $3, now(), now())
      ON CONFLICT (user_id)
      DO UPDATE SET
        respond_contact_id = EXCLUDED.respond_contact_id,
        phone_e164 = EXCLUDED.phone_e164,
        updated_at = now()
      `,
      [userId, Number(respondContactId || 0), phoneE164]
    );
  } finally {
    client.release();
  }
}

async function upsertRespondState(userId, state) {
  const client = await pool.connect();
  try {
    await client.query(
      `
      INSERT INTO respond_sync_state (
        user_id,
        last_role_tag,
        last_group_tag,
        last_creator_network_manager_tag,
        last_tier_tag,
        last_phone_e164,
        last_profile_pic_url,
        last_first_name,
        last_creator_id,
        last_tiktok_username,
        last_agency_status,
        updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
      ON CONFLICT (user_id)
      DO UPDATE SET
        last_role_tag = EXCLUDED.last_role_tag,
        last_group_tag = EXCLUDED.last_group_tag,
        last_creator_network_manager_tag = EXCLUDED.last_creator_network_manager_tag,
        last_tier_tag = EXCLUDED.last_tier_tag,
        last_phone_e164 = EXCLUDED.last_phone_e164,
        last_profile_pic_url = EXCLUDED.last_profile_pic_url,
        last_first_name = EXCLUDED.last_first_name,
        last_creator_id = EXCLUDED.last_creator_id,
        last_tiktok_username = EXCLUDED.last_tiktok_username,
        last_agency_status = EXCLUDED.last_agency_status,
        updated_at = now()
      `,
      [
        userId,
        state.last_role_tag,
        state.last_group_tag,
        state.last_creator_network_manager_tag,
        state.last_tier_tag,
        state.last_phone_e164,
        state.last_profile_pic_url,
        state.last_first_name,
        state.last_creator_id,
        state.last_tiktok_username,
        state.last_agency_status
      ]
    );
  } finally {
    client.release();
  }
}

function desiredTagsFromRow(row) {
  return uniqueStrings([
    safeString(row.role_tag),
    safeString(row.group_tag),
    safeString(row.creator_network_manager_tag),
    safeString(row.tier_tag)
  ]);
}

async function main() {
  console.log("Worker start");

  const limit = Number(envOptional("SYNC_LIMIT") || "600");
  const perContactPaceMs = Number(envOptional("RESPOND_IO_PER_CONTACT_PACE_MS") || "400");
  const tierTagUniverse = parseCsvTags(envOptional("TIER_TAGS_CSV"));

  let ok = 0;
  let fail = 0;
  let updated = 0;
  let deleted = 0;

  const rows = await fetchWork(limit);

  for (const r of rows) {
    if (shuttingDown) break;

    const userId = r.user_id;
    const phoneE164 = r.phone_e164;

    try {
      const agencyStatus = safeString(r.agency_status);

      if (agencyStatus === "left_agency") {
        const del = await deleteContact({ phoneE164 });
        const treatNotFoundAsOk = del.status === 400 || del.status === 404;

        if (!del.ok && !treatNotFoundAsOk) {
          throw new Error(`Delete contact failed HTTP ${del.status} ${del.text}`);
        }

        await upsertRespondState(userId, {
          last_role_tag: null,
          last_group_tag: null,
          last_creator_network_manager_tag: null,
          last_tier_tag: null,
          last_phone_e164: phoneE164,
          last_profile_pic_url: null,
          last_first_name: null,
          last_creator_id: null,
          last_tiktok_username: null,
          last_agency_status: agencyStatus
        });

        deleted += 1;
        ok += 1;
        console.log(`OK delete user_id=${userId} phone=${phoneE164}`);
        await sleep(perContactPaceMs);
        continue;
      }

      const firstName = safeString(r.tiktok_username) || `user_${userId}`;
      const profilePicUrl = safeString(r.profile_pic_url);

      const groupFieldValue = normalizeGroupField(r);
      const managerFieldValue = normalizeManagerField(r);

      const customFields = [
        { name: "neon_user_id", value: String(userId) },
        { name: "creator_id", value: safeString(r.creator_id) || null },
        { name: "tiktok_username", value: safeString(r.tiktok_username) || null },
        { name: "agency_status", value: agencyStatus || null },
        { name: "Group", value: groupFieldValue || null },
        { name: "Manager", value: managerFieldValue || null }
      ];

      const cu = await createOrUpdateContact({ phoneE164, firstName, profilePicUrl, customFields });
      if (!cu.ok) {
        throw new Error(`Create or update failed HTTP ${cu.status} ${cu.text}`);
      }

      const contactId = contactIdFromJson(cu.json);
      if (contactId) {
        await upsertRespondContacts(userId, phoneE164, contactId);
      }

      const roleDesired = safeString(r.role_tag);
      const tierDesired = safeString(r.tier_tag);

      const legacyRoleTags = ["role_creator", "role_manager"];
      const roleUniverse = ["Creator", "Manager"];
      const roleToDelete = uniqueStrings(legacyRoleTags.concat(roleUniverse));
      const roleToAdd = roleDesired ? [roleDesired] : [];

      const delRole = await deleteTags({ phoneE164, tags: roleToDelete });
      if (!delRole.ok) {
        throw new Error(`Delete role tags failed HTTP ${delRole.status} ${delRole.text}`);
      }

      if (roleToAdd.length > 0) {
        const addRole = await addTags({ phoneE164, tags: roleToAdd });
        if (!addRole.ok) {
          throw new Error(`Add role tag failed HTTP ${addRole.status} ${addRole.text}`);
        }
      }

      if (tierTagUniverse.length > 0) {
        const delTier = await deleteTags({ phoneE164, tags: tierTagUniverse });
        if (!delTier.ok) {
          throw new Error(`Delete tier tags failed HTTP ${delTier.status} ${delTier.text}`);
        }

        if (tierDesired) {
          const addTier = await addTags({ phoneE164, tags: [tierDesired] });
          if (!addTier.ok) {
            throw new Error(`Add tier tag failed HTTP ${addTier.status} ${addTier.text}`);
          }
        }
      }

      const desiredTags = desiredTagsFromRow(r);
      const nonRoleNonTierDesired = desiredTags.filter((t) => {
        if (t === "Creator" || t === "Manager") return false;
        if (tierTagUniverse.includes(t)) return false;
        if (t.toLowerCase().startsWith("tier:")) return false;
        return true;
      });

      if (nonRoleNonTierDesired.length > 0) {
        const addOther = await addTags({ phoneE164, tags: nonRoleNonTierDesired });
        if (!addOther.ok) {
          throw new Error(`Add tags failed HTTP ${addOther.status} ${addOther.text}`);
        }
      }

      await upsertRespondState(userId, {
        last_role_tag: roleDesired || null,
        last_group_tag: safeString(r.group_tag) || null,
        last_creator_network_manager_tag: safeString(r.creator_network_manager_tag) || null,
        last_tier_tag: tierDesired || null,
        last_phone_e164: phoneE164,
        last_profile_pic_url: profilePicUrl || null,
        last_first_name: firstName || null,
        last_creator_id: safeString(r.creator_id) || null,
        last_tiktok_username: safeString(r.tiktok_username) || null,
        last_agency_status: agencyStatus || null
      });

      updated += 1;
      ok += 1;

      console.log(
        `OK sync user_id=${userId} phone=${phoneE164} group=${groupFieldValue} manager=${managerFieldValue}`
      );
    } catch (e) {
      fail += 1;
      console.log(`FAIL user_id=${userId} phone=${phoneE164} err=${String(e.message || e)}`);
    }

    await sleep(perContactPaceMs);
  }

  console.log(`Summary total=${rows.length} ok=${ok} fail=${fail} updated=${updated} deleted=${deleted}`);
  await pool.end();
  console.log("Worker completed");
}

main().catch(async (e) => {
  console.error(String(e.message || e));
  try {
    await pool.end();
  } catch {
  }
  process.exitCode = 1;
});
