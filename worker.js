import fetch from "node-fetch";
import pg from "pg";

const { Pool } = pg;

function env(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
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
  const maxAttempts = Number(process.env.RESPOND_IO_RETRY_MAX || "6");
  const baseDelayMs = Number(process.env.RESPOND_IO_RETRY_BASE_MS || "1500");
  const maxDelayMs = Number(process.env.RESPOND_IO_RETRY_MAX_MS || "20000");

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

async function updateContactInRespond({ phoneE164, firstName, profilePicUrl, customFields }) {
  const url = urlWithPhone("RESPOND_IO_UPDATE_CONTACT_URL", phoneE164);
  const body = buildContactBody({ phoneE164, firstName, profilePicUrl, customFields });

  const res = await withQueueRetry(() =>
    http("PUT", url, body, "application/json, application/xml, multipart/form-data")
  );

  return res;
}

async function createContactInRespond({ phoneE164, firstName, profilePicUrl, customFields }) {
  const url = urlWithPhone("RESPOND_IO_CREATE_CONTACT_URL", phoneE164);
  const body = buildContactBody({ phoneE164, firstName, profilePicUrl, customFields });

  const res = await http("POST", url, body, "application/json");
  return res;
}

async function addTagsInRespond({ phoneE164, tags }) {
  const url = urlWithPhone("RESPOND_IO_ADD_TAGS_URL", phoneE164);

  const payload = uniqueStrings(tags);
  if (payload.length < 1) return { ok: true, status: 200, text: "", json: { contactId: 0 } };

  if (payload.length > 10) {
    return { ok: false, status: 400, text: `Too many tags: ${payload.length}`, json: null };
  }

  const res = await withQueueRetry(() =>
    http("POST", url, payload, "application/json, application/xml, multipart/form-data")
  );

  return res;
}

async function fetchJoiners(limit) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `
      SELECT
        user_id,
        phone_e164,
        tiktok_username,
        creator_id,
        agency_status,
        profile_pic_url,
        role_tag,
        group_tag,
        creator_network_manager_tag,
        tier_tag
      FROM v_respond_sync_users
      WHERE agency_status = 'in_agency'
        AND user_id NOT IN (SELECT user_id FROM respond_contacts)
      ORDER BY user_id
      LIMIT $1
      `,
      [limit]
    );
    return rows;
  } finally {
    client.release();
  }
}

async function fetchProfilePicUpdates(limit) {
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
        v.profile_pic_url
      FROM v_respond_sync_users v
      JOIN respond_contacts rc ON rc.user_id = v.user_id
      LEFT JOIN respond_sync_state ss ON ss.user_id = v.user_id
      WHERE v.agency_status = 'in_agency'
        AND v.profile_pic_url IS NOT NULL
        AND v.profile_pic_url <> ''
        AND COALESCE(ss.last_profile_pic_url, '') <> v.profile_pic_url
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

async function upsertMappingAndState({ userId, respondContactId, phoneE164, roleTag, groupTag, cnmTag, tierTag, profilePicUrl }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

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
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, now())
      ON CONFLICT (user_id)
      DO UPDATE SET
        last_role_tag = COALESCE(EXCLUDED.last_role_tag, respond_sync_state.last_role_tag),
        last_group_tag = COALESCE(EXCLUDED.last_group_tag, respond_sync_state.last_group_tag),
        last_creator_network_manager_tag = COALESCE(EXCLUDED.last_creator_network_manager_tag, respond_sync_state.last_creator_network_manager_tag),
        last_tier_tag = COALESCE(EXCLUDED.last_tier_tag, respond_sync_state.last_tier_tag),
        last_phone_e164 = COALESCE(EXCLUDED.last_phone_e164, respond_sync_state.last_phone_e164),
        last_profile_pic_url = COALESCE(EXCLUDED.last_profile_pic_url, respond_sync_state.last_profile_pic_url),
        updated_at = now()
      `,
      [
        userId,
        roleTag ?? null,
        groupTag ?? null,
        cnmTag ?? null,
        tierTag ?? null,
        phoneE164 ?? null,
        isNonEmptyString(profilePicUrl) ? profilePicUrl : null
      ]
    );

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function runJoiners() {
  const limit = Number(process.env.SYNC_LIMIT || "50");
  const delayAfterCreateMs = Number(process.env.RESPOND_IO_POST_CREATE_DELAY_MS || "900");
  const perContactPaceMs = Number(process.env.RESPOND_IO_PER_CONTACT_PACE_MS || "250");

  const joiners = await fetchJoiners(limit);

  let ok = 0;
  let fail = 0;

  for (const r of joiners) {
    if (shuttingDown) break;

    const userId = r.user_id;
    const phoneE164 = r.phone_e164;

    const firstName = safeString(r.tiktok_username) || `user_${userId}`;
    const profilePicUrl = safeString(r.profile_pic_url);

    const customFields = [
      { name: "neon_user_id", value: String(userId) },
      { name: "creator_id", value: safeString(r.creator_id) },
      { name: "tiktok_username", value: safeString(r.tiktok_username) },
      { name: "agency_status", value: safeString(r.agency_status) }
    ];

    const roleTag = r.role_tag;
    const groupTag = r.group_tag;
    const cnmTag = r.creator_network_manager_tag;
    const tierTag = r.tier_tag;

    const tagsToAdd = [roleTag, groupTag, cnmTag, tierTag];

    try {
      const upd = await updateContactInRespond({ phoneE164, firstName, profilePicUrl, customFields });

      if (!upd.ok) {
        const cre = await createContactInRespond({ phoneE164, firstName, profilePicUrl, customFields });

        const alreadyExists =
          cre.status === 403 && isNonEmptyString(cre.text) && cre.text.includes("Contact already exist");

        if (!cre.ok && !alreadyExists) {
          throw new Error(`Create failed HTTP ${cre.status} ${cre.text}`);
        }

        await sleep(delayAfterCreateMs);
      }

      const tagRes = await addTagsInRespond({ phoneE164, tags: tagsToAdd });
      if (!tagRes.ok) throw new Error(`Add tags failed HTTP ${tagRes.status} ${tagRes.text}`);

      const contactId = contactIdFromJson(tagRes.json);

      await upsertMappingAndState({
        userId,
        respondContactId: contactId,
        phoneE164,
        roleTag,
        groupTag,
        cnmTag,
        tierTag,
        profilePicUrl
      });

      ok += 1;
      console.log(`OK joiner user_id=${userId} phone=${phoneE164} contact_id=${contactId}`);
    } catch (e) {
      fail += 1;
      console.log(`FAIL joiner user_id=${userId} phone=${phoneE164} err=${String(e.message || e)}`);
    }

    await sleep(perContactPaceMs);
  }

  console.log(`Joiners finished processed=${joiners.length} ok=${ok} fail=${fail}`);
}

async function runProfilePicUpdates() {
  const limit = Number(process.env.UPDATE_LIMIT || "200");
  const perContactPaceMs = Number(process.env.RESPOND_IO_PER_CONTACT_PACE_MS || "250");

  const updates = await fetchProfilePicUpdates(limit);

  let ok = 0;
  let fail = 0;

  for (const r of updates) {
    if (shuttingDown) break;

    const userId = r.user_id;
    const phoneE164 = r.phone_e164;

    const firstName = safeString(r.tiktok_username) || `user_${userId}`;
    const profilePicUrl = safeString(r.profile_pic_url);

    const customFields = [
      { name: "neon_user_id", value: String(userId) },
      { name: "creator_id", value: safeString(r.creator_id) },
      { name: "tiktok_username", value: safeString(r.tiktok_username) },
      { name: "agency_status", value: safeString(r.agency_status) }
    ];

    try {
      const upd = await updateContactInRespond({ phoneE164, firstName, profilePicUrl, customFields });

      if (!upd.ok) {
        throw new Error(`Update failed HTTP ${upd.status} ${upd.text}`);
      }

      await upsertMappingAndState({
        userId,
        respondContactId: 0,
        phoneE164,
        roleTag: null,
        groupTag: null,
        cnmTag: null,
        tierTag: null,
        profilePicUrl
      });

      ok += 1;
      console.log(`OK profile_pic user_id=${userId} phone=${phoneE164}`);
    } catch (e) {
      fail += 1;
      console.log(`FAIL profile_pic user_id=${userId} phone=${phoneE164} err=${String(e.message || e)}`);
    }

    await sleep(perContactPaceMs);
  }

  console.log(`Profile pic updates finished processed=${updates.length} ok=${ok} fail=${fail}`);
}

async function main() {
  console.log("Worker start");
  await runJoiners();
  if (!shuttingDown) await runProfilePicUpdates();

  await pool.end();

  if (shuttingDown) {
    console.log("Worker exit clean after shutdown signal");
    process.exitCode = 0;
  } else {
    console.log("Worker completed");
  }
}

main().catch(async (e) => {
  console.error(String(e.message || e));
  try {
    await pool.end();
  } catch {
  }
  process.exitCode = 1;
});
