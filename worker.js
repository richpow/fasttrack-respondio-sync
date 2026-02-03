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

const pool = new Pool({
  connectionString: env("DATABASE_URL"),
  ssl: process.env.DATABASE_SSL === "true" ? { rejectUnauthorized: false } : undefined
});

function respondHeaders() {
  return {
    Accept: "application/json",
    Authorization: `Bearer ${env("RESPOND_IO_TOKEN")}`,
    "Content-Type": "application/json"
  };
}

async function httpJson(method, url, body, acceptHeaderOverride) {
  const headers = respondHeaders();
  if (acceptHeaderOverride) headers.Accept = acceptHeaderOverride;

  const res = await fetch(url, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined
  });

  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${text}`);
  }

  return json;
}

function buildUrl(templateEnvVar, phoneE164) {
  const baseUrl = env(templateEnvVar);
  const identifier = `phone:${phoneE164}`;
  return baseUrl.replace("{identifier}", identifier);
}

async function createContactInRespond({ phoneE164, firstName, customFields }) {
  const url = buildUrl("RESPOND_IO_CREATE_CONTACT_URL", phoneE164);

  const body = {
    firstName,
    phone: phoneE164,
    custom_fields: customFields
  };

  return httpJson("POST", url, body);
}

async function addTagsInRespond({ phoneE164, tags }) {
  const url = buildUrl("RESPOND_IO_ADD_TAGS_URL", phoneE164);

  const payload = uniqueStrings(tags);
  if (payload.length < 1) return null;

  if (payload.length > 10) {
    throw new Error(`Tag limit exceeded. Got ${payload.length} tags, respond.io allows max 10.`);
  }

  return httpJson("POST", url, payload, "application/json, application/xml, multipart/form-data");
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

async function upsertNeonMapping({ userId, respondContactId, phoneE164, roleTag, groupTag, cnmTag, tierTag }) {
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
      [userId, respondContactId, phoneE164]
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
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, now())
      ON CONFLICT (user_id)
      DO UPDATE SET
        last_role_tag = EXCLUDED.last_role_tag,
        last_group_tag = EXCLUDED.last_group_tag,
        last_creator_network_manager_tag = EXCLUDED.last_creator_network_manager_tag,
        last_tier_tag = EXCLUDED.last_tier_tag,
        last_phone_e164 = EXCLUDED.last_phone_e164,
        updated_at = now()
      `,
      [userId, roleTag, groupTag, cnmTag, tierTag, phoneE164]
    );

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function main() {
  const limit = Number(process.env.SYNC_LIMIT || "50");
  const delayMs = Number(process.env.RESPOND_IO_POST_CREATE_DELAY_MS || "900");

  const joiners = await fetchJoiners(limit);

  let ok = 0;
  let fail = 0;

  for (const r of joiners) {
    const userId = r.user_id;
    const phoneE164 = r.phone_e164;

    const firstName = safeString(r.tiktok_username) || `user_${userId}`;

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
      const createResp = await createContactInRespond({
        phoneE164,
        firstName,
        customFields
      });

      await sleep(delayMs);

      const tagResp = await addTagsInRespond({
        phoneE164,
        tags: tagsToAdd
      });

      const respondContactId =
        (tagResp && (tagResp.contactId ?? tagResp.contactID)) !== undefined
          ? (tagResp.contactId ?? tagResp.contactID)
          : 0;

      await upsertNeonMapping({
        userId,
        respondContactId,
        phoneE164,
        roleTag,
        groupTag,
        cnmTag,
        tierTag
      });

      ok += 1;
      console.log(
        `OK user_id=${userId} phone=${phoneE164} contact_id=${respondContactId} create=${JSON.stringify(createResp)} tags=${JSON.stringify(tagResp)}`
      );
    } catch (e) {
      fail += 1;
      console.log(`FAIL user_id=${userId} phone=${phoneE164} err=${String(e.message || e)}`);
    }
  }

  console.log(`Done processed=${joiners.length} ok=${ok} fail=${fail}`);
  await pool.end();

  if (fail > 0) process.exitCode = 2;
}

main().catch((e) => {
  console.error(String(e.message || e));
  process.exitCode = 1;
});
