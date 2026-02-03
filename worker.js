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
    Authorization: `Bearer ${env("RESPOND_IO_TOKEN")}`,
    "Content-Type": "application/json"
  };
}

async function httpJson(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: respondHeaders(),
    body: body ? JSON.stringify(body) : undefined
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

function extractContactId(payload) {
  if (!payload) return null;
  if (payload.id !== undefined && payload.id !== null) return payload.id;
  if (payload.data && payload.data.id !== undefined && payload.data.id !== null) return payload.data.id;
  if (payload.contact && payload.contact.id !== undefined && payload.contact.id !== null) return payload.contact.id;
  return null;
}

async function createOrUpsertContact({ phoneE164, name, fields }) {
  const url = env("RESPOND_IO_CREATE_CONTACT_URL");
  const body = {
    identifier: `phone:${phoneE164}`,
    name,
    custom_fields: fields
  };
  const json = await httpJson("POST", url, body);
  const contactId = extractContactId(json);
  if (!contactId) throw new Error("Create contact succeeded but no contact id found in response");
  return contactId;
}

async function addTags({ phoneE164, tags }) {
  const url = env("RESPOND_IO_ADD_TAGS_URL");
  const body = {
    identifier: `phone:${phoneE164}`,
    tags: uniqueStrings(tags)
  };
  await httpJson("POST", url, body);
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

async function main() {
  const limit = Number(process.env.SYNC_LIMIT || "50");
  const joiners = await fetchJoiners(limit);

  let ok = 0;
  let fail = 0;

  for (const r of joiners) {
    const phoneE164 = r.phone_e164;
    const userId = r.user_id;

    const name = isNonEmptyString(r.tiktok_username) ? r.tiktok_username : `user_${userId}`;

    const fields = {
      neon_user_id: String(userId),
      creator_id: isNonEmptyString(r.creator_id) ? String(r.creator_id) : "",
      tiktok_username: isNonEmptyString(r.tiktok_username) ? String(r.tiktok_username) : "",
      agency_status: isNonEmptyString(r.agency_status) ? String(r.agency_status) : ""
    };

    const roleTag = r.role_tag;
    const groupTag = r.group_tag;
    const cnmTag = r.creator_network_manager_tag;
    const tierTag = r.tier_tag;

    try {
      const contactId = await createOrUpsertContact({ phoneE164, name, fields });
      await addTags({ phoneE164, tags: [roleTag, groupTag, cnmTag, tierTag] });

      await upsertNeonMapping({
        userId,
        respondContactId: contactId,
        phoneE164,
        roleTag,
        groupTag,
        cnmTag,
        tierTag
      });

      ok += 1;
      console.log(`OK user_id=${userId} phone=${phoneE164} contact_id=${contactId}`);
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
