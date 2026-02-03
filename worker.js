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

function buildCreateContactUrl(phoneE164) {
  const baseUrl = env("RESPOND_IO_CREATE_CONTACT_URL");
  const identifier = `phone:${phoneE164}`;
  return baseUrl.replace("{identifier}", identifier);
}

async function createContactInRespond({ phoneE164, firstName, customFields }) {
  const url = buildCreateContactUrl(phoneE164);

  const body = {
    firstName,
    phone: phoneE164,
    custom_fields: customFields
  };

  return httpJson("POST", url, body);
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
        agency_status
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
    const userId = r.user_id;
    const phoneE164 = r.phone_e164;

    const firstName = safeString(r.tiktok_username) || `user_${userId}`;

    const customFields = [
      { name: "neon_user_id", value: String(userId) },
      { name: "creator_id", value: safeString(r.creator_id) },
      { name: "tiktok_username", value: safeString(r.tiktok_username) },
      { name: "agency_status", value: safeString(r.agency_status) }
    ];

    try {
      const resp = await createContactInRespond({
        phoneE164,
        firstName,
        customFields
      });

      ok += 1;
      console.log(`OK created contact user_id=${userId} phone=${phoneE164} resp=${JSON.stringify(resp)}`);
    } catch (e) {
      fail += 1;
      console.log(`FAIL create contact user_id=${userId} phone=${phoneE164} err=${String(e.message || e)}`);
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
