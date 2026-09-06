// End-to-end smoke test: spawns the built server over stdio and drives it
// with the official MCP client.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const SERVER = join(dirname(fileURLToPath(import.meta.url)), "..", "build", "index.js");

async function connect(extraArgs = []) {
  const client = new Client({ name: "smoke", version: "0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [SERVER, ...extraArgs],
    stderr: "pipe",
  });
  await client.connect(transport);
  return client;
}

function text(result) {
  return result.content.map((c) => c.text).join("\n");
}

async function call(client, name, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  return { ok: !result.isError, text: text(result) };
}

let client;
before(async () => {
  client = await connect();
});
after(async () => {
  await client.close();
});

test("initialize reports server info and instructions", async () => {
  const info = client.getServerVersion();
  assert.equal(info.name, "stoolap");
  assert.match(client.getInstructions(), /Stoolap is an embedded SQL database/);
});

test("exposes 30 tools with annotations, 2 resources and 1 prompt", async () => {
  const { tools } = await client.listTools();
  assert.equal(tools.length, 30);
  const byName = Object.fromEntries(tools.map((t) => [t.name, t]));
  assert.equal(byName.query.annotations.readOnlyHint, true);
  assert.equal(byName.execute.annotations.readOnlyHint, false);
  assert.equal(byName.drop.annotations.destructiveHint, true);
  const { resources } = await client.listResources();
  assert.deepEqual(
    resources.map((r) => r.uri).sort(),
    ["stoolap://schema", "stoolap://sql-reference"],
  );
  const { prompts } = await client.listPrompts();
  assert.deepEqual(prompts.map((p) => p.name), ["sql-assistant"]);
});

test("version tool reports engine 0.4.x", async () => {
  const r = await call(client, "version");
  assert.ok(r.ok, r.text);
  assert.match(r.text, /stoolap 0\.4\./);
});

test("create_table, execute with params, RETURNING, query", async () => {
  let r = await call(client, "create_table", {
    sql: "CREATE TABLE users (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT NOT NULL, score FLOAT, created TIMESTAMP DEFAULT NOW())",
  });
  assert.ok(r.ok, r.text);

  r = await call(client, "execute", {
    sql: "INSERT INTO users (name, score) VALUES ($1, $2)",
    params: ["alice", 1.5],
  });
  assert.equal(r.text, "Affected rows: 1");

  r = await call(client, "execute", {
    sql: "INSERT INTO users (name, score) VALUES (:name, :score) RETURNING id, name",
    params: { name: "bob", score: 2.5 },
  });
  assert.ok(r.ok, r.text);
  assert.deepEqual(JSON.parse(r.text), [{ id: 2, name: "bob" }]);

  r = await call(client, "query", {
    sql: "SELECT id, name FROM users WHERE score > ? ORDER BY id",
    params: [1],
  });
  assert.ok(r.ok, r.text);
  assert.deepEqual(JSON.parse(r.text), [
    { id: 1, name: "alice" },
    { id: 2, name: "bob" },
  ]);

  // TIMESTAMP values are serialized as ISO strings
  r = await call(client, "query", { sql: "SELECT created FROM users WHERE id = 1" });
  assert.match(JSON.parse(r.text)[0].created, /^\d{4}-\d{2}-\d{2}T/);
});

test("upsert with ON CONFLICT and EXCLUDED", async () => {
  const r = await call(client, "execute", {
    sql: "INSERT INTO users (id, name, score) VALUES (1, 'alice2', 9) ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score",
  });
  assert.ok(r.ok, r.text);
  const q = await call(client, "query", { sql: "SELECT score FROM users WHERE id = 1" });
  assert.deepEqual(JSON.parse(q.text), [{ score: 9 }]);
});

test("execute_batch inserts atomically", async () => {
  const r = await call(client, "execute_batch", {
    sql: "INSERT INTO users (name, score) VALUES ($1, $2)",
    params_array: [
      ["carol", 3],
      ["dave", 4],
    ],
  });
  assert.equal(r.text, "Affected rows: 2");
});

test("routing guards: multi-statement, misrouted writes, transaction control", async () => {
  let r = await call(client, "query", { sql: "SELECT 1; SELECT 2" });
  assert.ok(!r.ok);
  assert.match(r.text, /multiple SQL statements/);

  r = await call(client, "query", { sql: "DELETE FROM users" });
  assert.ok(!r.ok);
  assert.match(r.text, /read-only statements/);

  r = await call(client, "execute", { sql: "BEGIN" });
  assert.ok(!r.ok);
  assert.match(r.text, /begin_transaction/);

  r = await call(client, "query", { sql: "COMMIT" });
  assert.ok(!r.ok);

  // A string literal containing a semicolon is a single statement
  r = await call(client, "query", { sql: "SELECT 'a;b' AS s" });
  assert.ok(r.ok, r.text);

  // Backslash-escaped quotes must not hide a second statement
  r = await call(client, "query", { sql: "SELECT '\\''; DROP TABLE users; SELECT ''" });
  assert.ok(!r.ok);
  assert.match(r.text, /multiple SQL statements/);
  r = await call(client, "query", { sql: "SELECT '\\''; BEGIN; SELECT ''" });
  assert.ok(!r.ok);
  r = await call(client, "query", { sql: "SELECT 'it''s \\' fine' AS s" });
  assert.ok(r.ok, r.text);
  r = await call(client, "query", { sql: "( (SELECT 1 AS one) )" });
  assert.ok(r.ok, r.text);

  r = await call(client, "execute", { sql: "SELECT 1" });
  assert.ok(!r.ok);
  assert.match(r.text, /query tool/);
});

test("explain and explain analyze", async () => {
  let r = await call(client, "explain", { sql: "SELECT * FROM users WHERE id = 1" });
  assert.ok(r.ok, r.text);
  assert.match(r.text, /PK Lookup|Scan/);

  r = await call(client, "explain", {
    sql: "EXPLAIN SELECT * FROM users WHERE id = 1",
    analyze: true,
  });
  assert.ok(r.ok, r.text);
  assert.match(r.text, /actual/);

  r = await call(client, "explain", { sql: "DELETE FROM users", analyze: true });
  assert.ok(!r.ok);
  assert.match(r.text, /executes it/);
});

test("transactions: rollback discards, commit keeps, savepoints work", async () => {
  let r = await call(client, "begin_transaction", { isolation: "snapshot" });
  assert.ok(r.ok, r.text);

  r = await call(client, "execute", { sql: "INSERT INTO users (name) VALUES ('x')" });
  assert.ok(!r.ok);
  assert.match(r.text, /transaction is active/);

  r = await call(client, "transaction_execute", {
    sql: "INSERT INTO users (name, score) VALUES ($1, $2)",
    params: ["temp", 0],
  });
  assert.equal(r.text, "Affected rows: 1");

  r = await call(client, "transaction_execute", { sql: "CREATE TABLE nope (id INTEGER)" });
  assert.ok(!r.ok);
  assert.match(r.text, /DDL/);

  r = await call(client, "transaction_query", { sql: "SELECT COUNT(*) AS n FROM users WHERE name = 'temp'" });
  assert.deepEqual(JSON.parse(r.text), [{ n: 1 }]);

  r = await call(client, "rollback_transaction");
  assert.ok(r.ok, r.text);

  r = await call(client, "query", { sql: "SELECT COUNT(*) AS n FROM users WHERE name = 'temp'" });
  assert.deepEqual(JSON.parse(r.text), [{ n: 0 }]);

  // savepoint flow
  r = await call(client, "begin_transaction");
  assert.ok(r.ok, r.text);
  r = await call(client, "transaction_execute_batch", {
    sql: "INSERT INTO users (name, score) VALUES ($1, $2)",
    params_array: [["keep", 1]],
  });
  assert.equal(r.text, "Affected rows: 1");
  r = await call(client, "savepoint", { name: "sp1" });
  assert.ok(r.ok, r.text);
  r = await call(client, "transaction_execute", { sql: "INSERT INTO users (name) VALUES ('drop_me')" });
  assert.ok(r.ok, r.text);
  r = await call(client, "rollback_to_savepoint", { name: "sp1" });
  assert.ok(r.ok, r.text);
  // TODO: assert release_savepoint succeeds once an engine with the
  // RELEASE SAVEPOINT parser fix ships in @stoolap/node.
  r = await call(client, "savepoint", { name: "bad name" });
  assert.ok(!r.ok);
  r = await call(client, "commit_transaction");
  assert.ok(r.ok, r.text);

  r = await call(client, "query", {
    sql: "SELECT name FROM users WHERE name IN ('keep', 'drop_me') ORDER BY name",
  });
  assert.deepEqual(JSON.parse(r.text), [{ name: "keep" }]);

  r = await call(client, "commit_transaction");
  assert.ok(!r.ok);
  assert.match(r.text, /no active transaction/);
});

test("schema inspection tools and resources", async () => {
  let r = await call(client, "create_index", { sql: "CREATE INDEX idx_score ON users(score)" });
  assert.ok(r.ok, r.text);
  r = await call(client, "create_view", { sql: "CREATE VIEW top AS SELECT name FROM users WHERE score > 2" });
  assert.ok(r.ok, r.text);

  r = await call(client, "list_tables");
  assert.deepEqual(JSON.parse(r.text), ["users"]);
  r = await call(client, "list_views");
  assert.deepEqual(JSON.parse(r.text), ["top"]);

  r = await call(client, "describe_table", { table: "users" });
  assert.equal(JSON.parse(r.text)[0].Field, "id");

  r = await call(client, "show_indexes", { table: "users" });
  assert.ok(JSON.parse(r.text).some((i) => i.index_name === "idx_score"));

  r = await call(client, "show_create_table", { table: "users" });
  assert.match(r.text, /CREATE TABLE users/);
  r = await call(client, "show_create_view", { view: "top" });
  assert.match(r.text, /CREATE VIEW top/);

  r = await call(client, "get_schema");
  const schema = JSON.parse(r.text);
  assert.ok(schema.tables.users.columns.length >= 4);
  assert.ok(schema.views.top);

  const res = await client.readResource({ uri: "stoolap://schema" });
  assert.equal(res.contents[0].mimeType, "application/json");
  assert.ok(JSON.parse(res.contents[0].text).tables.users);

  const ref = await client.readResource({ uri: "stoolap://sql-reference" });
  assert.match(ref.contents[0].text, /CREATE TABLE users/);
  assert.match(ref.contents[0].text, /Stoolap SQL Reference/);

  const prompt = await client.getPrompt({ name: "sql-assistant" });
  assert.match(prompt.messages[0].content.text, /CREATE TABLE users/);

  r = await call(client, "drop", { sql: "DROP VIEW top" });
  assert.ok(r.ok, r.text);
  r = await call(client, "drop", { sql: "SELECT 1" });
  assert.ok(!r.ok);
});

test("vectors are returned as arrays", async () => {
  let r = await call(client, "create_table", {
    sql: "CREATE TABLE emb (id INTEGER PRIMARY KEY, v VECTOR(3))",
  });
  assert.ok(r.ok, r.text);
  r = await call(client, "create_index", {
    sql: "CREATE INDEX idx_emb ON emb(v) USING HNSW WITH (metric = 'cosine')",
  });
  assert.ok(r.ok, r.text);
  r = await call(client, "execute", { sql: "INSERT INTO emb VALUES (1, '[1, 0, 0]')" });
  assert.ok(r.ok, r.text);
  r = await call(client, "query", {
    sql: "SELECT id, v, VEC_DISTANCE_COSINE(v, '[1, 0, 0]') AS d FROM emb ORDER BY d LIMIT 1",
  });
  assert.deepEqual(JSON.parse(r.text), [{ id: 1, v: [1, 0, 0], d: 0 }]);
});

test("pragma: read, set, action, validation", async () => {
  let r = await call(client, "pragma", { name: "checkpoint_interval" });
  assert.deepEqual(JSON.parse(r.text), [{ checkpoint_interval: 60 }]);

  r = await call(client, "pragma", { name: "keep_snapshots", value: 5 });
  assert.deepEqual(JSON.parse(r.text), [{ keep_snapshots: 5 }]);

  r = await call(client, "pragma", { name: "vacuum" });
  assert.ok(r.ok, r.text);
  assert.match(r.text, /deleted_rows_cleaned/);

  r = await call(client, "pragma", { name: "keep_snapshots", value: "" });
  assert.ok(!r.ok);
  assert.match(r.text, /numeric/);

  r = await call(client, "pragma", { name: "sync_mode", value: 2 });
  assert.ok(!r.ok);
  assert.match(r.text, /does not accept a value/);

  r = await call(client, "pragma", { name: "bogus" });
  assert.ok(!r.ok);
  assert.match(r.text, /unknown pragma/);

  r = await call(client, "pragma", { name: "restore", value: "not-a-timestamp" });
  assert.ok(!r.ok);

  r = await call(client, "analyze_table", { table: "users" });
  assert.ok(r.ok, r.text);
  r = await call(client, "vacuum", { table: "users" });
  assert.ok(r.ok, r.text);
});

test("list_functions", async () => {
  let r = await call(client, "list_functions", { category: "datetime" });
  assert.match(r.text, /DATEDIFF\(end, start\)/);
  r = await call(client, "list_functions");
  assert.match(r.text, /## Aggregate Functions/);
  assert.match(r.text, /## Vector Functions/);
});

test("read-only mode rejects writes but allows reads and read transactions", async () => {
  const ro = await connect(["--read-only"]);
  try {
    let r = await call(ro, "execute", { sql: "CREATE TABLE t (id INTEGER)" });
    assert.ok(!r.ok);
    assert.match(r.text, /read-only mode/);

    r = await call(ro, "execute", { sql: "COPY t FROM '/etc/hosts'" });
    assert.match(r.text, /read-only mode/);

    r = await call(ro, "query", { sql: "SELECT '\\''; CREATE TABLE pwned (id INTEGER); SELECT ''" });
    assert.ok(!r.ok);
    r = await call(ro, "list_tables");
    assert.deepEqual(JSON.parse(r.text), []);

    r = await call(ro, "execute_batch", { sql: "INSERT INTO t VALUES ($1)", params_array: [[1]] });
    assert.match(r.text, /read-only mode/);

    r = await call(ro, "pragma", { name: "checkpoint" });
    assert.match(r.text, /read-only mode/);

    r = await call(ro, "pragma", { name: "sync_mode" });
    assert.ok(r.ok, r.text);

    r = await call(ro, "query", { sql: "SELECT 1 AS one" });
    assert.deepEqual(JSON.parse(r.text), [{ one: 1 }]);

    r = await call(ro, "begin_transaction");
    assert.ok(r.ok, r.text);
    r = await call(ro, "transaction_execute", { sql: "INSERT INTO t VALUES (1)" });
    assert.match(r.text, /read-only mode/);
    r = await call(ro, "transaction_query", { sql: "SELECT 2 AS two" });
    assert.ok(r.ok, r.text);
    r = await call(ro, "commit_transaction");
    assert.ok(r.ok, r.text);
  } finally {
    await ro.close();
  }
});
