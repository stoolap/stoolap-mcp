// End-to-end: spawns the built server over stdio and drives it with the MCP client.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
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

async function call(client, name, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  const text = result.content.map((c) => c.text).join("\n");
  return { ok: !result.isError, text };
}

async function rows(client, sql, params) {
  const r = await call(client, "query", { sql, params });
  assert.ok(r.ok, r.text);
  return JSON.parse(r.text).rows;
}

let client;
before(async () => {
  client = await connect();
});
after(async () => {
  await client.close();
});

test("initialize: server info, instructions, 14 tools, resources, prompt", async () => {
  assert.equal(client.getServerVersion().name, "stoolap");
  assert.match(client.getInstructions(), /engine 0\.5\.x/);
  const { tools } = await client.listTools();
  assert.deepEqual(tools.map((t) => t.name).sort(), [
    "begin_transaction",
    "commit_transaction",
    "execute",
    "execute_batch",
    "explain",
    "get_schema",
    "list_functions",
    "list_tables",
    "pragma",
    "query",
    "rollback_transaction",
    "savepoint",
    "verify",
    "version",
  ]);
  const byName = Object.fromEntries(tools.map((t) => [t.name, t]));
  assert.equal(byName.query.annotations.readOnlyHint, true);
  assert.equal(byName.execute.annotations.destructiveHint, true);
  const { resources } = await client.listResources();
  assert.deepEqual(resources.map((r) => r.uri).sort(), ["stoolap://schema", "stoolap://sql-reference"]);
  const { prompts } = await client.listPrompts();
  assert.deepEqual(prompts.map((p) => p.name), ["sql-assistant"]);
});

test("version reports engine 0.5, limits and the default timeout", async () => {
  const r = await call(client, "version");
  const v = JSON.parse(r.text);
  assert.match(v.engine, /^stoolap 0\.5\./);
  assert.equal(v.max_rows, 1000);
  assert.equal(v.query_timeout_ms, 30000);
  assert.equal(v.read_only, false);
});

test("execute, RETURNING, parameters, typed values", async () => {
  let r = await call(client, "execute", {
    sql: "CREATE TABLE users (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT NOT NULL, score FLOAT, created TIMESTAMP DEFAULT NOW(), emb VECTOR(3))",
  });
  assert.ok(r.ok, r.text);
  assert.deepEqual(JSON.parse(r.text), { affected_rows: 0 });

  r = await call(client, "execute", {
    sql: "INSERT INTO users (name, score, emb) VALUES ($1, $2, '[1, 0, 0]')",
    params: ["alice", 1.5],
  });
  assert.deepEqual(JSON.parse(r.text), { affected_rows: 1 });

  r = await call(client, "execute", {
    sql: "INSERT INTO users (name, score) VALUES (:name, :score) RETURNING id, name",
    params: { name: "bob", score: 2.5 },
  });
  assert.deepEqual(JSON.parse(r.text), { columns: ["id", "name"], rows: [[2, "bob"]], row_count: 1, truncated: false });

  const [[created, emb]] = await rows(client, "SELECT created, emb FROM users WHERE id = 1");
  assert.match(created, /^\d{4}-\d{2}-\d{2}T/);
  assert.deepEqual(emb, [1, 0, 0]);
});

test("duplicate column names and empty results keep their columns", async () => {
  const r = await call(client, "query", { sql: "SELECT a.id, b.id FROM users a JOIN users b ON a.id = b.id WHERE a.id = 1" });
  assert.deepEqual(JSON.parse(r.text).columns, ["id", "id"]);
  assert.deepEqual(JSON.parse(r.text).rows, [[1, 1]]);
  const e = JSON.parse((await call(client, "query", { sql: "SELECT id, name FROM users WHERE id < 0" })).text);
  assert.deepEqual(e, { columns: ["id", "name"], rows: [], row_count: 0, truncated: false });
});

test("NaN and Infinity survive JSON", async () => {
  const r = await rows(client, "SELECT CAST('NaN' AS FLOAT), CAST('Infinity' AS FLOAT), CAST('-Infinity' AS FLOAT)");
  assert.deepEqual(r, [["NaN", "Infinity", "-Infinity"]]);
});

test("row limit: server default and per call", async () => {
  let r = JSON.parse((await call(client, "query", { sql: "SELECT * FROM GENERATE_SERIES(1, 1500)" })).text);
  assert.equal(r.row_count, 1000);
  assert.equal(r.truncated, true);
  r = JSON.parse((await call(client, "query", { sql: "SELECT * FROM GENERATE_SERIES(1, 5)", max_rows: 3 })).text);
  assert.deepEqual(r.rows, [[1], [2], [3]]);
  assert.equal(r.truncated, true);
  r = JSON.parse((await call(client, "query", { sql: "SELECT * FROM GENERATE_SERIES(1, 3)", max_rows: 3 })).text);
  assert.equal(r.truncated, false);
});

test("upsert and engine errors come back as tool errors", async () => {
  let r = await call(client, "execute", {
    sql: "INSERT INTO users (id, name, score) VALUES (1, 'alice2', 9) ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score",
  });
  assert.ok(r.ok, r.text);
  assert.deepEqual(await rows(client, "SELECT score FROM users WHERE id = 1"), [[9]]);

  r = await call(client, "query", { sql: "SELECT nosuch FROM users" });
  assert.ok(!r.ok);
  r = await call(client, "query", { sql: "SELECT 9223372036854775807 + 1" });
  assert.ok(!r.ok);
  assert.match(r.text, /overflow/);
});

test("execute_batch is atomic outside a transaction", async () => {
  let r = await call(client, "execute_batch", {
    sql: "INSERT INTO users (name, score) VALUES ($1, $2)",
    params_array: [["carol", 3], ["dave", 4]],
  });
  assert.deepEqual(JSON.parse(r.text), { affected_rows: 2 });
  r = await call(client, "execute_batch", {
    sql: "INSERT INTO users (id, name) VALUES ($1, $2)",
    params_array: [[100, "x"], [1, "dup"]],
  });
  assert.ok(!r.ok);
  assert.deepEqual(await rows(client, "SELECT COUNT(*) FROM users WHERE id = 100"), [[0]]);
  r = await call(client, "execute_batch", { sql: "SELECT $1", params_array: [[1]] });
  assert.ok(!r.ok);
});

test("routing guards", async () => {
  const refused = [
    ["query", "SELECT 1; SELECT 2", /multiple SQL statements/],
    ["query", "SELECT 1 # '\n; DROP TABLE users; --'", /multiple SQL statements/],
    ["query", "DELETE FROM users", /read-only statements/],
    ["query", "COMMIT", /begin_transaction/],
    ["execute", "BEGIN", /begin_transaction/],
    ["execute", "SELECT 1", /Use query/],
    ["execute", "PRAGMA checkpoint", /pragma tool/],
  ];
  for (const [tool, sql, re] of refused) {
    const r = await call(client, tool, { sql });
    assert.ok(!r.ok, sql);
    assert.match(r.text, re, sql);
  }
  assert.deepEqual(await rows(client, "SELECT 'a;b' AS s"), [["a;b"]]);
  assert.deepEqual(await rows(client, "SELECT COUNT(*) FROM users"), [[4]]);
});

test("explain prints plan lines; analyze is refused for writes", async () => {
  let r = await call(client, "explain", { sql: "SELECT * FROM users WHERE id = 1" });
  assert.ok(r.ok, r.text);
  assert.match(r.text, /PkGet users/);
  r = await call(client, "explain", { sql: "EXPLAIN SELECT * FROM users WHERE id = 1", analyze: true });
  assert.match(r.text, /actual time=/);
  assert.match(r.text, /^Paths: /m);
  r = await call(client, "explain", { sql: "DELETE FROM users", analyze: true });
  assert.ok(!r.ok);
  assert.match(r.text, /refused for writes/);
  assert.deepEqual(await rows(client, "SELECT COUNT(*) FROM users"), [[4]]);
});

test("transactions: query and execute join it; rollback, savepoints, commit", async () => {
  let r = await call(client, "begin_transaction", { isolation: "snapshot" });
  assert.ok(r.ok, r.text);
  r = await call(client, "begin_transaction");
  assert.match(r.text, /already open/);

  r = await call(client, "execute", { sql: "INSERT INTO users (name) VALUES ('temp')" });
  assert.ok(r.ok, r.text);
  r = await call(client, "execute_batch", { sql: "INSERT INTO users (name) VALUES ($1)", params_array: [["temp"]] });
  assert.ok(r.ok, r.text);
  assert.deepEqual(await rows(client, "SELECT COUNT(*) FROM users WHERE name = 'temp'"), [[2]]);

  r = await call(client, "execute", { sql: "CREATE INDEX ix ON users(score)" });
  assert.ok(!r.ok);
  assert.match(r.text, /not undone by ROLLBACK/);
  r = await call(client, "execute", { sql: "TRUNCATE TABLE users" });
  assert.match(r.text, /not undone by ROLLBACK/);
  r = await call(client, "execute", { sql: "CREATE TABLE scratch (id INTEGER PRIMARY KEY)" });
  assert.ok(r.ok, r.text);

  r = await call(client, "rollback_transaction");
  assert.ok(r.ok, r.text);
  assert.deepEqual(await rows(client, "SELECT COUNT(*) FROM users WHERE name = 'temp'"), [[0]]);
  assert.deepEqual(JSON.parse((await call(client, "list_tables")).text).tables, ["users"]);

  await call(client, "begin_transaction");
  await call(client, "execute", { sql: "INSERT INTO users (name) VALUES ('keep')" });
  r = await call(client, "savepoint", { action: "create", name: "sp1" });
  assert.ok(r.ok, r.text);
  await call(client, "execute", { sql: "INSERT INTO users (name) VALUES ('drop_me')" });
  r = await call(client, "savepoint", { action: "rollback_to", name: "sp1" });
  assert.ok(r.ok, r.text);
  r = await call(client, "savepoint", { action: "release", name: "sp1" });
  assert.ok(r.ok, r.text);
  r = await call(client, "savepoint", { action: "create", name: "bad name" });
  assert.ok(!r.ok);
  r = await call(client, "commit_transaction");
  assert.ok(r.ok, r.text);
  assert.deepEqual(await rows(client, "SELECT name FROM users WHERE name IN ('keep', 'drop_me')"), [["keep"]]);

  r = await call(client, "commit_transaction");
  assert.match(r.text, /no transaction is open/);
  r = await call(client, "savepoint", { action: "create", name: "sp" });
  assert.match(r.text, /no transaction is open/);
});

test("schema tools and resources", async () => {
  await call(client, "execute", { sql: "CREATE TABLE orders (id INTEGER PRIMARY KEY, uid INTEGER REFERENCES users(id), total FLOAT, UNIQUE (uid, total))" });
  await call(client, "execute", { sql: "CREATE INDEX idx_score ON users(score)" });
  await call(client, "execute", { sql: "CREATE VIEW top AS SELECT name FROM users WHERE score > 2" });

  const list = JSON.parse((await call(client, "list_tables")).text);
  assert.deepEqual(list.tables.sort(), ["orders", "users"]);
  assert.deepEqual(list.views, ["top"]);

  let r = await call(client, "get_schema", { table: "orders" });
  assert.match(r.text, /CREATE TABLE orders/);
  assert.match(r.text, /FOREIGN KEY \(uid\) REFERENCES users\(id\)/);
  assert.match(r.text, /-- unique index \S+ on \(uid, total\): MULTICOLUMN/);
  assert.doesNotMatch(r.text, /CREATE TABLE users/);

  r = await call(client, "get_schema");
  assert.match(r.text, /-- index idx_score on \(score\): BTREE/);
  assert.match(r.text, /CREATE VIEW top/);

  r = await call(client, "get_schema", { table: "nosuch" });
  assert.ok(!r.ok);

  const schema = await client.readResource({ uri: "stoolap://schema" });
  assert.match(schema.contents[0].text, /CREATE TABLE users/);
  const ref = await client.readResource({ uri: "stoolap://sql-reference" });
  assert.match(ref.contents[0].text, /CREATE TABLE orders/);
  assert.match(ref.contents[0].text, /Stoolap SQL Reference \(engine 0\.5\.x\)/);
  assert.match(ref.contents[0].text, /## Window Functions/);
  const prompt = await client.getPrompt({ name: "sql-assistant" });
  assert.match(prompt.messages[0].content.text, /CREATE VIEW top/);
});

test("pragma: settings, reports, actions, validation", async () => {
  let r = await call(client, "pragma", { name: "checkpoint_interval" });
  assert.deepEqual(JSON.parse(r.text).rows, [[60]]);
  r = await call(client, "pragma", { name: "keep_snapshots", value: 5 });
  assert.deepEqual(JSON.parse(r.text).rows, [[5]]);
  r = await call(client, "pragma", { name: "memory_stats" });
  assert.ok(JSON.parse(r.text).columns.includes("table_name"));
  r = await call(client, "pragma", { name: "VACUUM" });
  assert.ok(r.ok, r.text);

  const bad = [
    [{ name: "keep_snapshots", value: "" }, /integer/],
    [{ name: "keep_snapshots", value: 1.5 }, /integer/],
    [{ name: "sync_mode", value: 2 }, /takes no value/],
    [{ name: "bogus" }, /unknown pragma/],
    [{ name: "restore", value: "not-a-timestamp" }, /snapshot timestamp/],
  ];
  for (const [args, re] of bad) {
    r = await call(client, "pragma", args);
    assert.ok(!r.ok, JSON.stringify(args));
    assert.match(r.text, re);
  }
});

test("query timeout stops a long statement and the session goes on", async () => {
  let r = await call(client, "pragma", { name: "query_timeout", value: 50 });
  assert.ok(r.ok, r.text);
  r = await call(client, "query", {
    sql: "SELECT COUNT(*) FROM GENERATE_SERIES(1, 10000000) a CROSS JOIN GENERATE_SERIES(1, 1000) b",
  });
  assert.ok(!r.ok);
  assert.match(r.text, /cancelled/);
  await call(client, "pragma", { name: "query_timeout", value: 30000 });
  assert.deepEqual(await rows(client, "SELECT 1"), [[1]]);
});

test("list_functions", async () => {
  let r = await call(client, "list_functions", { category: "datetime" });
  assert.match(r.text, /DATEDIFF\(end, start\)/);
  r = await call(client, "list_functions");
  assert.match(r.text, /## Aggregate Functions/);
  assert.match(r.text, /## Vector Functions/);
});

test("concurrent calls do not interleave inside a transaction", async () => {
  await call(client, "begin_transaction");
  const inserts = Array.from({ length: 20 }, (_, i) =>
    call(client, "execute", { sql: "INSERT INTO users (name) VALUES ($1)", params: [`c${i}`] }),
  );
  const results = await Promise.all([...inserts, call(client, "rollback_transaction")]);
  assert.ok(results.every((r) => r.ok));
  assert.deepEqual(await rows(client, "SELECT COUNT(*) FROM users WHERE name LIKE 'c%' AND name <> 'carol'"), [[0]]);
});

test("read-only mode refuses writes and allows reads and read transactions", async () => {
  const ro = await connect(["--read-only", "--max-rows", "2", "--query-timeout", "0"]);
  try {
    for (const [tool, args] of [
      ["execute", { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY)" }],
      ["execute", { sql: "COPY t FROM '/etc/hosts'" }],
      ["execute", { sql: "SET isolation_level = 'SNAPSHOT'" }],
      ["execute_batch", { sql: "INSERT INTO t VALUES ($1)", params_array: [[1]] }],
      ["pragma", { name: "checkpoint" }],
      ["pragma", { name: "keep_snapshots", value: 1 }],
    ]) {
      const r = await call(ro, tool, args);
      assert.ok(!r.ok, JSON.stringify(args));
      assert.match(r.text, /read-only mode/);
    }
    let r = await call(ro, "query", { sql: "SELECT 1 # '\n; CREATE TABLE pwned (id INTEGER PRIMARY KEY); --'" });
    assert.ok(!r.ok);
    r = await call(ro, "explain", { sql: "INSERT INTO t VALUES (1)", analyze: true });
    assert.ok(!r.ok);
    assert.deepEqual(JSON.parse((await call(ro, "list_tables")).text).tables, []);

    r = await call(ro, "pragma", { name: "query_timeout", value: 1000 });
    assert.ok(r.ok, r.text);
    const v = JSON.parse((await call(ro, "version")).text);
    assert.equal(v.read_only, true);
    assert.equal(v.max_rows, 2);
    assert.equal(v.query_timeout_ms, 1000);

    r = await call(ro, "begin_transaction");
    assert.ok(r.ok, r.text);
    assert.equal(JSON.parse((await call(ro, "query", { sql: "SELECT * FROM GENERATE_SERIES(1, 5)" })).text).row_count, 2);
    r = await call(ro, "commit_transaction");
    assert.ok(r.ok, r.text);
  } finally {
    await ro.close();
  }
});

test("a file database: query_timeout in the DSN, verify, data survives a restart", async () => {
  const dir = mkdtempSync(join(tmpdir(), "stoolap-mcp-"));
  try {
    const dsn = `file://${join(dir, "db")}?query_timeout=1234`;
    let c = await connect(["--path", dsn]);
    assert.equal(JSON.parse((await call(c, "version")).text).query_timeout_ms, 1234);
    await call(c, "execute", { sql: "CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)" });
    await call(c, "execute_batch", { sql: "INSERT INTO k VALUES ($1, $2)", params_array: [[1, "a"], [2, "b"]] });
    let r = await call(c, "pragma", { name: "checkpoint" });
    assert.ok(r.ok, r.text);
    r = await call(c, "verify");
    const report = JSON.parse(r.text);
    assert.deepEqual(report.columns, ["table_name", "segment_id", "status", "blocks", "detail"]);
    assert.ok(report.rows.length > 0);
    assert.ok(report.rows.every((row) => row[2] === "ok"));
    await c.close();

    c = await connect(["--path", dsn, "--query-timeout", "0"]);
    assert.equal(JSON.parse((await call(c, "version")).text).query_timeout_ms, 0);
    assert.deepEqual(JSON.parse((await call(c, "query", { sql: "SELECT v FROM k ORDER BY id" })).text).rows, [["a"], ["b"]]);
    await c.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
