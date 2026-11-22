import { test } from "node:test";
import assert from "node:assert/strict";
import { isCreateTable, parse } from "../build/sql.js";
import { parseArgs } from "../build/cli.js";

const kind = (sql) => {
  const st = parse(sql);
  return "error" in st ? `error: ${st.error}` : st.kind;
};

test("statements are classified by their effect", () => {
  const cases = {
    "SELECT 1": "read",
    "  ( (SELECT 1) )": "read",
    "with c as (select 1) select * from c": "read",
    "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c": "write",
    "WITH c AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM c)": "write",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r": "read",
    "SHOW TABLES": "read",
    "DESCRIBE t": "read",
    "EXPLAIN SELECT 1": "read",
    "EXPLAIN DELETE FROM t": "read",
    "EXPLAIN ANALYZE DELETE FROM t": "write",
    "EXPLAIN ANALYZE SELECT 1": "read",
    "EXPLAIN ANALYZE ANALYZE t": "admin",
    "EXPLAIN ANALYZE PRAGMA checkpoint": "pragma",
    "EXPLAIN COMMIT": "tx",
    "INSERT INTO t VALUES (1)": "write",
    "COPY t FROM '/tmp/x.csv'": "write",
    "CREATE TABLE t (id INTEGER)": "ddl",
    "TRUNCATE TABLE t": "ddl",
    "PRAGMA checkpoint": "pragma",
    "/**/PRAGMA checkpoint": "pragma",
    "SET isolation_level = 'SNAPSHOT'": "admin",
    "BEGIN": "tx",
    "VALUES (1)": "other",
    "/* x */ -- y\nSELECT 1": "read",
    "# comment\nDELETE FROM t": "write",
  };
  for (const [sql, k] of Object.entries(cases)) assert.equal(kind(sql), k, sql);
});

test("Unicode whitespace separates words, as in the engine lexer", () => {
  assert.equal(kind("WITH c AS (SELECT 1)\u0085INSERT INTO t SELECT * FROM c"), "write");
  assert.equal(kind("EXPLAIN ANALYZE\u0085DELETE FROM t"), "write");
  assert.equal(kind("\u0085COMMIT"), "tx");
  assert.equal(kind(" BEGIN"), "tx");
  assert.equal(kind("SELECT 1"), "read");
  assert.match(kind("﻿SELECT 1"), /byte order mark/);
});

test("a second statement is found past literals, identifiers and comments", () => {
  const single = [
    "SELECT 'a;b'",
    "SELECT 'it''s; fine'",
    "SELECT 'back\\'slash;'",
    'SELECT "odd;name" FROM t',
    "SELECT `odd;name` FROM t",
    "SELECT 1 -- ; DROP TABLE t",
    "SELECT 1 # ; DROP TABLE t",
    "SELECT 1 /* ; */",
    "SELECT 1;  -- trailing",
  ];
  for (const sql of single) assert.equal(kind(sql), "read", sql);

  const multi = [
    "SELECT 1; SELECT 2",
    "SELECT '\\''; DROP TABLE t; SELECT ''",
    // The engine reads # as a line comment, so the quote below opens no literal.
    "SELECT 1 # '\n; DROP TABLE t; --'",
    "SELECT 1 /* a */ ; DELETE FROM t",
    "SELECT 1;\u0085COMMIT",
  ];
  for (const sql of multi) assert.match(kind(sql), /multiple SQL statements/, sql);

  assert.match(kind("  ;  "), /empty/);
  assert.match(kind("SELECT 1\0; DROP TABLE t"), /NUL/);
});

test("CREATE TABLE, with or without AS SELECT, is told from other DDL", () => {
  const create = (sql) => isCreateTable(parse(sql));
  assert.equal(create("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0))"), true);
  assert.equal(create("/* x */ create table t (id INTEGER PRIMARY KEY)"), true);
  assert.equal(create("CREATE TABLE t AS SELECT 1 AS a"), true);
  assert.equal(create("CREATE INDEX i ON t(a)"), false);
  assert.equal(create("CREATE VIEW v AS SELECT 1 AS a"), false);
});

test("parseArgs", () => {
  assert.deepEqual(parseArgs([], "v"), { kind: "run", options: { path: ":memory:", readOnly: false, maxRows: 1000 } });
  const r = parseArgs(["--path", "./db", "--read-only", "--max-rows", "0", "--query-timeout", "500"], "v");
  assert.deepEqual(r, { kind: "run", options: { path: "./db", readOnly: true, maxRows: 0, queryTimeoutMs: 500 } });
  assert.equal(parseArgs(["--max-rows", "-1"], "v").code, 2);
  assert.equal(parseArgs(["--query-timeout"], "v").code, 2);
  assert.equal(parseArgs(["--bogus"], "v").code, 2);
  assert.deepEqual(parseArgs(["--version"], "1.2.3"), { kind: "exit", code: 0, out: "1.2.3" });
});
