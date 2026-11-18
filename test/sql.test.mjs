import { test } from "node:test";
import assert from "node:assert/strict";
import { classify, singleStatementError, stripExplain } from "../build/sql.js";
import { parseArgs } from "../build/cli.js";

test("classify routes statements by their effect", () => {
  const cases = {
    "SELECT 1": "read",
    "  ( (SELECT 1) )": "read",
    "with c as (select 1) select * from c": "read",
    "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c": "write",
    "WITH c AS (SELECT 1) DELETE FROM t WHERE id IN (SELECT * FROM c)": "write",
    "SHOW TABLES": "read",
    "DESCRIBE t": "read",
    "EXPLAIN SELECT 1": "read",
    "EXPLAIN DELETE FROM t": "read",
    "EXPLAIN ANALYZE DELETE FROM t": "write",
    "EXPLAIN ANALYZE SELECT 1": "read",
    "INSERT INTO t VALUES (1)": "write",
    "COPY t FROM '/tmp/x.csv'": "write",
    "CREATE TABLE t (id INTEGER)": "ddl",
    "TRUNCATE TABLE t": "ddl",
    "PRAGMA checkpoint": "admin",
    "SET isolation_level = 'SNAPSHOT'": "admin",
    "BEGIN": "tx",
    "EXPLAIN COMMIT": "tx",
    "VALUES (1)": "other",
    "/* x */ -- y\nSELECT 1": "read",
    "# comment\nDELETE FROM t": "write",
  };
  for (const [sql, kind] of Object.entries(cases)) assert.equal(classify(sql), kind, sql);
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
    "SELECT 1;  ",
  ];
  for (const sql of single) assert.equal(singleStatementError(sql), null, sql);

  const multi = [
    "SELECT 1; SELECT 2",
    "SELECT '\\''; DROP TABLE t; SELECT ''",
    // The engine reads # as a line comment, so the quote below opens no literal.
    "SELECT 1 # '\n; DROP TABLE t; --'",
    "SELECT 1 /* a */ ; DELETE FROM t",
  ];
  for (const sql of multi) assert.match(singleStatementError(sql) ?? "", /multiple SQL statements/, sql);

  assert.match(singleStatementError("  ;  ") ?? "", /empty/);
  assert.match(singleStatementError("SELECT 1\0; DROP TABLE t") ?? "", /NUL/);
});

test("stripExplain accepts both forms", () => {
  assert.equal(stripExplain("EXPLAIN ANALYZE SELECT 1"), "SELECT 1");
  assert.equal(stripExplain("explain select 1"), "select 1");
  assert.equal(stripExplain("SELECT 1"), "SELECT 1");
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
