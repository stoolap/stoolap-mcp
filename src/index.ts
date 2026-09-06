#!/usr/bin/env node

// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { createRequire } from "node:module";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { Database } from "@stoolap/node";

const require = createRequire(import.meta.url);
const { version: PKG_VERSION } = require("../package.json") as {
  version: string;
};

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

const USAGE = `Usage: stoolap-mcp [--path <database-path>] [--read-only]

Options:
  --path <path>   Database path or DSN (default: :memory:)
                  Engine options go in the query string, e.g.
                  ./mydata?sync_mode=full&checkpoint_interval=30
  --read-only     Reject every statement that writes data or schema
  --version       Print the server version and exit
  -h, --help      Show this help and exit

Examples:
  stoolap-mcp --path ./mydata
  stoolap-mcp --path ./mydata --read-only
  stoolap-mcp                      # in-memory database`;

const args = process.argv.slice(2);
let dbPath = ":memory:";
let readOnly = false;

for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === "--path") {
    if (i + 1 >= args.length) {
      console.error("error: --path requires a value\n\n" + USAGE);
      process.exit(2);
    }
    dbPath = args[++i];
  } else if (a === "--read-only") {
    readOnly = true;
  } else if (a === "--version") {
    console.log(PKG_VERSION);
    process.exit(0);
  } else if (a === "--help" || a === "-h") {
    console.error(USAGE);
    process.exit(0);
  } else {
    console.error(`error: unknown argument: ${a}\n\n` + USAGE);
    process.exit(2);
  }
}

// ---------------------------------------------------------------------------
// Database and transaction state
// ---------------------------------------------------------------------------

// All tools share one handle. Stoolap keeps transaction state per handle, so
// a SQL-level BEGIN issued here stays open across tool calls until COMMIT or
// ROLLBACK, and other handles never see the uncommitted rows.
const db = await Database.open(dbPath);

// Mirrors whether a SQL-level transaction is open on the shared handle.
let txActive = false;

// ---------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------

type ToolResult = {
  content: Array<{ type: "text"; text: string }>;
  isError?: boolean;
};

function ok(text: string): ToolResult {
  return { content: [{ type: "text", text }] };
}

function err(text: string): ToolResult {
  return { content: [{ type: "text", text: `Error: ${text}` }], isError: true };
}

function fail(e: unknown): ToolResult {
  return err(e instanceof Error ? e.message : String(e));
}

// The driver returns VECTOR columns as Float32Array (which JSON.stringify
// would render as {"0":..,"1":..}) and TIMESTAMP columns as Date objects.
function jsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Float32Array) return Array.from(value);
  if (typeof value === "bigint") return value.toString();
  return value;
}

function json(data: unknown): ToolResult {
  return ok(JSON.stringify(data, jsonReplacer, 2));
}

function readOnlyErr(): ToolResult {
  return err("server is in read-only mode");
}

function txActiveErr(): ToolResult {
  return err(
    "a transaction is active. Use transaction_execute/transaction_query inside it, or commit_transaction/rollback_transaction first.",
  );
}

function noTxErr(): ToolResult {
  return err("no active transaction. Call begin_transaction first.");
}

// ---------------------------------------------------------------------------
// SQL classification helpers
// ---------------------------------------------------------------------------

// Single-pass sanitizer: blanks out string literals, block comments and line
// comments so that keyword/semicolon detection never trips over their content.
// Literals support both '' and backslash escapes, matching the engine lexer.
function sanitizeSql(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    if (sql[i] === "'") {
      out += " ";
      i++;
      while (i < sql.length) {
        // The engine lexer treats backslash as an escape inside literals.
        if (sql[i] === "\\") {
          i += 2;
        } else if (sql[i] === "'" && sql[i + 1] === "'") {
          i += 2;
        } else if (sql[i] === "'") {
          i++;
          break;
        } else {
          i++;
        }
      }
      continue;
    }
    if (sql[i] === "/" && sql[i + 1] === "*") {
      out += " ";
      i += 2;
      while (i < sql.length) {
        if (sql[i] === "*" && sql[i + 1] === "/") {
          i += 2;
          break;
        }
        i++;
      }
      continue;
    }
    if (sql[i] === "-" && sql[i + 1] === "-") {
      out += " ";
      i += 2;
      while (i < sql.length && sql[i] !== "\n") i++;
      continue;
    }
    out += sql[i];
    i++;
  }
  return out;
}

// The engine executes every statement in a multi-statement string but only
// reports the last one, so each tool call is limited to a single statement.
function rejectMultiStatement(sql: string): ToolResult | null {
  const trimmed = sanitizeSql(sql).replace(/[\s;]+$/, "");
  if (trimmed.includes(";")) {
    return err(
      "multiple SQL statements are not allowed. Send one statement per call.",
    );
  }
  if (trimmed.trim() === "") return err("empty SQL statement");
  return null;
}

function firstWord(text: string): string {
  return text.replace(/^[\s(]+/, "").split(/[\s(;]/)[0].toUpperCase();
}

// First keyword of the statement, e.g. CREATE, INSERT, WITH.
function firstKeyword(sql: string): string {
  return firstWord(sanitizeSql(sql));
}

// First keyword of the statement that EXPLAIN [ANALYZE] would run.
function effectiveKeyword(sql: string): string {
  let rest = sanitizeSql(sql).trimStart();
  if (/^EXPLAIN\b/i.test(rest)) {
    rest = rest.slice(7).trimStart();
    if (/^ANALYZE\b/i.test(rest)) rest = rest.slice(7).trimStart();
    return firstWord(rest) || "EXPLAIN";
  }
  return firstWord(rest);
}

// Statements that must go through the dedicated transaction tools, because
// running them on the shared handle would desynchronise the server's view of
// the transaction state.
const TX_CONTROL = new Set([
  "BEGIN",
  "START",
  "COMMIT",
  "END",
  "ROLLBACK",
  "SAVEPOINT",
  "RELEASE",
]);

function rejectTxControl(sql: string): ToolResult | null {
  if (!TX_CONTROL.has(effectiveKeyword(sql))) return null;
  return err(
    "transaction control statements are not accepted here. Use begin_transaction, commit_transaction, rollback_transaction, savepoint, rollback_to_savepoint or release_savepoint.",
  );
}

// CREATE/DROP/ALTER/TRUNCATE. Only CREATE TABLE is rolled back reliably
// inside an explicit transaction (ALTER, CREATE INDEX/VIEW persist and
// DROP TABLE loses its rows), so DDL is kept out of transactions.
function isDDL(sql: string): boolean {
  const kw = firstKeyword(sql);
  return kw === "CREATE" || kw === "DROP" || kw === "ALTER" || kw === "TRUNCATE";
}

function hasReturning(sql: string): boolean {
  return /\bRETURNING\b/i.test(sanitizeSql(sql));
}

// Anything that changes data, schema or engine state. Looks past
// EXPLAIN [ANALYZE] because EXPLAIN ANALYZE executes the statement.
const WRITE_KEYWORDS = new Set([
  "INSERT",
  "UPDATE",
  "DELETE",
  "CREATE",
  "DROP",
  "ALTER",
  "TRUNCATE",
  "COPY",
  "VACUUM",
  "ANALYZE",
  "PRAGMA",
  "SET",
]);

function isWrite(sql: string): boolean {
  const kw = effectiveKeyword(sql);
  if (WRITE_KEYWORDS.has(kw)) return true;
  // WITH ... INSERT/UPDATE/DELETE (CTE-based DML)
  if (kw === "WITH" && /\)\s*(INSERT|UPDATE|DELETE)\b/i.test(sanitizeSql(sql)))
    return true;
  return false;
}

// Allowlist for the read-only query tools.
function isReadQuery(sql: string): boolean {
  const kw = effectiveKeyword(sql);
  if (
    kw === "SELECT" ||
    kw === "SHOW" ||
    kw === "DESCRIBE" ||
    kw === "DESC" ||
    kw === "EXPLAIN" ||
    kw === "VALUES"
  )
    return true;
  if (kw === "WITH") return !isWrite(sql);
  return false;
}

// Double-quote an identifier for safe interpolation into SQL.
function quoteId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

// Validate a name that is spliced into SQL without quoting.
function requireBareId(name: string, label: string): ToolResult | null {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    return err(`invalid ${label}: ${name}`);
  }
  return null;
}

// Validate that the SQL statement starts with one of the expected keywords.
function requireKeyword(
  sql: string,
  allowed: string[],
  toolName: string,
): ToolResult | null {
  const kw = firstKeyword(sql);
  if (!allowed.includes(kw)) {
    return err(
      `the ${toolName} tool only accepts ${allowed.join("/")} statements. Got: ${kw || "(empty)"}`,
    );
  }
  return null;
}

// ---------------------------------------------------------------------------
// Shared schemas
// ---------------------------------------------------------------------------

const paramValue = z.union([z.string(), z.number(), z.boolean(), z.null()]);

const paramsSchema = z
  .union([z.array(paramValue), z.record(z.string(), paramValue)])
  .optional()
  .describe(
    "Bind parameters: array for positional ($1, $2, ... or ?) or object for named (:key) placeholders",
  );

const paramsArraySchema = z
  .array(z.array(paramValue))
  .describe("Array of positional parameter arrays, one per row");

const sqlSchema = z.string().describe("SQL statement");

const tableSchema = z.string().describe("Table name");

// Tool annotations (MCP hints for clients).
const READ = { readOnlyHint: true, openWorldHint: false } as const;
const WRITE = {
  readOnlyHint: false,
  destructiveHint: true,
  openWorldHint: false,
} as const;
const ADDITIVE = {
  readOnlyHint: false,
  destructiveHint: false,
  openWorldHint: false,
} as const;

// ---------------------------------------------------------------------------
// Schema collection (shared by get_schema and the schema resource)
// ---------------------------------------------------------------------------

function firstColumn(row: Record<string, unknown>): string {
  return String(Object.values(row)[0]);
}

async function collectSchema() {
  const tables: Record<
    string,
    { columns: unknown; indexes: unknown; ddl: unknown }
  > = {};
  for (const row of await db.query("SHOW TABLES")) {
    const name = firstColumn(row);
    const q = quoteId(name);
    tables[name] = {
      columns: await db.query(`DESCRIBE ${q}`),
      indexes: await db.query(`SHOW INDEXES FROM ${q}`),
      ddl: await db.query(`SHOW CREATE TABLE ${q}`),
    };
  }
  const views: Record<string, { ddl: unknown }> = {};
  for (const row of await db.query("SHOW VIEWS")) {
    const name = firstColumn(row);
    views[name] = {
      ddl: await db.query(`SHOW CREATE VIEW ${quoteId(name)}`),
    };
  }
  return { tables, views };
}

// SHOW CREATE TABLE/VIEW return [{ Table, "Create Table" }] and
// [{ View, "Create View" }]: the DDL is the second column.
function ddlText(rows: Record<string, unknown>[], name: string): string {
  const row = rows[0];
  if (!row) return `-- ${name}`;
  const values = Object.values(row);
  return String(values[1] ?? values[0]);
}

async function buildSchemaText(): Promise<string> {
  try {
    let out = "";
    for (const row of await db.query("SHOW TABLES")) {
      const name = firstColumn(row);
      const q = quoteId(name);
      out += `${ddlText(await db.query(`SHOW CREATE TABLE ${q}`), name)};\n`;
      const indexes = await db.query(`SHOW INDEXES FROM ${q}`);
      if (indexes.length > 0) {
        out += `-- Indexes: ${JSON.stringify(indexes)}\n`;
      }
      out += "\n";
    }
    for (const row of await db.query("SHOW VIEWS")) {
      const name = firstColumn(row);
      out += `${ddlText(await db.query(`SHOW CREATE VIEW ${quoteId(name)}`), name)};\n\n`;
    }
    return out === "" ? "-- (no tables or views)\n" : out;
  } catch {
    return "-- (unable to read schema)\n";
  }
}

// ---------------------------------------------------------------------------
// MCP Server
// ---------------------------------------------------------------------------

const server = new McpServer(
  { name: "stoolap", version: PKG_VERSION },
  {
    instructions: `Stoolap is an embedded SQL database (engine 0.4.x). Use the provided MCP tools to interact with it.

Tool routing:
- "query" for SELECT/SHOW/DESCRIBE/EXPLAIN/VALUES and WITH ... SELECT. "execute" for INSERT/UPDATE/DELETE/COPY, DDL, SET, ANALYZE, VACUUM. "execute_batch" for bulk inserts.
- Statements with a RETURNING clause return rows through "execute".
- One statement per call. Transaction control (BEGIN/COMMIT/ROLLBACK/SAVEPOINT) only through the transaction tools.
- Call "get_schema" or "describe_table" before writing queries. "list_functions" lists every SQL function with its signature.

SQL essentials:
- Data types: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N). No BLOB, ARRAY, ENUM, DATE-only or INTERVAL column types.
- Parameters: positional $1, $2 (or ?) with array params, named :key with object params.
- Upsert: INSERT ... ON CONFLICT (cols) DO UPDATE SET col = EXCLUDED.col [WHERE ...] | DO NOTHING, or MySQL-style ON DUPLICATE KEY UPDATE. Use EXCLUDED.col, never VALUES(col).
- Joins: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL. Subqueries: scalar, IN, EXISTS, ANY/SOME/ALL, correlated, derived tables.
- CTEs: WITH, WITH RECURSIVE (UNION ALL only, max 10,000 iterations), also before INSERT/UPDATE/DELETE.
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, plus every aggregate with OVER. Frames: ROWS/RANGE. Named windows via WINDOW w AS (...).
- Aggregates support DISTINCT and FILTER (WHERE ...). GROUP BY ROLLUP/CUBE/GROUPING SETS with GROUPING(). SELECT DISTINCT ON (...) is supported.
- Set operations: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL].
- Operators: arithmetic, comparison, AND/OR/XOR/NOT, bitwise, ||, LIKE [ESCAPE]/ILIKE/GLOB/REGEXP, BETWEEN, IN, IS [NOT] DISTINCT FROM, JSON -> and ->>, vector <=>, INTERVAL arithmetic. Cast with CAST(x AS type); the :: syntax is not supported.
- NULL = NULL yields NULL. Use IS NOT DISTINCT FROM for NULL-safe equality.
- Indexes: BTREE (range/sort), HASH (equality), BITMAP (boolean), HNSW (vector k-NN). Auto-selected from the column type. DROP INDEX name ON table.
- Vectors: insert as '[0.1, 0.2, 0.3]'. k-NN: ORDER BY VEC_DISTANCE_L2|COSINE|IP(col, '[...]') LIMIT k. The HNSW index metric must match the distance function.
- Transactions: READ COMMITTED (default) or SNAPSHOT isolation, SAVEPOINT / ROLLBACK TO SAVEPOINT. DDL, TRUNCATE and COPY cannot run inside a transaction.
- Temporal: SELECT ... FROM t AS OF TIMESTAMP '...' | AS OF TRANSACTION n. VACUUM discards history.
- Bulk load: COPY table [(cols)] FROM '/path/file.csv' WITH (FORMAT CSV, HEADER true) reads a file on the server host.
- PRAGMA (via the "pragma" tool): checkpoint_interval, compact_threshold, target_volume_rows, keep_snapshots are read/write; sync_mode and wal_flush_trigger are read-only (set in the DSN); snapshot, checkpoint, restore, vacuum, volume_stats are actions.

Limitations (do NOT attempt these):
- No stored procedures, triggers, user-defined functions, GRANT/REVOKE, full-text search, materialized views, LISTEN/NOTIFY.
- UPDATE cannot change a PRIMARY KEY column (use DELETE + INSERT). UPDATE ... FROM is not supported.
- Foreign keys are single-column only (cascade depth 16). Composite PRIMARY KEY and table-level CHECK are fine.
- JSON: no JSON_SET/JSON_INSERT/JSON_REPLACE/JSON_REMOVE/JSON_CONTAINS. Modify JSON in the application and UPDATE the column.
- Views are read-only. AS OF cannot be combined with subqueries, and AS OF TRANSACTION does not work on tables whose rows were sealed into cold volumes.
- No GROUPS frames, no EXCLUDE clause, no WITH inside a subquery, one named window per WINDOW clause.
- TRUNCATE is never rolled back.
- Attach the "sql-assistant" prompt (or read stoolap://sql-reference) for the complete reference with the live schema.`,
  },
);

// ========================== QUERY & EXECUTE TOOLS ==========================

server.registerTool(
  "query",
  {
    title: "Run a read-only query",
    description:
      "Run a read-only SQL statement: SELECT, SHOW, DESCRIBE, EXPLAIN, VALUES, WITH ... SELECT and set operations (UNION, INTERSECT, EXCEPT). Returns rows as a JSON array of objects. Runs inside the active transaction if one is open.",
    inputSchema: { sql: z.string().describe("SQL query"), params: paramsSchema },
    annotations: READ,
  },
  async ({ sql, params }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    if (!isReadQuery(sql)) {
      return err(
        "query only accepts read-only statements (SELECT, SHOW, DESCRIBE, EXPLAIN, VALUES). Use execute for writes.",
      );
    }
    try {
      return json(await db.query(sql, params));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "execute",
  {
    title: "Execute a write statement",
    description:
      "Execute a statement that modifies data, schema or engine state: INSERT, UPDATE, DELETE, COPY ... FROM, CREATE, ALTER, DROP, TRUNCATE, SET, ANALYZE, VACUUM. Supports upsert (ON CONFLICT / ON DUPLICATE KEY UPDATE). Returns the rows when a RETURNING clause is present, otherwise the affected row count. Not allowed while a transaction is active (use transaction_execute).",
    inputSchema: { sql: sqlSchema, params: paramsSchema },
    annotations: WRITE,
  },
  async ({ sql, params }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    if (txActive) return txActiveErr();
    if (isReadQuery(sql)) {
      return err("execute does not run read-only statements. Use the query tool.");
    }
    if (readOnly && isWrite(sql)) return readOnlyErr();
    try {
      if (hasReturning(sql)) return json(await db.query(sql, params));
      const result = await db.execute(sql, params);
      return ok(isDDL(sql) ? "OK" : `Affected rows: ${result.changes}`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "execute_batch",
  {
    title: "Execute a statement for many parameter sets",
    description:
      "Execute the same SQL once per parameter set inside a single atomic transaction. The SQL is parsed once and reused. All rows succeed or all are rolled back. Returns the total affected row count.",
    inputSchema: {
      sql: z.string().describe("SQL statement with $1, $2, ... placeholders"),
      params_array: paramsArraySchema,
    },
    annotations: WRITE,
  },
  async ({ sql, params_array }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    if (txActive) return txActiveErr();
    if (readOnly) return readOnlyErr();
    if (params_array.length === 0) return err("params_array is empty");
    try {
      const result = db.executeBatchSync(sql, params_array);
      return ok(`Affected rows: ${result.changes}`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "explain",
  {
    title: "Show a query plan",
    description:
      "Show the execution plan for a statement. With analyze=true the statement is executed and actual row counts and timings are reported; analyze is refused for write statements because it would apply them.",
    inputSchema: {
      sql: z.string().describe("SQL statement to explain (without the EXPLAIN keyword)"),
      analyze: z
        .boolean()
        .optional()
        .describe("If true, use EXPLAIN ANALYZE (executes the statement)"),
      params: paramsSchema,
    },
    annotations: READ,
  },
  async ({ sql, analyze, params }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    // Tolerate an EXPLAIN [ANALYZE] prefix in the input.
    const body = sql
      .trimStart()
      .replace(/^EXPLAIN\b\s*/i, "")
      .replace(/^ANALYZE\b\s*/i, "");
    if (isWrite(body) && (analyze || readOnly)) {
      return err(
        analyze
          ? "EXPLAIN ANALYZE on a write statement executes it. Use analyze=false, or run the statement with execute."
          : "server is in read-only mode",
      );
    }
    try {
      const prefix = analyze ? "EXPLAIN ANALYZE" : "EXPLAIN";
      return json(await db.query(`${prefix} ${body}`, params));
    } catch (e) {
      return fail(e);
    }
  },
);

// ========================== TRANSACTION TOOLS ==========================

server.registerTool(
  "begin_transaction",
  {
    title: "Begin a transaction",
    description:
      "Begin a transaction on the server's connection. Only one transaction can be active at a time; transaction_execute and transaction_query run inside it until commit_transaction or rollback_transaction. READ COMMITTED (default) lets each statement see the latest committed data; SNAPSHOT freezes the view at BEGIN time.",
    inputSchema: {
      isolation: z
        .enum(["read_committed", "snapshot"])
        .optional()
        .describe("Isolation level (default: read_committed)"),
    },
    annotations: ADDITIVE,
  },
  async ({ isolation }) => {
    if (txActive) {
      return err("a transaction is already active. Commit or rollback first.");
    }
    try {
      await db.exec(
        isolation === "snapshot"
          ? "BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT"
          : "BEGIN TRANSACTION",
      );
      txActive = true;
      return ok(`Transaction started (isolation: ${isolation ?? "read_committed"})`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "transaction_execute",
  {
    title: "Execute inside the transaction",
    description:
      "Execute INSERT, UPDATE or DELETE inside the active transaction. Sees the transaction's uncommitted changes. Returns rows for a RETURNING clause, otherwise the affected row count. DDL, TRUNCATE and COPY are refused: run them with execute outside a transaction.",
    inputSchema: { sql: sqlSchema, params: paramsSchema },
    annotations: WRITE,
  },
  async ({ sql, params }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    if (!txActive) return noTxErr();
    if (isReadQuery(sql)) {
      return err("transaction_execute does not run read-only statements. Use transaction_query.");
    }
    if (readOnly && isWrite(sql)) return readOnlyErr();
    if (isDDL(sql)) {
      return err(
        "DDL (CREATE/ALTER/DROP/TRUNCATE) is not rolled back reliably inside a transaction. Run it with the execute tool outside a transaction.",
      );
    }
    try {
      if (hasReturning(sql)) return json(await db.query(sql, params));
      const result = await db.execute(sql, params);
      return ok(`Affected rows: ${result.changes}`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "transaction_query",
  {
    title: "Query inside the transaction",
    description:
      "Run a read-only statement inside the active transaction. Sees the transaction's uncommitted changes.",
    inputSchema: { sql: z.string().describe("SQL query"), params: paramsSchema },
    annotations: READ,
  },
  async ({ sql, params }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    if (!txActive) return noTxErr();
    if (!isReadQuery(sql)) {
      return err(
        "transaction_query only accepts read-only statements. Use transaction_execute for writes.",
      );
    }
    try {
      return json(await db.query(sql, params));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "transaction_execute_batch",
  {
    title: "Batch execute inside the transaction",
    description:
      "Execute the same SQL once per parameter set inside the active transaction. The rows are committed or rolled back together with the transaction. Stops at the first failing row.",
    inputSchema: {
      sql: z.string().describe("SQL statement with $1, $2, ... placeholders"),
      params_array: paramsArraySchema,
    },
    annotations: WRITE,
  },
  async ({ sql, params_array }) => {
    const bad = rejectMultiStatement(sql) ?? rejectTxControl(sql);
    if (bad) return bad;
    if (!txActive) return noTxErr();
    if (readOnly) return readOnlyErr();
    if (isDDL(sql)) return err("DDL is not allowed inside a transaction.");
    try {
      let total = 0;
      for (const params of params_array) {
        total += db.executeSync(sql, params).changes;
      }
      return ok(`Affected rows: ${total}`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "commit_transaction",
  {
    title: "Commit the transaction",
    description:
      "Commit the active transaction. All changes become permanent and visible to other connections.",
    inputSchema: {},
    annotations: ADDITIVE,
  },
  async () => {
    if (!txActive) return noTxErr();
    try {
      await db.exec("COMMIT");
      return ok("Transaction committed");
    } catch (e) {
      return fail(e);
    } finally {
      txActive = false;
    }
  },
);

server.registerTool(
  "rollback_transaction",
  {
    title: "Rollback the transaction",
    description: "Rollback the active transaction. All changes are discarded.",
    inputSchema: {},
    annotations: ADDITIVE,
  },
  async () => {
    if (!txActive) return noTxErr();
    try {
      await db.exec("ROLLBACK");
      return ok("Transaction rolled back");
    } catch (e) {
      return fail(e);
    } finally {
      txActive = false;
    }
  },
);

const savepointName = z.string().describe("Savepoint name (bare identifier)");

server.registerTool(
  "savepoint",
  {
    title: "Create a savepoint",
    description:
      "Create a savepoint inside the active transaction. rollback_to_savepoint undoes everything after it without ending the transaction.",
    inputSchema: { name: savepointName },
    annotations: ADDITIVE,
  },
  async ({ name }) => {
    if (!txActive) return noTxErr();
    const bad = requireBareId(name, "savepoint name");
    if (bad) return bad;
    try {
      await db.exec(`SAVEPOINT ${name}`);
      return ok(`Savepoint '${name}' created`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "rollback_to_savepoint",
  {
    title: "Rollback to a savepoint",
    description:
      "Undo all changes made after the savepoint. The transaction stays open and the savepoint remains usable.",
    inputSchema: { name: savepointName },
    annotations: ADDITIVE,
  },
  async ({ name }) => {
    if (!txActive) return noTxErr();
    const bad = requireBareId(name, "savepoint name");
    if (bad) return bad;
    try {
      await db.exec(`ROLLBACK TO SAVEPOINT ${name}`);
      return ok(`Rolled back to savepoint '${name}'`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "release_savepoint",
  {
    title: "Release a savepoint",
    description:
      "Remove a savepoint while keeping its changes. Savepoints are also discarded automatically on commit or rollback.",
    inputSchema: { name: savepointName },
    annotations: ADDITIVE,
  },
  async ({ name }) => {
    if (!txActive) return noTxErr();
    const bad = requireBareId(name, "savepoint name");
    if (bad) return bad;
    try {
      await db.exec(`RELEASE SAVEPOINT ${name}`);
      return ok(`Savepoint '${name}' released`);
    } catch (e) {
      return fail(e);
    }
  },
);

// ========================== SCHEMA INSPECTION TOOLS ==========================

server.registerTool(
  "list_tables",
  {
    title: "List tables",
    description: "List all tables in the database.",
    inputSchema: {},
    annotations: READ,
  },
  async () => {
    try {
      return json((await db.query("SHOW TABLES")).map(firstColumn));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "list_views",
  {
    title: "List views",
    description: "List all views in the database.",
    inputSchema: {},
    annotations: READ,
  },
  async () => {
    try {
      return json((await db.query("SHOW VIEWS")).map(firstColumn));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "describe_table",
  {
    title: "Describe a table",
    description:
      "Show the columns of a table: name, type, nullability, key, default and extra attributes (AUTO_INCREMENT, foreign keys).",
    inputSchema: { table: tableSchema },
    annotations: READ,
  },
  async ({ table }) => {
    try {
      return json(await db.query(`DESCRIBE ${quoteId(table)}`));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "show_create_table",
  {
    title: "Show CREATE TABLE",
    description:
      "Show the CREATE TABLE statement for a table, including constraints and foreign keys.",
    inputSchema: { table: tableSchema },
    annotations: READ,
  },
  async ({ table }) => {
    try {
      return json(await db.query(`SHOW CREATE TABLE ${quoteId(table)}`));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "show_create_view",
  {
    title: "Show CREATE VIEW",
    description: "Show the CREATE VIEW statement for a view.",
    inputSchema: { view: z.string().describe("View name") },
    annotations: READ,
  },
  async ({ view }) => {
    try {
      return json(await db.query(`SHOW CREATE VIEW ${quoteId(view)}`));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "show_indexes",
  {
    title: "Show indexes",
    description:
      "Show the indexes of a table: name, type (BTREE, HASH, BITMAP, HNSW, MULTICOLUMN, PRIMARYKEY), columns, uniqueness and options.",
    inputSchema: { table: tableSchema },
    annotations: READ,
  },
  async ({ table }) => {
    try {
      return json(await db.query(`SHOW INDEXES FROM ${quoteId(table)}`));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "get_schema",
  {
    title: "Get the full schema",
    description:
      "Get the complete database schema: every table with its columns, indexes and DDL, plus every view with its DDL. Call this first to understand the database before writing queries.",
    inputSchema: {},
    annotations: READ,
  },
  async () => {
    try {
      return json(await collectSchema());
    } catch (e) {
      return fail(e);
    }
  },
);

// ========================== SCHEMA MODIFICATION TOOLS ==========================

async function runDDL(
  sql: string,
  allowed: string[],
  toolName: string,
  success: string,
): Promise<ToolResult> {
  const bad =
    rejectMultiStatement(sql) ?? requireKeyword(sql, allowed, toolName);
  if (bad) return bad;
  if (readOnly) return readOnlyErr();
  if (txActive) return txActiveErr();
  try {
    await db.exec(sql);
    return ok(success);
  } catch (e) {
    return fail(e);
  }
}

server.registerTool(
  "create_table",
  {
    title: "Create a table",
    description:
      "Create a table. Column types: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N). Constraints: PRIMARY KEY (column or table-level composite), NOT NULL, UNIQUE, DEFAULT, CHECK (column or table-level), AUTO_INCREMENT, REFERENCES / FOREIGN KEY (single column, ON DELETE/UPDATE CASCADE | SET NULL | RESTRICT | NO ACTION). Supports IF NOT EXISTS and CREATE TABLE AS SELECT.",
    inputSchema: { sql: z.string().describe("Full CREATE TABLE statement") },
    annotations: ADDITIVE,
  },
  ({ sql }) => runDDL(sql, ["CREATE"], "create_table", "Table created"),
);

server.registerTool(
  "create_index",
  {
    title: "Create an index",
    description:
      "Create an index: CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table(col, ...) [USING BTREE | HASH | BITMAP | HNSW] [WITH (...)]. Default type follows the column: BTREE for INTEGER/FLOAT/TIMESTAMP, HASH for TEXT/JSON, BITMAP for BOOLEAN. HNSW options: m, ef_construction, ef_search, metric ('l2', 'cosine', 'ip').",
    inputSchema: { sql: z.string().describe("Full CREATE INDEX statement") },
    annotations: ADDITIVE,
  },
  ({ sql }) => runDDL(sql, ["CREATE"], "create_index", "Index created"),
);

server.registerTool(
  "create_view",
  {
    title: "Create a view",
    description:
      "Create a read-only view: CREATE VIEW [IF NOT EXISTS] name AS SELECT .... Views persist across restarts.",
    inputSchema: { sql: z.string().describe("Full CREATE VIEW statement") },
    annotations: ADDITIVE,
  },
  ({ sql }) => runDDL(sql, ["CREATE"], "create_view", "View created"),
);

server.registerTool(
  "alter_table",
  {
    title: "Alter a table",
    description:
      "Alter a table: ADD COLUMN, DROP COLUMN, RENAME COLUMN old TO new, MODIFY COLUMN col type [NOT NULL], RENAME TO new_name. MODIFY COLUMN does not validate existing rows.",
    inputSchema: { sql: z.string().describe("Full ALTER TABLE statement") },
    annotations: WRITE,
  },
  ({ sql }) => runDDL(sql, ["ALTER"], "alter_table", "Table altered"),
);

server.registerTool(
  "drop",
  {
    title: "Drop a table, view or index",
    description:
      "Drop an object: DROP TABLE [IF EXISTS] t, DROP VIEW [IF EXISTS] v, DROP INDEX [IF EXISTS] name ON table.",
    inputSchema: { sql: z.string().describe("Full DROP statement") },
    annotations: WRITE,
  },
  ({ sql }) => runDDL(sql, ["DROP"], "drop", "Dropped"),
);

// ========================== ADMIN TOOLS ==========================

server.registerTool(
  "analyze_table",
  {
    title: "Collect optimizer statistics",
    description:
      "Collect optimizer statistics for a table (histograms, distinct counts, min/max, null fraction). Run after bulk loads to improve query plans.",
    inputSchema: { table: tableSchema },
    annotations: { ...ADDITIVE, idempotentHint: true },
  },
  async ({ table }) => {
    if (readOnly) return readOnlyErr();
    if (txActive) return txActiveErr();
    try {
      await db.exec(`ANALYZE ${quoteId(table)}`);
      return ok(`Statistics collected for ${table}`);
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "vacuum",
  {
    title: "Vacuum",
    description:
      "Remove deleted rows and old MVCC versions and compact indexes, for one table or the whole database. This discards time-travel (AS OF) history.",
    inputSchema: {
      table: z.string().optional().describe("Table name (omit for the entire database)"),
    },
    annotations: WRITE,
  },
  async ({ table }) => {
    if (readOnly) return readOnlyErr();
    if (txActive) return txActiveErr();
    try {
      await db.exec(table ? `VACUUM ${quoteId(table)}` : "VACUUM");
      return ok(table ? `Vacuumed ${table}` : "Vacuumed entire database");
    } catch (e) {
      return fail(e);
    }
  },
);

// PRAGMA surface of engine 0.4.x.
const PRAGMA_SETTINGS = new Set([
  "checkpoint_interval",
  "snapshot_interval", // legacy alias of checkpoint_interval
  "compact_threshold",
  "target_volume_rows",
  "keep_snapshots",
]);
const PRAGMA_READONLY = new Set(["sync_mode", "wal_flush_trigger", "volume_stats"]);
const PRAGMA_ACTIONS = new Set(["snapshot", "checkpoint", "restore", "vacuum"]);

server.registerTool(
  "pragma",
  {
    title: "Read or change engine settings",
    description:
      "Read or set an engine setting, or run a maintenance action. Settings (read/write, numeric): checkpoint_interval (seconds between checkpoint cycles), compact_threshold (sub-target volumes before compaction), target_volume_rows (rows per cold volume, min 65536), keep_snapshots (backup snapshots retained). Read-only: sync_mode (0 none, 1 normal, 2 full), wal_flush_trigger, volume_stats (per-volume storage statistics). Actions: snapshot (write a backup snapshot), checkpoint (seal hot rows, truncate the WAL), vacuum (cleanup, returns counts), restore (replace ALL data with a backup snapshot; optional value = snapshot timestamp 'YYYYMMDD-HHMMSS.fff', latest when omitted).",
    inputSchema: {
      name: z.string().describe("PRAGMA name"),
      value: z
        .union([z.string(), z.number()])
        .optional()
        .describe("Value to set (omit to read). Numeric for settings, a snapshot timestamp for restore."),
    },
    annotations: WRITE,
  },
  async ({ name, value }) => {
    const pragma = name.toLowerCase();
    const bad = requireBareId(pragma, "pragma name");
    if (bad) return bad;
    const known =
      PRAGMA_SETTINGS.has(pragma) ||
      PRAGMA_READONLY.has(pragma) ||
      PRAGMA_ACTIONS.has(pragma);
    if (!known) {
      return err(
        `unknown pragma '${name}'. Known: ${[...PRAGMA_SETTINGS, ...PRAGMA_READONLY, ...PRAGMA_ACTIONS].join(", ")}`,
      );
    }
    const writes = value !== undefined || PRAGMA_ACTIONS.has(pragma);
    if (readOnly && writes) return readOnlyErr();
    if (txActive && writes) return txActiveErr();
    try {
      let sql = `PRAGMA ${pragma}`;
      if (value !== undefined) {
        if (pragma === "restore") {
          const ts = String(value);
          if (!/^\d{8}-\d{6}\.\d{3}$/.test(ts)) {
            return err("restore value must be a snapshot timestamp like 20260315-120000.000");
          }
          sql += ` = '${ts}'`;
        } else if (PRAGMA_SETTINGS.has(pragma)) {
          const n = Number(value);
          if (String(value).trim() === "" || !Number.isFinite(n)) {
            return err(`pragma ${pragma} value must be numeric`);
          }
          sql += ` = ${n}`;
        } else {
          return err(`pragma ${pragma} does not accept a value`);
        }
      }
      return json(await db.query(sql));
    } catch (e) {
      return fail(e);
    }
  },
);

server.registerTool(
  "version",
  {
    title: "Engine version",
    description: "Get the Stoolap engine version and build info, plus this server's version.",
    inputSchema: {},
    annotations: { ...READ, idempotentHint: true },
  },
  async () => {
    try {
      const rows = await db.query("SELECT VERSION() AS version");
      return ok(`${String(rows[0]?.version ?? "unknown")}\nstoolap-mcp ${PKG_VERSION}`);
    } catch (e) {
      return fail(e);
    }
  },
);

// ========================== FUNCTION REFERENCE ==========================

const FUNCTION_SECTIONS: Record<string, string> = {
  aggregate: `## Aggregate Functions (17)
COUNT(*) - count all rows
COUNT(expr) - count non-NULL values
COUNT(DISTINCT expr) - count distinct non-NULL values
SUM(expr) - sum (NULL for an empty set; booleans count as 1/0)
AVG(expr) - arithmetic mean
MIN(expr) / MAX(expr) - minimum / maximum (any type)
FIRST(expr) / LAST(expr) - first / last value in the group (order-dependent)
MEDIAN(expr) - 50th percentile (average of the two middle values for even counts)
STRING_AGG(expr, delimiter) - concatenate with delimiter
GROUP_CONCAT(expr, delimiter) - alias for STRING_AGG
ARRAY_AGG(expr) - collect values into a JSON array
STDDEV(expr) / STDDEV_SAMP(expr) - sample standard deviation (N-1, NULL for a single value)
STDDEV_POP(expr) - population standard deviation (N)
VARIANCE(expr) / VAR_SAMP(expr) - sample variance (N-1)
VAR_POP(expr) - population variance (N)

Modifiers: DISTINCT inside any aggregate; FILTER (WHERE cond) after any aggregate; all aggregates work as window functions with OVER.
GROUP BY extensions: ROLLUP(a, b), CUBE(a, b), GROUPING SETS((a, b), (a), ()); GROUPING(col) returns 1 on super-aggregate rows.`,

  window: `## Window Functions (11)
ROW_NUMBER() OVER (...) - sequential number within the partition
RANK() OVER (...) - rank with gaps for ties
DENSE_RANK() OVER (...) - rank without gaps
NTILE(n) OVER (...) - distribute rows into n buckets
LEAD(expr [, offset [, default]]) OVER (...) - value from a following row
LAG(expr [, offset [, default]]) OVER (...) - value from a preceding row
FIRST_VALUE(expr) / LAST_VALUE(expr) / NTH_VALUE(expr, n) OVER (...) - values from the window frame
PERCENT_RANK() OVER (...) - relative rank 0..1
CUME_DIST() OVER (...) - cumulative distribution 0..1

OVER ([PARTITION BY cols] [ORDER BY cols [NULLS FIRST|LAST]] [frame])
Frame: ROWS | RANGE BETWEEN {UNBOUNDED PRECEDING | n PRECEDING | CURRENT ROW} AND {CURRENT ROW | n FOLLOWING | UNBOUNDED FOLLOWING}
Named windows: SELECT ... OVER w FROM t WINDOW w AS (PARTITION BY x ORDER BY y); a named window can be extended: OVER (w ORDER BY z).
Not supported: GROUPS frames, EXCLUDE clause, more than one named window per WINDOW clause.`,

  string: `## String Functions (27)
UPPER(text) / LOWER(text) - change case
LENGTH(text) / CHAR_LENGTH(text) - length in characters (Unicode-aware)
CHAR(code) - character from a Unicode code point
CONCAT(a, b, ...) - concatenate (the || operator also works)
CONCAT_WS(separator, a, b, ...) - concatenate with separator, skipping NULLs
SUBSTRING(text, start [, length]) / SUBSTR(...) - 1-based substring (the FROM ... FOR syntax is not supported)
TRIM(text [, chars]) / LTRIM(text [, chars]) / RTRIM(text [, chars]) - strip whitespace or the given characters (no LEADING/TRAILING keywords)
REPLACE(text, from, to) - replace all occurrences
REVERSE(text) - reverse
LEFT(text, n) / RIGHT(text, n) - first / last n characters
REPEAT(text, n) - repeat n times
SPLIT_PART(text, delimiter, index) - 1-based part after splitting
POSITION(substr IN text) / STRPOS(text, substr) / INSTR(text, substr) / LOCATE(substr, text) - 1-based position, 0 if absent
LPAD(text, length, fill) / RPAD(text, length, fill) - pad to length
STARTS_WITH(text, prefix) / ENDS_WITH(text, suffix) / CONTAINS(text, substr) - BOOLEAN tests

Pattern matching: LIKE 'p' [ESCAPE 'c'] (case-sensitive, % and _), ILIKE (case-insensitive), GLOB (* ? [...]), REGEXP / RLIKE (regular expression). Each has a NOT form.`,

  math: `## Math Functions (22)
ABS(x), SIGN(x) - absolute value, sign (-1/0/1)
ROUND(x [, decimals]) - round half away from zero
FLOOR(x), CEILING(x) / CEIL(x) - round down / up
TRUNCATE(x, decimals) / TRUNC(x, decimals) - truncate to decimals
MOD(x, y) - remainder (also x % y)
POWER(base, exp) / POW(base, exp), SQRT(x), EXP(x)
LOG(x) - base-10 logarithm; LOG(base, x) - logarithm with base
LOG10(x), LOG2(x), LN(x) - base-10, base-2, natural logarithm
PI(), RANDOM() - constants and a random float in [0, 1)
SIN(x), COS(x), TAN(x) - trigonometry in radians

Arithmetic: + - * / %. Bitwise: & | ^ ~ << >>.`,

  datetime: `## Date/Time Functions (18)
NOW() / CURRENT_TIMESTAMP - current timestamp (UTC)
CURRENT_DATE, CURRENT_TIME - current date / time (no parentheses)
DATE_TRUNC(unit, ts) - truncate to year, quarter, month, week, day, hour, minute, second
TIME_TRUNC(interval, ts) - bucket by a duration string such as '15m', '30m', '1h', '4h', '1d'
EXTRACT(field FROM ts) - YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, DOW (0=Sunday), ISODOW (1=Monday), DOY, WEEK, QUARTER, EPOCH
YEAR(ts), MONTH(ts), DAY(ts), HOUR(ts), MINUTE(ts), SECOND(ts) - shorthands for EXTRACT
DATE_ADD(ts, n [, unit]) / DATE_SUB(ts, n [, unit]) - add / subtract n units (default unit: day; units: year, month, week, day, hour, minute, second)
DATEDIFF(end, start) / DATE_DIFF(end, start) - whole days between two dates (end minus start)
TO_CHAR(ts, format) - format with YYYY, YY, MM, MON, MONTH, DD, DY, DAY, HH24, HH, HH12, MI, SS

INTERVAL arithmetic: ts + INTERVAL '7 days', NOW() - INTERVAL '24 hours' (units: second(s), minute(s), hour(s), day(s), week(s), month(s), year(s); month and year are approximate).
Typed literals: TIMESTAMP '2025-01-01 12:00:00', DATE '2025-01-01', TIME '12:00:00'. Strings in ISO 8601 and common formats are accepted where a timestamp is expected.
Timestamps are stored and returned in UTC with nanosecond precision.`,

  json: `## JSON Functions (8)
JSON_EXTRACT(json, '$.path') - value at a dot-notation path (array indexing: $.items[0])
JSON_ARRAY_LENGTH(json [, '$.path']) - number of elements
JSON_ARRAY(v1, v2, ...) - build an array
JSON_OBJECT(k1, v1, k2, v2, ...) - build an object
JSON_TYPE(json [, '$.path']) / JSON_TYPEOF(...) - object, array, string, number, boolean or null
JSON_VALID(text) - whether the text is valid JSON
JSON_KEYS(json) - array of object keys

Operators: col -> 'key' (returns JSON, chainable), col ->> 'key' (returns TEXT), array index via -> 0.
Not available: JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_REMOVE, JSON_CONTAINS, JSON_CONTAINS_PATH, indexes on JSON paths. To modify JSON, rewrite the whole value with UPDATE.`,

  hash: `## Hash Functions (6)
MD5(text), SHA1(text), SHA256(text), SHA384(text), SHA512(text) - hex digests (32/40/64/96/128 chars)
CRC32(text) - CRC-32 checksum as INTEGER`,

  conditional: `## Conditional Functions (4)
COALESCE(a, b, ...) - first non-NULL argument
NULLIF(a, b) - NULL when a = b, otherwise a
IFNULL(expr, default) - default when expr is NULL
IIF(condition, true_value, false_value) - inline if
CASE expr WHEN v THEN r ... ELSE d END and CASE WHEN cond THEN r ... ELSE d END`,

  type: `## Type, Comparison and Collation Functions (5)
CAST(expr AS INTEGER | FLOAT | TEXT | BOOLEAN | TIMESTAMP | JSON) - convert types (the :: syntax is not supported)
TYPEOF(expr) - 'INTEGER', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP', 'JSON', 'VECTOR' or 'NULL'
COLLATE(expr, 'BINARY' | 'NOCASE' | 'NOACCENT' | 'NUMERIC') - comparison / sort collation
GREATEST(a, b, ...) / LEAST(a, b, ...) - largest / smallest argument

NULL operators: IS [NOT] NULL, IS [NOT] DISTINCT FROM (NULL-safe equality), IS [NOT] TRUE/FALSE.`,

  vector: `## Vector Functions (6)
VEC_DISTANCE_L2(a, b) - Euclidean distance (0..inf)
VEC_DISTANCE_COSINE(a, b) - cosine distance, 1 - cosine similarity (0..2)
VEC_DISTANCE_IP(a, b) - negative inner product
VEC_DIMS(v) - dimension count
VEC_NORM(v) - L2 norm
VEC_TO_TEXT(v) - '[0.1, 0.2, ...]'
a <=> b - L2 distance operator
NULL in, NULL out. Mismatched dimensions raise an error.

VECTOR(N) columns store N little-endian f32 values. Insert as the string literal '[0.1, 0.2, 0.3]'. Results are returned as JSON arrays of numbers.

k-NN (optimizer-detected): SELECT id, VEC_DISTANCE_COSINE(emb, '[...]') AS dist FROM t ORDER BY dist LIMIT 10
HNSW index: CREATE INDEX idx ON t(emb) USING HNSW WITH (metric = 'cosine', m = 32, ef_construction = 200, ef_search = 128)
  metric: 'l2' (default), 'cosine', 'ip'. m 2-64, ef_construction 50-1000, ef_search 10-1000; defaults are chosen from the dimension count.
  The index is used only when the query's distance function matches its metric; otherwise the scan is brute force. Several HNSW indexes with different metrics may exist on one column.
  With a WHERE clause the index fetches extra candidates and post-filters, falling back to brute force when too few remain.

EMBED(text) (only in engine builds with the 'semantic' feature) returns a 384-dimension all-MiniLM-L6-v2 embedding computed locally. Compute it once in a CTE when it is reused: WITH q AS (SELECT EMBED('query') AS v) SELECT ... FROM docs, q ORDER BY VEC_DISTANCE_COSINE(emb, q.v) LIMIT 10`,

  system: `## System and Table-Valued Functions
VERSION() - engine version string
SLEEP(seconds) - pause (testing only)
EMBED(text) - semantic embedding, see the vector category

GENERATE_SERIES(start, stop [, step]) in the FROM clause (max 10,000,000 rows):
  SELECT * FROM GENERATE_SERIES(1, 10)
  SELECT * FROM GENERATE_SERIES(0.0, 1.0, 0.1)
  SELECT * FROM GENERATE_SERIES(TIMESTAMP '2025-01-01', TIMESTAMP '2025-12-31', INTERVAL '1 month')
  SELECT * FROM GENERATE_SERIES(1, 100) AS gs(value)`,
};

const FUNCTION_CATEGORIES = [
  "all",
  ...Object.keys(FUNCTION_SECTIONS),
] as [string, ...string[]];

server.registerTool(
  "list_functions",
  {
    title: "List SQL functions",
    description:
      "List the built-in SQL functions (127 in the default engine build) with signatures and notes, grouped by category.",
    inputSchema: {
      category: z
        .enum(FUNCTION_CATEGORIES)
        .optional()
        .describe("Category filter (default: all)"),
    },
    annotations: { ...READ, idempotentHint: true },
  },
  async ({ category }) => {
    const cat = category ?? "all";
    if (cat === "all") return ok(Object.values(FUNCTION_SECTIONS).join("\n\n"));
    return ok(FUNCTION_SECTIONS[cat]);
  },
);

// ========================== SQL REFERENCE ==========================

const SQL_REFERENCE = `## Stoolap SQL Reference (engine 0.4.x)

### Data Types
| Type | Description | Notes |
|------|-------------|-------|
| INTEGER | 64-bit signed integer | PRIMARY KEY, AUTO_INCREMENT |
| FLOAT | 64-bit floating point | Scientific notation accepted |
| TEXT | UTF-8 string | No length limit |
| BOOLEAN | true/false | Case-insensitive literals, 1/0 convertible |
| TIMESTAMP | UTC, nanosecond precision | ISO 8601 and common formats; returned as ISO strings |
| JSON | Validated JSON document | -> and ->> operators; returned as text |
| VECTOR(N) | Fixed-dimension f32 array | Insert as '[0.1, 0.2, 0.3]'; returned as a JSON array |
Not available: BLOB/BINARY, ARRAY, ENUM, DATE-only, INTERVAL as a column type (INTERVAL works in expressions).

### Parameter Binding
- Positional: $1, $2, ... or ? with array params
- Named: :key with object params {key: value}

### DML
- SELECT [DISTINCT [ON (exprs)]] cols FROM t [alias] [AS OF ...] [JOIN ...] [WHERE] [GROUP BY cols | ROLLUP(...) | CUBE(...) | GROUPING SETS(...)] [HAVING] [WINDOW w AS (...)] [ORDER BY col [ASC|DESC] [NULLS FIRST|LAST]] [LIMIT n [OFFSET m]]
- INSERT INTO t [(cols)] VALUES (...), (...) | SELECT ... [ON CONFLICT [(cols)] DO UPDATE SET col = EXCLUDED.col, ... [WHERE cond] | DO NOTHING] [RETURNING cols]
- INSERT ... ON DUPLICATE KEY UPDATE col = EXCLUDED.col (MySQL style, same semantics). There is no VALUES(col) function.
- INSERT with DEFAULT: INSERT INTO t VALUES (1, DEFAULT, 'x')
- UPDATE t SET col = expr, col = DEFAULT ... [WHERE] [RETURNING cols]. Primary key columns cannot be updated (DELETE + INSERT instead). UPDATE ... FROM is not supported.
- DELETE FROM t [WHERE] [RETURNING cols]
- WITH ... before SELECT, INSERT, UPDATE and DELETE
- TRUNCATE TABLE t (never rolled back, fails when FK children exist)
- COPY t [(cols)] FROM '/path/file' WITH (FORMAT CSV | JSON, HEADER true|false, DELIMITER '|', NULL 'text') - bulk load from a file on the server host; not inside a transaction
- Set operations: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL]; ORDER BY/LIMIT/OFFSET apply to the whole result
- VALUES (1,'a'), (2,'b') AS v(id, name) as an inline table

### DDL
- CREATE TABLE [IF NOT EXISTS] t (col type [constraints], ..., [PRIMARY KEY (a, b)], [CHECK (expr)], [FOREIGN KEY (col) REFERENCES parent(col) [ON DELETE action] [ON UPDATE action]])
  - Column constraints: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT literal | NULL | CURRENT_TIMESTAMP | NOW(), CHECK (expr), AUTO_INCREMENT, REFERENCES parent(col)
  - Composite PRIMARY KEY and table-level CHECK are supported. Foreign keys are single-column only.
  - FK actions: CASCADE, SET NULL, RESTRICT, NO ACTION. Cascades recurse up to 16 levels; RESTRICT anywhere in the chain blocks the whole operation. An index is created automatically on FK columns. NULL FK values are never checked.
- CREATE TABLE [IF NOT EXISTS] t AS SELECT ...
- ALTER TABLE t ADD COLUMN col type | DROP COLUMN col | RENAME COLUMN old TO new | MODIFY COLUMN col type [NOT NULL] | RENAME TO new_name (MODIFY COLUMN does not validate existing rows)
- DROP TABLE [IF EXISTS] t
- CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON t(col1, col2) [USING BTREE | HASH | BITMAP | HNSW] [WITH (params)]
- DROP INDEX [IF EXISTS] name ON t (the ON clause is required)
- CREATE VIEW [IF NOT EXISTS] name AS SELECT ... / DROP VIEW [IF EXISTS] name (views are read-only, nest up to 32 levels, share the table namespace)

### Joins
INNER, LEFT [OUTER], RIGHT [OUTER], FULL OUTER, CROSS, NATURAL [LEFT|RIGHT] JOIN with ON cond or USING (cols). Self-joins and multi-table joins are fine. The optimizer picks hash, merge, index nested loop or nested loop joins. A correlated subquery inside JOIN ON is not supported.

### Subqueries and CTEs
- Scalar, IN / NOT IN, EXISTS / NOT EXISTS, ANY / SOME / ALL, derived tables, correlated (in SELECT, WHERE, HAVING, ORDER BY, CASE, aggregate arguments)
- WITH name [(cols)] AS (...), other AS (...) SELECT ...; CTEs referenced several times are materialized once
- WITH RECURSIVE name AS (anchor UNION ALL recursive) SELECT ... (UNION without ALL is rejected; max 10,000 iterations)
- Not supported: WITH inside a subquery

### Window Functions
ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE(n), LEAD(expr [, offset [, default]]), LAG(...), FIRST_VALUE(expr), LAST_VALUE(expr), NTH_VALUE(expr, n), PERCENT_RANK(), CUME_DIST(); every aggregate works with OVER.
OVER ([PARTITION BY cols] [ORDER BY cols] [ROWS | RANGE BETWEEN ... AND ...]); WINDOW w AS (...) with OVER w or OVER (w ORDER BY ...). GROUPS frames and EXCLUDE are not supported.

### Aggregates
COUNT(*), COUNT(expr), SUM, AVG, MIN, MAX, FIRST, LAST, MEDIAN, STRING_AGG(expr, delim), GROUP_CONCAT, ARRAY_AGG, STDDEV / STDDEV_SAMP (sample), STDDEV_POP (population), VARIANCE / VAR_SAMP (sample), VAR_POP (population). All accept DISTINCT and FILTER (WHERE cond).
GROUP BY ROLLUP(a, b), CUBE(a, b), GROUPING SETS((a, b), (a), ()); GROUPING(col) marks super-aggregate rows.

### Scalar Functions (98) - call list_functions for signatures
String (27): UPPER, LOWER, LENGTH, CHAR_LENGTH, CHAR, CONCAT, CONCAT_WS, SUBSTRING, SUBSTR, TRIM, LTRIM, RTRIM, REPLACE, REVERSE, LEFT, RIGHT, REPEAT, SPLIT_PART, POSITION, STRPOS, INSTR, LOCATE, LPAD, RPAD, STARTS_WITH, ENDS_WITH, CONTAINS
Math (22): ABS, ROUND, FLOOR, CEILING/CEIL, MOD, POWER/POW, SQRT, LOG (base 10, or LOG(base, x)), LOG10, LOG2, LN, EXP, SIGN, TRUNCATE/TRUNC, PI, RANDOM, SIN, COS, TAN
Date/time (18): NOW, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATE_TRUNC, TIME_TRUNC, EXTRACT, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD(ts, n [, unit]), DATE_SUB, DATEDIFF(end, start) in days, DATE_DIFF, TO_CHAR
JSON (8): JSON_EXTRACT, JSON_ARRAY_LENGTH, JSON_ARRAY, JSON_OBJECT, JSON_TYPE, JSON_TYPEOF, JSON_VALID, JSON_KEYS
Hash (6): MD5, SHA1, SHA256, SHA384, SHA512, CRC32
Conditional (4): COALESCE, NULLIF, IFNULL, IIF (plus CASE)
Type (5): CAST, TYPEOF, COLLATE, GREATEST, LEAST
Vector (6): VEC_DISTANCE_L2, VEC_DISTANCE_COSINE, VEC_DISTANCE_IP, VEC_DIMS, VEC_NORM, VEC_TO_TEXT (+ EMBED with the semantic feature)
System (2): VERSION, SLEEP. Table-valued (1): GENERATE_SERIES

### Operators
Comparison: = <> != < <= > >=. Logical: AND OR XOR NOT (three-valued). Arithmetic: + - * / %. Bitwise: & | ^ ~ << >>.
String: ||, LIKE [ESCAPE c], ILIKE, GLOB, REGEXP / RLIKE, each with NOT. Range: [NOT] BETWEEN, [NOT] IN (list or subquery).
NULL: IS [NOT] NULL, IS [NOT] DISTINCT FROM, IS [NOT] TRUE / FALSE. NULL = NULL is NULL; NULL propagates through expressions; aggregates skip NULLs except COUNT(*).
JSON: -> (JSON), ->> (TEXT), chainable. Vector: <=> (L2 distance). Typed literals: TIMESTAMP '...', DATE '...', TIME '...', INTERVAL '7 days'.
Casting only through CAST(expr AS type); the :: operator is not supported. Sorting: NULLs come last on ASC and first on DESC unless NULLS FIRST/LAST is given.

### Indexes
| Type | Best for | Default for | USING |
|------|----------|-------------|-------|
| BTREE | Range, equality, sorting | INTEGER, FLOAT, TIMESTAMP | BTREE |
| HASH | Equality, IN lists | TEXT, JSON | HASH |
| BITMAP | Low-cardinality | BOOLEAN | BITMAP |
| HNSW | Vector k-NN | - | HNSW |
Multiple single-column indexes are intersected/unioned by the optimizer. SHOW INDEXES reports BTREE, HASH, BITMAP, HNSW, MULTICOLUMN or PRIMARYKEY.
HNSW WITH (m, ef_construction, ef_search, metric): metric 'l2' (default) | 'cosine' | 'ip' must match the query's distance function.

### Transactions
- BEGIN [TRANSACTION] [ISOLATION LEVEL READ COMMITTED | SNAPSHOT | REPEATABLE READ | SERIALIZABLE]; COMMIT; ROLLBACK (START TRANSACTION and END are not recognised)
- READ COMMITTED (default): each statement sees the latest commits. SNAPSHOT: the whole transaction sees the state at BEGIN. REPEATABLE READ and SERIALIZABLE map to SNAPSHOT.
- SAVEPOINT name; ROLLBACK TO SAVEPOINT name; RELEASE SAVEPOINT name
- SET isolation_level = 'SNAPSHOT' changes the default for later transactions
- MVCC: readers never block writers; write conflicts surface at commit (or immediately for rows already sealed into cold volumes). A failed statement does not abort the transaction.
- DDL, TRUNCATE and COPY belong outside transactions: only CREATE TABLE is rolled back reliably; ALTER TABLE, CREATE INDEX and CREATE VIEW persist, DROP TABLE loses its rows, COPY is rejected.

### Temporal Queries (Time Travel)
- SELECT * FROM t AS OF TIMESTAMP '2025-01-01 00:00:00' | AS OF TRANSACTION 42, also on joined tables
- Cannot be combined with subqueries. AS OF TRANSACTION does not work on rows sealed into cold volumes. VACUUM removes history.

### Metadata
SHOW TABLES | SHOW VIEWS | SHOW INDEXES FROM t | SHOW CREATE TABLE t | SHOW CREATE VIEW v | DESCRIBE t | DESC t
EXPLAIN stmt | EXPLAIN ANALYZE stmt (executes the statement) | ANALYZE t (optimizer statistics) | VACUUM [t]

### PRAGMA
Read/write: checkpoint_interval (s, default 60), compact_threshold (default 4), target_volume_rows (default 1048576, min 65536), keep_snapshots (default 3)
Read-only: sync_mode (0 none, 1 normal, 2 full), wal_flush_trigger; set them in the DSN, e.g. file:///data/db?sync_mode=full
Actions: PRAGMA snapshot (backup), PRAGMA checkpoint (seal + compact + WAL truncate), PRAGMA restore [= 'YYYYMMDD-HHMMSS.fff'] (replaces all data, persistent databases only), PRAGMA vacuum, PRAGMA volume_stats
DSN-only options: wal_buffer_size, wal_max_size, wal_compression, volume_compression, compression, compression_threshold, checkpoint_on_close, commit_batch_size, sync_interval_ms, cleanup, cleanup_interval, deleted_row_retention, transaction_retention

### Known Limitations (do NOT attempt these)
- No stored procedures, triggers, user-defined functions, GRANT/REVOKE, full-text search, materialized views, LISTEN/NOTIFY
- No BLOB/BINARY, ARRAY, ENUM or INTERVAL column types
- JSON: no JSON_SET/INSERT/REPLACE/REMOVE/CONTAINS/CONTAINS_PATH, no indexes on JSON paths
- Foreign keys: single column only, max cascade depth 16
- UPDATE of a PRIMARY KEY column, UPDATE ... FROM, WITH inside a subquery, WITH RECURSIVE with plain UNION
- GROUPS window frames, EXCLUDE, several named windows in one WINDOW clause, correlated subquery inside JOIN ON, SELECT t.* ... ORDER BY other.col
- Views are read-only; AS OF cannot combine with subqueries; timestamps are UTC only (no time zone conversion); month/year intervals are approximate
- TRUNCATE cannot be rolled back; ALTER TABLE blocks concurrent writes; recursive CTEs stop at 10,000 iterations; GENERATE_SERIES stops at 10,000,000 rows

Available MCP tools: query, execute, execute_batch, explain, begin_transaction, transaction_execute, transaction_query, transaction_execute_batch, commit_transaction, rollback_transaction, savepoint, rollback_to_savepoint, release_savepoint, list_tables, list_views, describe_table, show_create_table, show_create_view, show_indexes, get_schema, create_table, create_index, create_view, alter_table, drop, analyze_table, vacuum, pragma, version, list_functions.`;

async function referenceWithSchema(): Promise<string> {
  const schemaText = await buildSchemaText();
  return `## Current Database Schema\n\n\`\`\`sql\n${schemaText}\`\`\`\n\n${SQL_REFERENCE}`;
}

// ========================== RESOURCES ==========================

server.registerResource(
  "schema",
  "stoolap://schema",
  {
    title: "Database schema",
    description:
      "Full database schema with all tables, views, columns, indexes, and DDL",
    mimeType: "application/json",
  },
  async (uri) => {
    try {
      return {
        contents: [
          {
            uri: uri.href,
            mimeType: "application/json",
            text: JSON.stringify(await collectSchema(), jsonReplacer, 2),
          },
        ],
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return {
        contents: [{ uri: uri.href, mimeType: "text/plain", text: `Error: ${msg}` }],
      };
    }
  },
);

server.registerResource(
  "sql-reference",
  "stoolap://sql-reference",
  {
    title: "Stoolap SQL reference",
    description:
      "Live database schema plus the complete Stoolap SQL reference: data types, functions, operators, joins, indexes, window functions, CTEs, transactions, temporal queries, vector search, and known limitations",
    mimeType: "text/markdown",
  },
  async (uri) => ({
    contents: [
      { uri: uri.href, mimeType: "text/markdown", text: await referenceWithSchema() },
    ],
  }),
);

// ========================== PROMPTS ==========================

server.registerPrompt(
  "sql-assistant",
  {
    title: "SQL assistant",
    description:
      "Injects the live database schema and the complete Stoolap SQL reference (data types, every function with its signature, operators, joins, indexes, window functions, CTEs, transactions, temporal queries, vector search, and known limitations).",
  },
  async () => ({
    messages: [
      {
        role: "user",
        content: {
          type: "text",
          text: `You are a SQL expert for a Stoolap database. Below is the live schema followed by the complete SQL reference. Write accurate, optimized queries using only supported features.\n\n${await referenceWithSchema()}`,
        },
      },
    ],
  }),
);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

let closing = false;

function shutdown(code = 0) {
  if (closing) return;
  closing = true;
  try {
    if (txActive) {
      db.execSync("ROLLBACK");
      txActive = false;
    }
  } catch {
    /* best effort */
  }
  try {
    db.closeSync();
  } catch {
    /* best effort */
  }
  process.exit(code);
}

process.on("SIGINT", () => shutdown(0));
process.on("SIGTERM", () => shutdown(0));
process.on("SIGHUP", () => shutdown(0));
// The client went away (stdin closed): close the database cleanly.
server.server.onclose = () => shutdown(0);

const transport = new StdioServerTransport();
await server.connect(transport);
console.error(
  `stoolap-mcp ${PKG_VERSION}: connected (path=${dbPath}, readOnly=${readOnly})`,
);
