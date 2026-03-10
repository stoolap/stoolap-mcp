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

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { Database } from "@stoolap/node";

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
let dbPath = ":memory:";
let readOnly = false;

for (let i = 0; i < args.length; i++) {
  if (args[i] === "--path" && i + 1 < args.length) {
    dbPath = args[++i];
  } else if (args[i] === "--read-only") {
    readOnly = true;
  } else if (args[i] === "--help" || args[i] === "-h") {
    console.error("Usage: stoolap-mcp [--path <database-path>] [--read-only]");
    console.error("");
    console.error("Options:");
    console.error("  --path <path>   Database path (default: :memory:)");
    console.error("  --read-only     Disable write operations");
    console.error("");
    console.error("Examples:");
    console.error("  stoolap-mcp --path ./mydata");
    console.error("  stoolap-mcp --path ./mydata --read-only");
    console.error("  stoolap-mcp  # in-memory database");
    process.exit(0);
  }
}

// ---------------------------------------------------------------------------
// Database and transaction state
// ---------------------------------------------------------------------------

const db = await Database.open(dbPath);

// SQL-based transactions for both isolation levels
let sqlTxActive = false;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type ToolResult = {
  content: Array<{ type: "text"; text: string }>;
  isError?: boolean;
};

function ok(text: string): ToolResult {
  return { content: [{ type: "text", text }] };
}

function fail(e: unknown): ToolResult {
  const msg = e instanceof Error ? e.message : String(e);
  return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
}

function json(data: unknown): ToolResult {
  return ok(JSON.stringify(data, null, 2));
}

function readOnlyErr(): ToolResult {
  return { content: [{ type: "text", text: "Error: server is in read-only mode" }], isError: true };
}

// Single-pass sanitizer: strips string literals, block comments, and line
// comments in one scan so that comment-like content inside literals (e.g.
// '--foo') and literal-like content inside comments never confuse each other.
function sanitizeSql(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    // Single-quoted string literal with '' escaping
    if (sql[i] === "'") {
      out += " ";
      i++;
      while (i < sql.length) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
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
    // Block comment /* ... */
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
    // Line comment -- ...
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

// Reject multi-statement SQL (semicolons outside comments and string literals)
function rejectMultiStatement(sql: string): ToolResult | null {
  const cleaned = sanitizeSql(sql);
  // Remove trailing whitespace and optional trailing semicolons
  const trimmed = cleaned.replace(/[\s;]+$/, "");
  if (trimmed.includes(";")) {
    return { content: [{ type: "text", text: "Error: multiple SQL statements are not allowed. Send one statement at a time." }], isError: true };
  }
  return null;
}

// Classify first keyword after sanitizing
function firstKeyword(sql: string): string {
  return sanitizeSql(sql).trimStart().split(/[\s(]/)[0].toUpperCase();
}

// Strip EXPLAIN [ANALYZE] prefix and return the underlying statement's first keyword
function effectiveKeyword(sql: string): string {
  const stripped = sanitizeSql(sql).trimStart();
  const upper = stripped.toUpperCase();
  if (upper.startsWith("EXPLAIN")) {
    let rest = stripped.slice(7).trimStart();
    if (rest.toUpperCase().startsWith("ANALYZE")) {
      rest = rest.slice(7).trimStart();
    }
    const kw = rest.split(/[\s(]/)[0].toUpperCase();
    return kw || "EXPLAIN";
  }
  return stripped.split(/[\s(]/)[0].toUpperCase();
}

function isDDL(sql: string): boolean {
  const kw = firstKeyword(sql);
  return kw === "CREATE" || kw === "DROP" || kw === "ALTER" || kw === "TRUNCATE";
}

function hasReturning(sql: string): boolean {
  return /\bRETURNING\b/i.test(sanitizeSql(sql));
}

// Double-quote an identifier for safe SQL interpolation, escaping inner " as ""
function quoteId(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

// Validate that a name is a bare SQL identifier (for savepoint/pragma names
// that are spliced into SQL without quoting)
function requireBareId(name: string, label: string): ToolResult | null {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    return { content: [{ type: "text", text: `Error: invalid ${label}: ${name}` }], isError: true };
  }
  return null;
}

// Check whether a statement is a write, looking past EXPLAIN [ANALYZE] prefixes
function isWrite(sql: string): boolean {
  const kw = effectiveKeyword(sql);
  if (
    kw === "INSERT" || kw === "UPDATE" || kw === "DELETE" ||
    kw === "CREATE" || kw === "DROP" || kw === "ALTER" || kw === "TRUNCATE" ||
    kw === "VACUUM" || kw === "ANALYZE" || kw === "PRAGMA" || kw === "SET"
  ) return true;
  // WITH ... INSERT/UPDATE/DELETE (CTE-based DML)
  if (kw === "WITH" && /\)\s*(INSERT|UPDATE|DELETE)\b/i.test(sanitizeSql(sql))) return true;
  return false;
}

// Allowlist: only these statement types are accepted by the query tool.
// Uses effectiveKeyword to look past EXPLAIN [ANALYZE] prefixes.
function isReadQuery(sql: string): boolean {
  const kw = effectiveKeyword(sql);
  if (
    kw === "SELECT" || kw === "SHOW" || kw === "DESCRIBE" ||
    kw === "DESC" || kw === "EXPLAIN" || kw === "VALUES"
  ) return true;
  // WITH ... SELECT (CTE query) — allowed only when the main statement is NOT a write
  if (kw === "WITH") return !isWrite(sql);
  return false;
}

function txActiveErr(): ToolResult {
  return { content: [{ type: "text", text: "Error: a transaction is active. Use transaction_execute/transaction_query within a transaction, or commit/rollback first." }], isError: true };
}

// Param schemas
const paramsSchema = z
  .union([
    z.array(z.union([z.string(), z.number(), z.boolean(), z.null()])),
    z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])),
  ])
  .optional()
  .describe("Parameters: array for positional ($1, $2, ...) or object for named (:key) binding");

// ---------------------------------------------------------------------------
// MCP Server
// ---------------------------------------------------------------------------

const server = new McpServer(
  {
    name: "stoolap",
    version: "0.1.0",
  },
  {
    instructions: `Stoolap is an embedded SQL database. Use the provided MCP tools to interact with it.

Key guidance:
- Data types: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N). No BLOB, ARRAY, ENUM, or INTERVAL column types.
- Parameter binding: positional $1, $2 (or ?) with array params, named :key with object params.
- Use the "query" tool for SELECT/SHOW/DESCRIBE. Use "execute" for INSERT/UPDATE/DELETE. Use "execute_batch" for bulk inserts.
- RETURNING clause is supported on INSERT, UPDATE, DELETE (routed through "query" since it returns rows).
- Joins: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL. Optimizer auto-selects algorithm.
- Subqueries: scalar, IN/NOT IN, EXISTS/NOT EXISTS, ANY/SOME/ALL, correlated, derived tables.
- CTEs: WITH, WITH RECURSIVE (max 10,000 iterations), multiple CTEs, column aliases.
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST. All aggregates work with OVER.
- GROUP BY extensions: ROLLUP, CUBE, GROUPING SETS, GROUPING() discriminator.
- Aggregates (17): COUNT, SUM, AVG, MIN, MAX, FIRST, LAST, MEDIAN, STRING_AGG, GROUP_CONCAT, ARRAY_AGG, STDDEV/STDDEV_POP, STDDEV_SAMP, VARIANCE/VAR_POP, VAR_SAMP. All support DISTINCT.
- 100+ scalar functions across: string, math, date/time, JSON (-> and ->> operators), hash, conditional, type, vector, system categories. Use "list_functions" tool for full list.
- Operators: arithmetic, comparison, logical, bitwise, LIKE/ILIKE/GLOB/REGEXP, BETWEEN, IN, IS [NOT] DISTINCT FROM, JSON (->/->>), vector (<=>).
- NULL: NULL = NULL returns NULL (not TRUE). Use IS NOT DISTINCT FROM for NULL-safe equality.
- Index types: BTree (range/sort), Hash (equality), Bitmap (boolean), HNSW (vector k-NN). Auto-selected from column type.
- HNSW params: m, ef_construction, ef_search, metric (l2/cosine/ip). Index metric must match distance function.
- Vector: insert as '[0.1, 0.2, 0.3]'. k-NN: ORDER BY VEC_DISTANCE_L2(col, '[...]') LIMIT k.
- Transactions: BEGIN with READ COMMITTED (default) or SNAPSHOT isolation. SAVEPOINT/ROLLBACK TO/RELEASE supported.
- Temporal queries: SELECT * FROM t AS OF TIMESTAMP '...' or AS OF TRANSACTION n.
- EXPLAIN / EXPLAIN ANALYZE for query plans.
- PRAGMA for settings. VACUUM for cleanup. ANALYZE for optimizer statistics.
- Set operations: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL].
- GENERATE_SERIES(start, stop [, step]) as table-valued function (max 10M rows).

Limitations (do NOT attempt these):
- No stored procedures, triggers, or user-defined functions.
- No GRANT/REVOKE, no full-text search, no materialized views, no LISTEN/NOTIFY.
- JSON: no JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_REMOVE, JSON_CONTAINS.
- Foreign keys: single-column only, max 16-level cascade.
- Views: read-only. CTEs in UPDATE/DELETE not supported.
- TRUNCATE cannot be rolled back.
- Use "get_schema" or "describe_table" to inspect the database before writing queries.
- Use "list_functions" to see all available SQL functions with signatures.
- Attach the "sql-assistant" prompt for the complete reference with the live schema.`,
  }
);

// ========================== QUERY & EXECUTE TOOLS ==========================

server.tool(
  "query",
  "Run a read-only SQL query. Supports SELECT, SHOW, DESCRIBE, EXPLAIN, and set operations (UNION, INTERSECT, EXCEPT). Returns results as JSON array of objects.",
  { sql: z.string().describe("SQL query"), params: paramsSchema },
  async ({ sql, params }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (!isReadQuery(sql)) {
      return { content: [{ type: "text", text: "Error: query tool only accepts read-only statements (SELECT, SHOW, DESCRIBE, EXPLAIN). Use the execute tool for writes." }], isError: true };
    }
    try {
      const rows = await db.query(sql, params);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "execute",
  "Execute a SQL statement that modifies data or schema. Supports INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE, and ON DUPLICATE KEY UPDATE (upsert). If RETURNING clause is present, returns the rows. Otherwise returns affected row count.",
  { sql: z.string().describe("SQL statement"), params: paramsSchema },
  async ({ sql, params }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (sqlTxActive) return txActiveErr();
    if (readOnly && isWrite(sql)) return readOnlyErr();
    try {
      // RETURNING clause should return rows, not count
      if (hasReturning(sql)) {
        const rows = await db.query(sql, params);
        return json(rows);
      }
      if (isDDL(sql)) {
        await db.exec(sql);
        return ok("OK");
      }
      const result = await db.execute(sql, params);
      return ok(`Affected rows: ${result.changes}`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "execute_batch",
  "Execute the same SQL with multiple parameter sets in a single atomic transaction. Parses SQL once, reuses for every row. All rows succeed or all are rolled back. Returns total affected rows.",
  {
    sql: z.string().describe("SQL statement with $1, $2, ... placeholders"),
    params_array: z
      .array(z.array(z.union([z.string(), z.number(), z.boolean(), z.null()])))
      .describe("Array of parameter arrays, one per row"),
  },
  async ({ sql, params_array }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (sqlTxActive) return txActiveErr();
    if (readOnly) return readOnlyErr();
    try {
      const result = db.executeBatchSync(sql, params_array);
      return ok(`Affected rows: ${result.changes}`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "explain",
  "Show the query execution plan. Use analyze=true to run the query and show actual runtime statistics (row counts, timing, join algorithm chosen).",
  {
    sql: z.string().describe("SQL query to explain"),
    analyze: z.boolean().optional().describe("If true, use EXPLAIN ANALYZE (executes the query)"),
    params: paramsSchema,
  },
  async ({ sql, analyze, params }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (readOnly && isWrite(sql)) return readOnlyErr();
    if (analyze && isWrite(sql)) {
      return { content: [{ type: "text", text: "Error: EXPLAIN ANALYZE on write statements (INSERT/UPDATE/DELETE/DDL) executes the statement and mutates data. Use EXPLAIN (without analyze) to inspect the plan, or use the execute tool to run the statement." }], isError: true };
    }
    try {
      const prefix = analyze ? "EXPLAIN ANALYZE" : "EXPLAIN";
      const rows = await db.query(`${prefix} ${sql}`, params);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

// ========================== TRANSACTION TOOLS ==========================

server.tool(
  "begin_transaction",
  "Begin a new transaction. Only one transaction can be active at a time. All transaction_execute and transaction_query calls will run within this transaction until commit or rollback. Supports isolation levels: READ COMMITTED (default, each statement sees latest committed data) and SNAPSHOT (entire transaction sees consistent snapshot from BEGIN time).",
  {
    isolation: z
      .enum(["read_committed", "snapshot"])
      .optional()
      .describe("Isolation level (default: read_committed)"),
  },
  async ({ isolation }) => {
    if (sqlTxActive) {
      return { content: [{ type: "text", text: "Error: a transaction is already active. Commit or rollback first." }], isError: true };
    }
    try {
      if (isolation === "snapshot") {
        db.execSync("BEGIN TRANSACTION ISOLATION LEVEL SNAPSHOT");
      } else {
        db.execSync("BEGIN TRANSACTION");
      }
      sqlTxActive = true;
      return ok(`Transaction started (isolation: ${isolation ?? "read_committed"})`);
    } catch (e) {
      sqlTxActive = false;
      return fail(e);
    }
  }
);

server.tool(
  "transaction_execute",
  "Execute a DML statement (INSERT, UPDATE, DELETE) within the active transaction. Sees uncommitted changes. If RETURNING clause is present, returns the rows. DDL (CREATE/ALTER/DROP) is not transactional and must be run outside transactions.",
  { sql: z.string().describe("SQL statement"), params: paramsSchema },
  async ({ sql, params }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction. Call begin_transaction first." }], isError: true };
    }
    if (readOnly && isWrite(sql)) return readOnlyErr();
    if (isDDL(sql)) {
      return { content: [{ type: "text", text: "Error: DDL (CREATE/ALTER/DROP/TRUNCATE) is auto-committed and cannot run inside a transaction. Use the execute tool outside a transaction instead." }], isError: true };
    }
    try {
      if (hasReturning(sql)) {
        const rows = db.querySync(sql, params);
        return json(rows);
      }
      const result = db.executeSync(sql, params);
      return ok(`Affected rows: ${result.changes}`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "transaction_query",
  "Run a SELECT query within the active transaction. Sees uncommitted changes. Supports all SQL features: aggregates, JOINs, GROUP BY, window functions, CTEs, subqueries.",
  { sql: z.string().describe("SQL query"), params: paramsSchema },
  async ({ sql, params }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction. Call begin_transaction first." }], isError: true };
    }
    if (!isReadQuery(sql)) {
      return { content: [{ type: "text", text: "Error: transaction_query only accepts read-only statements. Use transaction_execute for writes." }], isError: true };
    }
    try {
      const rows = db.querySync(sql, params);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "transaction_execute_batch",
  "Execute the same SQL with multiple parameter sets within the active transaction. All rows are part of the transaction (committed or rolled back together).",
  {
    sql: z.string().describe("SQL statement with $1, $2, ... placeholders"),
    params_array: z
      .array(z.array(z.union([z.string(), z.number(), z.boolean(), z.null()])))
      .describe("Array of parameter arrays, one per row"),
  },
  async ({ sql, params_array }) => {
    const multiErr = rejectMultiStatement(sql);
    if (multiErr) return multiErr;
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction. Call begin_transaction first." }], isError: true };
    }
    if (readOnly) return readOnlyErr();
    try {
      let total = 0;
      for (const params of params_array) {
        const result = db.executeSync(sql, params);
        total += result.changes;
      }
      return ok(`Affected rows: ${total}`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "commit_transaction",
  "Commit the active transaction. All changes become permanent and visible to other connections.",
  {},
  async () => {
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction" }], isError: true };
    }
    try {
      db.execSync("COMMIT");
      sqlTxActive = false;
      return ok("Transaction committed");
    } catch (e) {
      sqlTxActive = false;
      return fail(e);
    }
  }
);

server.tool(
  "rollback_transaction",
  "Rollback the active transaction. All changes are discarded.",
  {},
  async () => {
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction" }], isError: true };
    }
    try {
      db.execSync("ROLLBACK");
      sqlTxActive = false;
      return ok("Transaction rolled back");
    } catch (e) {
      sqlTxActive = false;
      return fail(e);
    }
  }
);

server.tool(
  "savepoint",
  "Create a savepoint within the active transaction. Use rollback_to_savepoint to undo changes back to this point without aborting the entire transaction.",
  { name: z.string().describe("Savepoint name") },
  async ({ name }) => {
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction. Call begin_transaction first." }], isError: true };
    }
    const err = requireBareId(name, "savepoint name");
    if (err) return err;
    try {
      db.execSync(`SAVEPOINT ${name}`);
      return ok(`Savepoint '${name}' created`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "rollback_to_savepoint",
  "Rollback to a savepoint within the active transaction. Undoes all changes after the savepoint but keeps the transaction open.",
  { name: z.string().describe("Savepoint name") },
  async ({ name }) => {
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction." }], isError: true };
    }
    const err = requireBareId(name, "savepoint name");
    if (err) return err;
    try {
      db.execSync(`ROLLBACK TO SAVEPOINT ${name}`);
      return ok(`Rolled back to savepoint '${name}'`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "release_savepoint",
  "Release (remove) a savepoint. The savepoint is no longer available for rollback, but changes are kept.",
  { name: z.string().describe("Savepoint name") },
  async ({ name }) => {
    if (!sqlTxActive) {
      return { content: [{ type: "text", text: "Error: no active transaction." }], isError: true };
    }
    const err = requireBareId(name, "savepoint name");
    if (err) return err;
    try {
      db.execSync(`RELEASE SAVEPOINT ${name}`);
      return ok(`Savepoint '${name}' released`);
    } catch (e) {
      return fail(e);
    }
  }
);

// ========================== SCHEMA INSPECTION TOOLS ==========================

server.tool(
  "list_tables",
  "List all tables in the database.",
  {},
  async () => {
    try {
      const rows = await db.query("SHOW TABLES");
      const tables = rows.map((r: Record<string, unknown>) => Object.values(r)[0]);
      return json(tables);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "list_views",
  "List all views in the database.",
  {},
  async () => {
    try {
      const rows = await db.query("SHOW VIEWS");
      const views = rows.map((r: Record<string, unknown>) => Object.values(r)[0]);
      return json(views);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "describe_table",
  "Show the schema of a table: column names, types, nullability, keys, defaults, and extra attributes (AUTO_INCREMENT, foreign keys).",
  { table: z.string().describe("Table name") },
  async ({ table }) => {
    try {
      const rows = await db.query(`DESCRIBE ${quoteId(table)}`);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "show_create_table",
  "Show the full CREATE TABLE DDL for a table, including all columns, constraints, and foreign keys.",
  { table: z.string().describe("Table name") },
  async ({ table }) => {
    try {
      const rows = await db.query(`SHOW CREATE TABLE ${quoteId(table)}`);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "show_create_view",
  "Show the full CREATE VIEW DDL for a view.",
  { view: z.string().describe("View name") },
  async ({ view }) => {
    try {
      const rows = await db.query(`SHOW CREATE VIEW ${quoteId(view)}`);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "show_indexes",
  "Show all indexes on a table: index name, type (BTree, Hash, Bitmap, HNSW), columns, and uniqueness.",
  { table: z.string().describe("Table name") },
  async ({ table }) => {
    try {
      const rows = await db.query(`SHOW INDEXES FROM ${quoteId(table)}`);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "get_schema",
  "Get the complete database schema: all tables with their columns, types, constraints, indexes, and DDL. Plus all views with their DDL. Call this first to understand the database before writing queries.",
  {},
  async () => {
    try {
      const tables = await db.query("SHOW TABLES");
      const schema: Record<string, { columns: unknown; indexes: unknown; ddl: unknown }> = {};
      for (const row of tables) {
        const name = String(Object.values(row)[0]);
        const q = quoteId(name);
        const cols = await db.query(`DESCRIBE ${q}`);
        const indexes = await db.query(`SHOW INDEXES FROM ${q}`);
        const ddl = await db.query(`SHOW CREATE TABLE ${q}`);
        schema[name] = { columns: cols, indexes, ddl };
      }
      const views = await db.query("SHOW VIEWS");
      const viewSchema: Record<string, { ddl: unknown }> = {};
      for (const row of views) {
        const name = String(Object.values(row)[0]);
        const ddl = await db.query(`SHOW CREATE VIEW ${quoteId(name)}`);
        viewSchema[name] = { ddl };
      }
      return json({ tables: schema, views: viewSchema });
    } catch (e) {
      return fail(e);
    }
  }
);

// ========================== SCHEMA MODIFICATION TOOLS ==========================

// Validate that the SQL statement starts with one of the expected keywords
function requireKeyword(sql: string, allowed: string[], toolName: string): ToolResult | null {
  const kw = firstKeyword(sql);
  if (!allowed.includes(kw)) {
    return { content: [{ type: "text", text: `Error: the ${toolName} tool only accepts ${allowed.join("/")} statements. Got: ${kw}` }], isError: true };
  }
  return null;
}

server.tool(
  "create_table",
  "Create a new table. Column types: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N). Constraints: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK, AUTO_INCREMENT, REFERENCES (foreign keys with ON DELETE/UPDATE CASCADE/SET NULL/RESTRICT). Supports IF NOT EXISTS and CREATE TABLE AS SELECT.",
  { sql: z.string().describe("Full CREATE TABLE statement") },
  async ({ sql }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    const kwErr = requireKeyword(sql, ["CREATE"], "create_table");
    if (kwErr) return kwErr;
    try {
      await db.exec(sql);
      return ok("Table created");
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "create_index",
  "Create an index. Types: BTREE (range/sort, default for INTEGER/FLOAT/TIMESTAMP), HASH (O(1) equality, default for TEXT/JSON), BITMAP (low-cardinality, default for BOOLEAN), HNSW (vector k-NN). Supports UNIQUE and composite (multi-column). HNSW WITH params: m (connections), ef_construction, ef_search, metric (l2/cosine/ip).",
  { sql: z.string().describe("Full CREATE [UNIQUE] INDEX statement") },
  async ({ sql }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    const kwErr = requireKeyword(sql, ["CREATE"], "create_index");
    if (kwErr) return kwErr;
    try {
      await db.exec(sql);
      return ok("Index created");
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "create_view",
  "Create a read-only view. Views persist across restarts. Supports IF NOT EXISTS.",
  { sql: z.string().describe("Full CREATE VIEW statement") },
  async ({ sql }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    const kwErr = requireKeyword(sql, ["CREATE"], "create_view");
    if (kwErr) return kwErr;
    try {
      await db.exec(sql);
      return ok("View created");
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "alter_table",
  "Alter a table: ADD COLUMN, DROP COLUMN, RENAME COLUMN old TO new, MODIFY COLUMN col new_type, RENAME TO new_name.",
  { sql: z.string().describe("Full ALTER TABLE statement") },
  async ({ sql }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    const kwErr = requireKeyword(sql, ["ALTER"], "alter_table");
    if (kwErr) return kwErr;
    try {
      await db.exec(sql);
      return ok("Table altered");
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "drop",
  "Drop a table, view, or index. Supports IF EXISTS. For indexes: DROP INDEX name ON table.",
  { sql: z.string().describe("Full DROP TABLE/VIEW/INDEX statement") },
  async ({ sql }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    const kwErr = requireKeyword(sql, ["DROP"], "drop");
    if (kwErr) return kwErr;
    try {
      await db.exec(sql);
      return ok("Dropped");
    } catch (e) {
      return fail(e);
    }
  }
);

// ========================== ADMIN TOOLS ==========================

server.tool(
  "analyze_table",
  "Collect optimizer statistics for a table (histograms, distinct counts, min/max, null fraction). Run after bulk loads to improve query plan quality.",
  { table: z.string().describe("Table name") },
  async ({ table }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    try {
      await db.exec(`ANALYZE ${quoteId(table)}`);
      return ok(`Statistics collected for ${table}`);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "vacuum",
  "Clean up deleted rows, old MVCC versions, and compact indexes. Can target a specific table or the entire database. Note: destroys time-travel history.",
  { table: z.string().optional().describe("Table name (omit for entire database)") },
  async ({ table }) => {
    if (readOnly) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    try {
      await db.exec(table ? `VACUUM ${quoteId(table)}` : "VACUUM");
      return ok(table ? `Vacuumed ${table}` : "Vacuumed entire database");
    } catch (e) {
      return fail(e);
    }
  }
);

// PRAGMAs that perform write actions even without a value parameter
const writePragmas = new Set(["vacuum", "snapshot", "checkpoint"]);

server.tool(
  "pragma",
  "Get or set database configuration. Readable/writable: sync_mode (0=None, 1=Normal, 2=Full), snapshot_interval (seconds), keep_snapshots (count), wal_flush_trigger (bytes). Action-only: snapshot (manual snapshot), checkpoint (alias), vacuum (returns cleanup stats). Additional options available via connection string DSN.",
  {
    name: z.string().describe("PRAGMA name"),
    value: z.union([z.string(), z.number()]).optional().describe("Value to set (omit to read current value)"),
  },
  async ({ name, value }) => {
    const err = requireBareId(name, "pragma name");
    if (err) return err;
    if (readOnly && (value !== undefined || writePragmas.has(name.toLowerCase()))) return readOnlyErr();
    if (sqlTxActive) return txActiveErr();
    try {
      if (value !== undefined) {
        // Validate that numeric values are actually numbers to prevent injection
        const numValue = Number(value);
        if (isNaN(numValue)) {
          return { content: [{ type: "text", text: `Error: pragma value must be numeric` }], isError: true };
        }
        await db.exec(`PRAGMA ${name} = ${numValue}`);
        return ok(`PRAGMA ${name} set to ${numValue}`);
      }
      const rows = await db.query(`PRAGMA ${name}`);
      return json(rows);
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "version",
  "Get the Stoolap engine version and build info.",
  {},
  async () => {
    try {
      const rows = await db.query("SELECT VERSION() AS version");
      return ok(String(rows[0]?.version ?? "unknown"));
    } catch (e) {
      return fail(e);
    }
  }
);

server.tool(
  "list_functions",
  "List all available SQL functions grouped by category with signatures and descriptions.",
  {
    category: z
      .enum(["all", "aggregate", "window", "string", "math", "datetime", "json", "hash", "conditional", "type", "vector", "system"])
      .optional()
      .describe("Filter by category (default: all)"),
  },
  async ({ category }) => {
    const cat = category ?? "all";
    const sections: Record<string, string> = {};

    sections.aggregate = `## Aggregate Functions
COUNT(*) - count all rows
COUNT(expr) - count non-NULL values
COUNT(DISTINCT expr) - count distinct non-NULL values
SUM(expr) - sum of numeric values (returns NULL for empty set)
SUM(DISTINCT expr) - sum of distinct values
AVG(expr) - average of numeric values
AVG(DISTINCT expr) - average of distinct values
MIN(expr) - minimum value (works on all types)
MAX(expr) - maximum value (works on all types)
FIRST(expr) - first value in group (order-dependent)
LAST(expr) - last value in group (order-dependent)
MEDIAN(expr) - 50th percentile (interpolated for even counts)
STRING_AGG(expr, delimiter) - concatenate with delimiter
GROUP_CONCAT(expr, delimiter) - alias for STRING_AGG
ARRAY_AGG(expr) - collect values into JSON array
STDDEV(expr) - population standard deviation (alias for STDDEV_POP)
STDDEV_SAMP(expr) - sample standard deviation (N-1 denominator)
STDDEV_POP(expr) - population standard deviation (N denominator)
VARIANCE(expr) - population variance (alias for VAR_POP)
VAR_SAMP(expr) - sample variance (N-1 denominator)
VAR_POP(expr) - population variance (N denominator)

All aggregate functions support DISTINCT modifier and work as window functions with OVER clause.
GROUP BY extensions: ROLLUP, CUBE, GROUPING SETS. Use GROUPING(col) to detect super-aggregate rows.`;

    sections.window = `## Window Functions
ROW_NUMBER() OVER (...) - sequential row number within partition
RANK() OVER (...) - rank with gaps for ties
DENSE_RANK() OVER (...) - rank without gaps
NTILE(n) OVER (...) - distribute rows into n buckets
LEAD(expr [, offset [, default]]) OVER (...) - access following row value
LAG(expr [, offset [, default]]) OVER (...) - access preceding row value
FIRST_VALUE(expr) OVER (...) - first value in window frame
LAST_VALUE(expr) OVER (...) - last value in window frame
NTH_VALUE(expr, n) OVER (...) - nth value in window frame
PERCENT_RANK() OVER (...) - relative rank as fraction 0..1
CUME_DIST() OVER (...) - cumulative distribution 0..1

OVER clause: PARTITION BY cols ORDER BY cols [frame]
Frame specs: ROWS|RANGE BETWEEN {UNBOUNDED PRECEDING|n PRECEDING|CURRENT ROW} AND {CURRENT ROW|n FOLLOWING|UNBOUNDED FOLLOWING}
Named windows: SELECT ... OVER w FROM t WINDOW w AS (PARTITION BY x ORDER BY y)`;

    sections.string = `## String Functions
UPPER(text) - convert to uppercase
LOWER(text) - convert to lowercase
LENGTH(text) - number of characters (Unicode-aware)
CHAR_LENGTH(text) - number of characters (alias: CHARACTER_LENGTH)
CHAR(code) - character from Unicode code point
CONCAT(a, b, ...) - concatenate strings (|| operator also works)
CONCAT_WS(separator, a, b, ...) - concatenate with separator, skips NULLs
SUBSTRING(text, start [, length]) - extract substring (1-based)
SUBSTR(text, start [, length]) - alias for SUBSTRING
TRIM([LEADING|TRAILING|BOTH] [chars FROM] text) - remove characters
LTRIM(text [, chars]) - trim left
RTRIM(text [, chars]) - trim right
REPLACE(text, from, to) - replace all occurrences
REVERSE(text) - reverse string
LEFT(text, n) - first n characters
RIGHT(text, n) - last n characters
REPEAT(text, n) - repeat string n times
SPLIT_PART(text, delimiter, index) - extract part by delimiter (1-based)
POSITION(substr IN text) - find position (1-based, 0 if not found)
STRPOS(text, substr) - alias for POSITION
INSTR(text, substr) - alias for POSITION
LOCATE(substr, text) - alias for POSITION
LPAD(text, length, fill) - left-pad to length
RPAD(text, length, fill) - right-pad to length
STARTS_WITH(text, prefix) - returns BOOLEAN
ENDS_WITH(text, suffix) - returns BOOLEAN
CONTAINS(text, substr) - returns BOOLEAN

Pattern matching operators:
  LIKE 'pattern' [ESCAPE 'char'] - case-sensitive (% = any chars, _ = one char). ESCAPE to match literal % or _
  ILIKE 'pattern' - case-insensitive LIKE
  GLOB 'pattern' - shell-style (* = any, ? = one, [...] = char class)
  REGEXP 'pattern' - full regular expression matching`;

    sections.math = `## Math Functions
ABS(x) - absolute value
ROUND(x [, decimals]) - round to n decimal places (default 0)
FLOOR(x) - round down to integer
CEILING(x) / CEIL(x) - round up to integer
MOD(x, y) - modulus (also: x % y)
POWER(base, exp) / POW(base, exp) - exponentiation
SQRT(x) - square root
LOG(base, x) - logarithm with base (2-arg) or natural log (1-arg)
LOG10(x) - base-10 logarithm
LOG2(x) - base-2 logarithm
LN(x) - natural logarithm
EXP(x) - e^x
SIGN(x) - returns -1, 0, or 1
TRUNCATE(x, decimals) / TRUNC(x, decimals) - truncate to n decimal places
PI() - returns 3.141592653589793
RANDOM() - random float between 0.0 and 1.0
SIN(x) - sine (radians)
COS(x) - cosine (radians)
TAN(x) - tangent (radians)

Arithmetic operators: +, -, *, /, % (modulo)
Bitwise operators: & (AND), | (OR), ^ (XOR), ~ (NOT), << (left shift), >> (right shift)`;

    sections.datetime = `## Date/Time Functions
NOW() - current timestamp (UTC)
CURRENT_DATE - current date (no parens needed)
CURRENT_TIME - current time (no parens needed)
CURRENT_TIMESTAMP - alias for NOW()
DATE_TRUNC(unit, timestamp) - truncate to unit: year, quarter, month, week, day, hour, minute, second
TIME_TRUNC(interval, timestamp) - truncate to interval: 15m, 30m, 1h, 4h, 1d (for time-series bucketing)
EXTRACT(field FROM timestamp) - extract field: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, DOW (0=Sun), ISODOW (1=Mon), DOY, WEEK, QUARTER, EPOCH
YEAR(timestamp) - shorthand for EXTRACT(YEAR ...)
MONTH(timestamp) - shorthand for EXTRACT(MONTH ...)
DAY(timestamp) - shorthand for EXTRACT(DAY ...)
HOUR(timestamp) - shorthand for EXTRACT(HOUR ...)
MINUTE(timestamp) - shorthand for EXTRACT(MINUTE ...)
SECOND(timestamp) - shorthand for EXTRACT(SECOND ...)
DATE_ADD(timestamp, interval, unit) - add interval (units: year, month, week, day, hour, minute, second)
DATE_SUB(timestamp, interval, unit) - subtract interval
DATEDIFF(unit, start, end) / DATE_DIFF(unit, start, end) - difference in units
TO_CHAR(timestamp, format) - format as string. Patterns: YYYY, YY, MM, MON, MONTH, DD, DY, DAY, HH24, HH/HH12, MI, SS

INTERVAL arithmetic: timestamp + INTERVAL '7 days', NOW() - INTERVAL '24 hours'
Supported interval units: second(s), minute(s), hour(s), day(s), week(s), month(s), year(s)
Typed literals: TIMESTAMP '2025-01-01 12:00:00', DATE '2025-01-01', TIME '12:00:00'
All timestamps stored and returned as UTC with nanosecond precision.`;

    sections.json = `## JSON Functions
JSON_EXTRACT(json, '$.path') - extract value by dot-notation path (supports array indexing: $.items[0])
JSON_ARRAY_LENGTH(json) - number of elements in root array
JSON_ARRAY_LENGTH(json, '$.path') - number of elements at path
JSON_ARRAY(val1, val2, ...) - construct a JSON array
JSON_OBJECT(key1, val1, key2, val2, ...) - construct a JSON object
JSON_TYPE(json) - type of root value: object, array, string, number, boolean, null
JSON_TYPEOF(json) - alias for JSON_TYPE
JSON_TYPE(json, '$.path') - type at path
JSON_VALID(text) - returns true if text is valid JSON
JSON_KEYS(json) - returns array of object keys

JSON operators:
  json_col -> 'key' - extract as JSON (returns JSON type)
  json_col ->> 'key' - extract as TEXT (returns string)
  json_col -> 'nested' -> 'key' - chained extraction

Limitations: no JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_REMOVE, JSON_CONTAINS. To modify JSON, extract, modify in application, and UPDATE.`;

    sections.hash = `## Hash Functions
MD5(text) - MD5 hash as hex string (32 chars)
SHA1(text) - SHA-1 hash as hex string (40 chars)
SHA256(text) - SHA-256 hash as hex string (64 chars)
SHA384(text) - SHA-384 hash as hex string (96 chars)
SHA512(text) - SHA-512 hash as hex string (128 chars)
CRC32(text) - CRC-32 as integer`;

    sections.conditional = `## Conditional Functions
COALESCE(a, b, ...) - first non-NULL argument
NULLIF(a, b) - returns NULL if a = b, otherwise a
IFNULL(expr, default) - returns default if expr is NULL (alias for COALESCE with 2 args)
IIF(condition, true_val, false_val) - inline if-then-else

CASE expressions:
  Simple: CASE expr WHEN val1 THEN result1 WHEN val2 THEN result2 ELSE default END
  Searched: CASE WHEN cond1 THEN result1 WHEN cond2 THEN result2 ELSE default END`;

    sections.type = `## Type, Comparison, and Collation Functions
CAST(expr AS type) - convert between types. Target types: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON
TYPEOF(expr) - returns type name as text: "integer", "float", "text", "boolean", "timestamp", "json", "null"
COLLATE(expr, collation) - apply collation for sorting/comparison. Collations: BINARY (exact byte), NOCASE (case-insensitive), NOACCENT (accent-insensitive), NUMERIC (numeric-aware string sort)
GREATEST(a, b, ...) - largest value (NULL-safe)
LEAST(a, b, ...) - smallest value (NULL-safe)

NULL operators:
  IS NULL / IS NOT NULL
  IS DISTINCT FROM / IS NOT DISTINCT FROM (NULL-safe equality: NULL IS NOT DISTINCT FROM NULL = true)`;

    sections.vector = `## Vector Functions
VEC_DISTANCE_L2(a, b) - Euclidean (L2) distance (0 to inf)
VEC_DISTANCE_COSINE(a, b) - cosine distance (1 - cosine_similarity, range 0 to 2)
VEC_DISTANCE_IP(a, b) - inner product distance (negative dot product)
VEC_DIMS(vec) - number of dimensions
VEC_NORM(vec) - L2 norm (magnitude)
VEC_TO_TEXT(vec) - convert vector to bracket string '[0.1, 0.2, ...]'
a <=> b - L2 distance operator (shorthand for VEC_DISTANCE_L2)
NULL input to any vector function returns NULL.
Dimension mismatch between vectors returns an error.

VECTOR(N) column type with fixed dimension N (f32 per element).
Insert as string literal: '[0.1, 0.2, 0.3]'
Returned as Float32Array in JavaScript.

k-NN search pattern (auto-detected by optimizer):
  SELECT id, VEC_DISTANCE_L2(embedding, '[0.1, 0.2, 0.3]') AS dist FROM t ORDER BY dist LIMIT 10

HNSW index for O(log N) approximate nearest neighbor:
  CREATE INDEX idx ON t(col) USING HNSW WITH (metric = 'cosine', m = 32, ef_construction = 200, ef_search = 128)
  Metric aliases: 'l2'/'euclidean', 'cosine', 'ip'/'inner_product'/'dot'
  Default metric: l2. Default m/ef auto-selected based on dimensions.
  CRITICAL: Index metric MUST match query distance function. A cosine index is only used for VEC_DISTANCE_COSINE queries. Mismatch falls back to brute-force.
  Multiple HNSW indexes with different metrics can coexist on the same column.
  With WHERE clause: HNSW fetches 4x candidates, post-filters. Falls back to brute-force if filtered results < k.

Semantic search with EMBED() (requires --features semantic):
  EMBED(text) returns 384-dim vector (all-MiniLM-L6-v2 model, pure Rust, no external API).
  ~30ms per embedding. First call downloads model (~90MB).
  TIP: Use CTE to compute EMBED() once per query instead of repeating:
    WITH q AS (SELECT EMBED('search text') AS vec) SELECT title, VEC_DISTANCE_COSINE(emb, q.vec) AS dist FROM docs, q ORDER BY dist LIMIT 10
  Hybrid search: combine semantic similarity with SQL filters:
    SELECT title, VEC_DISTANCE_COSINE(emb, EMBED('query')) AS dist FROM docs WHERE category = 'Legal' ORDER BY dist LIMIT 5`;

    sections.system = `## System and Table-Valued Functions
VERSION() - returns Stoolap engine version string
SLEEP(seconds) - pause execution for N seconds (float)
EMBED(text) - generate 384-dimensional semantic embedding vector (all-MiniLM-L6-v2 model, requires --features semantic)

GENERATE_SERIES(start, stop [, step]) - table-valued function, use in FROM clause:
  SELECT * FROM GENERATE_SERIES(1, 10)  -- integers 1..10
  SELECT * FROM GENERATE_SERIES(0.0, 1.0, 0.1)  -- floats
  SELECT * FROM GENERATE_SERIES(TIMESTAMP '2025-01-01', TIMESTAMP '2025-12-31', INTERVAL '1 month')  -- timestamps
  SELECT * FROM GENERATE_SERIES(1, 100) AS gs(value)  -- with column alias
Also available as scalar: SELECT GENERATE_SERIES(1, 5) returns JSON array [1,2,3,4,5]`;

    if (cat === "all") {
      const all = Object.values(sections).join("\n\n");
      return ok(all);
    }
    if (sections[cat]) {
      return ok(sections[cat]);
    }
    return { content: [{ type: "text", text: `Error: unknown category '${cat}'` }], isError: true };
  }
);

// ========================== RESOURCES ==========================

server.resource(
  "schema",
  "stoolap://schema",
  { description: "Full database schema with all tables, views, columns, indexes, and DDL" },
  async () => {
    try {
      const tables = await db.query("SHOW TABLES");
      const schema: Record<string, unknown> = {};
      for (const row of tables) {
        const name = String(Object.values(row)[0]);
        const q = quoteId(name);
        const cols = await db.query(`DESCRIBE ${q}`);
        const indexes = await db.query(`SHOW INDEXES FROM ${q}`);
        const ddl = await db.query(`SHOW CREATE TABLE ${q}`);
        schema[name] = { columns: cols, indexes, ddl };
      }
      const views = await db.query("SHOW VIEWS");
      const viewSchema: Record<string, unknown> = {};
      for (const row of views) {
        const name = String(Object.values(row)[0]);
        const ddl = await db.query(`SHOW CREATE VIEW ${quoteId(name)}`);
        viewSchema[name] = { ddl };
      }
      return {
        contents: [
          {
            uri: "stoolap://schema",
            mimeType: "application/json",
            text: JSON.stringify({ tables: schema, views: viewSchema }, null, 2),
          },
        ],
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return {
        contents: [{ uri: "stoolap://schema", mimeType: "text/plain", text: `Error: ${msg}` }],
      };
    }
  }
);

// ========================== SHARED HELPERS ==========================

async function buildSchemaText(): Promise<string> {
  let schemaText = "";
  try {
    const tables = await db.query("SHOW TABLES");
    for (const row of tables) {
      const name = String(Object.values(row)[0]);
      const q = quoteId(name);
      const ddlRows = await db.query(`SHOW CREATE TABLE ${q}`);
      const ddl = ddlRows[0] ? String(Object.values(ddlRows[0])[1] ?? Object.values(ddlRows[0])[0]) : `-- ${name}`;
      const indexRows = await db.query(`SHOW INDEXES FROM ${q}`);
      schemaText += `${ddl};\n`;
      if (indexRows.length > 0) {
        schemaText += `-- Indexes: ${JSON.stringify(indexRows)}\n`;
      }
      schemaText += "\n";
    }
    const views = await db.query("SHOW VIEWS");
    for (const row of views) {
      const name = String(Object.values(row)[0]);
      const ddlRows = await db.query(`SHOW CREATE VIEW ${quoteId(name)}`);
      const ddl = ddlRows[0] ? String(Object.values(ddlRows[0])[1] ?? Object.values(ddlRows[0])[0]) : `-- ${name}`;
      schemaText += `${ddl};\n\n`;
    }
  } catch {
    schemaText = "(unable to read schema)\n";
  }
  return schemaText;
}

const sqlReference = `## Stoolap SQL Reference

### Data Types
| Type | Description | Notes |
|------|-------------|-------|
| INTEGER | 64-bit signed integer | PRIMARY KEY, AUTO_INCREMENT |
| FLOAT | 64-bit floating point | Scientific notation ok |
| TEXT | UTF-8 variable-length string | No length limit |
| BOOLEAN | true/false | Case-insensitive |
| TIMESTAMP | Nanosecond precision, UTC | ISO 8601 + many formats |
| JSON | Validated JSON | -> (as JSON) and ->> (as TEXT) operators |
| VECTOR(N) | Fixed-dimension f32 array | Insert as '[0.1, 0.2, 0.3]' |
| NULL | Absence of value | Unless NOT NULL constraint |

### Parameter Binding
- Positional: $1, $2, ... with array params
- Positional anonymous: ? (numbered in order)
- Named: :key with object params {key: value}

### DML Commands
- SELECT [DISTINCT] cols FROM table [alias] [WHERE] [GROUP BY [ROLLUP|CUBE|GROUPING SETS]] [HAVING] [ORDER BY col [ASC|DESC] [NULLS FIRST|LAST]] [LIMIT n [OFFSET m]]
- INSERT INTO t [(cols)] VALUES (...), (...) [ON DUPLICATE KEY UPDATE col = expr] [RETURNING cols]
- INSERT INTO t [(cols)] SELECT ... [RETURNING cols]
- INSERT with DEFAULT keyword: INSERT INTO t VALUES (1, DEFAULT, 'text') uses column defaults
- UPDATE t SET col = expr, ... [WHERE] [RETURNING cols]
- DELETE FROM t [WHERE] [RETURNING cols]
- ON DUPLICATE KEY UPDATE: col = col + $1, col = expr (no VALUES() function, use column refs or params)
- TRUNCATE TABLE t (non-transactional, cannot rollback, fails if FK children exist)
- Set operations: UNION [ALL], INTERSECT [ALL], EXCEPT [ALL] (all six variants supported)
- VALUES (1,'a'), (2,'b') as inline table in FROM clause, with column aliases: VALUES (1,'a'), (2,'b') AS t(id, name)

### DDL Commands
- CREATE TABLE [IF NOT EXISTS] t (col type [constraints], ..., [FOREIGN KEY(col) REFERENCES parent(col) [ON DELETE action] [ON UPDATE action]])
  - Column constraints: PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT expr, CHECK(expr), AUTO_INCREMENT, REFERENCES
  - FK actions: CASCADE, SET NULL, RESTRICT, NO ACTION (single-column FKs only, max 16-level cascade)
  - FK columns allow NULL values (NULL is never matched by FK checks)
- CREATE TABLE [IF NOT EXISTS] t AS SELECT ... (creates table from query results)
- ALTER TABLE t ADD COLUMN | DROP COLUMN | RENAME COLUMN old TO new | MODIFY COLUMN col type | RENAME TO new_name
- DROP TABLE [IF EXISTS] t
- CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON t(col1, col2) [USING BTREE|HASH|BITMAP|HNSW] [WITH (params)]
- DROP INDEX [IF EXISTS] name ON t
- CREATE VIEW [IF NOT EXISTS] name AS SELECT ... (read-only, persists)
- DROP VIEW [IF EXISTS] name

### Joins
INNER JOIN, LEFT [OUTER] JOIN, RIGHT [OUTER] JOIN, FULL OUTER JOIN, CROSS JOIN, NATURAL JOIN, NATURAL LEFT JOIN, NATURAL RIGHT JOIN
Join clauses: ON condition | USING (col1, col2)
Self-joins and multi-table joins supported.
Optimizer auto-selects algorithm: Hash Join, Merge Join, Index Nested Loop, Nested Loop.

### Subqueries
- Scalar: SELECT (SELECT MAX(x) FROM t2) AS mx FROM t1
- IN / NOT IN: WHERE id IN (SELECT id FROM t2)
- EXISTS / NOT EXISTS: WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.id)
- ANY / SOME / ALL: WHERE price > ALL (SELECT price FROM competitor)
- Derived tables: SELECT * FROM (SELECT ... ) AS sub
- Correlated: references outer query columns

### Common Table Expressions (CTEs)
- WITH name AS (...) SELECT ... FROM name
- WITH RECURSIVE name AS (anchor UNION ALL recursive) SELECT ...
- Multiple CTEs: WITH a AS (...), b AS (...) SELECT ...
- CTE column aliases: WITH name(col1, col2) AS (...)
- CTEs in INSERT...SELECT
- Max recursion depth: 10,000 iterations

### Window Functions
ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE(n), LEAD(expr [,offset [,default]]), LAG(expr [,offset [,default]]), FIRST_VALUE(expr), LAST_VALUE(expr), NTH_VALUE(expr, n), PERCENT_RANK(), CUME_DIST()
All aggregates (SUM, AVG, COUNT, MIN, MAX, etc.) work as window functions with OVER.
OVER ([PARTITION BY cols] [ORDER BY cols] [frame])
Frame: ROWS|RANGE BETWEEN {UNBOUNDED PRECEDING|n PRECEDING|CURRENT ROW} AND {CURRENT ROW|n FOLLOWING|UNBOUNDED FOLLOWING}
Named windows: WINDOW w AS (...) ... OVER w

### GROUP BY Extensions
- GROUP BY ROLLUP(a, b) - hierarchical subtotals + grand total
- GROUP BY CUBE(a, b) - all 2^n grouping combinations
- GROUP BY GROUPING SETS((a, b), (a), ()) - explicit groupings
- GROUPING(col) returns 0 (regular) or 1 (super-aggregate) to distinguish levels

### Aggregate Functions (17)
COUNT(*), COUNT(expr), COUNT(DISTINCT expr), SUM(expr), SUM(DISTINCT), AVG(expr), AVG(DISTINCT), MIN(expr), MAX(expr), FIRST(expr), LAST(expr), MEDIAN(expr), STRING_AGG(expr, delim), GROUP_CONCAT(expr, delim), ARRAY_AGG(expr), STDDEV/STDDEV_POP(expr) (population), STDDEV_SAMP(expr) (sample, N-1), VARIANCE/VAR_POP(expr) (population), VAR_SAMP(expr) (sample, N-1)
All support DISTINCT modifier. All work as window functions with OVER clause.

### Scalar Functions (103)

**String (27):** UPPER, LOWER, LENGTH (characters, Unicode-aware), CHAR_LENGTH/CHARACTER_LENGTH, CHAR, CONCAT, CONCAT_WS, SUBSTRING(s, start [,len]), SUBSTR, TRIM([LEADING|TRAILING|BOTH] [chars FROM] s), LTRIM, RTRIM, REPLACE, REVERSE, LEFT, RIGHT, REPEAT, SPLIT_PART(s, delim, idx), POSITION(sub IN s), STRPOS, INSTR, LOCATE, LPAD, RPAD, STARTS_WITH, ENDS_WITH, CONTAINS

**Math (22):** ABS, ROUND(x [,dec]), FLOOR, CEILING/CEIL, MOD, POWER/POW, SQRT, LOG, LOG10, LOG2, LN, EXP, SIGN, TRUNCATE/TRUNC(x, dec), PI(), RANDOM(), SIN, COS, TAN

**Date/Time (18):** NOW(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATE_TRUNC(unit, ts), TIME_TRUNC(interval, ts), EXTRACT(field FROM ts), YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DATE_ADD(ts, n, unit), DATE_SUB(ts, n, unit), DATEDIFF/DATE_DIFF(unit, start, end), TO_CHAR(ts, fmt)
EXTRACT fields: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, DOW, ISODOW, DOY, WEEK, QUARTER, EPOCH
TO_CHAR patterns: YYYY, YY, MM, MON, MONTH, DD, DY, DAY, HH24, HH, MI, SS
DATE_TRUNC units: year, quarter, month, week, day, hour, minute, second
TIME_TRUNC intervals: 15m, 30m, 1h, 4h, 1d
INTERVAL arithmetic: ts + INTERVAL '7 days', ts - INTERVAL '1 hour'
Units: second(s), minute(s), hour(s), day(s), week(s), month(s), year(s)

**JSON (8):** JSON_EXTRACT(json, '$.path'), JSON_ARRAY_LENGTH(json [,'$.path']), JSON_ARRAY(vals...), JSON_OBJECT(k1, v1, ...), JSON_TYPE/JSON_TYPEOF(json [,'$.path']), JSON_VALID(text), JSON_KEYS(json)
Operators: -> (extract as JSON), ->> (extract as TEXT), chainable: col->'a'->'b'

**Hash (6):** MD5, SHA1, SHA256, SHA384, SHA512, CRC32

**Conditional (4):** COALESCE(a, b, ...), NULLIF(a, b), IFNULL(expr, default), IIF(cond, true_val, false_val)
CASE: simple (CASE x WHEN v1 THEN r1 END) and searched (CASE WHEN c1 THEN r1 END)

**Type (5):** CAST(expr AS type), TYPEOF(expr), COLLATE(expr, collation) (BINARY/NOCASE/NOACCENT/NUMERIC), GREATEST(a, b, ...), LEAST(a, b, ...)

**Vector (6):** VEC_DISTANCE_L2(a, b), VEC_DISTANCE_COSINE(a, b), VEC_DISTANCE_IP(a, b), VEC_DIMS(v), VEC_NORM(v), VEC_TO_TEXT(v)
Operator: <=> (L2 distance shorthand). k-NN: ORDER BY VEC_DISTANCE_L2(col, '[...]') LIMIT k (auto-optimized)
Insert vectors as string literal: '[0.1, 0.2, 0.3]'. Dimension mismatch returns error. NULL input returns NULL.

**System (3):** VERSION(), SLEEP(seconds), EMBED(text) (384-dim semantic embedding, all-MiniLM-L6-v2 model, pure Rust, no external API, requires --features semantic)
**Table-valued (1):** GENERATE_SERIES(start, stop [, step]) in FROM clause. Supports INTEGER, FLOAT, TIMESTAMP. Column aliases: GENERATE_SERIES(1, 10) AS gs(value)

### Operators
Comparison: =, <>, !=, <, <=, >, >=
Logical: AND, OR, XOR, NOT
Arithmetic: +, -, *, /, %
Bitwise: &, |, ^, ~, <<, >>
String: || (concat), LIKE [ESCAPE char], NOT LIKE, ILIKE, NOT ILIKE, GLOB, NOT GLOB, REGEXP/RLIKE, NOT REGEXP
Range: BETWEEN, NOT BETWEEN, IN (...), NOT IN (...)
NULL: IS NULL, IS NOT NULL, IS DISTINCT FROM, IS NOT DISTINCT FROM
NULL rules: NULL = NULL returns NULL (not TRUE), use IS NOT DISTINCT FROM for NULL-safe equality. NULL propagates through expressions (1 + NULL = NULL, 'a' || NULL = NULL). Three-valued logic: TRUE OR NULL = TRUE, FALSE AND NULL = FALSE, NOT NULL = NULL. Aggregates (except COUNT(*)) skip NULLs.
JSON: -> (as JSON), ->> (as TEXT), col[index] (array indexing)
Vector: <=> (L2 distance)
Typed literals: TIMESTAMP '2025-01-01 12:00:00', DATE '2025-01-01', TIME '12:00:00'
INTERVAL: ts + INTERVAL '7 days', ts - INTERVAL '1 hour' (units: second, minute, hour, day, week, month, year)

### Index Types
| Type | Best For | Default For | USING |
|------|----------|-------------|-------|
| BTree | Range, equality, sorting | INTEGER, FLOAT, TIMESTAMP | BTREE |
| Hash | O(1) equality, IN lists | TEXT, JSON | HASH |
| Bitmap | Low-cardinality boolean ops | BOOLEAN | BITMAP |
| HNSW | Vector k-NN search | - | HNSW |
Multiple single-column indexes are auto-intersected/unioned by the optimizer.
HNSW WITH params: m (connections, 2-64), ef_construction (50-1000), ef_search (10-1000), metric (l2/euclidean, cosine, ip/inner_product/dot)
Defaults auto-selected from vector dimensions. Index metric MUST match query distance function (cosine index only for VEC_DISTANCE_COSINE). Mismatch falls back to brute-force.
Multiple HNSW indexes with different metrics can exist on the same column.

### Transactions
- BEGIN [TRANSACTION ISOLATION LEVEL {READ COMMITTED | SNAPSHOT | SERIALIZABLE | REPEATABLE READ}]
- COMMIT / ROLLBACK
- SAVEPOINT name / ROLLBACK TO SAVEPOINT name / RELEASE SAVEPOINT name
- SET isolation_level = 'SNAPSHOT' (change default isolation level)
- Two effective levels: READ COMMITTED (default, each statement sees latest commits) and SNAPSHOT (entire tx sees consistent snapshot from BEGIN). SERIALIZABLE and REPEATABLE READ map to SNAPSHOT.
- MVCC: readers never block writers, writers never block readers. Conflicts detected at commit.

### Temporal Queries (Time Travel)
- SELECT * FROM t AS OF TIMESTAMP '2025-01-01 00:00:00' (query past state)
- SELECT * FROM t AS OF TRANSACTION 42
- AS OF in JOIN: FROM t1 AS OF TIMESTAMP '...' JOIN t2 AS OF TIMESTAMP '...'
- VACUUM destroys time-travel history

### Metadata Commands
SHOW TABLES | SHOW VIEWS | SHOW INDEXES FROM t | SHOW CREATE TABLE t | SHOW CREATE VIEW v | DESCRIBE t | DESC t

### PRAGMA
Settings: sync_mode (0=None, 1=Normal, 2=Full), snapshot_interval (seconds), keep_snapshots (count), wal_flush_trigger (bytes)
Actions: snapshot (manual snapshot), checkpoint (alias), vacuum (manual cleanup, returns deleted_rows_cleaned/old_versions_cleaned/transactions_cleaned)
Additional DSN options (set via connection string, not PRAGMA): wal_buffer_size, wal_max_size, wal_compression (bool), snapshot_compression (bool), compression_threshold (bytes), commit_batch_size, sync_interval_ms, cleanup (bool), cleanup_interval (seconds), deleted_row_retention (seconds), transaction_retention (seconds)

### EXPLAIN
EXPLAIN SELECT ... (query plan) | EXPLAIN ANALYZE SELECT ... (plan + actual runtime stats)
ANALYZE table_name (collect optimizer statistics)

### Known Limitations (DO NOT attempt these)
- No stored procedures, triggers, or user-defined functions
- No GRANT/REVOKE (embedded database, no access control)
- No full-text search (use LIKE, ILIKE, GLOB, REGEXP instead)
- No materialized views
- No LISTEN/NOTIFY
- No BLOB/BINARY, ARRAY, ENUM, or INTERVAL column types
- JSON: no JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_REMOVE, JSON_CONTAINS, JSON_CONTAINS_PATH
- Foreign keys: single-column only (no composite), max 16-level cascade, no recursive CASCADE (ON UPDATE CASCADE does not cascade to grandchild tables)
- Timestamps: UTC only (no time zone conversion), approximate month/year intervals (30 days, 365 days)
- Views: read-only (no INSERT/UPDATE/DELETE on views), max 32-level nesting
- CTEs in UPDATE/DELETE not supported
- AS OF cannot combine with subqueries
- TRUNCATE cannot be rolled back, fails if FK children exist
- CHECK constraints: column-level only (no table-level CHECK)
- ALTER TABLE: blocking operation, no existing data validation on MODIFY COLUMN
- ON DUPLICATE KEY UPDATE: no VALUES() function (unlike MySQL), use column references
- Recursive CTEs: max 10,000 iterations
- GENERATE_SERIES: max 10,000,000 rows per call

Use the available MCP tools: query, execute, execute_batch, explain, begin_transaction, transaction_execute, transaction_query, transaction_execute_batch, commit_transaction, rollback_transaction, savepoint, rollback_to_savepoint, release_savepoint, list_tables, list_views, describe_table, show_create_table, show_create_view, show_indexes, get_schema, create_table, create_index, create_view, alter_table, drop, analyze_table, vacuum, pragma, version, list_functions.`;

server.resource(
  "sql-reference",
  "stoolap://sql-reference",
  { description: "Complete Stoolap SQL reference with live database schema: data types, 130+ functions, operators, joins, indexes, window functions, CTEs, transactions, temporal queries, vector search, and known limitations" },
  async () => {
    const schemaText = await buildSchemaText();
    return {
      contents: [
        {
          uri: "stoolap://sql-reference",
          mimeType: "text/markdown",
          text: `## Current Database Schema\n\n\`\`\`sql\n${schemaText}\`\`\`\n\n${sqlReference}`,
        },
      ],
    };
  }
);

// ========================== PROMPTS ==========================

server.prompt(
  "sql-assistant",
  "Injects live database schema and complete Stoolap SQL reference (all data types, 130+ functions with signatures, all operators, join types, index types, window functions, CTEs, transactions, temporal queries, vector search, and known limitations). Attach to give the model full context.",
  {},
  async () => {
    const schemaText = await buildSchemaText();
    return {
      messages: [
        {
          role: "user" as const,
          content: {
            type: "text" as const,
            text: `You are a SQL expert for a Stoolap database. Below is the live schema followed by the complete SQL reference. Write accurate, optimized queries using only supported features.\n\n## Current Database Schema\n\n\`\`\`sql\n${schemaText}\`\`\`\n\n${sqlReference}`,
          },
        },
      ],
    };
  }
);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const transport = new StdioServerTransport();
await server.connect(transport);
console.error(`stoolap-mcp: connected (path=${dbPath}, readOnly=${readOnly})`);

function shutdown() {
  try {
    if (sqlTxActive) {
      db.execSync("ROLLBACK");
      sqlTxActive = false;
    }
  } catch { /* best effort */ }
  db.closeSync();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
