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
import { z } from "zod";
import { FUNCTION_SECTIONS, INSTRUCTIONS, SQL_REFERENCE } from "./reference.js";
import type { Params, Session } from "./session.js";
import {
  classify,
  hasReturning,
  isBareId,
  quoteId,
  singleStatementError,
  stripExplain,
} from "./sql.js";

type ToolResult = {
  content: Array<{ type: "text"; text: string }>;
  isError?: boolean;
};

function ok(text: string): ToolResult {
  return { content: [{ type: "text", text }] };
}

function json(data: unknown): ToolResult {
  return ok(JSON.stringify(data));
}

function err(text: string): ToolResult {
  return { content: [{ type: "text", text: `Error: ${text}` }], isError: true };
}

function failure(e: unknown): ToolResult {
  return err(e instanceof Error ? e.message : String(e));
}

const READ_ONLY_ERR = "the server is in read-only mode";

const paramValue = z.union([z.string(), z.number(), z.boolean(), z.null()]);
const params = z
  .union([z.array(paramValue), z.record(z.string(), paramValue)])
  .optional()
  .describe("Bind parameters: an array for $1, $2 or ?, an object for :name");
const maxRows = z
  .number()
  .int()
  .min(1)
  .optional()
  .describe("Row limit for this call (default: the server's --max-rows)");

const READ = { readOnlyHint: true, destructiveHint: false, openWorldHint: false } as const;
const WRITE = { readOnlyHint: false, destructiveHint: true, openWorldHint: false } as const;
const ADDITIVE = { readOnlyHint: false, destructiveHint: false, openWorldHint: false } as const;

// DDL that the engine does not undo with the transaction.
function ddlInTxError(sql: string): string | null {
  const w = sql.trim().split(/\s+/, 3).map((s) => s.toUpperCase());
  if (w[0] === "CREATE" && w[1] === "TABLE") return null;
  return "this statement is not undone by ROLLBACK (ALTER, CREATE INDEX and CREATE VIEW persist; DROP TABLE and TRUNCATE lose rows). Commit or roll back first, then run it outside the transaction.";
}

const PRAGMA_SETTINGS = new Set([
  "checkpoint_interval",
  "snapshot_interval",
  "compact_threshold",
  "target_volume_rows",
  "keep_snapshots",
  "hot_max_rows",
  "hot_max_bytes",
  "group_cache_mb",
  "index_cache_mb",
  "index_build_mb",
  "query_timeout",
]);
const PRAGMA_REPORTS = new Set([
  "sync_mode",
  "wal_flush_trigger",
  "volume_stats",
  "memory_stats",
  "group_cache_stats",
  "index_stats",
  "index_read_stats",
  "verify",
]);
const PRAGMA_ACTIONS = new Set(["checkpoint", "snapshot", "restore", "vacuum", "index_backfill"]);
// Session-only settings that change no data: allowed in read-only mode.
const PRAGMA_SESSION = new Set(["query_timeout"]);

async function schemaText(s: Session, table?: string): Promise<string> {
  const tables = table
    ? [table]
    : (await s.objects("SHOW TABLES")).map((r) => String(Object.values(r)[0]));
  let out = "";
  for (const t of tables) {
    const ddl = await s.objects(`SHOW CREATE TABLE ${quoteId(t)}`);
    out += `${String(Object.values(ddl[0] ?? {})[1] ?? `-- ${t}`)};\n`;
    for (const ix of await s.objects(`SHOW INDEXES FROM ${quoteId(t)}`)) {
      if (ix.index_type === "PRIMARYKEY") continue;
      const cols = String(ix.column_name).replace(/^\(|\)$/g, "");
      const unique = ix.is_unique ? "unique " : "";
      const opts = ix.options ? ` (${String(ix.options)})` : "";
      out += `-- ${unique}index ${String(ix.index_name)} on (${cols}): ${String(ix.index_type)}${opts}\n`;
    }
    out += "\n";
  }
  if (!table) {
    for (const r of await s.objects("SHOW VIEWS")) {
      const v = String(Object.values(r)[0]);
      const ddl = await s.objects(`SHOW CREATE VIEW ${quoteId(v)}`);
      out += `${String(Object.values(ddl[0] ?? {})[1] ?? `-- ${v}`)};\n\n`;
    }
  }
  return out === "" ? "-- no tables or views\n" : out;
}

export function createServer(s: Session, version: string): McpServer {
  const server = new McpServer({ name: "stoolap", version }, { instructions: INSTRUCTIONS });

  // Every handler runs through the session queue, one call at a time.
  const tool = <A>(fn: (args: A) => Promise<ToolResult>) => (args: A) =>
    s.run(async () => {
      try {
        return await fn(args);
      } catch (e) {
        return failure(e);
      }
    });

  // ---------------------------------------------------------------- SQL

  server.registerTool(
    "query",
    {
      title: "Run a read-only query",
      description:
        "Run one read-only statement: SELECT, WITH ... SELECT, SHOW, DESCRIBE or EXPLAIN. Returns {columns, rows, row_count, truncated}; truncated is true when the result had more rows than the row limit. Runs inside the open transaction, if any.",
      inputSchema: { sql: z.string().describe("One SQL statement"), params, max_rows: maxRows },
      annotations: READ,
    },
    tool(async ({ sql, params: p, max_rows }: { sql: string; params?: Params; max_rows?: number }) => {
      const bad = singleStatementError(sql);
      if (bad) return err(bad);
      const kind = classify(sql);
      if (kind === "tx") return err("use begin_transaction, commit_transaction, rollback_transaction or savepoint.");
      if (kind !== "read") return err("query runs read-only statements only. Use execute for writes.");
      return json(await s.rows(sql, p, max_rows));
    }),
  );

  server.registerTool(
    "execute",
    {
      title: "Run a write statement",
      description:
        "Run one statement that changes data, schema or settings: INSERT, UPDATE, DELETE, COPY ... FROM, CREATE, ALTER, DROP, TRUNCATE, ANALYZE, VACUUM, SET. Returns the affected row count, or the rows when the statement has RETURNING. Runs inside the open transaction, if any.",
      inputSchema: { sql: z.string().describe("One SQL statement"), params, max_rows: maxRows },
      annotations: WRITE,
    },
    tool(async ({ sql, params: p, max_rows }: { sql: string; params?: Params; max_rows?: number }) => {
      const bad = singleStatementError(sql);
      if (bad) return err(bad);
      const kind = classify(sql);
      if (kind === "tx") return err("use begin_transaction, commit_transaction, rollback_transaction or savepoint.");
      if (kind === "read") return err("execute does not run read-only statements. Use query.");
      if (/^\s*PRAGMA\b/i.test(sql)) return err("use the pragma tool.");
      if (s.readOnly) return err(READ_ONLY_ERR);
      if (kind === "ddl" && s.txActive) {
        const e = ddlInTxError(sql);
        if (e) return err(e);
      }
      if (hasReturning(sql)) return json(await s.rows(sql, p, max_rows));
      const changes = await s.execute(sql, p);
      return json({ affected_rows: changes });
    }),
  );

  server.registerTool(
    "execute_batch",
    {
      title: "Run one statement for many parameter sets",
      description:
        "Run one INSERT, UPDATE or DELETE once per parameter set. Outside a transaction all sets apply atomically: either every one succeeds or none does. Inside a transaction the rows join it. Returns the total affected row count.",
      inputSchema: {
        sql: z.string().describe("One statement with $1, $2 or ? placeholders"),
        params_array: z.array(z.array(paramValue)).min(1).describe("One positional parameter array per execution"),
      },
      annotations: WRITE,
    },
    tool(async ({ sql, params_array }: { sql: string; params_array: unknown[][] }) => {
      const bad = singleStatementError(sql);
      if (bad) return err(bad);
      if (classify(sql) !== "write" || hasReturning(sql)) {
        return err("execute_batch runs INSERT, UPDATE or DELETE without RETURNING.");
      }
      if (s.readOnly) return err(READ_ONLY_ERR);
      return json({ affected_rows: s.batch(sql, params_array) });
    }),
  );

  server.registerTool(
    "explain",
    {
      title: "Show a query plan",
      description:
        "Show the plan of one statement, one step per line. A line starting with 'else' is the fallback used when the step above cannot run. With analyze=true the statement runs and the plan shows actual time and rows plus a final 'Paths:' line naming the paths taken; analyze is refused for statements that write.",
      inputSchema: {
        sql: z.string().describe("The statement, with or without EXPLAIN"),
        analyze: z.boolean().optional().describe("Run it and report actual time and rows"),
        params,
      },
      annotations: READ,
    },
    tool(async ({ sql, analyze, params: p }: { sql: string; analyze?: boolean; params?: Params }) => {
      const bad = singleStatementError(sql);
      if (bad) return err(bad);
      const body = stripExplain(sql);
      const kind = classify(body);
      if (kind === "tx") return err("transaction control statements have no plan.");
      if (analyze && kind !== "read") {
        return err("EXPLAIN ANALYZE runs the statement, so it is refused for writes. Use analyze=false.");
      }
      const r = await s.rows(`${analyze ? "EXPLAIN ANALYZE" : "EXPLAIN"} ${body}`, p, 0);
      return ok(r.rows.map((row) => String(row[0])).join("\n"));
    }),
  );

  // ---------------------------------------------------------------- transactions

  server.registerTool(
    "begin_transaction",
    {
      title: "Begin a transaction",
      description:
        "Begin a transaction. Until commit_transaction or rollback_transaction, query, execute and execute_batch run inside it and see its uncommitted changes. read_committed (default): each statement sees the latest commits; snapshot: every statement sees the database as of BEGIN.",
      inputSchema: {
        isolation: z.enum(["read_committed", "snapshot"]).optional().describe("Default: read_committed"),
      },
      annotations: ADDITIVE,
    },
    tool(async ({ isolation }: { isolation?: "read_committed" | "snapshot" }) => {
      if (s.txActive) return err("a transaction is already open. Commit or roll it back first.");
      const level = isolation ?? "read_committed";
      await s.begin(level);
      return ok(`Transaction started (${level})`);
    }),
  );

  server.registerTool(
    "commit_transaction",
    {
      title: "Commit the transaction",
      description: "Commit the open transaction.",
      inputSchema: {},
      annotations: ADDITIVE,
    },
    tool(async () => {
      if (!s.txActive) return err("no transaction is open.");
      await s.end(true);
      return ok("Committed");
    }),
  );

  server.registerTool(
    "rollback_transaction",
    {
      title: "Roll back the transaction",
      description: "Roll back the open transaction and discard its changes.",
      inputSchema: {},
      annotations: ADDITIVE,
    },
    tool(async () => {
      if (!s.txActive) return err("no transaction is open.");
      await s.end(false);
      return ok("Rolled back");
    }),
  );

  server.registerTool(
    "savepoint",
    {
      title: "Savepoints",
      description:
        "Inside the open transaction: 'create' a savepoint, 'rollback_to' it (undo later changes; the savepoint stays) or 'release' it (keep the changes; drops it and every later savepoint).",
      inputSchema: {
        action: z.enum(["create", "rollback_to", "release"]),
        name: z.string().describe("Savepoint name (letters, digits, _)"),
      },
      annotations: ADDITIVE,
    },
    tool(async ({ action, name }: { action: "create" | "rollback_to" | "release"; name: string }) => {
      if (!s.txActive) return err("no transaction is open. Call begin_transaction first.");
      if (!isBareId(name)) return err(`invalid savepoint name: ${name}`);
      const sql = {
        create: `SAVEPOINT ${name}`,
        rollback_to: `ROLLBACK TO SAVEPOINT ${name}`,
        release: `RELEASE SAVEPOINT ${name}`,
      }[action];
      await s.exec(sql);
      return ok(`${sql}: done`);
    }),
  );

  // ---------------------------------------------------------------- schema

  server.registerTool(
    "list_tables",
    {
      title: "List tables and views",
      description: "List the names of all tables and views.",
      inputSchema: {},
      annotations: READ,
    },
    tool(async () => {
      const first = (r: Record<string, unknown>) => String(Object.values(r)[0]);
      return json({
        tables: (await s.objects("SHOW TABLES")).map(first),
        views: (await s.objects("SHOW VIEWS")).map(first),
      });
    }),
  );

  server.registerTool(
    "get_schema",
    {
      title: "Get the schema",
      description:
        "The schema: CREATE TABLE with constraints and foreign keys, a comment line per secondary index (including UNIQUE constraints and the indexes made for foreign keys), and CREATE VIEW. Pass a table name for one table. Call this before writing queries.",
      inputSchema: { table: z.string().optional().describe("One table (default: everything)") },
      annotations: READ,
    },
    tool(async ({ table }: { table?: string }) => ok(await schemaText(s, table))),
  );

  // ---------------------------------------------------------------- engine

  server.registerTool(
    "pragma",
    {
      title: "Engine settings, reports and maintenance",
      description:
        "Read or change an engine setting, read a report, or run a maintenance action. Settings (numeric): checkpoint_interval, compact_threshold, target_volume_rows, keep_snapshots, hot_max_rows, hot_max_bytes, group_cache_mb, index_cache_mb, index_build_mb, query_timeout (ms, this session). Reports: sync_mode, wal_flush_trigger, volume_stats, memory_stats, group_cache_stats, index_stats, index_read_stats, verify. Actions: checkpoint, snapshot, vacuum, index_backfill (value = max volumes), restore (value = snapshot timestamp 'YYYYMMDD-HHMMSS.fff', latest when omitted; replaces ALL data).",
      inputSchema: {
        name: z.string().describe("PRAGMA name"),
        value: z.union([z.string(), z.number()]).optional().describe("Value to set; omit to read"),
      },
      annotations: WRITE,
    },
    tool(async ({ name, value }: { name: string; value?: string | number }) => {
      const pragma = name.toLowerCase();
      const known = PRAGMA_SETTINGS.has(pragma) || PRAGMA_REPORTS.has(pragma) || PRAGMA_ACTIONS.has(pragma);
      if (!known) {
        return err(`unknown pragma '${name}'. Known: ${[...PRAGMA_SETTINGS, ...PRAGMA_REPORTS, ...PRAGMA_ACTIONS].join(", ")}`);
      }
      const writes = value !== undefined || PRAGMA_ACTIONS.has(pragma);
      if (writes && s.readOnly && !PRAGMA_SESSION.has(pragma)) return err(READ_ONLY_ERR);
      let sql = `PRAGMA ${pragma}`;
      if (value !== undefined) {
        if (pragma === "restore") {
          const ts = String(value);
          if (!/^\d{8}-\d{6}\.\d{3}$/.test(ts)) return err("restore takes a snapshot timestamp like 20260315-120000.000");
          sql += ` = '${ts}'`;
        } else if (PRAGMA_SETTINGS.has(pragma) || pragma === "index_backfill") {
          const n = typeof value === "number" ? value : Number(value);
          if (String(value).trim() === "" || !Number.isInteger(n)) return err(`pragma ${pragma} takes an integer`);
          sql += ` = ${n}`;
        } else {
          return err(`pragma ${pragma} takes no value`);
        }
      }
      return json(await s.rows(sql, undefined, 0));
    }),
  );

  server.registerTool(
    "verify",
    {
      title: "Verify stored data",
      description:
        "Read every column volume of a file database, checking each block's checksum, and report each segment as ok, damaged, unreadable or failed. Changes nothing. Empty for an in-memory database or one with nothing sealed yet.",
      inputSchema: {},
      annotations: READ,
    },
    tool(async () => json(await s.rows("PRAGMA verify", undefined, 0))),
  );

  const categories = ["all", ...Object.keys(FUNCTION_SECTIONS)] as [string, ...string[]];
  server.registerTool(
    "list_functions",
    {
      title: "List SQL functions",
      description: "Built-in SQL functions with signatures and semantics, by category.",
      inputSchema: { category: z.enum(categories).optional().describe("Default: all") },
      annotations: { ...READ, idempotentHint: true },
    },
    tool(async ({ category }: { category?: string }) =>
      ok(!category || category === "all" ? Object.values(FUNCTION_SECTIONS).join("\n\n") : FUNCTION_SECTIONS[category]),
    ),
  );

  server.registerTool(
    "version",
    {
      title: "Versions",
      description: "The engine version and build, and this server's version and limits.",
      inputSchema: {},
      annotations: { ...READ, idempotentHint: true },
    },
    tool(async () => {
      const [engine] = await s.objects("SELECT VERSION() AS v");
      const [timeout] = await s.objects("PRAGMA query_timeout");
      return json({
        engine: engine?.v,
        server: `stoolap-mcp ${version}`,
        read_only: s.readOnly,
        max_rows: s.maxRows,
        query_timeout_ms: timeout?.query_timeout,
      });
    }),
  );

  // ---------------------------------------------------------------- resources and prompt

  const reference = async () =>
    `## Current schema\n\n\`\`\`sql\n${await schemaText(s)}\`\`\`\n\n${SQL_REFERENCE}\n\n${Object.values(FUNCTION_SECTIONS).join("\n\n")}`;

  server.registerResource(
    "schema",
    "stoolap://schema",
    { title: "Database schema", description: "The live schema: tables, indexes and views", mimeType: "application/sql" },
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: "application/sql", text: await s.run(() => schemaText(s)) }],
    }),
  );

  server.registerResource(
    "sql-reference",
    "stoolap://sql-reference",
    {
      title: "Stoolap SQL reference",
      description: "The live schema plus the Stoolap 0.5 SQL reference and every function",
      mimeType: "text/markdown",
    },
    async (uri) => ({
      contents: [{ uri: uri.href, mimeType: "text/markdown", text: await s.run(reference) }],
    }),
  );

  server.registerPrompt(
    "sql-assistant",
    {
      title: "SQL assistant",
      description: "The live schema and the complete Stoolap SQL reference, as a prompt.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text: `You write SQL for a Stoolap database. Use only what the reference below supports.\n\n${await s.run(reference)}`,
          },
        },
      ],
    }),
  );

  return server;
}
