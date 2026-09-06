# stoolap-mcp

MCP (Model Context Protocol) server for [Stoolap](https://github.com/stoolap/stoolap), an embedded SQL database. Lets AI assistants query, manage, and analyze Stoolap databases with full access to all SQL features.

Works with any MCP-compatible AI client: Claude Desktop, Claude Code, Cursor, Windsurf, Cline, and others.

Version 0.4.x of this server targets the Stoolap 0.4.x engine (volume-based storage) through [`@stoolap/node`](https://github.com/stoolap/stoolap-node).

## Quick Start

### Claude Desktop

Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "stoolap": {
      "command": "npx",
      "args": ["-y", "@stoolap/mcp", "--path", "./mydata"]
    }
  }
}
```

### Claude Code

```bash
claude mcp add stoolap -- npx -y @stoolap/mcp --path ./mydata
```

### In-memory (no persistence)

```json
{
  "mcpServers": {
    "stoolap": {
      "command": "npx",
      "args": ["-y", "@stoolap/mcp"]
    }
  }
}
```

### Read-only mode

```json
{
  "mcpServers": {
    "stoolap": {
      "command": "npx",
      "args": ["-y", "@stoolap/mcp", "--path", "./mydata", "--read-only"]
    }
  }
}
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--path <path>` | `:memory:` | Database path or DSN. Engine options go in the query string, e.g. `./mydata?sync_mode=full&checkpoint_interval=30`. |
| `--read-only` | `false` | Reject every statement that writes data, schema or engine state. Read-only transactions (begin, query, commit) are still allowed for consistent reads. |
| `--version` | | Print the server version and exit. |

The first `npx` run compiles the small native addon of `@stoolap/node`, which can take a while; MCP clients with a short startup timeout may report a failed connection on that first run. Retry once the install has finished, or install the package globally beforehand (`npm install -g @stoolap/mcp`).

## Tools (30)

Every tool carries MCP annotations (`readOnlyHint`, `destructiveHint`), so clients can auto-approve the read-only ones.

### Query and Analysis

| Tool | Description |
|------|-------------|
| `query` | Run SELECT, SHOW, DESCRIBE, EXPLAIN, VALUES and WITH ... SELECT. Returns rows as JSON. Runs inside the active transaction if one is open. |
| `execute` | Run INSERT, UPDATE, DELETE, COPY ... FROM, DDL, SET, ANALYZE, VACUUM with parameter binding. Supports upsert (ON CONFLICT / ON DUPLICATE KEY UPDATE) and RETURNING. Returns rows for RETURNING, otherwise the affected row count. |
| `execute_batch` | Execute the same SQL with multiple parameter sets in a single atomic transaction. |
| `explain` | Show the query plan. `analyze=true` runs the statement and reports actual row counts and timings (refused for write statements). |

### Transaction Control

| Tool | Description |
|------|-------------|
| `begin_transaction` | Begin a transaction with optional isolation level (`read_committed` or `snapshot`). One active transaction at a time. |
| `transaction_execute` | Execute INSERT, UPDATE or DELETE inside the active transaction. DDL, TRUNCATE and COPY are refused. |
| `transaction_query` | Run a read-only statement inside the active transaction. |
| `transaction_execute_batch` | Execute the same SQL with multiple parameter sets inside the active transaction. |
| `commit_transaction` | Commit the active transaction. |
| `rollback_transaction` | Rollback the active transaction. |
| `savepoint` | Create a named savepoint. |
| `rollback_to_savepoint` | Undo changes made after a savepoint without ending the transaction. |
| `release_savepoint` | Remove a savepoint, keeping its changes. |

### Schema Inspection

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables |
| `list_views` | List all views |
| `describe_table` | Columns, types, nullability, keys, defaults and extras |
| `show_create_table` | Full CREATE TABLE DDL including constraints and foreign keys |
| `show_create_view` | Full CREATE VIEW DDL |
| `show_indexes` | Indexes of a table: name, type, columns, uniqueness, options |
| `get_schema` | The complete schema: every table with columns, indexes and DDL, plus every view |

### Schema Modification

| Tool | Description |
|------|-------------|
| `create_table` | INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N) columns; PRIMARY KEY (including composite), NOT NULL, UNIQUE, DEFAULT, CHECK, AUTO_INCREMENT, single-column foreign keys; IF NOT EXISTS; CREATE TABLE AS SELECT |
| `create_index` | BTREE, HASH, BITMAP or HNSW indexes, UNIQUE and composite. HNSW options: m, ef_construction, ef_search, metric |
| `create_view` | Read-only view that persists across restarts |
| `alter_table` | ADD COLUMN, DROP COLUMN, RENAME COLUMN, MODIFY COLUMN, RENAME TO |
| `drop` | DROP TABLE / VIEW / INDEX ... ON table (supports IF EXISTS) |

### Database Administration

| Tool | Description |
|------|-------------|
| `analyze_table` | Collect optimizer statistics for a table |
| `vacuum` | Remove deleted rows and old MVCC versions, compact indexes (discards time-travel history) |
| `pragma` | Read or set `checkpoint_interval`, `compact_threshold`, `target_volume_rows`, `keep_snapshots`; read `sync_mode`, `wal_flush_trigger`, `volume_stats`; run `snapshot`, `checkpoint`, `vacuum`, `restore` |
| `version` | Engine and server version |
| `list_functions` | All built-in SQL functions with signatures, grouped by category |

## Auto-injected Instructions

The server sends [MCP instructions](https://modelcontextprotocol.io/specification/2025-03-26/server/utilities/instructions) during the connection handshake, so any AI client receives a compact Stoolap SQL reference on connect: data types, tool routing, upsert syntax, index and vector rules, transaction rules, and the known limitations of the 0.4.x engine.

For the full reference with the live schema, attach the `sql-assistant` prompt or read `stoolap://sql-reference`.

## Resources

| URI | Description |
|-----|-------------|
| `stoolap://schema` | Full database schema with all tables, views, columns, indexes, and DDL statements (JSON) |
| `stoolap://sql-reference` | Live database schema plus the complete Stoolap SQL reference (Markdown) |

## Prompts

| Prompt | Description |
|--------|-------------|
| `sql-assistant` | Same content as `stoolap://sql-reference` delivered as an MCP prompt. Use whichever your client supports. |

## SQL Coverage

- **7 data types**: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N)
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, self-joins, multi-table
- **Subqueries**: scalar, IN/NOT IN, EXISTS/NOT EXISTS, ANY/SOME/ALL, correlated, derived tables
- **CTEs**: WITH, WITH RECURSIVE, multiple CTEs, column aliases, WITH before INSERT/UPDATE/DELETE
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST, every aggregate with OVER, named windows
- **Aggregates**: 17 functions with DISTINCT and FILTER; GROUP BY ROLLUP, CUBE, GROUPING SETS; DISTINCT ON
- **Scalar functions**: 98 functions across string, math, date/time, JSON, hash, conditional, type and vector categories
- **Upsert**: ON CONFLICT DO UPDATE / DO NOTHING with EXCLUDED, ON DUPLICATE KEY UPDATE
- **Bulk load**: COPY table FROM 'file.csv' WITH (FORMAT CSV, HEADER true)
- **Transactions**: READ COMMITTED and SNAPSHOT isolation, savepoints
- **Temporal queries**: AS OF TIMESTAMP, AS OF TRANSACTION
- **Indexes**: BTree, Hash, Bitmap, HNSW (vector), unique, composite
- **Vector search**: k-NN with L2, cosine and inner product distances, HNSW indexing
- **EXPLAIN / EXPLAIN ANALYZE**

## Safety

- **Single statement per call**: the engine executes every statement of a multi-statement string but reports only the last one, so semicolon-separated batches are rejected.
- **Tool routing**: `query` accepts only read statements, `execute` is blocked while a transaction is open, and transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT) are only reachable through the transaction tools, so the server always knows the connection's transaction state.
- **Read-only mode** rejects every write, including COPY, DDL, SET, ANALYZE, VACUUM and PRAGMA actions.
- **COPY ... FROM reads files on the host** with the server process's permissions, so an assistant can load any readable file into a table. Run with `--read-only` when that is not acceptable.
- **DDL outside transactions**: only CREATE TABLE is rolled back reliably by the engine, so DDL, TRUNCATE and COPY are refused inside a transaction.
- **EXPLAIN ANALYZE** is refused for write statements because it executes them.
- **Injection guards**: table and view names are double-quoted, savepoint and pragma names must be bare identifiers, pragma values are validated per pragma.
- The database is closed cleanly (open transaction rolled back, checkpoint on close) when the client disconnects or the process receives SIGINT/SIGTERM.

## Requirements

- Node.js >= 20
- `@stoolap/node` (installed automatically) with prebuilt engine libraries for Linux (x64, arm64), macOS (x64, arm64) and Windows (x64). A C compiler is needed for its small N-API addon. CI exercises Linux and macOS.

## Development

```bash
git clone https://github.com/stoolap/stoolap-mcp.git
cd stoolap-mcp
npm install
npm test          # builds, then runs the end-to-end smoke tests against the built server
node build/index.js --path ./mydata
```

Releases are published to npm from the `v*` tag workflow using npm trusted publishing (OIDC); no token is needed.

## License

Apache-2.0
