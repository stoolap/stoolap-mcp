# stoolap-mcp

MCP (Model Context Protocol) server for [Stoolap](https://github.com/stoolap/stoolap), an embedded SQL database. Lets AI assistants query, manage, and analyze Stoolap databases with full access to all SQL features.

Works with any MCP-compatible AI client: Claude Desktop, Claude Code, Cursor, Windsurf, Cline, and others.

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

## Tools (30)

### Query and Analysis

| Tool | Description |
|------|-------------|
| `query` | Run SELECT, SHOW, DESCRIBE queries. Supports JOINs, CTEs, window functions, subqueries, set operations, JSON operators, vector search, temporal queries (AS OF), and all aggregate/scalar functions. Returns results as JSON. |
| `execute` | Run INSERT, UPDATE, DELETE with parameter binding ($1, $2, ...). Supports ON DUPLICATE KEY UPDATE (upsert), RETURNING clause, and expression-based updates. Returns affected row count. |
| `execute_batch` | Execute the same SQL with multiple parameter sets in a single atomic transaction. Parses SQL once, reuses for every row. All rows succeed or all are rolled back. |
| `explain` | Show query execution plan. Set analyze=true to run the query and show actual runtime stats (row counts, timing, join algorithms). |

### Transaction Control

| Tool | Description |
|------|-------------|
| `begin_transaction` | Begin a new transaction with optional isolation level (read_committed or snapshot). Only one active transaction at a time. |
| `transaction_execute` | Execute a DML statement within the active transaction. Sees uncommitted changes. Supports RETURNING clause. |
| `transaction_query` | Run a SELECT query within the active transaction. Sees uncommitted changes. Supports aggregates, JOINs, GROUP BY, window functions, CTEs, and subqueries. |
| `transaction_execute_batch` | Execute the same SQL with multiple parameter sets within the active transaction. |
| `commit_transaction` | Commit the active transaction. All changes become permanent. |
| `rollback_transaction` | Rollback the active transaction. All changes are discarded. |
| `savepoint` | Create a named savepoint within the active transaction. |
| `rollback_to_savepoint` | Rollback to a savepoint, undoing changes after it without aborting the transaction. |
| `release_savepoint` | Release (remove) a savepoint. Changes are kept. |

### Schema Inspection

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables |
| `list_views` | List all views |
| `describe_table` | Show columns, types, nullability, keys, defaults, and extras (AUTO_INCREMENT, foreign keys) |
| `show_create_table` | Get the full CREATE TABLE DDL including all constraints |
| `show_create_view` | Get the full CREATE VIEW DDL |
| `show_indexes` | Show all indexes on a table (type, columns, uniqueness) |
| `get_schema` | Get the complete database schema: all tables with columns, indexes, DDL, plus all views |

### Schema Modification

| Tool | Description |
|------|-------------|
| `create_table` | Create a table with INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N) columns. Supports PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT, CHECK, AUTO_INCREMENT, REFERENCES (foreign keys), IF NOT EXISTS, and CREATE TABLE AS SELECT. |
| `create_index` | Create BTREE, HASH, BITMAP, or HNSW indexes. Supports UNIQUE and composite (multi-column). HNSW accepts m, ef_construction, ef_search, metric (l2/cosine/ip) params. |
| `create_view` | Create a read-only view (persists across restarts) |
| `alter_table` | ADD COLUMN, DROP COLUMN, RENAME COLUMN, MODIFY COLUMN (change type), RENAME TO |
| `drop` | Drop a table, view, or index (supports IF EXISTS) |

### Database Administration

| Tool | Description |
|------|-------------|
| `analyze_table` | Collect optimizer statistics (histograms, distinct counts, min/max) for better query plans |
| `vacuum` | Clean up deleted rows, old MVCC versions, and compact indexes |
| `pragma` | Get/set database config: sync_mode, snapshot_interval, keep_snapshots, wal_flush_trigger. Trigger manual snapshot or vacuum. |
| `version` | Get the Stoolap engine version |
| `list_functions` | List all 130+ built-in SQL functions with signatures, grouped by category (aggregate, window, string, math, datetime, json, hash, conditional, type, vector, system) |

## Auto-injected Instructions

The server provides built-in [MCP instructions](https://modelcontextprotocol.io/specification/2025-03-26/server/utilities/instructions) that are automatically sent to the AI during the connection handshake. This means any AI client receives a comprehensive Stoolap SQL reference on connect, covering data types, supported syntax, all operator categories, index types, vector search, transaction isolation levels, and known limitations, without the user needing to configure anything. The AI can write correct Stoolap SQL from the first query.

For deeper reference (live schema + full function signatures), attach the `sql-assistant` prompt.

## Resources

| URI | Description |
|-----|-------------|
| `stoolap://schema` | Full database schema with all tables, views, columns, indexes, and DDL statements |

## Prompts

| Prompt | Description |
|--------|-------------|
| `sql-assistant` | Injects the live database schema and a complete Stoolap SQL reference (data types, all 130+ functions with signatures, operators, index types, join types, window functions, CTEs, transactions, EXPLAIN, PRAGMA, and known limitations). Provides deeper detail than the auto-injected instructions. |

## SQL Coverage

The MCP server exposes the full Stoolap SQL surface through the `query` and `execute` tools:

- **7 data types**: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N)
- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL, self-joins, multi-table
- **Subqueries**: scalar, IN/NOT IN, EXISTS/NOT EXISTS, ANY/SOME/ALL, correlated, derived tables
- **CTEs**: WITH, WITH RECURSIVE, multiple CTEs, column aliases
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE, PERCENT_RANK, CUME_DIST (plus all aggregates with OVER)
- **GROUP BY extensions**: ROLLUP, CUBE, GROUPING SETS, GROUPING()
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX, MEDIAN, STRING_AGG, ARRAY_AGG, STDDEV, VARIANCE, and more
- **100+ scalar functions**: string, math, date/time, JSON, hash, conditional, vector, type conversion
- **Operators**: arithmetic, comparison, logical, bitwise, LIKE/ILIKE/GLOB/REGEXP, JSON (->/->>), vector (<=>), BETWEEN, IN, IS [NOT] DISTINCT FROM, INTERVAL
- **Transactions**: BEGIN with isolation levels (READ COMMITTED, SNAPSHOT), COMMIT, ROLLBACK, SAVEPOINT
- **Temporal queries**: AS OF TIMESTAMP, AS OF TRANSACTION
- **Index types**: BTree, Hash, Bitmap, HNSW (vector), Unique, Composite
- **Vector search**: k-NN with L2, cosine, inner product distances and HNSW indexing
- **EXPLAIN / EXPLAIN ANALYZE** for query plan inspection

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--path <path>` | `:memory:` | Database path. Use `:memory:` for in-memory or a file path for persistence. |
| `--read-only` | `false` | Disable write operations. Read-only transactions (begin, query, commit) are still allowed for consistent reads. |

## Requirements

- Node.js >= 18
- The `@stoolap/node` package (installed automatically as a dependency)

The Stoolap native addon is bundled with `@stoolap/node` via prebuilt binaries for Linux (x64, arm64) and macOS (x64, arm64).

## Building from Source

```bash
git clone https://github.com/stoolap/stoolap-mcp.git
cd stoolap-mcp
npm install
npm run build
node build/index.js --path ./mydata
```

## License

Apache-2.0
