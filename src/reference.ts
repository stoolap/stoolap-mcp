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

// Reference texts for engine 0.5.0. Every behaviour stated here was run
// against the engine; keep it that way when editing.

export const INSTRUCTIONS = `Stoolap is an embedded SQL database (engine 0.5.x). Use these tools to work with it.

Tools:
- "query" for SELECT, WITH ... SELECT, SHOW, DESCRIBE and EXPLAIN. Results come back as {columns, rows, row_count, truncated}; rows are cut at a row limit, so add LIMIT or aggregate instead of reading whole tables.
- "execute" for INSERT, UPDATE, DELETE, COPY, CREATE, ALTER, DROP, TRUNCATE, ANALYZE, VACUUM and SET. RETURNING makes it return rows.
- "execute_batch" runs one statement for many parameter sets. "explain" shows a plan.
- "get_schema" (all tables, or one with "table") before writing queries; "list_functions" for function signatures.
- One statement per call. Transactions only through begin_transaction, commit_transaction, rollback_transaction and savepoint; while one is open, query, execute and execute_batch run inside it.

SQL essentials:
- Types: INTEGER, FLOAT, TEXT, BOOLEAN, TIMESTAMP, JSON, VECTOR(N). A PRIMARY KEY column is INTEGER; composite PRIMARY KEY (a, b) is allowed.
- Parameters: $1, $2 or ? with an array, :name with an object. Do not mix the two styles.
- Cast only with CAST(x AS type); "::" is not supported.
- Upsert: INSERT ... ON CONFLICT (cols) DO UPDATE SET c = EXCLUDED.c | DO NOTHING, or ON DUPLICATE KEY UPDATE c = EXCLUDED.c.
- A TEXT that reads as a number compares as that number; any other TEXT compared with a number gives NULL. Integer overflow is an error, division by zero is NULL. SUM over INTEGER is INTEGER, AVG is FLOAT.
- Unknown or ambiguous column names and table qualifiers are errors; name an aliased table by its alias.
- Every query may stop with "query cancelled" when it runs past the server's time limit: narrow it, or add an index.
- DDL inside a transaction: CREATE TABLE rolls back with it; the server refuses the other DDL there because ALTER, CREATE INDEX and CREATE VIEW would persist after ROLLBACK, and DROP TABLE and TRUNCATE would lose rows.

Not supported: stored procedures, triggers, user-defined functions, GRANT/REVOKE, full-text search, materialized views, MERGE, UPDATE ... FROM, composite foreign keys, GROUPS window frames, EXCLUDE, two named windows in one WINDOW clause, JSON_SET and other JSON modifiers, a statement that is only VALUES (...).

Read stoolap://sql-reference or attach the "sql-assistant" prompt for the full reference with the live schema.`;

export const FUNCTION_SECTIONS: Record<string, string> = {
  aggregate: `## Aggregate Functions
COUNT(*), COUNT(expr), COUNT(DISTINCT expr)
SUM(expr) - INTEGER for INTEGER input (exact; "Integer overflow" error past the range), FLOAT otherwise; NULL for no rows; booleans count as 1/0
AVG(expr) - always FLOAT
MIN(expr) / MAX(expr) - any type; NaN sorts above every number
FIRST(expr) / LAST(expr) - first / last value in the group (order-dependent)
MEDIAN(expr)
STRING_AGG(expr [, separator] [ORDER BY ...]) - separator defaults to ','; GROUP_CONCAT is an alias
ARRAY_AGG(expr) - JSON array; NULL for no rows
STDDEV / STDDEV_SAMP, VARIANCE / VAR_SAMP - sample (N-1); STDDEV_POP, VAR_POP - population (N)

Every aggregate takes DISTINCT and FILTER (WHERE cond) and works as a window function with OVER.
GROUP BY ROLLUP(a, b), CUBE(a, b), GROUPING SETS((a, b), (a), ()); GROUPING(col) is 1 on super-aggregate rows.`,

  window: `## Window Functions
ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE(n)
LEAD(expr [, offset [, default]]), LAG(expr [, offset [, default]])
FIRST_VALUE(expr), LAST_VALUE(expr), NTH_VALUE(expr, n)
PERCENT_RANK(), CUME_DIST()

OVER ([PARTITION BY cols] [ORDER BY cols [NULLS FIRST|LAST]] [ROWS | RANGE BETWEEN ... AND ...])
Named window: WINDOW w AS (...), used as OVER w or OVER (w ORDER BY ...). One named window per WINDOW clause.
Not supported: GROUPS frames, EXCLUDE.`,

  string: `## String Functions
UPPER, LOWER, LENGTH / CHAR_LENGTH (characters), CHAR(code)
CONCAT(a, b, ...), CONCAT_WS(sep, a, b, ...) (skips NULLs), a || b
SUBSTRING(s, start [, len]) / SUBSTR - 1-based; no FROM ... FOR form
TRIM / LTRIM / RTRIM(s [, chars]) - no LEADING/TRAILING keywords
REPLACE(s, from, to), REVERSE(s), LEFT(s, n), RIGHT(s, n), REPEAT(s, n)
SPLIT_PART(s, delim, n) - 1-based; an empty delimiter returns the whole string
POSITION(sub IN s), STRPOS(s, sub), INSTR(s, sub), LOCATE(sub, s) - 1-based, 0 if absent
LPAD(s, len, fill), RPAD(s, len, fill)
STARTS_WITH(s, p), ENDS_WITH(s, p), CONTAINS(s, sub) - BOOLEAN
REPEAT, LPAD, RPAD and REPLACE refuse results over 10 MB.

Patterns: LIKE [ESCAPE 'c'] (case-sensitive), ILIKE, GLOB, REGEXP / RLIKE, each with NOT.`,

  math: `## Math Functions
ABS, SIGN, ROUND(x [, decimals]) (half away from zero), FLOOR, CEILING / CEIL, TRUNCATE / TRUNC(x, decimals)
MOD(x, y) or x % y - keeps the fraction: MOD(-7.5, 2) = -1.5
POWER / POW, SQRT, EXP, LN, LOG(x) (base 10), LOG(base, x), LOG10, LOG2
PI(), RANDOM() (float in [0, 1)), SIN, COS, TAN (radians)

Arithmetic + - * / %; bitwise & | ^ ~ << >> (shift count 0-63). INTEGER overflow, ABS and negation of the minimum included, is an error. Division by zero is NULL.`,

  datetime: `## Date/Time Functions
NOW() / CURRENT_TIMESTAMP (UTC), CURRENT_DATE, CURRENT_TIME
DATE_TRUNC(unit, ts) - year, quarter, month, week, day, hour, minute, second
TIME_TRUNC(duration, ts) - buckets such as '15m', '1h', '1d'
EXTRACT(field FROM ts) - YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, DOW (0 = Sunday), ISODOW, DOY, WEEK, QUARTER, EPOCH
YEAR(ts), MONTH(ts), DAY(ts), HOUR(ts), MINUTE(ts), SECOND(ts)
DATE_ADD(ts, n [, unit]) / DATE_SUB - default unit day
DATEDIFF(end, start) / DATE_DIFF - whole days, end minus start
TO_CHAR(ts, format) - YYYY, YY, MM, MON, MONTH, DD, DY, DAY, HH24, HH, HH12, MI, SS

INTERVAL: ts + INTERVAL '7 days', n * INTERVAL '1 hour', -INTERVAL '1 day'. Month and year steps follow the calendar (Jan 31 + 1 month = Feb 28). One unit per literal: '1 month 1 day' is an error.
Literals: TIMESTAMP '2026-01-01 12:00:00', DATE '2026-01-01', TIME '12:00:00'. Timestamps are UTC, nanosecond precision, stored between 1677-09-21 and 2262-04-11.`,

  json: `## JSON Functions
JSON_EXTRACT(json, '$.path') - dot paths with [n] indexes
JSON_ARRAY_LENGTH(json [, path]), JSON_TYPE / JSON_TYPEOF(json [, path]), JSON_VALID(text), JSON_KEYS(json)
JSON_ARRAY(v, ...), JSON_OBJECT(k, v, ...)
col -> 'key' returns JSON (chainable), col ->> 'key' returns TEXT, col -> 0 indexes an array.
JSON compares only with = and <>. Not available: JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_REMOVE, JSON_CONTAINS, indexes on JSON paths; rewrite the whole value with UPDATE.`,

  hash: `## Hash Functions
MD5, SHA1, SHA256, SHA384, SHA512 - hex digests; CRC32 - INTEGER`,

  conditional: `## Conditional Functions
COALESCE(a, b, ...), NULLIF(a, b), IFNULL(a, b), IIF(cond, a, b)
CASE x WHEN v THEN r ... ELSE d END, CASE WHEN cond THEN r ... ELSE d END`,

  type: `## Type and Comparison Functions
CAST(x AS INTEGER | FLOAT | TEXT | BOOLEAN | TIMESTAMP | JSON) - a FLOAT out of INTEGER range is an error
TYPEOF(x) - 'INTEGER', 'FLOAT', 'TEXT', 'BOOLEAN', 'TIMESTAMP', 'JSON', 'VECTOR' or 'NULL'
COLLATE(x, 'BINARY' | 'NOCASE' | 'NOACCENT' | 'NUMERIC')
GREATEST(a, ...), LEAST(a, ...)

Comparison: TEXT that reads as a number compares as that number, other TEXT against a number is NULL. BOOLEAN and TIMESTAMP are not numbers. NaN equals NaN and sorts above every number; 0.0 equals -0.0. Mixed-type sort order: BOOLEAN, numbers, TEXT, TIMESTAMP.
NULL: IS [NOT] NULL, IS [NOT] DISTINCT FROM (NULL-safe equality), IS [NOT] TRUE / FALSE. NULL = NULL is NULL.`,

  vector: `## Vector Functions
VEC_DISTANCE_L2(a, b), VEC_DISTANCE_COSINE(a, b) (0..2), VEC_DISTANCE_IP(a, b) (negative inner product)
VEC_DIMS(v), VEC_NORM(v), VEC_TO_TEXT(v); a <=> b is L2 distance
VECTOR(N) values are written as '[0.1, 0.2, 0.3]' and returned as JSON arrays. Mismatched dimensions are an error. VECTOR compares only with = and <>.

k-NN: SELECT id, VEC_DISTANCE_COSINE(emb, '[...]') AS d FROM t ORDER BY d LIMIT 10
HNSW: CREATE INDEX i ON t(emb) USING HNSW WITH (metric = 'cosine', m = 32, ef_construction = 200, ef_search = 128). metric 'l2' (default), 'cosine' or 'ip' must match the query's distance function, or the query scans.
EMBED(text) exists only in engine builds with the semantic feature: a 384-dimension embedding computed locally.`,

  system: `## System and Table-Valued Functions
VERSION(), CURRENT_TRANSACTION_ID() (NULL outside a transaction), SLEEP(seconds)
GENERATE_SERIES(start, stop [, step]) in FROM, up to 10,000,000 rows (more is an error):
  SELECT * FROM GENERATE_SERIES(1, 10) AS gs(value)
  SELECT * FROM GENERATE_SERIES(TIMESTAMP '2026-01-01', TIMESTAMP '2026-12-31', INTERVAL '1 month')`,
};

export const SQL_REFERENCE = `## Stoolap SQL Reference (engine 0.5.x)

### Types
INTEGER (64-bit), FLOAT (64-bit), TEXT (UTF-8), BOOLEAN, TIMESTAMP (UTC, ns), JSON, VECTOR(N) (f32).
No BLOB, ARRAY, ENUM, DATE-only or INTERVAL column types. A single-column PRIMARY KEY must be INTEGER.

### Statements
- SELECT [DISTINCT [ON (exprs)]] ... FROM t [AS a] [AS OF TIMESTAMP '...' | AS OF TRANSACTION n] [JOIN ...] [WHERE] [GROUP BY ... | ROLLUP | CUBE | GROUPING SETS] [HAVING] [WINDOW w AS (...)] [ORDER BY ... [NULLS FIRST|LAST]] [LIMIT n [OFFSET m]]
- Set operations UNION [ALL], INTERSECT [ALL], EXCEPT [ALL]; INTERSECT binds tighter. ORDER BY / LIMIT apply to the whole result.
- INSERT INTO t [(cols)] VALUES (...), ... | SELECT ... [ON CONFLICT [(cols)] DO UPDATE SET c = EXCLUDED.c [WHERE ...] | DO NOTHING] [RETURNING ...]
- ON DUPLICATE KEY UPDATE c = EXCLUDED.c is also accepted. ON CONFLICT DO UPDATE refuses to touch one row twice in a statement.
- UPDATE t SET c = expr, ... [WHERE] [RETURNING]. An INTEGER primary key can be updated (ON UPDATE CASCADE follows). No UPDATE ... FROM.
- DELETE FROM t [WHERE] [RETURNING]
- WITH [RECURSIVE] ... before SELECT, INSERT, UPDATE, DELETE, and inside subqueries. Recursive CTEs take UNION ALL only and stop silently after 10,000 iterations.
- TRUNCATE TABLE t; COPY t [(cols)] FROM '/path' WITH (FORMAT CSV | JSON, HEADER true, DELIMITER ',', NULL '') reads a file on the server host.
- Subqueries: scalar, IN, EXISTS, ANY/SOME/ALL, row comparisons (a, b) IN (SELECT x, y ...), correlated (also in JOIN ON), derived tables.
- Joins: INNER, LEFT, RIGHT, FULL, CROSS, NATURAL, ON or USING.
- Writing one column twice in an INSERT column list or an UPDATE SET is an error. Text after a complete statement is a parse error.

### DDL
- CREATE TABLE [IF NOT EXISTS] t (col type [PRIMARY KEY] [AUTO_INCREMENT] [NOT NULL] [UNIQUE] [DEFAULT v] [CHECK (...)] [REFERENCES p(col) [ON DELETE|UPDATE CASCADE | SET NULL | RESTRICT | NO ACTION]], ..., [PRIMARY KEY (a, b)], [CHECK (...)], [FOREIGN KEY (c) REFERENCES p(col)]) [CLUSTER BY (cols)]
- CREATE TABLE t AS SELECT ...
- Foreign keys are single-column; self-references are allowed. DROP TABLE is refused while another table references it, unless DROP TABLE t CASCADE.
- ALTER TABLE t ADD COLUMN c type [DEFAULT v] | DROP COLUMN c | RENAME COLUMN a TO b | MODIFY COLUMN c type [NOT NULL] | RENAME TO n | CLUSTER BY (cols) | DROP CLUSTER BY | AUTO_INCREMENT = n
- MODIFY COLUMN converts existing rows and refuses a change that breaks NOT NULL or UNIQUE. ADD COLUMN takes no CHECK, UNIQUE, PRIMARY KEY, AUTO_INCREMENT or REFERENCES.
- CREATE [UNIQUE] INDEX [IF NOT EXISTS] i ON t(a [, b]) [USING BTREE | HASH | BITMAP | HNSW] [WITH (...)]; DROP INDEX [IF EXISTS] i ON t
- Default index type: BTREE for INTEGER, FLOAT, TIMESTAMP and TEXT; HASH for JSON; BITMAP for BOOLEAN; HNSW for VECTOR. One index per column.
- CREATE VIEW [IF NOT EXISTS] v AS SELECT ...; DROP VIEW [IF EXISTS] v. Views are read-only and take no parameters or AS OF.

### Transactions
- begin_transaction with READ COMMITTED (default: each statement sees the latest commits) or SNAPSHOT (the view at BEGIN).
- savepoint create / rollback_to (keeps the savepoint) / release (drops it and later ones).
- A failed statement is undone and leaves the transaction open. Unique violations are reported when the statement ends.
- Inside a transaction CREATE TABLE rolls back; ALTER TABLE, CREATE INDEX and CREATE VIEW would persist and TRUNCATE and DROP TABLE would lose rows, so the server refuses them. COPY and VACUUM are refused by the engine.

### Temporal queries
SELECT ... FROM t AS OF TIMESTAMP '...' | AS OF TRANSACTION n, also in joins and subqueries. All AS OF clauses resolve against the statement's start. Points older than the history kept (VACUUM, TRUNCATE, sealing and compaction cut it) give an error.

### Errors instead of guesses
Unknown or ambiguous columns and qualifiers, a missing parameter, mixing $n and :name, a fractional LIMIT and INTEGER overflow are errors. A runtime error fails the whole statement wherever it occurs.

### Metadata and plans
SHOW TABLES, SHOW VIEWS, SHOW INDEXES FROM t, SHOW CREATE TABLE t, SHOW CREATE VIEW v, DESCRIBE t.
EXPLAIN prints one plan step per row (column "plan"). A line starting with "else" is the fallback used when the step above cannot run. EXPLAIN ANALYZE runs the statement, adds (actual time=..., rows=...) to the top line and ends with "Paths: ..." naming the paths taken.
ANALYZE t collects optimizer statistics. VACUUM [t] removes deleted rows and old versions and discards AS OF history.

### PRAGMA (pragma tool)
- Settings: checkpoint_interval (s, 60), compact_threshold (4), target_volume_rows (1048576, min 65536), keep_snapshots (3), hot_max_rows (262144, 0 off), hot_max_bytes (0 off), group_cache_mb (64), index_cache_mb (16), index_build_mb (64), query_timeout (ms for this session, 0 off)
- Read-only: sync_mode (0 none, 1 normal, 2 full), wal_flush_trigger; set them in the DSN
- Reports: volume_stats, memory_stats, group_cache_stats, index_stats, index_read_stats, verify (reads every volume and reports ok, damaged, unreadable or failed per segment; changes nothing)
- Actions: checkpoint, snapshot, restore [= 'YYYYMMDD-HHMMSS.fff'], vacuum, index_backfill [= n]

### Storage notes
Recent rows live in a row store; checkpoints seal older rows into compressed column volumes. Single-column BTREE indexes on INTEGER, FLOAT, TIMESTAMP or TEXT, and HASH indexes on TEXT, also reach sealed rows; other indexes on sealed rows fall back to pruned scans. If a volume file is damaged or missing, statements on that table fail with an error naming it, and other tables keep working.

### Not supported
Stored procedures, triggers, user-defined functions, GRANT/REVOKE, full-text search, materialized views, LISTEN/NOTIFY, MERGE, UPDATE ... FROM, composite foreign keys, GROUPS frames, EXCLUDE, two named windows in one WINDOW clause, JSON modifier functions, the :: cast, START TRANSACTION, a statement that is only VALUES (...) or TABLE t, compound interval literals.`;
