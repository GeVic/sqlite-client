# SQLite Scratch

A minimal, read-only SQLite database reader written in TypeScript for the CodeCrafters "Build your own SQLite" challenge. It parses the SQLite file format directly (pages, headers, varints, B-tree structures) and executes a small subset of SQL.

## Features

- Reads a SQLite database file directly from disk
- Lists tables from the schema
- Executes a limited subset of SELECT queries
- Counts rows efficiently for a single table
- Uses index B-trees for simple equality WHERE clauses (TEXT columns)
- Outputs results as pipe-delimited lines (col1|col2|...)

## Prerequisites

- Bun (v1.x or newer)
- macOS or Linux shell (should also work on Windows via WSL)

## Project Structure

- `app/main.ts`: Core implementation (file parser, B-tree traversal, query engine)
- `package.json`: Script to run the program via Bun
- `sample.db`, `companies.db`: Example SQLite databases

## Setup

- Install dependencies (dev types only):
  - `bun install`

No other runtime dependencies are required.

## Usage

Run the program by passing a database path and a command:

- `bun run app/main.ts <db_path> "<command>"`

Examples:
- `bun run app/main.ts sample.db ".dbinfo"`
- `bun run app/main.ts sample.db ".tables"`
- `bun run app/main.ts sample.db "SELECT COUNT(*) FROM apples"`
- `bun run app/main.ts sample.db "SELECT id,name FROM apples"`
- `bun run app/main.ts sample.db "SELECT id,name FROM apples WHERE name = 'Granny Smith'"`

You can also use the npm script form:
- `npm run dev -- <db_path> "<command>"`

Output formatting:
- SELECT results are printed as `|`-separated values, one row per line.

## Supported Commands

- Meta commands:
  - `.dbinfo`
    - Prints page size and number of schema entries (tables/indexes on the first page)
  - `.tables`
    - Prints user tables (excludes internal tables prefixed with `sqlite_`)

- SQL (subset):
  - `SELECT COUNT(*) FROM <table>`
  - `SELECT col1,col2,... FROM <table>`
  - `SELECT col1,col2,... FROM <table> WHERE <text_column> = 'value'`
    - If a single-column TEXT index exists for `<text_column>`, it will be used to find matching rowids quickly.
    - Otherwise, falls back to a full table scan.

Notes:
- Column list must be explicit (`SELECT id,name`). `SELECT *` is not supported.

## How It Works (Internals)

- File header
  - Reads the 100-byte database header and derives the page size from offset 16.
- Schema and first page
  - Treats page 1 as the schema table (sqlite_master-like). Reads the page header and cell pointer array to discover tables and indexes.
- Records and varints
  - Implements SQLite varint decoding.
  - Parses record headers and serial types to extract column values (INTEGER, FLOAT, TEXT, BLOB; limited handling of large integers and blobs).
- Table B-tree traversal
  - Supports table interior pages (0x05) and table leaf pages (0x0D).
  - Reads cells, decodes payloads, and returns row values.
  - Handles INTEGER PRIMARY KEY tables by treating the rowid appropriately.
- Index B-tree traversal
  - Supports index interior pages (0x02) and index leaf pages (0x0A).
  - For `WHERE col = 'value'` (TEXT), traverses the index to collect matching rowids.
  - Fetches rows by rowid using a targeted table traversal to avoid scanning the entire table.
- Column resolution
  - Parses the `CREATE TABLE` SQL (naively) to map requested column names to positions.

## Limitations

This is a learning-oriented implementation that intentionally supports a narrow slice of SQLite:

- Read-only; no INSERT/UPDATE/DELETE/DDL
- Only `SELECT` with:
  - Explicit column lists (`SELECT *` is not supported)
  - Optional simple equality `WHERE <text_column> = 'literal'`
- Only single-column TEXT indexes are utilized for WHERE equality
- No JOINs, ORDER BY, LIMIT/OFFSET, expressions, functions, or multi-condition WHERE clauses
- Naive `CREATE TABLE` parsing (may break with complex definitions, constraints, quoting, or composite keys)
- Limited type coverage:
  - 64-bit integers may be imprecise (JavaScript number limits)
  - BLOBs are read but not formatted nicely for output
- Payloads that span overflow pages are not handled
- Assumes well-formed databases; minimal error handling
- Simplistic index detection (index name matching on column substring)

## Development

- Run locally:
  - `bun run app/main.ts <db> "<command>"`
- Script alias:
  - `npm run dev -- <db> "<command>"`

Suggested next steps (if you want to extend this):
- Add a tiny SQL parser and support `SELECT *`
- Support more WHERE operators and multiple conditions
- Implement overflow page handling
- Improve type handling (BigInt for 64-bit integers)
- Robust index detection and support for numeric indexes
- Better formatting for BLOB/NULL values
- Additional meta commands (e.g., `.schema`)

## Acknowledgements

Built as part of the CodeCrafters "Build your own SQLite" challenge.