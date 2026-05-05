# dbscope

A MySQL binlog transaction analyzer. Parses one or more binlog files and reports per-transaction details: GTID, time range, size, affected tables, row counts, and (optionally) reconstructed SQL.

## Install

```sh
go install github.com/vczyh/dbscope@latest
```

Or download a prebuilt binary from the [Releases](https://github.com/vczyh/dbscope/releases) page.

## Usage

```sh
dbscope binlog mysql-bin.000001 [mysql-bin.000002 ...]
```

Common flags:

| Flag | Description |
| --- | --- |
| `--col` | Columns to display (see list below). |
| `--sort, -s` | Rank by `size`, `duration`, or `rows`. Empty preserves order. |
| `--top, -n` | Keep only the top N transactions (memory bounded). |
| `--sql` | Decode row events into SQL statements. |
| `--stream` | Print each transaction as it is parsed (suitable for huge files). |
| `--vertical, -G` | Render rows vertically, like MySQL's `\G`. |
| `--continue` | Also parse same-prefix binlog files with larger numeric suffixes. |
| `--duration` | Filter by transaction duration (e.g. `1s`, `1m`). |
| `--size` | Filter by transaction size in bytes. |
| `--start-time`, `--end-time` | Filter by transaction time range. |
| `-H/-P/-u/-p` | MySQL server to fetch column names from when binlog metadata is absent. |

Available columns:

`no`, `file`, `gtid`, `st`, `et`, `dur`, `size`,
`insertRow`, `updateRow`, `deleteRow`,
`insertStmt`, `updateStmt`, `deleteStmt`,
`sql`, `from`.

`*Row` columns count rows changed; `*Stmt` columns count logical SQL statements (one statement may span multiple row events).

## Column names in decoded SQL

`dbscope` resolves column names in this order:

1. Binlog row metadata (requires `binlog_row_metadata=FULL` on the source server).
2. Live MySQL `INFORMATION_SCHEMA.COLUMNS` lookup, when `-H` is provided.
3. Positional fallback (`@1`, `@2`, ...).

Results from (2) are cached per `schema.table` for the duration of the run.

## Subcommands

- `binlog [files...]` — analyze binlog files.
- `repl` — replicate from a live MySQL server and analyze events as they stream in.
- `version` — print version, commit, build date, and Go version.
