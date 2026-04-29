package mysql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

var (
	ErrCannotDetermineReplica = errors.New("cannot determine replica")
)

type Transaction struct {
	BinlogFile          string
	GTID                string
	When                time.Time
	startTime           time.Time
	OriginalCommitTime  time.Time
	ImmediateCommitTime time.Time
	StartPos            uint32
	EndPos              uint32
	Size                uint64
	Tables              map[string]bool
	Inserts             int
	Updates             int
	Deletes             int

	// Per-statement counts. A single SQL statement on the master may emit
	// many rows across multiple row events; these track the logical
	// statement count, while Inserts/Updates/Deletes track row counts.
	InsertStmts int
	UpdateStmts int
	DeleteStmts int

	SQLs       []string
	IsDDL      bool
	originSize uint64
}

func (t *Transaction) StartTime() time.Time {
	return t.startTime
}

func (t *Transaction) EndTime() time.Time {
	if !t.OriginalCommitTime.IsZero() {
		return t.OriginalCommitTime
	}
	return t.When
}

func (t *Transaction) Duration() time.Duration {
	return t.EndTime().Sub(t.startTime)
}

func (t *Transaction) IsReplica() (bool, error) {
	if t.OriginalCommitTime.IsZero() || t.ImmediateCommitTime.IsZero() {
		return false, ErrCannotDetermineReplica
	}
	return t.ImmediateCommitTime.After(t.OriginalCommitTime), nil
}

func (t *Transaction) TotalRows() int {
	return t.Inserts + t.Updates + t.Deletes
}

func (t *Transaction) TableList() string {
	tables := make([]string, 0, len(t.Tables))
	for table := range t.Tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return strings.Join(tables, ", ")
}

func (p *Parser) buildInsertSQLs(re *replication.RowsEvent) []string {
	schema := string(re.Table.Schema)
	table := string(re.Table.Table)
	sqls := make([]string, 0, len(re.Rows))
	for _, row := range re.Rows {
		cols := make([]string, len(row))
		vals := make([]string, len(row))
		for i, v := range row {
			cols[i] = "`" + p.columnName(re, i) + "`"
			vals[i] = formatValue(v)
		}
		sqls = append(sqls, fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
			schema, table, strings.Join(cols, ", "), strings.Join(vals, ", ")))
	}
	return sqls
}

func (p *Parser) buildUpdateSQLs(re *replication.RowsEvent) []string {
	schema := string(re.Table.Schema)
	table := string(re.Table.Table)
	sqls := make([]string, 0, len(re.Rows)/2)
	for i := 0; i+1 < len(re.Rows); i += 2 {
		before := re.Rows[i]
		after := re.Rows[i+1]
		sets := make([]string, len(after))
		for j, v := range after {
			sets[j] = fmt.Sprintf("`%s`=%s", p.columnName(re, j), formatValue(v))
		}
		wheres := make([]string, len(before))
		for j, v := range before {
			wheres[j] = formatWhere(p.columnName(re, j), v)
		}
		sqls = append(sqls, fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s",
			schema, table, strings.Join(sets, ", "), strings.Join(wheres, " AND ")))
	}
	return sqls
}

func (p *Parser) buildDeleteSQLs(re *replication.RowsEvent) []string {
	schema := string(re.Table.Schema)
	table := string(re.Table.Table)
	sqls := make([]string, 0, len(re.Rows))
	for _, row := range re.Rows {
		wheres := make([]string, len(row))
		for j, v := range row {
			wheres[j] = formatWhere(p.columnName(re, j), v)
		}
		sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s",
			schema, table, strings.Join(wheres, " AND ")))
	}
	return sqls
}

// columnName resolves a column name with this priority:
//  1. binlog row metadata (re.Table.ColumnName, only set with binlog_row_metadata=FULL)
//  2. Schema fetcher (e.g. live MySQL INFORMATION_SCHEMA), cached per schema.table
//  3. positional fallback (@1, @2, ...)
func (p *Parser) columnName(re *replication.RowsEvent, idx int) string {
	if re.Table != nil && idx < len(re.Table.ColumnName) && len(re.Table.ColumnName[idx]) > 0 {
		return string(re.Table.ColumnName[idx])
	}
	if p != nil && p.schema != nil && re.Table != nil {
		key := string(re.Table.Schema) + "." + string(re.Table.Table)
		cols, ok := p.colCache[key]
		if !ok {
			cols, _ = p.schema.ColumnNames(string(re.Table.Schema), string(re.Table.Table))
			if p.colCache == nil {
				p.colCache = make(map[string][]string)
			}
			p.colCache[key] = cols
		}
		if idx < len(cols) && cols[idx] != "" {
			return cols[idx]
		}
	}
	return fmt.Sprintf("@%d", idx+1)
}

func formatWhere(col string, v interface{}) string {
	if v == nil {
		return fmt.Sprintf("`%s` IS NULL", col)
	}
	return fmt.Sprintf("`%s`=%s", col, formatValue(v))
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + escapeString(val) + "'"
	case []byte:
		return fmt.Sprintf("X'%x'", val)
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05") + "'"
	case bool:
		if val {
			return "1"
		}
		return "0"
	case float32, float64:
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func escapeString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, c := range s {
		switch c {
		case '\\':
			b.WriteString(`\\`)
		case '\'':
			b.WriteString(`\'`)
		case '\x00':
			b.WriteString(`\0`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}
