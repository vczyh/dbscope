package mysql

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

type Transaction struct {
	BinlogFile       string
	GTID             string
	StartTime        time.Time
	EndTime          time.Time
	QueryExecSeconds uint32
	StartPos         uint32
	EndPos           uint32
	Size             uint64
	Tables           map[string]bool
	Inserts          int
	Updates          int
	Deletes          int
	Query            string
	SQLs             []string
	IsDDL            bool

	originSize uint64
}

func (t *Transaction) TotalRows() int {
	return t.Inserts + t.Updates + t.Deletes
}

// Duration returns EndTime - StartTime. If EndTime is zero, returns 0.
func (t *Transaction) Duration() time.Duration {
	if t.EndTime.IsZero() {
		return 0
	}
	return t.EndTime.Sub(t.StartTime)
}

func (t *Transaction) TableList() string {
	tables := make([]string, 0, len(t.Tables))
	for table := range t.Tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return strings.Join(tables, ", ")
}

// HandleEvent processes a single binlog event, updating the current transaction state.
// When a transaction completes, onCommit is called. Returns the (possibly new) current transaction.
// If decodeSQL is true, row events are decoded into SQL statements stored on the transaction.
func HandleEvent(e *replication.BinlogEvent, current *Transaction, decodeSQL bool, onCommit func(*Transaction)) *Transaction {
	finish := func() {
		// it won't happen
		if current.originSize != 0 && current.originSize != current.Size {
			panic("transaction size does not match transaction_length")
		}

		onCommit(current)
		current = nil
	}

	eventTime := time.Unix(int64(e.Header.Timestamp), 0)

	switch e.Header.EventType {
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT:
		gtidEvent, ok := e.Event.(*replication.GTIDEvent)
		if !ok {
			if tagged, ok2 := e.Event.(*replication.GtidTaggedLogEvent); ok2 {
				gtidEvent = &tagged.GTIDEvent
			}
		}
		if gtidEvent != nil {
			u, _ := uuid.FromBytes(gtidEvent.SID)
			gtid := fmt.Sprintf("%s:%d", u.String(), gtidEvent.GNO)
			if current != nil {
				current.EndPos = e.Header.LogPos - e.Header.EventSize
				finish()
			}
			current = &Transaction{
				originSize: gtidEvent.TransactionLength,
				GTID:       gtid,
				//StartTime:  time.Unix(int64(e.Header.Timestamp), 0),
				StartPos: e.Header.LogPos - e.Header.EventSize,
				Size:     uint64(e.Header.EventSize),
				Tables:   make(map[string]bool),
			}
		}

	case replication.ANONYMOUS_GTID_EVENT:
		current = &Transaction{
			GTID: "ANONYMOUS",
			//StartTime: eventTime,
			StartPos: e.Header.LogPos - e.Header.EventSize,
			Size:     uint64(e.Header.EventSize),
			Tables:   make(map[string]bool),
		}

	case replication.QUERY_EVENT:
		current.Size += uint64(e.Header.EventSize)
		current.EndTime = eventTime
		qe := e.Event.(*replication.QueryEvent)
		query := strings.ToUpper(strings.TrimSpace(string(qe.Query)))
		current.QueryExecSeconds = qe.ExecutionTime
		switch query {
		case "COMMIT", "ROLLBACK":
			current.EndTime = eventTime
			finish()
		case "BEGIN":
			current.StartTime = eventTime
		default:
			if current.StartTime.IsZero() {
				current.StartTime = eventTime
			}
			if current.EndTime.IsZero() {
				current.EndTime = eventTime
			}
			current.IsDDL = true
			current.Query = string(qe.Query)
			if len(qe.Schema) > 0 {
				current.Tables[string(qe.Schema)+".<DDL>"] = true
			}
		}

	case replication.TABLE_MAP_EVENT:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			current.EndTime = eventTime
			tme := e.Event.(*replication.TableMapEvent)
			tableName := fmt.Sprintf("%s.%s", tme.Schema, tme.Table)
			current.Tables[tableName] = true
		}

	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			current.EndTime = eventTime
			re := e.Event.(*replication.RowsEvent)
			current.Inserts += len(re.Rows)
			if decodeSQL {
				current.SQLs = append(current.SQLs, buildInsertSQLs(re)...)
			}
		}

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.PARTIAL_UPDATE_ROWS_EVENT:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			current.EndTime = eventTime
			re := e.Event.(*replication.RowsEvent)
			current.Updates += len(re.Rows) / 2
			if decodeSQL {
				current.SQLs = append(current.SQLs, buildUpdateSQLs(re)...)
			}
		}

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			current.EndTime = eventTime
			re := e.Event.(*replication.RowsEvent)
			current.Deletes += len(re.Rows)
			if decodeSQL {
				current.SQLs = append(current.SQLs, buildDeleteSQLs(re)...)
			}
		}

	case replication.XID_EVENT:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			current.EndPos = e.Header.LogPos
			current.EndTime = eventTime
			finish()
		}

	case replication.STOP_EVENT:
		if current != nil {
			current.EndPos = e.Header.LogPos - e.Header.EventSize
			finish()
		}

	case replication.ROTATE_EVENT:
		if current != nil {
			current.EndPos = e.Header.LogPos - e.Header.EventSize
			finish()
		}

	default:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
		}
	}

	return current
}

func buildInsertSQLs(re *replication.RowsEvent) []string {
	schema := string(re.Table.Schema)
	table := string(re.Table.Table)
	sqls := make([]string, 0, len(re.Rows))
	for _, row := range re.Rows {
		cols := make([]string, len(row))
		vals := make([]string, len(row))
		for i, v := range row {
			cols[i] = "`" + columnName(re, i) + "`"
			vals[i] = formatValue(v)
		}
		sqls = append(sqls, fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s);",
			schema, table, strings.Join(cols, ", "), strings.Join(vals, ", ")))
	}
	return sqls
}

func buildUpdateSQLs(re *replication.RowsEvent) []string {
	schema := string(re.Table.Schema)
	table := string(re.Table.Table)
	sqls := make([]string, 0, len(re.Rows)/2)
	for i := 0; i+1 < len(re.Rows); i += 2 {
		before := re.Rows[i]
		after := re.Rows[i+1]
		sets := make([]string, len(after))
		for j, v := range after {
			sets[j] = fmt.Sprintf("`%s`=%s", columnName(re, j), formatValue(v))
		}
		wheres := make([]string, len(before))
		for j, v := range before {
			wheres[j] = formatWhere(columnName(re, j), v)
		}
		sqls = append(sqls, fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s;",
			schema, table, strings.Join(sets, ", "), strings.Join(wheres, " AND ")))
	}
	return sqls
}

func buildDeleteSQLs(re *replication.RowsEvent) []string {
	schema := string(re.Table.Schema)
	table := string(re.Table.Table)
	sqls := make([]string, 0, len(re.Rows))
	for _, row := range re.Rows {
		wheres := make([]string, len(row))
		for j, v := range row {
			wheres[j] = formatWhere(columnName(re, j), v)
		}
		sqls = append(sqls, fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;",
			schema, table, strings.Join(wheres, " AND ")))
	}
	return sqls
}

func columnName(re *replication.RowsEvent, idx int) string {
	if re.Table != nil && idx < len(re.Table.ColumnName) && len(re.Table.ColumnName[idx]) > 0 {
		return string(re.Table.ColumnName[idx])
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
