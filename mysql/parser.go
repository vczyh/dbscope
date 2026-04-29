package mysql

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

type Parser struct {
	decodeSQL bool
	dur       time.Duration
	size      uint64
	st        time.Time
	et        time.Time

	schema   Schema
	colCache map[string][]string

	curFile string
	pending *pendingStmt
}

type rowKind int

const (
	rowInsert rowKind = iota
	rowUpdate
	rowDelete
)

// pendingStmt accumulates rows from one logical SQL statement that the master
// split across multiple row events. The accumulated rows are flushed to SQL
// once a row event with STMT_END_F arrives.
type pendingStmt struct {
	kind   rowKind
	sample *replication.RowsEvent // any event of this statement, used for Table info
	rows   [][]interface{}
}

type Config struct {
	DecodeSQL bool

	Duration  time.Duration
	Size      uint64
	StartTime time.Time
	EndTime   time.Time

	// Schema, when set, is consulted for column names if the binlog itself
	// does not carry them (binlog_row_metadata != FULL).
	Schema Schema
}

func NewParser(c *Config) *Parser {
	p := &Parser{
		decodeSQL: c.DecodeSQL,
		dur:       c.Duration,
		size:      c.Size,
		st:        c.StartTime,
		et:        c.EndTime,
		schema:    c.Schema,
		colCache:  make(map[string][]string),
	}
	return p
}

func (p *Parser) ParseBinlog(filename string) ([]*Transaction, error) {
	p.curFile = filename

	parser := replication.NewBinlogParser()
	parser.SetParseTime(true)

	var transactions []*Transaction
	var current *Transaction
	//base := filepath.Base(filename)

	err := parser.ParseFile(filename, 0, func(e *replication.BinlogEvent) error {
		current = p.HandleEvent(e, current, func(t *Transaction) {
			transactions = append(transactions, t)
		})
		return nil
	})

	//if current != nil && (current.IsDDL || current.TotalRows() > 0) {
	//	transactions = append(transactions, current)
	//}
	if current != nil {
		fmt.Fprintln(os.Stderr, "transaction is not complete")
	}

	if err != nil {
		return nil, err
	}

	return transactions, nil

}

func (p *Parser) ParseBinlogStream(filename string, onTxn func(*Transaction)) error {
	p.curFile = filename

	parser := replication.NewBinlogParser()
	parser.SetParseTime(true)

	var current *Transaction

	err := parser.ParseFile(filename, 0, func(e *replication.BinlogEvent) error {
		current = p.HandleEvent(e, current, func(t *Transaction) {
			onTxn(t)
		})
		return nil
	})

	if current != nil && (current.IsDDL || current.TotalRows() > 0) {
		onTxn(current)
	}

	return err
}

func (p *Parser) HandleEvent(e *replication.BinlogEvent, current *Transaction, onCommit func(*Transaction)) *Transaction {
	when := time.Unix(int64(e.Header.Timestamp), 0)
	decodeSQL := p.decodeSQL

	finish := func() {
		// it won't happen
		if current.originSize != 0 && current.originSize != current.Size {
			panic("transaction size does not match transaction_length")
		}
		// Drop any orphaned per-statement buffer (should be empty if STMT_END_F
		// arrived correctly for every statement).
		p.pending = nil
		if current.Size >= p.size &&
			current.Duration() >= p.dur &&
			(p.et.IsZero() || current.EndTime().Before(p.et)) &&
			(p.st.IsZero() || current.When.After(p.st)) {
			onCommit(current)
		}
		current = nil
	}

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
				When:                when,
				originSize:          gtidEvent.TransactionLength,
				BinlogFile:          p.curFile,
				GTID:                gtid,
				OriginalCommitTime:  time.UnixMicro(int64(gtidEvent.OriginalCommitTimestamp)),
				ImmediateCommitTime: time.UnixMicro(int64(gtidEvent.ImmediateCommitTimestamp)),
				StartPos:            e.Header.LogPos - e.Header.EventSize,
				Size:                uint64(e.Header.EventSize),
				Tables:              make(map[string]bool),
			}
		}

	case replication.ANONYMOUS_GTID_EVENT:
		// TODO
		panic("unsupported ANONYMOUS_GTID_EVENT")
		//current = &Transaction{
		//	BinlogFile: p.curFile,
		//	GTID:       "ANONYMOUS",
		//	StartPos:   e.Header.LogPos - e.Header.EventSize,
		//	Size:       uint64(e.Header.EventSize),
		//	Tables:     make(map[string]bool),
		//}

	case replication.QUERY_EVENT:
		current.Size += uint64(e.Header.EventSize)
		qe := e.Event.(*replication.QueryEvent)
		query := string(qe.Query)
		//current.QueryExecSeconds += qe.ExecutionTime
		current.When = when
		current.SQLs = append(current.SQLs, query)
		switch strings.ToUpper(strings.TrimSpace(query)) {
		case "COMMIT", "ROLLBACK":
			//if current.CommitTime.IsZero() {
			//	current.CommitTime = when
			//}
			finish()
		case "BEGIN":
			current.startTime = when
		default:
			if current.startTime.IsZero() {
				current.startTime = when
			}
			//if current.CommitTime.IsZero() {
			//	current.CommitTime = eventTime
			//}
			//current.IsDDL = true
			//current.Query = string(qe.Query)
			//if len(qe.Schema) > 0 {
			//	current.Tables[string(qe.Schema)+".<DDL>"] = true
			//}
		}

	case replication.TABLE_MAP_EVENT:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			tme := e.Event.(*replication.TableMapEvent)
			tableName := fmt.Sprintf("%s.%s", tme.Schema, tme.Table)
			current.Tables[tableName] = true
		}

	//case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
	//	if current != nil {
	//		current.Size += uint64(e.Header.EventSize)
	//		re := e.Event.(*replication.RowsEvent)
	//		current.Inserts += len(re.Rows)
	//		if decodeSQL {
	//			current.SQLs = append(current.SQLs, p.buildInsertSQLs(re)...)
	//		}
	//	}

	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.PARTIAL_UPDATE_ROWS_EVENT,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		re := e.Event.(*replication.RowsEvent)
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
		}
		if current != nil {
			stmtEnd := re.Flags&replication.STMT_END_F != 0
			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				current.Inserts += len(re.Rows)
				if stmtEnd {
					current.InsertStmts++
				}
				if decodeSQL {
					p.accumulateRows(rowInsert, re, current)
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.PARTIAL_UPDATE_ROWS_EVENT:
				current.Updates += len(re.Rows) / 2
				if stmtEnd {
					current.UpdateStmts++
				}
				if decodeSQL {
					p.accumulateRows(rowUpdate, re, current)
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				current.Deletes += len(re.Rows)
				if stmtEnd {
					current.DeleteStmts++
				}
				if decodeSQL {
					p.accumulateRows(rowDelete, re, current)
				}
			}
		}

	//case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
	//	if current != nil {
	//		current.Size += uint64(e.Header.EventSize)
	//		re := e.Event.(*replication.RowsEvent)
	//		current.Deletes += len(re.Rows)
	//		if decodeSQL {
	//			current.SQLs = append(current.SQLs, p.buildDeleteSQLs(re)...)
	//		}
	//	}

	case replication.XID_EVENT:
		if current != nil {
			current.Size += uint64(e.Header.EventSize)
			current.EndPos = e.Header.LogPos
			current.When = when
			//if current.CommitTime.IsZero() {
			//	current.CommitTime = eventTime
			//}
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

// accumulateRows buffers rows from a row event. When the event carries
// STMT_END_F, the buffered rows are flushed as SQL statements onto current.
// The master may split a single SQL statement across multiple row events
// (each capped at the binlog event size); the flag marks the last one.
func (p *Parser) accumulateRows(kind rowKind, re *replication.RowsEvent, current *Transaction) {
	if p.pending == nil {
		p.pending = &pendingStmt{kind: kind, sample: re, rows: re.Rows}
	} else {
		p.pending.rows = append(p.pending.rows, re.Rows...)
	}
	if re.Flags&replication.STMT_END_F != 0 {
		p.flushPending(current)
	}
}

func (p *Parser) flushPending(current *Transaction) {
	if p.pending == nil {
		return
	}
	merged := *p.pending.sample
	merged.Rows = p.pending.rows
	switch p.pending.kind {
	case rowInsert:
		current.SQLs = append(current.SQLs, p.buildInsertSQLs(&merged)...)
	case rowUpdate:
		current.SQLs = append(current.SQLs, p.buildUpdateSQLs(&merged)...)
	case rowDelete:
		current.SQLs = append(current.SQLs, p.buildDeleteSQLs(&merged)...)
	}
	p.pending = nil
}
