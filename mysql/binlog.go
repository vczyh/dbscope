package mysql

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

//// ParseBinlog parses a binlog file and returns all transactions.
//// If decodeSQL is true, row events are decoded into SQL statements on each transaction.
//func ParseBinlog(filename string, decodeSQL bool) ([]*Transaction, error) {
//	parser := replication.NewBinlogParser()
//	parser.SetParseTime(true)
//
//	var transactions []*Transaction
//	var current *Transaction
//	base := filepath.Base(filename)
//
//	err := parser.ParseFile(filename, 0, func(e *replication.BinlogEvent) error {
//		current = HandleEvent(e, current, decodeSQL, func(t *Transaction) {
//			if t.BinlogFile == "" {
//				t.BinlogFile = base
//			}
//			transactions = append(transactions, t)
//		})
//		if current != nil && current.BinlogFile == "" {
//			current.BinlogFile = base
//		}
//		return nil
//	})
//
//	//if current != nil && (current.IsDDL || current.TotalRows() > 0) {
//	//	transactions = append(transactions, current)
//	//}
//	if current != nil {
//		fmt.Fprintln(os.Stderr, "transaction is not complete")
//	}
//
//	if err != nil {
//		return nil, err
//	}
//
//	return transactions, nil
//}
//
//// ParseBinlogStream parses a binlog file and calls onTxn for each completed transaction.
//// If decodeSQL is true, row events are decoded into SQL statements on each transaction.
//func ParseBinlogStream(filename string, decodeSQL bool, onTxn func(*Transaction)) error {
//	parser := replication.NewBinlogParser()
//	parser.SetParseTime(true)
//
//	var current *Transaction
//	base := filepath.Base(filename)
//
//	err := parser.ParseFile(filename, 0, func(e *replication.BinlogEvent) error {
//		current = HandleEvent(e, current, decodeSQL, func(t *Transaction) {
//			if t.BinlogFile == "" {
//				t.BinlogFile = base
//			}
//			onTxn(t)
//		})
//		if current != nil && current.BinlogFile == "" {
//			current.BinlogFile = base
//		}
//		return nil
//	})
//
//	if current != nil && (current.IsDDL || current.TotalRows() > 0) {
//		if current.BinlogFile == "" {
//			current.BinlogFile = base
//		}
//		onTxn(current)
//	}
//
//	return err
//}
//
//// SortTransactions sorts transactions by the given field.
//func SortTransactions(txns []*Transaction, by string) {
//	switch by {
//	case "rows":
//		sort.Slice(txns, func(i, j int) bool {
//			return txns[i].TotalRows() > txns[j].TotalRows()
//		})
//	case "time":
//		sort.Slice(txns, func(i, j int) bool {
//			return txns[i].StartTime.Before(txns[j].StartTime)
//		})
//	default: // size
//		sort.Slice(txns, func(i, j int) bool {
//			return txns[i].Size > txns[j].Size
//		})
//	}
//}

// NewSyncer creates a BinlogSyncer configured to connect to the given MySQL server.
func NewSyncer(host string, port uint16, user, password string, serverID uint32) *replication.BinlogSyncer {
	cfg := replication.BinlogSyncerConfig{
		ServerID: serverID,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return replication.NewBinlogSyncer(cfg)
}

// QueryCurrentPos queries the current binlog position from the MySQL server.
func QueryCurrentPos(host string, port uint16, user, password string) (gomysql.Position, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := client.Connect(addr, user, password, "")
	if err != nil {
		return gomysql.Position{}, err
	}
	defer conn.Close()

	r, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return gomysql.Position{}, err
	}

	if r.RowNumber() == 0 {
		return gomysql.Position{}, fmt.Errorf("SHOW MASTER STATUS returned no rows, binlog may not be enabled")
	}

	file, _ := r.GetString(0, 0)
	pos, _ := r.GetUint(0, 1)

	return gomysql.Position{Name: file, Pos: uint32(pos)}, nil
}
