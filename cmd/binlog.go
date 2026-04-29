package cmd

import (
	"dbscope/render"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	dbmysql "dbscope/mysql"
	"dbscope/topn"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

//const (
//	execTimeMax    = "max"
//	execTimeOnly   = "exec"
//	execTimeIgnore = "ignore"
//)

const (
	headerNo         = "no"
	headerBinlog     = "file"
	headerGTID       = "gtid"
	headerStartTime  = "st"
	headerEndTime    = "et"
	headerDuration   = "dur"
	headerSize       = "size"
	headerInsertRow  = "insertRow"
	headerUpdateRow  = "updateRow"
	headerDeleteRow  = "deleteRow"
	headerInsertStmt = "insertStmt"
	headerUpdateStmt = "updateStmt"
	headerDeleteStmt = "deleteStmt"
	headerSQL        = "sql"
	headerFrom       = "from"
)

var (
	topN         int
	sortBy       string
	streamMode   bool
	decodeSQL    bool
	verticalMode bool
	continueRest bool
	//execTimeStrategy string

	duration  time.Duration
	size      uint64
	startTime string
	endTime   string

	schemaHost     string
	schemaPort     uint16
	schemaUser     string
	schemaPassword string

	columns []string
)

func init() {
	binlogCmd.Flags().IntVarP(&topN, "top", "n", 0, "Keep only the first/top N transactions (memory bounded), 0 for all")
	binlogCmd.Flags().StringVarP(&sortBy, "sort", "s", "", "Ranking metric: size, duration, rows. Empty (default) preserves original order")
	binlogCmd.Flags().BoolVar(&streamMode, "stream", false, "Stream mode: print each transaction as it is parsed, suitable for large files")
	binlogCmd.Flags().BoolVar(&decodeSQL, "sql", false, "Decode row events into the corresponding SQL statements")
	binlogCmd.Flags().BoolVarP(&verticalMode, "vertical", "G", false, "Display each row vertically (similar to MySQL's \\G)")
	binlogCmd.Flags().BoolVar(&continueRest, "continue", false, "Also parse same-prefix binlog files in the last file's directory with larger numeric suffixes")

	binlogCmd.Flags().DurationVar(&duration, "duration", 0, "Filter by duration of each transaction(such as 1s, 1m)")
	binlogCmd.Flags().Uint64Var(&size, "size", 0, "Filter by size of eatch transaction(B)")
	binlogCmd.Flags().StringVar(&startTime, "start-time", "", "Filter by start time of each transaction")
	binlogCmd.Flags().StringVar(&endTime, "end-time", "", "Filter by stop time of each transaction")

	binlogCmd.Flags().StringVarP(&schemaHost, "host", "H", "", "MySQL server host (used to fetch column names when binlog metadata is missing)")
	binlogCmd.Flags().Uint16VarP(&schemaPort, "port", "P", 3306, "MySQL server port")
	binlogCmd.Flags().StringVarP(&schemaUser, "user", "u", "root", "MySQL username")
	binlogCmd.Flags().StringVarP(&schemaPassword, "password", "p", "", "MySQL password")

	//binlogCmd.Flags().StringVarP(&execTimeStrategy, "exec-time-strategy", "e", execTimeMax,
	//	"How exec_time affects displayed duration: max (max of event duration and exec_time), exec (always exec_time), ignore (ignore exec_time)")
	binlogCmd.Flags().StringSliceVar(&columns, "col", []string{
		headerNo,
		headerGTID,
		headerStartTime,
		headerInsertRow,
		headerUpdateRow,
		headerDeleteRow,
	},
		"Specify the columns of the table",
	)
	rootCmd.AddCommand(binlogCmd)
}

var binlogCmd = &cobra.Command{
	Use:   "binlog [files...]",
	Short: "Analyze transactions in binlog files",
	Long:  "Parse one or more MySQL binlog files and display detailed transaction information.",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		//switch execTimeStrategy {
		//case execTimeMax, execTimeOnly, execTimeIgnore:
		//default:
		//	return fmt.Errorf("invalid --exec-time-strategy %q (expected max, exec, or duration)", execTimeStrategy)
		//}

		switch sortBy {
		case "", "size", "duration", "rows":
		default:
			return fmt.Errorf("invalid --sort %q (expected size, duration, or rows)", sortBy)
		}

		var st time.Time
		var err error
		if startTime != "" {
			st, err = time.ParseInLocation("2006-01-02 15:04:05", startTime, time.Local)
			if err != nil {
				return err
			}
		}
		var et time.Time
		if endTime != "" {
			et, err = time.ParseInLocation("2006-01-02 15:04:05", endTime, time.Local)
			if err != nil {
				return err
			}
		}

		var schema dbmysql.Schema
		if schemaHost != "" {
			s, err := dbmysql.NewMySQLSchema(schemaHost, schemaPort, schemaUser, schemaPassword)
			if err != nil {
				return fmt.Errorf("connect to schema source: %w", err)
			}
			defer s.Close()
			schema = s
		}

		if continueRest && len(args) > 0 {
			extra, err := findFollowingBinlogs(args[len(args)-1])
			if err != nil {
				return fmt.Errorf("failed to discover following binlog files: %v", err)
			}
			args = append(args, extra...)
		}

		if streamMode {
			if sortBy != "" || topN != 0 {
				fmt.Fprintln(os.Stderr, "--sort and --top are ignored in stream mode")
			}
			return runStream(args, schema)
		}

		parser := dbmysql.NewParser(&dbmysql.Config{
			DecodeSQL: true,
			Duration:  duration,
			Size:      size,
			StartTime: st,
			EndTime:   et,
			Schema:    schema,
		})

		var allTxns []*dbmysql.Transaction

		switch {
		case topN > 0 && sortBy != "":
			less := transactionLess(sortBy)
			tracker := topn.New(topN, less)
			for _, file := range args {
				if err := parser.ParseBinlogStream(file, tracker.Add); err != nil {
					return fmt.Errorf("failed to parse %s: %w", file, err)
				}
			}
			allTxns = tracker.Drain()

		case topN > 0:
			collected := make([]*dbmysql.Transaction, 0, topN)
			onTxn := func(t *dbmysql.Transaction) {
				if len(collected) < topN {
					collected = append(collected, t)
				}
			}
			for _, file := range args {
				if err := parser.ParseBinlogStream(file, onTxn); err != nil {
					return fmt.Errorf("failed to parse %s: %w", file, err)
				}
			}
			allTxns = collected

		default:
			for _, file := range args {
				txns, err := parser.ParseBinlog(file)
				if err != nil {
					return fmt.Errorf("failed to parse %s: %w", file, err)
				}
				allTxns = append(allTxns, txns...)
			}
			if sortBy != "" {
				less := transactionLess(sortBy)
				sort.Slice(allTxns, func(i, j int) bool { return less(allTxns[j], allTxns[i]) })
			}
		}

		if len(allTxns) == 0 {
			fmt.Println("No transactions found.")
			return nil
		}

		printTransactions(allTxns)
		return nil
	},
}

func runStream(files []string, schema dbmysql.Schema) error {
	seq := 0
	headerPrinted := false

	onTxn := func(t *dbmysql.Transaction) {
		seq++
		if !headerPrinted {
			printStreamHeader()
			headerPrinted = true
		}
		printStreamRow(seq, t)
	}

	parser := dbmysql.NewParser(&dbmysql.Config{DecodeSQL: true, Schema: schema})
	for _, file := range files {
		if err := parser.ParseBinlogStream(file, onTxn); err != nil {
			return fmt.Errorf("failed to parse %s: %w", file, err)
		}
	}

	if seq == 0 {
		fmt.Println("No transactions found.")
	}
	return nil
}

func formatSize(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func printStreamHeader() {
	fmt.Fprintf(os.Stdout, "%-6s  %-40s  %-19s  %10s  %6s  %6s  %6s  %s\n",
		"#", "GTID", "StartTime",
		"Size", "Insert", "Update",
		"Delete", "Tables",
	)
}

func printStreamRow(seq int, t *dbmysql.Transaction) {
	tableList := t.TableList()
	if t.IsDDL {
		tableList = "[DDL] " + tableList
	}

	fmt.Fprintf(os.Stdout, "%-6d  %-40s  %-19s  %10s  %6d  %6d  %6d  %s\n",
		seq,
		t.GTID,
		t.StartTime().Format("2006-01-02 15:04:05"),
		//formatSize(t.Size),
		strconv.Itoa(int(t.Size)),
		t.Inserts,
		t.Updates,
		t.Deletes,
		tableList,
	)
	for _, sql := range t.SQLs {
		fmt.Fprintf(os.Stdout, "    %s\n", sql)
	}
}

func printTransactions(txns []*dbmysql.Transaction) {
	var totalSize uint64
	var totalInserts, totalUpdates, totalDeletes int
	allTables := make(map[string]bool)
	for _, t := range txns {
		totalSize += t.Size
		totalInserts += t.Inserts
		totalUpdates += t.Updates
		totalDeletes += t.Deletes
		for tbl := range t.Tables {
			allTables[tbl] = true
		}
	}

	// Summary table
	st := table.NewWriter()
	st.SetOutputMirror(os.Stdout)
	st.SetTitle("Transaction Analysis Summary")
	st.AppendRows([]table.Row{
		{"Transactions", len(txns)},
		{"Total Size", formatSize(totalSize)},
		{"Inserts", totalInserts},
		{"Updates", totalUpdates},
		{"Deletes", totalDeletes},
		{"Tables", len(allTables)},
	})
	style := table.StyleLight
	style.Title.Align = text.AlignCenter
	st.SetStyle(style)
	st.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, WidthMin: 20},
	})
	st.Render()
	fmt.Println()

	// Detail table
	headerColumns := map[string]render.Column{
		headerNo:         {Key: headerNo, Header: "#", Align: text.AlignRight},
		headerBinlog:     {Key: headerBinlog, Header: "File"},
		headerGTID:       {Key: headerGTID, Header: "GTID", WidthMin: 38},
		headerStartTime:  {Key: headerStartTime, Header: "StartTime", WidthMin: 19},
		headerEndTime:    {Key: headerEndTime, Header: "EndTime", WidthMin: 19},
		headerDuration:   {Key: headerDuration, Header: "Duration", Align: text.AlignRight},
		headerSize:       {Key: headerSize, Header: "Size", Align: text.AlignRight},
		headerInsertRow:  {Key: headerInsertRow, Header: "InsertRow", Align: text.AlignRight},
		headerUpdateRow:  {Key: headerUpdateRow, Header: "UpdateRow", Align: text.AlignRight},
		headerDeleteRow:  {Key: headerDeleteRow, Header: "DeleteRow", Align: text.AlignRight},
		headerInsertStmt: {Key: headerInsertStmt, Header: "InsertStmt", Align: text.AlignRight},
		headerUpdateStmt: {Key: headerUpdateStmt, Header: "UpdateStmt", Align: text.AlignRight},
		headerDeleteStmt: {Key: headerDeleteStmt, Header: "DeleteStmt", Align: text.AlignRight},
		headerSQL:        {Key: headerSQL, Header: "SQL", WidthMax: 10},
		headerFrom:       {Key: headerFrom, Header: "From", Align: text.AlignRight},
	}
	dt := render.NewTable[*dbmysql.Transaction](func(rowIdx int, col string, row *dbmysql.Transaction) any {
		switch col {
		case headerNo:
			return rowIdx + 1
		case headerBinlog:
			return filepath.Base(row.BinlogFile)
		case headerGTID:
			return row.GTID
		case headerStartTime:
			return row.StartTime().Format("2006-01-02 15:04:05")
		case headerEndTime:
			if row.EndTime().IsZero() {
				return ""
			}
			return row.EndTime().Format("2006-01-02 15:04:05")
		case headerDuration:
			return row.Duration()
			//return computeDuration(row, execTimeStrategy)
		case headerSize:
			return row.Size
		case headerInsertRow:
			return row.Inserts
		case headerUpdateRow:
			return row.Updates
		case headerDeleteRow:
			return row.Deletes
		case headerInsertStmt:
			return row.InsertStmts
		case headerUpdateStmt:
			return row.UpdateStmts
		case headerDeleteStmt:
			return row.DeleteStmts
		//case headerQuery:
		//	return row.Query
		case headerSQL:
			return strings.Join(row.SQLs, "\n")
		case headerFrom:
			v := "S"
			repl, err := row.IsReplica()
			if err != nil {
				v = "unknown"
			}
			if repl {
				v = "R"
			}
			return v
		default:
			return "[unknown]"
		}
	})

	for _, col := range columns {
		if def, ok := headerColumns[col]; ok {
			dt.AddColumn(def)
		}
	}
	dt.SetVertical(verticalMode)
	dt.AddRows(txns)
	dt.Render(os.Stdout)
}

func computeDuration(t *dbmysql.Transaction, strategy string) time.Duration {
	natural := t.Duration()
	// todo
	//exec := time.Duration(t.QueryExecSeconds) * time.Second
	exec := time.Duration(0) * time.Second
	switch strategy {
	//case execTimeOnly:
	//	return exec
	//case execTimeIgnore:
	//	return natural
	default: // execTimeMax
		if exec > natural {
			return exec
		}
		return natural
	}
}

var binlogNameRe = regexp.MustCompile(`^(.+)\.(\d+)$`)

func findFollowingBinlogs(base string) ([]string, error) {
	dir := filepath.Dir(base)
	m := binlogNameRe.FindStringSubmatch(filepath.Base(base))
	if m == nil {
		return nil, fmt.Errorf("filename %q is not in <prefix>.<number> form", filepath.Base(base))
	}
	prefix := m[1]
	digits := len(m[2])
	baseNum, err := strconv.Atoi(m[2])
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	type item struct {
		num  int
		path string
	}
	var matches []item
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		em := binlogNameRe.FindStringSubmatch(e.Name())
		if em == nil || em[1] != prefix || len(em[2]) != digits {
			continue
		}
		num, _ := strconv.Atoi(em[2])
		if num <= baseNum {
			continue
		}
		matches = append(matches, item{num, filepath.Join(dir, e.Name())})
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].num < matches[j].num })

	paths := make([]string, len(matches))
	for i, m := range matches {
		paths[i] = m.path
	}
	return paths, nil
}

func transactionLess(by string) func(a, b *dbmysql.Transaction) bool {
	switch by {
	case "duration":
		return func(a, b *dbmysql.Transaction) bool { return a.Duration() < b.Duration() }
	case "rows":
		return func(a, b *dbmysql.Transaction) bool { return a.TotalRows() < b.TotalRows() }
	default: // "size"
		return func(a, b *dbmysql.Transaction) bool { return a.Size < b.Size }
	}
}
