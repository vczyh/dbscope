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

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

const (
	execTimeMax    = "max"
	execTimeOnly   = "exec"
	execTimeIgnore = "ignore"
)

const (
	headerNo        = "no"
	headerBinlog    = "binlog"
	headerGTID      = "gtid"
	headerStartTime = "st"
	headerEndTime   = "et"
	headerDuration  = "dur"
	headerSize      = "size"
	headerInsert    = "insert"
	headerUpdate    = "update"
	headerDelete    = "delete"
	headerQuery     = "query"
	headerSQL       = "sql"
)

var (
	topN             int
	sortBy           string
	streamMode       bool
	decodeSQL        bool
	verticalMode     bool
	continueRest     bool
	execTimeStrategy string
	columns          []string
)

func init() {
	binlogCmd.Flags().IntVarP(&topN, "top", "n", 0, "Show only top N transactions (by sort field), 0 for all")
	//binlogCmd.Flags().StringVarP(&sortBy, "sort", "s", "size", "Sort field: size, rows, time")
	binlogCmd.Flags().BoolVar(&streamMode, "stream", false, "Stream mode: print each transaction as it is parsed, suitable for large files")
	binlogCmd.Flags().BoolVar(&decodeSQL, "sql", false, "Decode row events into the corresponding SQL statements")
	binlogCmd.Flags().BoolVarP(&verticalMode, "vertical", "G", false, "Display each row vertically (similar to MySQL's \\G)")
	binlogCmd.Flags().BoolVar(&continueRest, "continue", false, "Also parse same-prefix binlog files in the last file's directory with larger numeric suffixes")
	binlogCmd.Flags().StringVarP(&execTimeStrategy, "exec-time-strategy", "e", execTimeMax,
		"How exec_time affects displayed duration: max (max of event duration and exec_time), exec (always exec_time), ignore (ignore exec_time)")
	binlogCmd.Flags().StringSliceVar(&columns, "col", []string{
		headerNo,
		headerGTID,
		headerStartTime,
		headerInsert,
		headerUpdate,
		headerDelete,
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
		switch execTimeStrategy {
		case execTimeMax, execTimeOnly, execTimeIgnore:
		default:
			return fmt.Errorf("invalid --exec-time-strategy %q (expected max, exec, or duration)", execTimeStrategy)
		}

		if continueRest && len(args) > 0 {
			extra, err := findFollowingBinlogs(args[len(args)-1])
			if err != nil {
				return fmt.Errorf("failed to discover following binlog files: %v", err)
			}
			args = append(args, extra...)
		}

		if streamMode {
			if sortBy != "size" || topN != 0 {
				fmt.Fprintln(os.Stderr, "--sort and --top are ignored in stream mode")
			}
			return runStream(args)
		}

		var allTxns []*dbmysql.Transaction

		for _, file := range args {
			txns, err := dbmysql.ParseBinlog(file, decodeSQL)
			if err != nil {
				return fmt.Errorf("failed to parse %s: %w", file, err)
			}
			allTxns = append(allTxns, txns...)
		}

		if len(allTxns) == 0 {
			fmt.Println("No transactions found.")
			return nil
		}

		//dbmysql.SortTransactions(allTxns, sortBy)

		if topN > 0 && topN < len(allTxns) {
			allTxns = allTxns[:topN]
		}

		printTransactions(allTxns)
		return nil
	},
}

func runStream(files []string) error {
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

	for _, file := range files {
		if err := dbmysql.ParseBinlogStream(file, decodeSQL, onTxn); err != nil {
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
		t.StartTime.Format("2006-01-02 15:04:05"),
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
		headerNo:        {Key: headerNo, Header: "#", Align: text.AlignRight},
		headerBinlog:    {Key: headerBinlog, Header: "BinlogFile"},
		headerGTID:      {Key: headerGTID, Header: "GTID", WidthMin: 38},
		headerStartTime: {Key: headerStartTime, Header: "StartTime", WidthMin: 19},
		headerEndTime:   {Key: headerEndTime, Header: "EndTime", WidthMin: 19},
		headerDuration:  {Key: headerDuration, Header: "Duration", Align: text.AlignRight},
		headerSize:      {Key: headerSize, Header: "Size", Align: text.AlignRight},
		headerInsert:    {Key: headerInsert, Header: "Insert", Align: text.AlignRight},
		headerUpdate:    {Key: headerUpdate, Header: "Update", Align: text.AlignRight},
		headerDelete:    {Key: headerDelete, Header: "Delete", Align: text.AlignRight},
		headerQuery:     {Key: headerQuery, Header: "Query", WidthMax: 20},
		headerSQL:       {Key: headerSQL, Header: "SQL", WidthMax: 10},
	}
	dt := render.NewTable[*dbmysql.Transaction](func(rowIdx int, col string, row *dbmysql.Transaction) any {
		switch col {
		case headerNo:
			return rowIdx + 1
		case headerBinlog:
			return row.BinlogFile
		case headerGTID:
			return row.GTID
		case headerStartTime:
			return row.StartTime.Format("2006-01-02 15:04:05")
		case headerEndTime:
			if row.EndTime.IsZero() {
				return ""
			}
			return row.EndTime.Format("2006-01-02 15:04:05")
		case headerDuration:
			return computeDuration(row, execTimeStrategy)
		case headerSize:
			return row.Size
		case headerInsert:
			return row.Inserts
		case headerUpdate:
			return row.Updates
		case headerDelete:
			return row.Deletes
		case headerQuery:
			return row.Query
		case headerSQL:
			return strings.Join(row.SQLs, "\n")
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
	exec := time.Duration(t.QueryExecSeconds) * time.Second
	switch strategy {
	case execTimeOnly:
		return exec
	case execTimeIgnore:
		return natural
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
