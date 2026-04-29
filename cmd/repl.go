package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	dbmysql "dbscope/mysql"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/spf13/cobra"
)

var (
	replHost      string
	replPort      uint16
	replUser      string
	replPassword  string
	replServerID  uint32
	replStartFile string
	replStartPos  uint32
	replDecodeSQL bool
)

func init() {
	replCmd.Flags().StringVarP(&replHost, "host", "H", "127.0.0.1", "MySQL server host")
	replCmd.Flags().Uint16VarP(&replPort, "port", "P", 3306, "MySQL server port")
	replCmd.Flags().StringVarP(&replUser, "user", "u", "root", "MySQL username")
	replCmd.Flags().StringVarP(&replPassword, "password", "p", "", "MySQL password")
	replCmd.Flags().Uint32Var(&replServerID, "server-id", 100, "Replication server-id (must differ from other replicas)")
	replCmd.Flags().StringVar(&replStartFile, "start-file", "", "Start binlog filename (empty = current position)")
	replCmd.Flags().Uint32Var(&replStartPos, "start-pos", 4, "Start binlog position")
	replCmd.Flags().BoolVar(&replDecodeSQL, "sql", false, "Decode row events into the corresponding SQL statements")

	_ = replCmd.MarkFlagRequired("password")

	rootCmd.AddCommand(replCmd)
}

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Stream binlog from a MySQL server and analyze transactions",
	Long:  "Connect to a MySQL server via the replication protocol, receive binlog events in real-time, and stream transaction info.",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Determine start position
		var startPos gomysql.Position
		if replStartFile != "" {
			startPos = gomysql.Position{Name: replStartFile, Pos: replStartPos}
		} else {
			pos, err := dbmysql.QueryCurrentPos(replHost, replPort, replUser, replPassword)
			if err != nil {
				return fmt.Errorf("failed to query current binlog position: %v", err)
			}
			startPos = pos
		}
		fmt.Fprintf(os.Stderr, "Starting sync from %s:%d...\n", startPos.Name, startPos.Pos)

		syncer := dbmysql.NewSyncer(replHost, replPort, replUser, replPassword, replServerID)
		defer syncer.Close()

		streamer, err := syncer.StartSync(startPos)
		if err != nil {
			return fmt.Errorf("failed to start binlog sync: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			cancel()
		}()

		seq := 0
		headerPrinted := false
		var current *dbmysql.Transaction

		for {
			ev, err := streamer.GetEvent(ctx)
			if err != nil {
				if ctx.Err() != nil {
					fmt.Fprintln(os.Stderr)
					return nil
				}
				return fmt.Errorf("failed to get binlog event: %v", err)
			}

			parser := dbmysql.NewParser(&dbmysql.Config{DecodeSQL: replDecodeSQL})
			current = parser.HandleEvent(ev, current, func(t *dbmysql.Transaction) {
				seq++
				if !headerPrinted {
					printStreamHeader()
					headerPrinted = true
				}
				printStreamRow(seq, t)
			})
		}
	},
}
