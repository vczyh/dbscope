package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dbscope",
	Short: "MySQL binlog transaction analyzer",
	Long:  "dbscope analyzes MySQL binlog files to extract transaction details including size, affected tables, and row change counts.",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}