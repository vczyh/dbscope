package cmd

import (
	"fmt"
	"runtime"

	"github.com/vczyh/dbscope/version"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("dbscope %s\n", version.Version)
		fmt.Printf("  commit: %s\n", version.Commit)
		fmt.Printf("  built:  %s\n", version.Date)
		fmt.Printf("  go:     %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	},
}
