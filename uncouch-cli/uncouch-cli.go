package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func main() {
	// defer profile.Start().Stop()
	cmdPrint := &cobra.Command{
		Use:   "print [string to print]",
		Short: "Print anything to the screen",
		Long: `print is for printing anything back to the screen.
For many years people have printed back to the screen.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			slog.Info("Print " + strings.Join(args, " "))
		},
	}

	cmdData := &cobra.Command{
		Use:   "data filename",
		Short: "Dump .couch file data as JSON lines to stdout",
		Args:  cobra.MinimumNArgs(1),
		RunE:  cmdDataFunc,
	}

	cmdHeaders := &cobra.Command{
		Use:   "headers filename path",
		Short: "Dump headers as uncompressed binary blocks to specified path",
		Args:  cobra.MinimumNArgs(2),
		RunE:  cmdHeadersFunc,
	}

	rootCmd := &cobra.Command{
		Use:   "uncouch-cli",
		Short: "Manage Uncouch related commands",
	}

	rootCmd.AddCommand(cmdPrint)
	rootCmd.AddCommand(cmdData)
	rootCmd.AddCommand(cmdHeaders)

	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
