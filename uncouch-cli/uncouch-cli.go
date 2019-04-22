package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func main() {
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
		Use:   "data [filename]",
		Short: "Dump .couch file data to stdout or specified file",
		Args:  cobra.MinimumNArgs(1),
		RunE:  cmdDataFunc,
	}

	cmdSandbox := &cobra.Command{
		Use:   "sandbox [filename]",
		Short: "Sandbox routine",
		// Args:  cobra.MinimumNArgs(1),
		RunE: cmdSandboxFunc,
	}

	rootCmd := &cobra.Command{
		Use:   "uncouch-cli",
		Short: "Manage Uncouch related commands",
	}

	rootCmd.AddCommand(cmdPrint)
	rootCmd.AddCommand(cmdSandbox)
	rootCmd.AddCommand(cmdData)

	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
