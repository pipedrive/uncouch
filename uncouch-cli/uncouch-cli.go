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

	cmdUntar := &cobra.Command{
		Use:   "untar filename path workers writers",
		Short: "Uncompress .tar file with couch data and create files with processed JSON data in specified output folder.",
		Args:  cobra.MinimumNArgs(4),
		RunE:  cmdUntarFunc,
	}

	cmdHeaders := &cobra.Command{
		Use:   "headers filename path",
		Short: "Dump headers as uncompressed bianry blocks to specified path",
		Args:  cobra.MinimumNArgs(2),
		RunE:  cmdHeadersFunc,
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
	rootCmd.AddCommand(cmdUntar)
	rootCmd.AddCommand(cmdHeaders)

	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
