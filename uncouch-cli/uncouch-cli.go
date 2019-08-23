package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func main() {

	var (
	tmp_dir string
	output_dir string
	workers_Q int
	)

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
		Use:   "untar filename",
		Short: "Uncompress .tar file with couch data and create files with processed JSON data in specified output folder.",
		Long: "Options:\ninput: string - Location of the file to process.\noutput: string - Location of the folder for the output files.",
		Args:  cobra.MinimumNArgs(1),
		RunE:  func(cmd *cobra.Command, args []string) (error){
			inputFile := args[0]
			err := cmdUntarFunc(inputFile, output_dir, tmp_dir, uint(workers_Q))
			return err
		},
	}

	cmdUntar.Flags().StringVarP(&output_dir, "dest", "d", "", "Folder for output files (required).")
	cmdUntar.Flags().StringVarP(&tmp_dir, "temp", "t", "", "Folder to store untarred files.")
	cmdUntar.Flags().IntVarP(&workers_Q, "workers", "w", 10, "Number of parallel workers (default 10).")
	cmdUntar.MarkFlagRequired("destination")

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
	rootCmd.AddCommand(cmdUntar)
	rootCmd.AddCommand(cmdHeaders)

	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
