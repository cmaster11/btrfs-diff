package main

import (
	"fmt"
	"github.com/cmaster11/btrfs-diff/pkg"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
	"regexp"
)

var rootCmd *cobra.Command

var argIgnore []string
var argJSON bool

func init() {
	rootCmd = &cobra.Command{
		Args: cobra.MatchAll(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			argFile := args[0]

			var ignorePaths pkg.DiffIgnorePaths

			for _, reStr := range argIgnore {
				ignorePaths = append(ignorePaths, regexp.MustCompile(reStr))
			}

			processArgs := &pkg.ProcessFileWithOutputArgs{
				ArgFile:     argFile,
				IgnorePaths: ignorePaths,
				JSON:        argJSON,
			}

			if argJSON {
				pkg.InfoMode = false
				pkg.DebugMode = false
			}

			if err := pkg.ProcessFileAndOutput(processArgs); err != nil {
				return errors.Wrapf(err, "failed to process snapshot file")
			}
			return nil
		},
	}
	rootCmd.Flags().StringArrayVar(&argIgnore, "ignore", []string{}, "regex list of node paths to ignore")
	rootCmd.Flags().BoolVar(&argJSON, "json", false, "if defined, output json instead of debug logging")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
