package main

import (
	"os"

	"github.com/resonateio/resonate-on-scylladb/cmd/resonate/cmds"
	"github.com/spf13/cobra"
)

var serverAddr string

var rootCmd = &cobra.Command{
	Use:   "resonate",
	Short: "Resonate server and client CLI",
	PersistentPreRun: func(_ *cobra.Command, _ []string) {
		cmds.SetServerAddr(serverAddr)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&serverAddr, "server", "s", "http://localhost:8001", "server address")
	rootCmd.AddCommand(
		cmds.ServeCmd(),
		cmds.PromiseCmd(),
		cmds.ScheduleCmd(),
		cmds.InvokeCmd(),
	)
}
