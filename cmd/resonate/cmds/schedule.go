package cmds

import (
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/spf13/cobra"
)

func ScheduleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Manage schedules",
	}
	cmd.AddCommand(
		scheduleGetCmd(),
		scheduleCreateCmd(),
		scheduleDeleteCmd(),
		scheduleSearchCmd(),
	)
	return cmd
}

func scheduleGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("schedule.get", core.ScheduleGetData{ID: args[0]})
		},
	}
}

func scheduleCreateCmd() *cobra.Command {
	var (
		cron                string
		promiseID           string
		promiseTimeout      time.Duration
		promiseParamData    string
		promiseParamHeaders map[string]string
		promiseTags         map[string]string
	)
	cmd := &cobra.Command{
		Use:   "create <id>",
		Short: "Create a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			pt := promiseTimeout.Milliseconds()
			return send("schedule.create", core.ScheduleCreateData{
				ID:             &id,
				Cron:           cron,
				PromiseID:      &promiseID,
				PromiseTimeout: &pt,
				PromiseParam:   core.Value{Headers: promiseParamHeaders, Data: promiseParamData},
				PromiseTags:    promiseTags,
			})
		},
	}
	cmd.Flags().StringVar(&cron, "cron", "", "cron expression")
	cmd.Flags().StringVar(&promiseID, "promise-id", "", "promise ID (template)")
	cmd.Flags().DurationVar(&promiseTimeout, "promise-timeout", time.Hour, "promise timeout duration")
	cmd.Flags().StringVar(&promiseParamData, "promise-param-data", "", "promise param data")
	cmd.Flags().StringToStringVar(&promiseParamHeaders, "promise-param-header", map[string]string{}, "promise param header (key=value)")
	cmd.Flags().StringToStringVar(&promiseTags, "promise-tag", map[string]string{}, "promise tag (key=value)")
	_ = cmd.MarkFlagRequired("cron")
	_ = cmd.MarkFlagRequired("promise-id")
	return cmd
}

func scheduleDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a schedule",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("schedule.delete", core.ScheduleDeleteData{ID: args[0]})
		},
	}
}

func scheduleSearchCmd() *cobra.Command {
	var (
		tags   map[string]string
		limit  int
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search schedules",
		RunE: func(cmd *cobra.Command, _ []string) error {
			data := core.ScheduleSearchData{
				Tags:   tags,
				Cursor: cursor,
			}
			if cmd.Flags().Changed("limit") {
				data.Limit = &limit
			}
			return send("schedule.search", data)
		},
	}
	cmd.Flags().StringToStringVar(&tags, "tag", map[string]string{}, "filter by tag (key=value)")
	cmd.Flags().IntVar(&limit, "limit", 0, "max results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	return cmd
}
