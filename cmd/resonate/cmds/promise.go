package cmds

import (
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/spf13/cobra"
)

func PromiseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promise",
		Short: "Manage promises",
	}
	cmd.AddCommand(
		promiseGetCmd(),
		promiseCreateCmd(),
		promiseSettleCmd(),
		promiseSearchCmd(),
		promiseRegisterCallbackCmd(),
		promiseRegisterListenerCmd(),
	)
	return cmd
}

func promiseGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get a promise",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("promise.get", core.PromiseGetData{ID: args[0]})
		},
	}
}

func promiseCreateCmd() *cobra.Command {
	var (
		timeout      time.Duration
		paramData    string
		paramHeaders map[string]string
		tags         map[string]string
	)
	cmd := &cobra.Command{
		Use:   "create <id>",
		Short: "Create a promise",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			ta := time.Now().Add(timeout).UnixMilli()
			return send("promise.create", core.PromiseCreateData{
				ID:        &id,
				TimeoutAt: &ta,
				Param:     &core.Value{Headers: paramHeaders, Data: paramData},
				Tags:      tags,
			})
		},
	}
	cmd.Flags().DurationVarP(&timeout, "timeout", "t", time.Hour, "timeout duration")
	cmd.Flags().StringVar(&paramData, "param-data", "", "param data")
	cmd.Flags().StringToStringVar(&paramHeaders, "param-header", map[string]string{}, "param header (key=value)")
	cmd.Flags().StringToStringVar(&tags, "tag", map[string]string{}, "tag (key=value)")
	return cmd
}

func promiseSettleCmd() *cobra.Command {
	var (
		state        string
		valueData    string
		valueHeaders map[string]string
	)
	cmd := &cobra.Command{
		Use:   "settle <id>",
		Short: "Settle a promise",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("promise.settle", core.PromiseSettleData{
				ID:    args[0],
				State: state,
				Value: &core.Value{Headers: valueHeaders, Data: valueData},
			})
		},
	}
	cmd.Flags().StringVar(&state, "state", "", "resolved | rejected | rejected_canceled")
	cmd.Flags().StringVar(&valueData, "value-data", "", "value data")
	cmd.Flags().StringToStringVar(&valueHeaders, "value-header", map[string]string{}, "value header (key=value)")
	_ = cmd.MarkFlagRequired("state")
	return cmd
}

func promiseSearchCmd() *cobra.Command {
	var (
		state  string
		tags   map[string]string
		limit  int
		cursor string
	)
	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search promises",
		RunE: func(cmd *cobra.Command, _ []string) error {
			data := core.PromiseSearchData{
				State:  state,
				Tags:   tags,
				Cursor: cursor,
			}
			if cmd.Flags().Changed("limit") {
				data.Limit = &limit
			}
			return send("promise.search", data)
		},
	}
	cmd.Flags().StringVar(&state, "state", "", "filter by state")
	cmd.Flags().StringToStringVar(&tags, "tag", map[string]string{}, "filter by tag (key=value)")
	cmd.Flags().IntVar(&limit, "limit", 0, "max results")
	cmd.Flags().StringVar(&cursor, "cursor", "", "pagination cursor")
	return cmd
}

func promiseRegisterCallbackCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register-callback <awaited> <awaiter>",
		Short: "Register a callback on a promise",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("promise.register_callback", core.PromiseRegisterCallbackData{
				Awaited: args[0],
				Awaiter: args[1],
			})
		},
	}
}

func promiseRegisterListenerCmd() *cobra.Command {
	var address string
	cmd := &cobra.Command{
		Use:   "register-listener <awaited>",
		Short: "Register a listener on a promise",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("promise.register_listener", core.PromiseRegisterListenerData{
				Awaited: args[0],
				Address: address,
			})
		},
	}
	cmd.Flags().StringVar(&address, "address", "", "listener address")
	_ = cmd.MarkFlagRequired("address")
	return cmd
}
