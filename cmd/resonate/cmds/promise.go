package cmds

import (
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/spf13/cobra"
)

func PromiseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "promises",
		Aliases: []string{"promise"},
		Short:   "Manage promises",
	}
	cmd.PersistentFlags().StringVar(&origin, "origin", "", "Request origin")
	cmd.AddCommand(
		promiseGetCmd(),
		promiseCreateCmd(),
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
