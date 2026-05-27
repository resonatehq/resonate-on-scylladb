package cmds

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/spf13/cobra"
)

type invokeParam struct {
	Func    string `json:"func"`
	Args    []any  `json:"args"`
	Version int    `json:"version"`
}

func InvokeCmd() *cobra.Command {
	var (
		funcName string
		args     []string
		jsonArgs string
		version  int
		timeout  time.Duration
		target   string
		delay    time.Duration
	)
	cmd := &cobra.Command{
		Use:   "invoke <promise-id>",
		Short: "Invoke a function",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, cmdArgs []string) error {
			var invokeArgs []any
			if jsonArgs != "" {
				if err := json.Unmarshal([]byte(jsonArgs), &invokeArgs); err != nil {
					return fmt.Errorf("parse --json-args: %w", err)
				}
			} else {
				invokeArgs = make([]any, len(args))
				for i, a := range args {
					var v any
					if err := json.Unmarshal([]byte(a), &v); err != nil {
						invokeArgs[i] = a
					} else {
						invokeArgs[i] = v
					}
				}
			}

			paramBytes, err := json.Marshal(invokeParam{Func: funcName, Args: invokeArgs, Version: version})
			if err != nil {
				return err
			}

			tags := map[string]string{"resonate:invoke": target}
			if delay > 0 {
				tags["resonate:delay"] = fmt.Sprintf("%d", time.Now().Add(delay).UnixMilli())
			}

			id := cmdArgs[0]
			ta := time.Now().Add(timeout + delay).UnixMilli()
			return send("promise.create", core.PromiseCreateData{
				ID:        &id,
				TimeoutAt: &ta,
				Param:     &core.Value{Data: string(paramBytes)},
				Tags:      tags,
			})
		},
	}
	cmd.Flags().StringVarP(&funcName, "func", "f", "", "function to invoke")
	cmd.Flags().StringArrayVar(&args, "arg", []string{}, "function argument (repeatable)")
	cmd.Flags().StringVar(&jsonArgs, "json-args", "", "function arguments as JSON array")
	cmd.Flags().IntVar(&version, "version", 1, "function version")
	cmd.Flags().DurationVarP(&timeout, "timeout", "t", time.Hour, "promise timeout")
	cmd.Flags().StringVar(&target, "target", "poll://any@default", "invoke target")
	cmd.Flags().DurationVar(&delay, "delay", 0, "invoke delay")
	_ = cmd.MarkFlagRequired("func")
	return cmd
}
