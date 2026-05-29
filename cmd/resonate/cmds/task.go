package cmds

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/spf13/cobra"
)

func TaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "tasks",
		Aliases: []string{"task"},
		Short:   "Manage tasks",
	}
	cmd.PersistentFlags().StringVar(&origin, "origin", "", "Request origin")
	cmd.AddCommand(
		taskGetCmd(),
		taskCreateCmd(),
		taskAcquireCmd(),
		taskReleaseCmd(),
		taskHeartbeatCmd(),
		taskFulfillCmd(),
		taskSuspendCmd(),
		taskFenceCmd(),
		taskHaltCmd(),
		taskContinueCmd(),
	)
	return cmd
}

func taskGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Get a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("task.get", core.TaskGetData{ID: args[0]})
		},
	}
}

func taskCreateCmd() *cobra.Command {
	var (
		pid             string
		ttl             int
		actionTimeout   time.Duration
		actionParamData string
		actionParamHdrs map[string]string
		actionTags      map[string]string
	)
	cmd := &cobra.Command{
		Use:   "create <id>",
		Short: "Create a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			ta := time.Now().Add(actionTimeout).UnixMilli()
			corrID := fmt.Sprintf("%016x", rand.Uint64())
			return send("task.create", core.TaskCreateData{
				PID: &pid,
				TTL: &ttl,
				Action: core.PromiseCreateReq{
					Kind: "promise.create",
					Head: core.RequestHead{CorrID: corrID},
					Data: core.PromiseCreateData{
						ID:        &id,
						TimeoutAt: &ta,
						Param:     &core.Value{Headers: actionParamHdrs, Data: actionParamData},
						Tags:      actionTags,
					},
				},
			})
		},
	}
	cmd.Flags().StringVar(&pid, "pid", "", "process ID")
	cmd.Flags().IntVar(&ttl, "ttl", 0, "time to live (ms)")
	cmd.Flags().DurationVar(&actionTimeout, "action-timeout", 0, "action timeout duration")
	cmd.Flags().StringVar(&actionParamData, "action-param-data", "", "action param data")
	cmd.Flags().StringToStringVar(&actionParamHdrs, "action-param-header", map[string]string{}, "action param header (key=value)")
	cmd.Flags().StringToStringVar(&actionTags, "action-tag", map[string]string{}, "action tag (key=value)")
	_ = cmd.MarkFlagRequired("pid")
	_ = cmd.MarkFlagRequired("ttl")
	_ = cmd.MarkFlagRequired("action-timeout")
	return cmd
}

func taskAcquireCmd() *cobra.Command {
	var (
		version int
		pid     string
		ttl     int
	)
	cmd := &cobra.Command{
		Use:   "acquire <id>",
		Short: "Acquire a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("task.acquire", core.TaskAcquireData{
				ID:      args[0],
				Version: &version,
				PID:     pid,
				TTL:     ttl,
			})
		},
	}
	cmd.Flags().IntVar(&version, "version", 0, "task version")
	cmd.Flags().StringVar(&pid, "pid", "", "process ID")
	cmd.Flags().IntVar(&ttl, "ttl", 0, "time to live (ms)")
	_ = cmd.MarkFlagRequired("version")
	_ = cmd.MarkFlagRequired("pid")
	_ = cmd.MarkFlagRequired("ttl")
	return cmd
}

func taskReleaseCmd() *cobra.Command {
	var version int
	cmd := &cobra.Command{
		Use:   "release <id>",
		Short: "Release a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("task.release", core.TaskReleaseData{
				ID:      args[0],
				Version: &version,
			})
		},
	}
	cmd.Flags().IntVar(&version, "version", 0, "task version")
	_ = cmd.MarkFlagRequired("version")
	return cmd
}

func taskHeartbeatCmd() *cobra.Command {
	var (
		pid      string
		taskArgs []string
	)
	cmd := &cobra.Command{
		Use:   "heartbeat",
		Short: "Send task heartbeat",
		RunE: func(_ *cobra.Command, _ []string) error {
			tasks := make([]core.TaskRef, 0, len(taskArgs))
			for _, s := range taskArgs {
				parts := strings.SplitN(s, ":", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid task format %q: expected id:version", s)
				}
				v, err := strconv.Atoi(parts[1])
				if err != nil {
					return fmt.Errorf("invalid version in %q: %w", s, err)
				}
				tasks = append(tasks, core.TaskRef{ID: parts[0], Version: v})
			}
			return send("task.heartbeat", core.TaskHeartbeatData{
				PID:   &pid,
				Tasks: tasks,
			})
		},
	}
	cmd.Flags().StringVar(&pid, "pid", "", "process ID")
	cmd.Flags().StringArrayVar(&taskArgs, "task", []string{}, "task id:version (repeatable)")
	_ = cmd.MarkFlagRequired("pid")
	_ = cmd.MarkFlagRequired("task")
	return cmd
}

func taskFulfillCmd() *cobra.Command {
	var (
		version      int
		state        string
		valueData    string
		valueHeaders map[string]string
	)
	cmd := &cobra.Command{
		Use:   "fulfill <id>",
		Short: "Fulfill a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			corrID := fmt.Sprintf("%016x", rand.Uint64())
			return send("task.fulfill", core.TaskFulfillData{
				ID:      id,
				Version: &version,
				Action: &core.PromiseSettleReq{
					Kind: "promise.settle",
					Head: core.RequestHead{CorrID: corrID},
					Data: core.PromiseSettleData{
						ID:    id,
						State: state,
						Value: &core.Value{Headers: valueHeaders, Data: valueData},
					},
				},
			})
		},
	}
	cmd.Flags().IntVar(&version, "version", 0, "task version")
	cmd.Flags().StringVar(&state, "state", "", "resolved | rejected | rejected_canceled")
	cmd.Flags().StringVar(&valueData, "value-data", "", "value data")
	cmd.Flags().StringToStringVar(&valueHeaders, "value-header", map[string]string{}, "value header (key=value)")
	_ = cmd.MarkFlagRequired("version")
	_ = cmd.MarkFlagRequired("state")
	return cmd
}

func taskSuspendCmd() *cobra.Command {
	var (
		version   int
		callbacks []string
	)
	cmd := &cobra.Command{
		Use:   "suspend <id>",
		Short: "Suspend a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			actions := make([]core.PromiseRegisterCallbackReq, 0, len(callbacks))
			for _, awaited := range callbacks {
				corrID := fmt.Sprintf("%016x", rand.Uint64())
				actions = append(actions, core.PromiseRegisterCallbackReq{
					Kind: "promise.register_callback",
					Head: core.RequestHead{CorrID: corrID},
					Data: core.PromiseRegisterCallbackData{
						Awaited: awaited,
						Awaiter: id,
					},
				})
			}
			return send("task.suspend", core.TaskSuspendData{
				ID:      id,
				Version: &version,
				Actions: actions,
			})
		},
	}
	cmd.Flags().IntVar(&version, "version", 0, "task version")
	cmd.Flags().StringArrayVar(&callbacks, "on", []string{}, "awaited promise ID (repeatable; awaiter is always the task ID)")
	_ = cmd.MarkFlagRequired("version")
	return cmd
}

func taskFenceCmd() *cobra.Command {
	var (
		version    int
		actionJSON string
	)
	cmd := &cobra.Command{
		Use:   "fence <id>",
		Short: "Fence a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("task.fence", core.TaskFenceData{
				ID:      args[0],
				Version: &version,
				Action:  json.RawMessage(actionJSON),
			})
		},
	}
	cmd.Flags().IntVar(&version, "version", 0, "task version")
	cmd.Flags().StringVar(&actionJSON, "action", "", "action JSON (promise.create or promise.settle envelope)")
	_ = cmd.MarkFlagRequired("version")
	_ = cmd.MarkFlagRequired("action")
	return cmd
}

func taskHaltCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "halt <id>",
		Short: "Halt a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("task.halt", core.TaskHaltData{ID: args[0]})
		},
	}
}

func taskContinueCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "continue <id>",
		Short: "Continue a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return send("task.continue", core.TaskContinueData{ID: args[0]})
		},
	}
}
