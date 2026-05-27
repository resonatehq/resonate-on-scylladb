package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// =============================================================================
// Origin resolution
// =============================================================================

// resolveOrigin determines the partition key origin for a promise/task lookup.
// Priority: head > tag > id. Returns an error if head and tag are both present
// but disagree.
func resolveOrigin(headOrigin, tagOrigin, id string) (string, error) {
	switch {
	case headOrigin != "" && tagOrigin != "" && headOrigin != tagOrigin:
		return "", fmt.Errorf("resonate:origin mismatch: head=%q tag=%q", headOrigin, tagOrigin)
	case headOrigin != "":
		return headOrigin, nil
	case tagOrigin != "":
		return tagOrigin, nil
	default:
		return id, nil
	}
}

// =============================================================================
// ID validation (TODO 5)
// =============================================================================

const maxIDLen = 1024

func validateID(s string) error {
	if s == "" {
		return fmt.Errorf("id must not be empty")
	}
	if len(s) > maxIDLen {
		return fmt.Errorf("id exceeds maximum length of %d", maxIDLen)
	}
	if strings.ContainsRune(s, 0) {
		return fmt.Errorf("id must not contain null bytes")
	}
	return nil
}

// =============================================================================
// Promise validators (TODO 4)
// =============================================================================

func (d *PromiseGetData) Validate() error {
	return validateID(d.ID)
}

func (d *PromiseCreateData) Validate() error {
	if d.ID == nil {
		return fmt.Errorf("id is required")
	}
	if err := validateID(*d.ID); err != nil {
		return err
	}
	if d.TimeoutAt == nil {
		return fmt.Errorf("timeoutAt is required")
	}
	if *d.TimeoutAt < 0 {
		return fmt.Errorf("timeoutAt must be >= 0")
	}
	if d.Param == nil {
		return fmt.Errorf("param is required")
	}
	if d.Tags == nil {
		return fmt.Errorf("tags is required")
	}
	return nil
}

func (d *PromiseSettleData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	switch d.State {
	case "resolved", "rejected", "rejected_canceled":
	default:
		return fmt.Errorf("state must be one of: resolved, rejected, rejected_canceled")
	}
	if d.Value == nil {
		return fmt.Errorf("value is required")
	}
	return nil
}

func (d *PromiseRegisterCallbackData) Validate() error {
	if d.Awaited == "" {
		return fmt.Errorf("awaited is required")
	}
	if d.Awaiter == "" {
		return fmt.Errorf("awaiter is required")
	}
	return nil
}

func (d *PromiseRegisterListenerData) Validate() error {
	if d.Awaited == "" {
		return fmt.Errorf("awaited is required")
	}
	if d.Address == "" {
		return fmt.Errorf("address is required")
	}
	return nil
}

// =============================================================================
// Task validators (TODO 4)
// =============================================================================

func (d *TaskGetData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	return nil
}

func (d *TaskCreateData) Validate() error {
	if d.PID == nil {
		return fmt.Errorf("pid is required")
	}
	if *d.PID == "" {
		return fmt.Errorf("pid must not be empty")
	}
	if d.TTL == nil {
		return fmt.Errorf("ttl is required")
	}
	if *d.TTL <= 0 {
		return fmt.Errorf("ttl must be greater than 0")
	}
	return d.Action.Data.Validate()
}

func (d *TaskAcquireData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	if d.Version == nil {
		return fmt.Errorf("version is required")
	}
	if *d.Version < 0 {
		return fmt.Errorf("version must be >= 0")
	}
	if d.PID == "" {
		return fmt.Errorf("pid is required")
	}
	if d.TTL <= 0 {
		return fmt.Errorf("ttl must be greater than 0")
	}
	return nil
}

func (d *TaskReleaseData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	if d.Version == nil {
		return fmt.Errorf("version is required")
	}
	if *d.Version < 0 {
		return fmt.Errorf("version must be >= 0")
	}
	return nil
}

// TaskSuspendData: do NOT validate ID (tests 2.45-2.46 expect 404 for missing ID).
func (d *TaskSuspendData) Validate() error {
	if d.Version == nil {
		return fmt.Errorf("version is required")
	}
	if *d.Version < 0 {
		return fmt.Errorf("version must be >= 0")
	}
	return nil
}

func (d *TaskFulfillData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	if d.Version == nil {
		return fmt.Errorf("version is required")
	}
	if *d.Version < 0 {
		return fmt.Errorf("version must be >= 0")
	}
	if d.Action == nil {
		return fmt.Errorf("action is required")
	}
	if d.Action.Data.ID != "" && d.Action.Data.ID != d.ID {
		return fmt.Errorf("action.data.id must match task id")
	}
	return d.Action.Data.Validate()
}

func (d *TaskFenceData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	if d.Version == nil {
		return fmt.Errorf("version is required")
	}
	if *d.Version < 0 {
		return fmt.Errorf("version must be >= 0")
	}
	var innerEnv struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(d.Action, &innerEnv); err != nil {
		return fmt.Errorf("action: invalid JSON: %s", err)
	}
	switch innerEnv.Kind {
	case "promise.create", "promise.settle":
	default:
		return fmt.Errorf("action.kind must be promise.create or promise.settle")
	}
	return nil
}

func (d *TaskHeartbeatData) Validate() error {
	if d.PID == nil {
		return fmt.Errorf("pid is required")
	}
	if *d.PID == "" {
		return fmt.Errorf("pid must not be empty")
	}
	if d.Tasks == nil {
		return fmt.Errorf("tasks is required")
	}
	return nil
}

func (d *TaskHaltData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	return nil
}

func (d *TaskContinueData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	return nil
}

// =============================================================================
// Schedule validators (TODO 4)
// =============================================================================

func (d *ScheduleGetData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	return nil
}

func (d *ScheduleCreateData) Validate() error {
	if d.ID == nil {
		return fmt.Errorf("id is required")
	}
	if err := validateID(*d.ID); err != nil {
		return err
	}
	if d.PromiseID == nil {
		return fmt.Errorf("promiseId is required")
	}
	if *d.PromiseID == "" {
		return fmt.Errorf("promiseId must not be empty")
	}
	if d.PromiseTimeout == nil {
		return fmt.Errorf("promiseTimeout is required")
	}
	if *d.PromiseTimeout <= 0 {
		return fmt.Errorf("promiseTimeout must be greater than 0")
	}
	return nil
}

func (d *ScheduleDeleteData) Validate() error {
	if d.ID == "" {
		return fmt.Errorf("id is required")
	}
	return nil
}
