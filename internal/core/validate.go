package core

import (
	"encoding/json"
	"fmt"
	"strings"
)

// =============================================================================
// Origin resolution
// =============================================================================

// originFromID returns the partition-key origin for an id: the substring
// before the first '.' separator, or the full id when no '.' is present.
func originFromID(id string) string {
	if i := strings.IndexByte(id, '.'); i >= 0 {
		return id[:i]
	}
	return id
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
	// Rule 4
	if prefix := d.Tags["resonate:prefix"]; strings.Contains(prefix, ".") {
		return fmt.Errorf("resonate:prefix must not contain '.'")
	}
	// Rule 5
	if origin := d.Tags["resonate:origin"]; strings.Contains(origin, ".") {
		return fmt.Errorf("resonate:origin must not contain '.'")
	}
	// Rule 1
	if origin := d.Tags["resonate:origin"]; origin != "" {
		if *d.ID != origin && !strings.HasPrefix(*d.ID, origin+".") {
			return fmt.Errorf("id must be prefixed by resonate:origin")
		}
	}
	// Rule 2
	if branch := d.Tags["resonate:branch"]; branch != "" {
		if *d.ID != branch && !strings.HasPrefix(*d.ID, branch+".") {
			return fmt.Errorf("id must be prefixed by resonate:branch")
		}
	}
	// Rule 3
	if parent := d.Tags["resonate:parent"]; parent != "" {
		if *d.ID != parent && !strings.HasPrefix(*d.ID, parent+".") {
			return fmt.Errorf("id must be prefixed by resonate:parent")
		}
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
	if originFromID(d.Awaiter) != originFromID(d.Awaited) {
		return fmt.Errorf("awaiter and awaited must belong to the same origin")
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
	if len(d.Tasks) == 0 {
		return fmt.Errorf("tasks must not be empty")
	}
	if len(d.Tasks) > 1 {
		first := originFromID(d.Tasks[0].ID)
		for _, t := range d.Tasks[1:] {
			if originFromID(t.ID) != first {
				return fmt.Errorf("all tasks must belong to the same origin")
			}
		}
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
	if strings.Contains(*d.ID, ".") {
		return fmt.Errorf("schedule id must not contain '.'")
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
