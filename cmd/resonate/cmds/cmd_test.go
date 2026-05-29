package cmds

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/cobra"
)

func captureRequest(t *testing.T, args []string) map[string]any {
	t.Helper()

	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	SetServerAddr(srv.URL)
	t.Cleanup(func() { origin = "" })

	root := &cobra.Command{Use: "resonate"}
	root.AddCommand(PromiseCmd(), ScheduleCmd(), TaskCmd())
	root.SetArgs(args)
	_ = root.Execute()

	var got map[string]any
	_ = json.Unmarshal(body, &got)
	return got
}

func kindOf(t *testing.T, got map[string]any) string {
	t.Helper()
	k, _ := got["kind"].(string)
	return k
}

func headOf(t *testing.T, got map[string]any) map[string]any {
	t.Helper()
	h, _ := got["head"].(map[string]any)
	return h
}

func dataOf(t *testing.T, got map[string]any) map[string]any {
	t.Helper()
	d, _ := got["data"].(map[string]any)
	return d
}

// --- Promise tests ---

func TestPromiseGet(t *testing.T) {
	got := captureRequest(t, []string{"promise", "get", "my-id"})
	if kindOf(t, got) != "promise.get" {
		t.Errorf("kind = %q, want promise.get", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "my-id" {
		t.Errorf("data.id = %v, want my-id", d["id"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestPromiseCreate(t *testing.T) {
	got := captureRequest(t, []string{
		"promise", "create", "my-id",
		"--timeout", "1h",
		"--param-data", "foo",
		"--param-header", "k=v",
		"--tag", "env=prod",
	})
	if kindOf(t, got) != "promise.create" {
		t.Errorf("kind = %q, want promise.create", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "my-id" {
		t.Errorf("data.id = %v, want my-id", d["id"])
	}
	param := d["param"].(map[string]any)
	if param["data"] != "foo" {
		t.Errorf("data.param.data = %v, want foo", param["data"])
	}
	headers := param["headers"].(map[string]any)
	if headers["k"] != "v" {
		t.Errorf("data.param.headers.k = %v, want v", headers["k"])
	}
	tags := d["tags"].(map[string]any)
	if tags["env"] != "prod" {
		t.Errorf("data.tags.env = %v, want prod", tags["env"])
	}
	ta, ok := d["timeoutAt"].(float64)
	if !ok || ta <= 0 {
		t.Errorf("data.timeoutAt must be positive, got %v", d["timeoutAt"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestPromiseRegisterCallback(t *testing.T) {
	got := captureRequest(t, []string{"promise", "register-callback", "awaited-id", "awaiter-id"})
	if kindOf(t, got) != "promise.register_callback" {
		t.Errorf("kind = %q, want promise.register_callback", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["awaited"] != "awaited-id" {
		t.Errorf("data.awaited = %v, want awaited-id", d["awaited"])
	}
	if d["awaiter"] != "awaiter-id" {
		t.Errorf("data.awaiter = %v, want awaiter-id", d["awaiter"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestPromiseRegisterListener(t *testing.T) {
	got := captureRequest(t, []string{"promise", "register-listener", "awaited-id", "--address", "http://x"})
	if kindOf(t, got) != "promise.register_listener" {
		t.Errorf("kind = %q, want promise.register_listener", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["awaited"] != "awaited-id" {
		t.Errorf("data.awaited = %v, want awaited-id", d["awaited"])
	}
	if d["address"] != "http://x" {
		t.Errorf("data.address = %v, want http://x", d["address"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

// --- Schedule tests ---

func TestScheduleGet(t *testing.T) {
	got := captureRequest(t, []string{"schedule", "get", "sched-id"})
	if kindOf(t, got) != "schedule.get" {
		t.Errorf("kind = %q, want schedule.get", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "sched-id" {
		t.Errorf("data.id = %v, want sched-id", d["id"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestScheduleCreate(t *testing.T) {
	got := captureRequest(t, []string{
		"schedule", "create", "sched-id",
		"--cron", "* * * * *",
		"--promise-id", "tmpl",
		"--promise-timeout", "1h",
	})
	if kindOf(t, got) != "schedule.create" {
		t.Errorf("kind = %q, want schedule.create", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "sched-id" {
		t.Errorf("data.id = %v, want sched-id", d["id"])
	}
	if d["cron"] != "* * * * *" {
		t.Errorf("data.cron = %v, want * * * * *", d["cron"])
	}
	if d["promiseId"] != "tmpl" {
		t.Errorf("data.promiseId = %v, want tmpl", d["promiseId"])
	}
	pt, ok := d["promiseTimeout"].(float64)
	if !ok || pt <= 0 {
		t.Errorf("data.promiseTimeout must be positive, got %v", d["promiseTimeout"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestScheduleDelete(t *testing.T) {
	got := captureRequest(t, []string{"schedule", "delete", "sched-id"})
	if kindOf(t, got) != "schedule.delete" {
		t.Errorf("kind = %q, want schedule.delete", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "sched-id" {
		t.Errorf("data.id = %v, want sched-id", d["id"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

// --- Task tests ---

func TestTaskGet(t *testing.T) {
	got := captureRequest(t, []string{"task", "get", "task-id"})
	if kindOf(t, got) != "task.get" {
		t.Errorf("kind = %q, want task.get", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskCreate(t *testing.T) {
	got := captureRequest(t, []string{
		"task", "create", "task-id",
		"--pid", "pid-1",
		"--ttl", "30000",
		"--action-timeout", "1h",
	})
	if kindOf(t, got) != "task.create" {
		t.Errorf("kind = %q, want task.create", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["pid"] != "pid-1" {
		t.Errorf("data.pid = %v, want pid-1", d["pid"])
	}
	if d["ttl"] != float64(30000) {
		t.Errorf("data.ttl = %v, want 30000", d["ttl"])
	}
	action := d["action"].(map[string]any)
	if action["kind"] != "promise.create" {
		t.Errorf("data.action.kind = %v, want promise.create", action["kind"])
	}
	ad := action["data"].(map[string]any)
	if ad["id"] != "task-id" {
		t.Errorf("data.action.data.id = %v, want task-id", ad["id"])
	}
	ta, ok := ad["timeoutAt"].(float64)
	if !ok || ta <= 0 {
		t.Errorf("data.action.data.timeoutAt must be positive, got %v", ad["timeoutAt"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskAcquire(t *testing.T) {
	got := captureRequest(t, []string{
		"task", "acquire", "task-id",
		"--version", "1",
		"--pid", "pid-1",
		"--ttl", "30000",
	})
	if kindOf(t, got) != "task.acquire" {
		t.Errorf("kind = %q, want task.acquire", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if d["version"] != float64(1) {
		t.Errorf("data.version = %v, want 1", d["version"])
	}
	if d["pid"] != "pid-1" {
		t.Errorf("data.pid = %v, want pid-1", d["pid"])
	}
	if d["ttl"] != float64(30000) {
		t.Errorf("data.ttl = %v, want 30000", d["ttl"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskRelease(t *testing.T) {
	got := captureRequest(t, []string{"task", "release", "task-id", "--version", "2"})
	if kindOf(t, got) != "task.release" {
		t.Errorf("kind = %q, want task.release", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if d["version"] != float64(2) {
		t.Errorf("data.version = %v, want 2", d["version"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskSuspend(t *testing.T) {
	got := captureRequest(t, []string{
		"task", "suspend", "task-id",
		"--version", "1",
		"--on", "p-awaited",
	})
	if kindOf(t, got) != "task.suspend" {
		t.Errorf("kind = %q, want task.suspend", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if d["version"] != float64(1) {
		t.Errorf("data.version = %v, want 1", d["version"])
	}
	actions, ok := d["actions"].([]any)
	if !ok || len(actions) != 1 {
		t.Fatalf("data.actions must have 1 entry, got %v", d["actions"])
	}
	a := actions[0].(map[string]any)
	if a["kind"] != "promise.register_callback" {
		t.Errorf("data.actions[0].kind = %v, want promise.register_callback", a["kind"])
	}
	ad := a["data"].(map[string]any)
	if ad["awaited"] != "p-awaited" {
		t.Errorf("data.actions[0].data.awaited = %v, want p-awaited", ad["awaited"])
	}
	if ad["awaiter"] != "task-id" {
		t.Errorf("data.actions[0].data.awaiter = %v, want task-id", ad["awaiter"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskSuspendMultipleCallbacks(t *testing.T) {
	got := captureRequest(t, []string{
		"task", "suspend", "task-id",
		"--version", "1",
		"--on", "promise-a",
		"--on", "promise-c",
	})
	d := dataOf(t, got)
	actions, ok := d["actions"].([]any)
	if !ok || len(actions) != 2 {
		t.Fatalf("data.actions must have 2 entries, got %v", d["actions"])
	}
	a0 := actions[0].(map[string]any)["data"].(map[string]any)
	if a0["awaited"] != "promise-a" || a0["awaiter"] != "task-id" {
		t.Errorf("actions[0]: got awaited=%v awaiter=%v, want promise-a task-id", a0["awaited"], a0["awaiter"])
	}
	a1 := actions[1].(map[string]any)["data"].(map[string]any)
	if a1["awaited"] != "promise-c" || a1["awaiter"] != "task-id" {
		t.Errorf("actions[1]: got awaited=%v awaiter=%v, want promise-c task-id", a1["awaited"], a1["awaiter"])
	}
}

func TestTaskFulfill(t *testing.T) {
	got := captureRequest(t, []string{
		"task", "fulfill", "task-id",
		"--version", "3",
		"--state", "resolved",
		"--value-data", "ok",
	})
	if kindOf(t, got) != "task.fulfill" {
		t.Errorf("kind = %q, want task.fulfill", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if d["version"] != float64(3) {
		t.Errorf("data.version = %v, want 3", d["version"])
	}
	action := d["action"].(map[string]any)
	if action["kind"] != "promise.settle" {
		t.Errorf("data.action.kind = %v, want promise.settle", action["kind"])
	}
	ad := action["data"].(map[string]any)
	if ad["state"] != "resolved" {
		t.Errorf("data.action.data.state = %v, want resolved", ad["state"])
	}
	value := ad["value"].(map[string]any)
	if value["data"] != "ok" {
		t.Errorf("data.action.data.value.data = %v, want ok", value["data"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskFence(t *testing.T) {
	actionJSON := `{"kind":"promise.settle","head":{"corrId":"x"},"data":{"id":"p","state":"resolved","value":{}}}`
	got := captureRequest(t, []string{
		"task", "fence", "task-id",
		"--version", "1",
		"--action", actionJSON,
	})
	if kindOf(t, got) != "task.fence" {
		t.Errorf("kind = %q, want task.fence", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if d["version"] != float64(1) {
		t.Errorf("data.version = %v, want 1", d["version"])
	}
	action, ok := d["action"].(map[string]any)
	if !ok {
		t.Fatalf("data.action must be a JSON object, got %T", d["action"])
	}
	if action["kind"] != "promise.settle" {
		t.Errorf("data.action.kind = %v, want promise.settle", action["kind"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskHeartbeat(t *testing.T) {
	got := captureRequest(t, []string{
		"task", "heartbeat",
		"--pid", "pid-1",
		"--task", "task-id:1",
		"--task", "other-id:2",
	})
	if kindOf(t, got) != "task.heartbeat" {
		t.Errorf("kind = %q, want task.heartbeat", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["pid"] != "pid-1" {
		t.Errorf("data.pid = %v, want pid-1", d["pid"])
	}
	tasks, ok := d["tasks"].([]any)
	if !ok || len(tasks) != 2 {
		t.Fatalf("data.tasks must have 2 entries, got %v", d["tasks"])
	}
	t0 := tasks[0].(map[string]any)
	if t0["id"] != "task-id" || t0["version"] != float64(1) {
		t.Errorf("tasks[0]: got {id:%v version:%v}, want {task-id 1}", t0["id"], t0["version"])
	}
	t1 := tasks[1].(map[string]any)
	if t1["id"] != "other-id" || t1["version"] != float64(2) {
		t.Errorf("tasks[1]: got {id:%v version:%v}, want {other-id 2}", t1["id"], t1["version"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskHalt(t *testing.T) {
	got := captureRequest(t, []string{"task", "halt", "task-id"})
	if kindOf(t, got) != "task.halt" {
		t.Errorf("kind = %q, want task.halt", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

func TestTaskContinue(t *testing.T) {
	got := captureRequest(t, []string{"task", "continue", "task-id"})
	if kindOf(t, got) != "task.continue" {
		t.Errorf("kind = %q, want task.continue", kindOf(t, got))
	}
	d := dataOf(t, got)
	if d["id"] != "task-id" {
		t.Errorf("data.id = %v, want task-id", d["id"])
	}
	if headOf(t, got)["corrId"] == "" {
		t.Error("head.corrId must be non-empty")
	}
}

// --- --origin flag tests ---

func TestOriginPromise(t *testing.T) {
	got := captureRequest(t, []string{"promise", "get", "my-id", "--origin", "my-service"})
	h := headOf(t, got)
	if h["resonate:origin"] != "my-service" {
		t.Errorf("head[resonate:origin] = %v, want my-service", h["resonate:origin"])
	}
}

func TestOriginTask(t *testing.T) {
	got := captureRequest(t, []string{"task", "halt", "my-task", "--origin", "ops-console"})
	h := headOf(t, got)
	if h["resonate:origin"] != "ops-console" {
		t.Errorf("head[resonate:origin] = %v, want ops-console", h["resonate:origin"])
	}
}

func TestOriginSchedule(t *testing.T) {
	got := captureRequest(t, []string{"schedule", "get", "sched-id", "--origin", "debugger"})
	h := headOf(t, got)
	if h["resonate:origin"] != "debugger" {
		t.Errorf("head[resonate:origin] = %v, want debugger", h["resonate:origin"])
	}
}

func TestOriginAbsent(t *testing.T) {
	got := captureRequest(t, []string{"promise", "get", "my-id"})
	h := headOf(t, got)
	v, _ := h["resonate:origin"].(string)
	if v != "" {
		t.Errorf("head[resonate:origin] = %q, want absent or empty", v)
	}
}
