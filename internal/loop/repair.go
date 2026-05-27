package loop

import (
	"log"
	"sync"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

const defaultRepairInterval = 100 * time.Millisecond

// Repair is a background process that periodically scans for inconsistent
// state and corrects it by re-inserting any missing timeout entries.
// Implements base.Background.
type Repair struct {
	handler  *core.Handler
	interval time.Duration
	stop     chan struct{}
	once     sync.Once
}

func NewRepair(handler *core.Handler) *Repair {
	return &Repair{
		handler:  handler,
		interval: defaultRepairInterval,
	}
}

func (r *Repair) Init() {
	r.stop = make(chan struct{})
	r.once = sync.Once{}
	go r.loop()
}

func (r *Repair) Stop() {
	r.once.Do(func() { close(r.stop) })
}

func (r *Repair) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.tick()
		case <-r.stop:
			return
		}
	}
}

// tick performs a full scan of the promises table and re-inserts any missing
// timeout entries. INSERT in Cassandra/ScyllaDB is idempotent for the same
// primary key, so duplicate inserts are safe.
func (r *Repair) tick() {
	now := time.Now().UnixMilli()

	var (
		id, origin, state string
		taskState         string
		timeoutAt         int64
		taskTimeoutRetry  *int64
		taskTimeoutLease  *int64
	)

	iter := r.handler.Session.Query(
		`SELECT id, origin, state, timeout_at,
		        task_state, task_timeout_retry, task_timeout_lease
		 FROM promises`,
	).Iter()

	for iter.Scan(&id, &origin, &state, &timeoutAt,
		&taskState, &taskTimeoutRetry, &taskTimeoutLease) {

		// Only repair root promises (non-root promises don't have timeout entries).
		if origin != id {
			continue
		}

		// Re-insert promise_timeouts if the promise is still pending and not yet expired.
		if state == "pending" && timeoutAt > now {
			if err := r.handler.Session.Query(
				`INSERT INTO promise_timeouts (bucket, timeout_at, origin, promise_id) VALUES (?, ?, ?, ?)`,
				r.handler.BucketFor(timeoutAt), timeoutAt, origin, id,
			).Exec(); err != nil {
				log.Printf("repair: insert promise_timeouts(%s): %v", id, err)
			}
		}

		// Re-insert task retry timeout if the task is pending and has a retry deadline.
		if taskState == "pending" && taskTimeoutRetry != nil {
			if err := r.handler.Session.Query(
				`INSERT INTO task_timeouts (bucket, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, 0, ?, ?, ?)`,
				r.handler.BucketFor(*taskTimeoutRetry), *taskTimeoutRetry, id, origin, timeoutAt,
			).Exec(); err != nil {
				log.Printf("repair: insert task retry timeout(%s): %v", id, err)
			}
		}

		// Re-insert task lease timeout if the task is acquired and has a lease deadline.
		if taskState == "acquired" && taskTimeoutLease != nil {
			if err := r.handler.Session.Query(
				`INSERT INTO task_timeouts (bucket, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, 1, ?, ?, ?)`,
				r.handler.BucketFor(*taskTimeoutLease), *taskTimeoutLease, id, origin, timeoutAt,
			).Exec(); err != nil {
				log.Printf("repair: insert task lease timeout(%s): %v", id, err)
			}
		}
	}

	if err := iter.Close(); err != nil {
		log.Printf("repair: scan promises: %v", err)
	}
}
