package loop

import (
	"bytes"
	"context"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

const defaultWorkerTTL = 30_000 // milliseconds
const workerTTLDivisor = 5      // interval = workerTTL / workerTTLDivisor

// TimeoutProcessor manages worker registration, shard assignment, and per-shard
// timeout processing goroutines. Implements base.Background.
type TimeoutProcessor struct {
	handler   *core.Handler
	interval  time.Duration
	workerID  gocql.UUID
	shards    int
	workerTTL int // milliseconds; TTL for the workers table row

	stop chan struct{}
	once sync.Once
}

func NewTimeoutProcessor(handler *core.Handler, workerID gocql.UUID, shards, workerTTL int) *TimeoutProcessor {
	if shards <= 0 {
		shards = 1
	}
	if workerTTL <= 0 {
		workerTTL = defaultWorkerTTL
	}
	return &TimeoutProcessor{
		handler:   handler,
		interval:  time.Duration(workerTTL/workerTTLDivisor) * time.Millisecond,
		workerID:  workerID,
		shards:    shards,
		workerTTL: workerTTL,
	}
}

func (p *TimeoutProcessor) Init() {
	p.stop = make(chan struct{})
	p.once = sync.Once{}
	go p.coordinator()
}

func (p *TimeoutProcessor) Stop() {
	p.once.Do(func() { close(p.stop) })
}

// coordinator runs the heartbeat loop. On each tick it:
//  1. Upserts the worker row with TTL so the row expires if this process dies.
//  2. Reads all alive workers and computes which shards this worker owns.
//  3. If the assigned shard set changed, cancels all running table goroutines,
//     waits for them to exit, then starts fresh ones for the new shard set.
func (p *TimeoutProcessor) coordinator() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	var (
		cancel        context.CancelFunc = func() {}
		wg            sync.WaitGroup
		currentShards []int
	)

	for {
		select {
		case <-p.stop:
			cancel()
			wg.Wait()
			return
		case <-ticker.C:
		}

		// 1. Upsert heartbeat.
		if err := p.handler.Session.Query(
			`INSERT INTO workers (worker_id) VALUES (?) USING TTL ?`,
			p.workerID, p.workerTTL/1000,
		).Exec(); err != nil {
			// Non-fatal: if the upsert fails we still process our last known shards.
			_ = err
		}

		// 2. Read alive workers and compute shard assignment.
		var allWorkers []gocql.UUID
		iter := p.handler.Session.Query(`SELECT worker_id FROM workers`).Iter()
		var wid gocql.UUID
		for iter.Scan(&wid) {
			allWorkers = append(allWorkers, wid)
		}
		iter.Close()

		newShards := assignedShards(p.workerID, allWorkers, p.shards)

		// 3. Rebalance if the shard set changed.
		if !slices.Equal(currentShards, newShards) {
			log.Printf("timeout coordinator %s: shard rebalance %v → %v (workers: %d)",
				p.workerID, currentShards, newShards, len(allWorkers))
			cancel()
			wg.Wait()

			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())

			for _, s := range newShards {
				shard := int16(s)
				wg.Add(3)
				go p.tableLoop(ctx, &wg, p.handler.TickPromiseTimeoutsAt, shard)
				go p.tableLoop(ctx, &wg, p.handler.TickTaskTimeoutsAt, shard)
				go p.tableLoop(ctx, &wg, p.handler.TickScheduleTimeoutsAt, shard)
			}
			currentShards = newShards
		}
	}
}

// tableLoop drives one (tickFn, shard) pair. It runs its own tick loop
// independently of other table goroutines so scans proceed concurrently.
func (p *TimeoutProcessor) tableLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	tickFn func(context.Context, int64, int16, func(string)),
	shard int16,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		tickFn(ctx, start.UnixMilli(), shard, func(string) {})
		elapsed := time.Since(start)

		remaining := p.interval - elapsed
		if remaining <= 0 {
			remaining = 0
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(remaining):
		}
	}
}

// assignedShards returns the subset of [0, shards) that belong to workerID
// given the full set of alive workers. Assignment is stable: shards are
// distributed round-robin over workers sorted by UUID bytes.
func assignedShards(workerID gocql.UUID, allWorkers []gocql.UUID, n int) []int {
	if len(allWorkers) == 0 {
		return nil
	}
	sorted := make([]gocql.UUID, len(allWorkers))
	copy(sorted, allWorkers)
	slices.SortFunc(sorted, func(a, b gocql.UUID) int {
		return bytes.Compare(a[:], b[:])
	})

	myIndex := slices.Index(sorted, workerID)
	if myIndex == -1 {
		return nil
	}

	var shards []int
	for s := 0; s < n; s++ {
		if s%len(sorted) == myIndex {
			shards = append(shards, s)
		}
	}
	return shards
}
