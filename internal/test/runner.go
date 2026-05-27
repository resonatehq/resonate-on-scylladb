package test

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
)

// Fiber is a cooperatively scheduled coroutine.
// It communicates with the runner via two unbuffered channels:
// suspend (fiber -> runner) and resume (runner -> fiber).
type Fiber struct {
	ID      string
	Label   string
	suspend chan struct{}
	resume  chan struct{}
	done    bool
	verbose bool
	// onYield, if set, is invoked synchronously inside yield(label) before the
	// fiber suspends. The Runner uses it to push a trace event when tracing is
	// enabled. nil is a no-op.
	onYield func(label string)
}

// NewFiber creates a fiber with the given ID.
func NewFiber(id string) *Fiber {
	return &Fiber{
		ID:      id,
		suspend: make(chan struct{}),
		resume:  make(chan struct{}),
	}
}

// killedError is the sentinel value panicked by yield() when the fiber is killed.
type killedError struct{}

// Start launches the fiber's goroutine. Blocks until the fiber yields for the
// first time or completes.
func (f *Fiber) Start(fn func(yield func(string))) {
	go func() {
		defer close(f.suspend)
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(killedError); !ok {
					panic(r)
				}
			}
		}()
		fn(f.yield)
	}()
	if _, ok := <-f.suspend; !ok {
		f.done = true
	}
}

// Advance resumes the fiber and blocks until it yields again or completes.
// Returns false if the fiber has finished.
func (f *Fiber) Advance() bool {
	if f.done {
		return false
	}
	f.resume <- struct{}{}
	if _, ok := <-f.suspend; !ok {
		f.done = true
		return false
	}
	return true
}

// Done reports whether the fiber has completed.
func (f *Fiber) Done() bool { return f.done }

// Kill interrupts the fiber at its current yield point.
// Blocks until the fiber's goroutine exits.
func (f *Fiber) Kill() {
	if f.done {
		return
	}
	close(f.resume)
	<-f.suspend // wait for goroutine to exit via deferred close
	f.done = true
}

func (f *Fiber) yield(label string) {
	if f.verbose {
		fmt.Printf("  [%s] yield %s label=%s\n", f.ID, f.Label, label)
	}
	if f.onYield != nil {
		f.onYield(label)
	}
	f.suspend <- struct{}{}
	if _, ok := <-f.resume; !ok {
		panic(killedError{})
	}
	if f.verbose {
		fmt.Printf("  [%s] resume\n", f.ID)
	}
}

// Runner drives fibers deterministically.
type Runner[I any, O any] struct {
	handler func(I, func(string)) O
	// OnKill, if set, is called with the original input when a fiber is killed.
	// Its return value is sent to the fiber's result channel so callers always
	// receive a value. If nil, the zero value of O is sent instead.
	OnKill       func(I) O
	fibers       map[string]*Fiber
	rng          *rand.Rand
	nextID       int
	Verbose      bool
	CurrentFiber *Fiber
	// Clock is a logical counter bumped on every Spawn and Tick.
	// done maps fiber ID to the Clock value at which it completed.
	Clock int64
	done  map[string]int64
	// Trace, if non-nil, accumulates a per-test trace of every fiber lifecycle
	// event (spawn / advance / yield / kill / done). Tests dump this ring on
	// failure so an agent can see the proximate cause without re-running.
	Trace *ring
}

// emit pushes a trace event if Trace is set. Clock is stamped from the runner.
func (s *Runner[I, O]) emit(e traceEvent) {
	if s.Trace == nil {
		return
	}
	e.Clock = s.Clock
	s.Trace.push(e)
}

// NewRunner creates a runner with the given handler and RNG seed.
func NewRunner[I any, O any](handler func(I, func(string)) O, seed int64) *Runner[I, O] {
	return &Runner[I, O]{
		handler: handler,
		fibers:  make(map[string]*Fiber),
		rng:     rand.New(rand.NewSource(seed)),
		done:    make(map[string]int64),
	}
}

// Spawn creates and starts a fiber for the given input.
// Returns the fiber ID and a buffered channel that will receive the result.
// Clock is bumped before the fiber starts; done[id] is set if the fiber
// completes without yielding.
func (s *Runner[I, O]) Spawn(input I) (string, <-chan O) {
	s.Clock++
	s.nextID++
	id := fiberID(s.nextID)
	ch := make(chan O, 1)
	f := NewFiber(id)
	f.verbose = s.Verbose
	if s.Trace != nil {
		f.onYield = func(label string) {
			s.emit(traceEvent{FiberID: id, Kind: "yield", Label: label})
		}
	}
	s.fibers[id] = f
	s.CurrentFiber = f
	s.emit(traceEvent{FiberID: id, Kind: "spawn"})
	onKill := s.OnKill
	f.Start(func(yield func(string)) {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(killedError); ok {
					var result O
					if onKill != nil {
						result = onKill(input)
					}
					ch <- result
					return
				}
				panic(r)
			}
		}()
		ch <- s.handler(input, yield)
	})
	if f.Done() {
		s.done[id] = s.Clock
		s.emit(traceEvent{FiberID: id, Kind: "done"})
		delete(s.fibers, id)
	}
	return id, ch
}

// Active reports whether any fibers are live.
func (s *Runner[I, O]) Active() bool { return len(s.fibers) > 0 }

// ActiveCount returns the number of live fibers.
func (s *Runner[I, O]) ActiveCount() int { return len(s.fibers) }

// Tick advances one fiber — the named one, or a random one if no id is given.
// Clock is bumped; done[id] is set if the fiber completes on this tick.
func (s *Runner[I, O]) Tick(id ...string) {
	if !s.Active() {
		return
	}
	s.Clock++
	target := s.pickFiber(id...)
	s.emit(traceEvent{FiberID: target, Kind: "advance"})
	if !s.fibers[target].Advance() {
		s.done[target] = s.Clock
		s.emit(traceEvent{FiberID: target, Kind: "done"})
		delete(s.fibers, target)
	}
}

// TickOrKill advances a random fiber, or kills it with probability killProb.
// Returns true if the fiber was killed (leaving the DB in a partial state).
func (s *Runner[I, O]) TickOrKill(killProb float64) bool {
	if !s.Active() {
		return false
	}
	s.Clock++
	target := s.pickFiber()
	if s.rng.Float64() < killProb {
		s.emit(traceEvent{FiberID: target, Kind: "kill"})
		s.fibers[target].Kill()
		s.done[target] = s.Clock
		delete(s.fibers, target)
		return true
	}
	s.emit(traceEvent{FiberID: target, Kind: "advance"})
	if !s.fibers[target].Advance() {
		s.done[target] = s.Clock
		s.emit(traceEvent{FiberID: target, Kind: "done"})
		delete(s.fibers, target)
	}
	return false
}

// Exec runs all fibers to completion.
func (s *Runner[I, O]) Exec() {
	for s.Active() {
		s.Tick()
	}
}

func (s *Runner[I, O]) pickFiber(id ...string) string {
	if len(id) > 0 {
		return id[0]
	}
	ids := make([]string, 0, len(s.fibers))
	for k := range s.fibers {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	return ids[s.rng.Intn(len(ids))]
}

func fiberID(n int) string {
	return "fiber-" + strconv.Itoa(n)
}

// Call records a spawned fiber's input and its result channel.
type Call[I, O any] struct {
	Input  I
	Result <-chan O
}

// Fuzzer drives a Runner with randomly generated inputs,
// interleaving Spawn and Tick calls.
type Fuzzer[I, O any] struct {
	Sched *Runner[I, O]
	Gen   func(*rand.Rand) I
	rng   *rand.Rand
}

// NewFuzzer creates a Fuzzer wrapping sched, using gen to produce inputs.
func NewFuzzer[I, O any](sched *Runner[I, O], gen func(*rand.Rand) I, seed int64) *Fuzzer[I, O] {
	return &Fuzzer[I, O]{
		Sched: sched,
		Gen:   gen,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

// Run randomly interleaves n Spawn calls with Tick calls until quiescent.
// Returns all calls in spawn order.
func (f *Fuzzer[I, O]) Run(n int) []Call[I, O] {
	var calls []Call[I, O]
	for n > 0 || f.Sched.Active() {
		if n > 0 && (!f.Sched.Active() || f.rng.Intn(2) == 0) {
			input := f.Gen(f.rng)
			_, ch := f.Sched.Spawn(input)
			calls = append(calls, Call[I, O]{Input: input, Result: ch})
			n--
		} else {
			f.Sched.Tick()
		}
	}
	return calls
}
