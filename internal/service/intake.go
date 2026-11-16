package service

import (
	"context"
	"sync"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// LiveSource is a live message intake (e.g. the future Whatsmeow client).
// Run starts each configured source AFTER the recovery snapshot has been
// taken, hands it a Sink that feeds api.ProcessLiveMessage, and calls
// Stop at the very start of shutdown.
//
// Contract:
//   - Start must not block for the source's lifetime; run the event loop
//     in a goroutine and return once the source is accepting events.
//   - After Stop returns the source must not call sink again. Any call
//     it does make anyway is rejected with ErrIntakeClosed (the message
//     is NOT stored), so a source that needs at-least-once delivery
//     must not ack events it delivered after Stop.
//
// No implementation exists yet: the Whatsmeow source is its own issue
// (see the TODO in internal/ingestion/whatsapp/live.go).
type LiveSource interface {
	Name() string
	Start(ctx context.Context, sink ingestion.Sink) error
	Stop(ctx context.Context) error
}

// ErrIntakeClosed is returned by a source's sink once that source has
// been stopped (or the service is shutting down).
// It is ingestion.ErrSinkClosed, so sources can recognize it without
// importing this package.
var ErrIntakeClosed = ingestion.ErrSinkClosed

// intake tracks every in-flight sink call across all sources, so shutdown
// can wait for them to drain before the final window flush. Each call
// runs under workCtx, which is deliberately NOT the signal context: a
// SIGTERM must not abort a message half-way through storage. workCtx is
// only cancelled if draining exceeds its budget.
type intake struct {
	workCtx context.Context
	// timeout bounds each process call (PEGASUS_LIVE_SINK_TIMEOUT); 0 = none.
	timeout time.Duration
	process func(ctx context.Context, m ingestion.RawMessage) error

	wg sync.WaitGroup
}

// gate is one source's view of the intake: a sink that can be closed
// independently of the others, right after that source's Stop returns.
type gate struct {
	in     *intake
	mu     sync.Mutex
	closed bool
}

func (in *intake) newGate() *gate { return &gate{in: in} }

func (g *gate) sink(m ingestion.RawMessage) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrIntakeClosed
	}
	// Add under the same lock close() takes, so once close() returns no
	// new call can slip into the WaitGroup behind drain's back.
	g.in.wg.Add(1)
	g.mu.Unlock()
	defer g.in.wg.Done()
	ctx := g.in.workCtx
	if g.in.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.in.timeout)
		defer cancel()
	}
	return g.in.process(ctx, m)
}

func (g *gate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

// drain waits for in-flight sink calls, up to ctx's deadline. It returns
// ctx.Err() if they did not finish in time.
func (in *intake) drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		in.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
