// Package wasourcetest provides an in-memory wasource.Client for tests: no
// network, events injected with Emit and dispatched synchronously to the
// registered handlers, as whatsmeow's own dispatch does.
package wasourcetest

import (
	"context"
	"sync"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type FakeClient struct {
	mu            sync.Mutex
	handlers      []func(any)
	own           types.JID
	connects      int
	disconnects   int
	autoReconnect bool

	// LIDs maps @lid JIDs to phone-number JIDs (PNForLID).
	LIDs map[types.JID]types.JID
	// ConnectErr, if set, is consulted on the n-th Connect (1-based).
	ConnectErr func(n int) error
	// NoConnectedEvent suppresses the Connected event a successful
	// Connect normally emits.
	NoConnectedEvent bool
	// Record, if set, receives "add_handler", "connect", "disconnect".
	Record func(event string)
}

// New returns a fake paired as own (may carry a device suffix).
func New(own types.JID) *FakeClient {
	return &FakeClient{own: own, LIDs: map[types.JID]types.JID{}, autoReconnect: true}
}

func (f *FakeClient) rec(e string) {
	if f.Record != nil {
		f.Record(e)
	}
}

func (f *FakeClient) AddEventHandler(h func(any)) uint32 {
	f.mu.Lock()
	f.handlers = append(f.handlers, h)
	n := len(f.handlers)
	f.mu.Unlock()
	f.rec("add_handler")
	return uint32(n)
}

func (f *FakeClient) Connect() error {
	f.mu.Lock()
	f.connects++
	n := f.connects
	errFn, silent := f.ConnectErr, f.NoConnectedEvent
	f.mu.Unlock()
	f.rec("connect")
	if errFn != nil {
		if err := errFn(n); err != nil {
			return err
		}
	}
	if !silent {
		f.Emit(&events.Connected{})
	}
	return nil
}

func (f *FakeClient) Disconnect() {
	f.mu.Lock()
	f.disconnects++
	f.mu.Unlock()
	f.rec("disconnect")
}

func (f *FakeClient) IsConnected() bool { return true }
func (f *FakeClient) OwnJID() types.JID { return f.own }

func (f *FakeClient) PNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.LIDs[lid], nil
}

func (f *FakeClient) DisableAutoReconnect() {
	f.mu.Lock()
	f.autoReconnect = false
	f.mu.Unlock()
}

// Emit delivers evt to every registered handler, synchronously.
func (f *FakeClient) Emit(evt any) {
	f.mu.Lock()
	hs := append([]func(any){}, f.handlers...)
	f.mu.Unlock()
	for _, h := range hs {
		h(evt)
	}
}

func (f *FakeClient) ConnectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects
}

func (f *FakeClient) DisconnectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.disconnects
}

func (f *FakeClient) AutoReconnectEnabled() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.autoReconnect
}
