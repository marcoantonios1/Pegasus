package wasource

import (
	"context"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// Client is the narrow slice of *whatsmeow.Client the source uses, so tests
// can inject a fake event source with no network.
type Client interface {
	AddEventHandler(handler func(evt any)) uint32
	Connect() error
	Disconnect()
	IsConnected() bool
	// OwnJID is the paired account's phone-number JID (may carry a device
	// suffix; callers canonicalize).
	OwnJID() types.JID
	// PNForLID maps a @lid JID to its phone-number JID, or returns an empty
	// JID if the store has no mapping.
	PNForLID(ctx context.Context, lid types.JID) (types.JID, error)
	// DisableAutoReconnect stops whatsmeow's own reconnect loop (used once a
	// fatal state is reached, so nothing keeps hammering the server).
	DisableAutoReconnect()
	// ClearSession deletes the paired device from the session store (after
	// a logout). Idempotent.
	ClearSession(ctx context.Context) error
}

// whatsmeowClient adapts *whatsmeow.Client to Client.
type whatsmeowClient struct {
	c         *whatsmeow.Client
	container *sqlstore.Container
	jid       types.JID // the paired device, captured before whatsmeow can clear Store.ID
}

func (w whatsmeowClient) AddEventHandler(h func(evt any)) uint32 {
	return w.c.AddEventHandler(whatsmeow.EventHandler(h))
}
func (w whatsmeowClient) Connect() error    { return w.c.Connect() }
func (w whatsmeowClient) Disconnect()       { w.c.Disconnect() }
func (w whatsmeowClient) IsConnected() bool { return w.c.IsConnected() }
func (w whatsmeowClient) OwnJID() types.JID {
	if w.c.Store.ID == nil {
		return types.JID{}
	}
	return *w.c.Store.ID
}
func (w whatsmeowClient) PNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	return w.c.Store.LIDs.GetPNForLID(ctx, lid)
}
func (w whatsmeowClient) DisableAutoReconnect() {
	w.c.EnableAutoReconnect = false
}

// ClearSession loads a fresh copy of the device row rather than touching
// w.c.Store, which whatsmeow may be deleting concurrently on its own
// goroutine after a logout. Deleting an already-deleted row is a no-op.
func (w whatsmeowClient) ClearSession(ctx context.Context) error {
	dev, err := w.container.GetDevice(ctx, w.jid)
	if err != nil || dev == nil {
		return err
	}
	return dev.Delete(ctx)
}
