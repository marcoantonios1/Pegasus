package wasource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp"
)

// ErrPairTimeout means no QR code was scanned before WhatsApp stopped
// issuing them.
var ErrPairTimeout = errors.New("pairing timed out: no QR code was scanned in time; run `pair` again")

// pairOverallTimeout bounds the whole pairing session in case the QR
// channel never reports its own timeout.
const pairOverallTimeout = 5 * time.Minute

// Pair links this process as a new WhatsApp device: it shows QR codes in
// the terminal (out) until one is scanned and PairSuccess arrives, then
// prints the paired account with the number redacted. If the session is
// already paired it says so and returns nil.
//
// The service must be stopped first: two clients on one session replace
// each other (StreamReplaced).
func Pair(ctx context.Context, cfg Config, out io.Writer) error {
	cfg = cfg.withDefaults()
	path, err := ResolveSessionPath(cfg.SessionPath)
	if err != nil {
		return err
	}
	wlog := waLog.Stdout("whatsmeow", cfg.LogLevel, false)
	container, err := OpenStore(ctx, path, wlog.Sub("Database"))
	if err != nil {
		return err
	}
	defer container.Close()

	dev, err := pairedDevice(ctx, container)
	if err == nil {
		fmt.Fprintf(out, "Already paired as %s (session %s). Nothing to do.\n", redactedAccount(*dev.ID), path)
		fmt.Fprintln(out, "To re-pair: unlink the device on the phone (Settings → Linked devices), delete the session file, then run pair again.")
		return nil
	}
	if !errors.Is(err, ErrNotPaired) {
		return err
	}

	cli := whatsmeow.NewClient(dev, wlog.Sub("Client"))
	defer cli.Disconnect()

	var (
		mu     sync.Mutex
		paired *types.JID
	)
	cli.AddEventHandler(func(evt any) {
		if ps, ok := evt.(*events.PairSuccess); ok {
			mu.Lock()
			id := ps.ID
			paired = &id
			mu.Unlock()
		}
	})

	ctx, cancel := context.WithTimeout(ctx, pairOverallTimeout)
	defer cancel()
	qrChan, err := cli.GetQRChannel(ctx)
	if err != nil {
		return fmt.Errorf("get QR channel: %w", err)
	}
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("connect to WhatsApp: %w", err)
	}

	fmt.Fprintln(out, "Scan this QR code with WhatsApp on the phone: Settings → Linked devices → Link a device.")
	for item := range qrChan {
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			fmt.Fprintln(out)
			qrterminal.GenerateHalfBlock(item.Code, qrterminal.L, out)
			fmt.Fprintf(out, "(code refreshes in %s)\n", item.Timeout.Round(time.Second))
		case whatsmeow.QRChannelSuccess.Event:
			mu.Lock()
			id := paired
			mu.Unlock()
			account := "(unknown)"
			if id != nil {
				account = redactedAccount(*id)
			}
			fmt.Fprintf(out, "Paired as %s. Session saved to %s.\n", account, path)
			fmt.Fprintln(out, "Treat that file as a credential. You can now start the service.")
			return nil
		case whatsmeow.QRChannelTimeout.Event:
			return ErrPairTimeout
		case whatsmeow.QRChannelEventError:
			return fmt.Errorf("pairing failed: %v", item.Error)
		default:
			return fmt.Errorf("pairing failed: %s", item.Event)
		}
	}
	if ctx.Err() != nil {
		return ErrPairTimeout
	}
	return errors.New("pairing ended without success")
}

func redactedAccount(id types.JID) string {
	return whatsapp.RedactJID(id.ToNonAD())
}
