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

// pairOverallTimeout bounds the QR phase in case the QR channel never
// reports its own timeout.
const pairOverallTimeout = 5 * time.Minute

// After the QR is scanned the link is not finished yet: WhatsApp drops the
// socket, the client reconnects and logs in as the new device, and the
// phone pushes its initial sync (history-sync notifications, which
// whatsmeow acknowledges automatically while connected). Disconnecting
// straight after PairSuccess skips all of that, and the phone then logs the
// half-linked device out (observed: 401 about 90s after pairing). So pair
// stays connected until that settles.
type settleTimes struct {
	reconnect time.Duration // max wait for the post-pair Connected
	min       time.Duration // stay at least this long after connecting
	quiet     time.Duration // ... and until no history sync for this long
	max       time.Duration // hard cap after connecting
}

var defaultSettle = settleTimes{reconnect: time.Minute, min: 30 * time.Second, quiet: 20 * time.Second, max: 3 * time.Minute}

// errLoggedOutWhilePairing: the phone rejected the link during setup.
var errLoggedOutWhilePairing = errors.New("WhatsApp logged the new device out while finishing the link; run `pair` again and keep WhatsApp open on the phone until it finishes")

// waitForInitialSync consumes post-pairing events until the link has
// settled (see settleTimes). It returns how many history-sync payloads
// arrived, and an error if the device was logged out or never reconnected.
func waitForInitialSync(ctx context.Context, evts <-chan any, t settleTimes) (historySyncs int, err error) {
	reconnect := time.NewTimer(t.reconnect)
	defer reconnect.Stop()
	for connected := false; !connected; {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-reconnect.C:
			return 0, errors.New("paired, but WhatsApp did not reconnect the new device; the link may not have completed. Check Linked devices on the phone and run `pair` again if it is missing")
		case e := <-evts:
			switch e.(type) {
			case *events.Connected:
				connected = true
			case *events.LoggedOut:
				return 0, errLoggedOutWhilePairing
			}
		}
	}

	hardCap := time.NewTimer(t.max)
	defer hardCap.Stop()
	minDone := time.After(t.min)
	quiet := time.NewTimer(t.quiet)
	defer quiet.Stop()
	minElapsed, isQuiet := false, false
	for !(minElapsed && isQuiet) {
		select {
		case <-ctx.Done():
			return historySyncs, ctx.Err()
		case <-hardCap.C:
			return historySyncs, nil
		case <-minDone:
			minElapsed = true
		case <-quiet.C:
			isQuiet = true
		case e := <-evts:
			switch e.(type) {
			case *events.HistorySync:
				historySyncs++
				isQuiet = false
				if !quiet.Stop() {
					select {
					case <-quiet.C:
					default:
					}
				}
				quiet.Reset(t.quiet)
			case *events.LoggedOut:
				return historySyncs, errLoggedOutWhilePairing
			}
		}
	}
	return historySyncs, nil
}

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
	// Post-pairing events for waitForInitialSync. Never block whatsmeow's
	// dispatch: drop if full (only Connected/HistorySync/LoggedOut matter,
	// and a dropped HistorySync just ends the wait slightly sooner).
	settleEvts := make(chan any, 256)
	cli.AddEventHandler(func(evt any) {
		switch e := evt.(type) {
		case *events.PairSuccess:
			mu.Lock()
			id := e.ID
			paired = &id
			mu.Unlock()
		case *events.Connected, *events.HistorySync, *events.LoggedOut:
			select {
			case settleEvts <- evt:
			default:
			}
		}
	})

	qrCtx, cancel := context.WithTimeout(ctx, pairOverallTimeout)
	defer cancel()
	qrChan, err := cli.GetQRChannel(qrCtx)
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
			fmt.Fprintf(out, "QR scanned for %s. Finishing the link: staying connected while WhatsApp sends its initial sync.\n", account)
			fmt.Fprintln(out, "Keep WhatsApp open on the phone; this takes 30s to 3 minutes. Don't interrupt it.")
			n, err := waitForInitialSync(ctx, settleEvts, defaultSettle)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "Paired as %s (initial sync: %d history payload(s) received, not imported). Session saved to %s.\n", account, n, path)
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
	if qrCtx.Err() != nil {
		return ErrPairTimeout
	}
	return errors.New("pairing ended without success")
}

func redactedAccount(id types.JID) string {
	return whatsapp.RedactJID(id.ToNonAD())
}
