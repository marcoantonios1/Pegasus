// Throwaway manual-test CLI for the live WhatsApp (whatsmeow) adapter. Not
// part of the adapter itself — delete after use.
//
// Links a new WhatsApp "linked device" session by QR pairing (scan with
// your phone: WhatsApp > Linked Devices > Link a Device), then prints every
// RawMessage the adapter produces from live traffic, incoming and outgoing.
//
// Session state is stored in a throwaway Postgres database so re-running
// this doesn't require re-pairing every time:
//
//	docker exec pegasus-postgres psql -U pegasus -d pegasus -c "CREATE DATABASE whatsmeow_test;"
//	go run ./cmd/wa_live_test
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp"
)

func main() {
	ctx := context.Background()

	dsn := "postgres://pegasus:pegasus@localhost:5432/whatsmeow_test?sslmode=disable"
	if v := os.Getenv("WA_TEST_DSN"); v != "" {
		dsn = v
	}

	container, err := sqlstore.New(ctx, "pgx", dsn, waLog.Stdout("Database", "WARN", true))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open session store:", err)
		os.Exit(1)
	}

	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "get device:", err)
		os.Exit(1)
	}

	client := whatsmeow.NewClient(deviceStore, waLog.Stdout("Client", "WARN", true))

	handler := whatsapp.NewEventHandler(func(rm ingestion.RawMessage) error {
		text := "<nil>"
		if rm.Text != nil {
			text = *rm.Text
		}
		mediaURL := "<nil>"
		if rm.MediaURL != nil {
			mediaURL = *rm.MediaURL
		}
		fmt.Printf(">> RawMessage external_id=%s conversation=%s sender=%s media_type=%s text=%q media_url=%q timestamp=%s\n",
			rm.ExternalID, rm.ConversationID, rm.SenderExternalID, rm.MediaType, text, mediaURL, rm.Timestamp.Format("15:04:05"))
		return nil
	})
	client.AddEventHandler(handler.Handle)

	if client.Store.ID == nil {
		qrChan, err := client.GetQRChannel(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "get QR channel:", err)
			os.Exit(1)
		}
		if err := client.Connect(); err != nil {
			fmt.Fprintln(os.Stderr, "connect:", err)
			os.Exit(1)
		}
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("Scan this QR code with WhatsApp: Settings > Linked Devices > Link a Device")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			} else {
				fmt.Println("QR login event:", evt.Event)
			}
		}
	} else if err := client.Connect(); err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}

	fmt.Println("connected — send/receive a few WhatsApp messages, then Ctrl+C to stop")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	client.Disconnect()
}
