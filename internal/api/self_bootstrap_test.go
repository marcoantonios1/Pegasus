package api

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/identity"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
	"github.com/marcoantonios1/Pegasus/internal/testdb"
)

// These tests run on a fresh database each: they need control over the
// single is_self entity, which the shared dev database already holds.

const (
	selfWAJID   = "96170000001@s.whatsapp.net"
	selfWALabel = "Marco" // how .txt exports label his own messages (checked against real exports)
	// selfWALabel2 is a second configured export label. The import test uses
	// it so the speaker relabelling is actually exercised (with "Marco" the
	// label and the self name coincide).
	selfWALabel2  = "Marco Phone"
	selfIGName    = "Marco Antonios IG"
	selfAliasName = "Marco Antonios"
)

func freshStores(t *testing.T) *testStores {
	t.Helper()
	pool, _ := testdb.FreshPool(t)
	return &testStores{
		pool: pool, entities: memory.NewEntityStore(pool), edges: memory.NewEdgeStore(pool),
		messages: memory.NewMessageStore(pool), embeds: memory.NewEmbeddingStore(pool),
		relStats: memory.NewRelationshipStatsStore(pool),
	}
}

func marcoRegistry(t *testing.T) *identity.Registry {
	t.Helper()
	reg, problems := identity.New(identity.Config{
		SelfName: "Marco", Aliases: []string{selfAliasName},
		WhatsApp: []string{"+961 70 000 001", selfWALabel, selfWALabel2}, Instagram: []string{selfIGName},
	})
	if len(problems) > 0 {
		t.Fatalf("registry: %v", problems)
	}
	return reg
}

// capturingExtractor records every window (for speaker-label checks) and
// returns the same triples for each.
type capturingExtractor struct {
	mu      sync.Mutex
	windows []extraction.Window
	triples []extraction.ExtractedTriple
}

func (c *capturingExtractor) ExtractWindow(_ context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.windows = append(c.windows, w)
	return c.triples, nil
}

func (c *capturingExtractor) speakers() map[string]string { // text -> speaker
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]string{}
	for _, w := range c.windows {
		for _, m := range w.Messages {
			out[m.Text] = m.Speaker
		}
	}
	return out
}

func newBootstrappedAPI(t *testing.T, ts *testStores, ex Extractor) *API {
	t.Helper()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: &fakeTriager{candidate: true}, Extractor: ex, Logger: t.Logf,
		LiveWindowFlushAfter: time.Hour, LiveWindowFlushPollInterval: time.Hour,
	})
	if _, err := a.BootstrapSelf(context.Background(), marcoRegistry(t)); err != nil {
		t.Fatalf("BootstrapSelf: %v", err)
	}
	return a
}

func liveText(platform, conv, sender, text string) ingestion.RawMessage {
	return ingestion.RawMessage{
		ExternalID: "self-test-" + uuid.NewString(), Platform: platform, ConversationID: conv,
		SenderExternalID: sender, Timestamp: time.Now(), MediaType: ingestion.MediaTypeText, Text: &text,
	}
}

func flushLive(t *testing.T, a *API) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := a.StartLiveWindowFlusher(ctx)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("final flush did not finish")
	}
}

func senderOf(t *testing.T, ts *testStores, externalID string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	var msgID, senderID uuid.UUID
	if err := ts.pool.QueryRow(context.Background(), `SELECT id, sender_id FROM messages WHERE external_id = $1`, externalID).Scan(&msgID, &senderID); err != nil {
		t.Fatalf("message %s: %v", externalID, err)
	}
	return msgID, senderID
}

// --- (13) bootstrap ---

func TestBootstrapSelf_CreatesExactlyOneAndIsStable(t *testing.T) {
	ts := freshStores(t)
	ctx := context.Background()

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	if _, err := a.resolveSender(ctx, identity.WhatsApp, selfWAJID); !errors.Is(err, errSelfNotBootstrapped) {
		t.Fatalf("resolveSender before bootstrap: %v, want errSelfNotBootstrapped", err)
	}

	self, err := a.BootstrapSelf(ctx, marcoRegistry(t))
	if err != nil {
		t.Fatal(err)
	}
	if !self.IsSelf || self.CanonicalName != "Marco" {
		t.Fatalf("self entity: %+v", self)
	}
	if n, _ := ts.entities.CountSelf(ctx); n != 1 {
		t.Fatalf("is_self rows = %d, want 1", n)
	}

	again, err := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).BootstrapSelf(ctx, marcoRegistry(t))
	if err != nil || again.ID != self.ID {
		t.Fatalf("second start: %v, id %v want %v", err, again, self.ID)
	}
	// Case differences in the configured name are fine.
	reg, _ := identity.New(identity.Config{SelfName: "MARCO", WhatsApp: []string{selfWAJID}})
	if got, err := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).BootstrapSelf(ctx, reg); err != nil || got.ID != self.ID {
		t.Fatalf("case-insensitive name: %v", err)
	}
}

func TestBootstrapSelf_ZeroIdentitiesFails(t *testing.T) {
	if _, problems := identity.New(identity.Config{SelfName: "Marco"}); len(problems) == 0 {
		t.Fatal("a self name with no identity on any platform must be rejected")
	}
	ts := freshStores(t)
	if _, err := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).BootstrapSelf(context.Background(), nil); err == nil {
		t.Fatal("BootstrapSelf(nil) must fail")
	}
}

func TestBootstrapSelf_RefusesConflicts(t *testing.T) {
	ctx := context.Background()

	t.Run("existing self with a different name", func(t *testing.T) {
		ts := freshStores(t)
		old, err := ts.entities.GetOrCreateSelf(ctx, selfWAJID) // an old lazily-created self
		if err != nil {
			t.Fatal(err)
		}
		_, err = New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).BootstrapSelf(ctx, marcoRegistry(t))
		var sc *SelfConflictError
		if !errors.As(err, &sc) || !strings.Contains(err.Error(), old.ID.String()) || !strings.Contains(err.Error(), `"Marco"`) {
			t.Fatalf("got %v", err)
		}
		if n, _ := ts.entities.CountSelf(ctx); n != 1 {
			t.Errorf("no auto-repair expected; is_self rows = %d", n)
		}
	})

	for _, held := range []string{"Marco", selfAliasName, selfWAJID, selfIGName} {
		t.Run("non-self entity holds "+held, func(t *testing.T) {
			ts := freshStores(t)
			contact, _, err := ts.entities.GetOrCreateByCanonicalName(ctx, held, "person")
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).BootstrapSelf(ctx, marcoRegistry(t))
			var sc *SelfConflictError
			if !errors.As(err, &sc) || !strings.Contains(err.Error(), contact.ID.String()) {
				t.Fatalf("want a SelfConflictError naming %s, got %v", contact.ID, err)
			}
			if self, _ := ts.entities.GetSelf(ctx); self != nil {
				t.Error("a self entity was created despite the conflict")
			}
		})
	}
}

func TestBootstrapSelf_ConcurrentStartsProduceOneSelf(t *testing.T) {
	ts := freshStores(t)
	ctx := context.Background()
	const n = 8
	ids := make([]uuid.UUID, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, err := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).BootstrapSelf(ctx, marcoRegistry(t))
			errs[i] = err
			if e != nil {
				ids[i] = e.ID
			}
		}(i)
	}
	wg.Wait()
	for i := range ids {
		if errs[i] != nil || ids[i] != ids[0] {
			t.Fatalf("bootstrap %d: id=%v err=%v (first id %v)", i, ids[i], errs[i], ids[0])
		}
	}
	if c, _ := ts.entities.CountSelf(ctx); c != 1 {
		t.Fatalf("is_self rows = %d", c)
	}
}

// --- (14) platform scoping, through ProcessLiveMessage ---

func TestResolveSender_PlatformScoped(t *testing.T) {
	ts := freshStores(t)
	ctx := context.Background()
	a := newBootstrappedAPI(t, ts, &capturingExtractor{})
	self := a.Self()

	for _, c := range []struct {
		platform, sender string
		wantSelf         bool
	}{
		{identity.WhatsApp, selfWAJID, true},
		{identity.Instagram, selfIGName, true},
		{identity.Instagram, selfWAJID, false}, // a WhatsApp identity, on Instagram
		{identity.WhatsApp, selfIGName, false}, // an Instagram identity, on WhatsApp
		{identity.Instagram, "96170000001", false},
	} {
		raw := liveText(c.platform, "scope-"+uuid.NewString(), c.sender, "hello")
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("%s %q: %v", c.platform, c.sender, err)
		}
		_, sender := senderOf(t, ts, raw.ExternalID)
		if (sender == self.ID) != c.wantSelf {
			t.Errorf("%s sender %q: attributed to self = %v, want %v", c.platform, c.sender, sender == self.ID, c.wantSelf)
		}
	}

	// An unconfigured sender whose name collides with the self entity is
	// refused, not silently attributed to Marco.
	raw := liveText(identity.Instagram, "collide-"+uuid.NewString(), "marco", "not him")
	if err := a.ProcessLiveMessage(ctx, raw); err == nil || !strings.Contains(err.Error(), "collides with the self entity") {
		t.Errorf("colliding unconfigured sender: %v", err)
	}
}

// --- (15) + (17): cross-platform attribution and speaker labels ---

func TestSelf_CrossPlatformAttributionAndSpeakerLabels(t *testing.T) {
	ts := freshStores(t)
	ctx := context.Background()
	ex := &capturingExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Marco", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
		{Subject: strings.ToLower(selfAliasName), Predicate: "likes", Object: "coffee", ObjectType: "literal", Confidence: 0.8},
		{Subject: selfWAJID, Predicate: "likes", Object: "mate", ObjectType: "literal", Confidence: 0.8},
	}}
	a := newBootstrappedAPI(t, ts, ex)
	self := a.Self()

	// WhatsApp live and Instagram (RawMessage through ProcessLiveMessage).
	waSelf := liveText(identity.WhatsApp, "wa-"+uuid.NewString(), selfWAJID, "self:wa-live")
	waContact := liveText(identity.WhatsApp, waSelf.ConversationID, "96170000999@s.whatsapp.net", "contact:wa-live")
	igSelf := liveText(identity.Instagram, "inbox/ig_"+uuid.NewString(), selfIGName, "self:ig")
	for _, m := range []ingestion.RawMessage{waSelf, waContact, igSelf} {
		if err := a.ProcessLiveMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	flushLive(t, a)

	// WhatsApp export import: his messages carry an export label.
	exportPath := filepath.Join(t.TempDir(), "Self Import "+uuid.NewString()+".txt")
	writeExportFile(t, exportPath, selfWALabel2, "2/3/26", []string{"self:import"})
	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp: %v", err)
	}

	// Recovery: a message left in an open window by a "crashed" API.
	crashed := newBootstrappedAPI(t, ts, ex)
	recSelf := liveText(identity.WhatsApp, "rec-"+uuid.NewString(), selfWAJID, "self:recovery")
	if err := crashed.ProcessLiveMessage(ctx, recSelf); err != nil {
		t.Fatal(err)
	}
	recovered := newBootstrappedAPI(t, ts, ex)
	snap, err := recovered.SnapshotUnprocessedLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.ProcessRecoveredMessages(ctx, snap); err != nil {
		t.Fatal(err)
	}

	// Every self message is stored with sender_id = the self entity.
	var selfMsgIDs []uuid.UUID
	for _, ext := range []string{waSelf.ExternalID, igSelf.ExternalID, recSelf.ExternalID} {
		id, sender := senderOf(t, ts, ext)
		if sender != self.ID {
			t.Errorf("message %s stored with sender %v, want self %v", ext, sender, self.ID)
		}
		selfMsgIDs = append(selfMsgIDs, id)
	}
	var importSender uuid.UUID
	if err := ts.pool.QueryRow(ctx, `SELECT sender_id FROM messages WHERE raw_text = 'self:import'`).Scan(&importSender); err != nil || importSender != self.ID {
		t.Errorf("import self message sender = %v (%v), want self %v", importSender, err, self.ID)
	}
	if _, contactSender := senderOf(t, ts, waContact.ExternalID); contactSender == self.ID {
		t.Error("contact message attributed to self")
	}

	// Every edge whose subject was "Marco", the alias, or his JID landed on
	// the self entity: no second "Marco" entity exists.
	rows, err := ts.pool.Query(ctx, `SELECT DISTINCT subject_id FROM edges WHERE predicate = 'likes'`)
	if err != nil {
		t.Fatal(err)
	}
	var subjects []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		_ = rows.Scan(&id)
		subjects = append(subjects, id)
	}
	rows.Close()
	if len(subjects) != 1 || subjects[0] != self.ID {
		t.Errorf("edge subjects = %v, want only the self entity %v", subjects, self.ID)
	}
	var objects []string
	_ = ts.pool.QueryRow(ctx, `SELECT array_agg(DISTINCT object_literal ORDER BY object_literal) FROM edges WHERE subject_id = $1`, self.ID).Scan(&objects)
	if strings.Join(objects, ",") != "coffee,mate,tea" {
		t.Errorf("self edges = %v, want tea/coffee/mate", objects)
	}
	for _, name := range []string{selfAliasName, selfWAJID} {
		if e, _ := ts.entities.GetByCanonicalName(ctx, name); e != nil {
			t.Errorf("a separate entity %q was created for Marco", name)
		}
	}

	// (17) Speaker labels: "Marco" for him on every path; contacts unchanged.
	sp := ex.speakers()
	for _, text := range []string{"self:wa-live", "self:ig", "self:import", "self:recovery"} {
		if sp[text] != "Marco" {
			t.Errorf("speaker for %q = %q, want \"Marco\"", text, sp[text])
		}
	}
	if sp["contact:wa-live"] != "96170000999@s.whatsapp.net" {
		t.Errorf("contact speaker = %q", sp["contact:wa-live"])
	}
}

// --- (16) corrections: honoured for self identities, not for contacts ---

func TestSelf_CorrectionGatePerPlatform(t *testing.T) {
	ts := freshStores(t)
	ctx := context.Background()

	cases := []struct {
		name, platform, sender string
		honoured               bool
	}{
		{"whatsapp self", identity.WhatsApp, selfWAJID, true},
		{"instagram self", identity.Instagram, selfIGName, true},
		{"whatsapp contact", identity.WhatsApp, "96170000777@s.whatsapp.net", false},
		{"instagram contact", identity.Instagram, "Some Contact", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			subject, _, err := ts.entities.GetOrCreateByCanonicalName(ctx, "Birthday Person "+uuid.NewString(), "person")
			if err != nil {
				t.Fatal(err)
			}
			april := "April"
			old := &memory.Edge{SubjectID: subject.ID, Predicate: "has_birthday", ObjectLiteral: &april, Confidence: 0.7, Importance: 0.5,
				SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
			if err := ts.edges.Create(ctx, old); err != nil {
				t.Fatal(err)
			}
			idx := 0
			ex := &capturingExtractor{triples: []extraction.ExtractedTriple{{
				Subject: subject.CanonicalName, Predicate: "has_birthday", Object: "March", ObjectType: "literal",
				Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &april,
			}}}
			a := newBootstrappedAPI(t, ts, ex)
			if err := a.ProcessLiveMessage(ctx, liveText(c.platform, "corr-"+uuid.NewString(), c.sender, "actually it's March")); err != nil {
				t.Fatal(err)
			}
			flushLive(t, a)

			var supersededBy *uuid.UUID
			if err := ts.pool.QueryRow(ctx, `SELECT superseded_by FROM edges WHERE id = $1`, old.ID).Scan(&supersededBy); err != nil {
				t.Fatal(err)
			}
			if got := supersededBy != nil; got != c.honoured {
				t.Errorf("old edge superseded = %v, want %v", got, c.honoured)
			}
			var march, corrections int
			_ = ts.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE is_correction) FROM edges WHERE subject_id = $1 AND object_literal = 'March'`, subject.ID).Scan(&march, &corrections)
			if march != 1 {
				t.Errorf("March edges = %d, want 1", march)
			}
			if !c.honoured && corrections != 0 {
				t.Errorf("a contact's correction was stored as a correction")
			}
		})
	}
}
