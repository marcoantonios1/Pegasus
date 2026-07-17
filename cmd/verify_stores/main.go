package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:55432/pegasus?sslmode=disable")
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	messageStore := memory.NewMessageStore(pool)

	// Entity CRUD
	e := &memory.Entity{
		Type:          "person",
		CanonicalName: "Alice",
		IsSelf:        false,
		Metadata:      map[string]any{"nickname": "Al"},
	}
	if err := entityStore.Create(ctx, e); err != nil {
		log.Fatalf("entity create: %v", err)
	}
	fmt.Printf("entity created: id=%s created_at=%s\n", e.ID, e.CreatedAt)

	got, err := entityStore.GetByID(ctx, e.ID)
	if err != nil {
		log.Fatalf("entity get: %v", err)
	}
	fmt.Printf("entity fetched: %+v\n", got)

	got.CanonicalName = "Alice Smith"
	if err := entityStore.Update(ctx, got); err != nil {
		log.Fatalf("entity update: %v", err)
	}
	fmt.Printf("entity updated: name=%s updated_at=%s\n", got.CanonicalName, got.UpdatedAt)

	// second entity for edge subject/object
	e2 := &memory.Entity{Type: "topic", CanonicalName: "Coffee"}
	if err := entityStore.Create(ctx, e2); err != nil {
		log.Fatalf("entity2 create: %v", err)
	}

	// Message (needed as edge source_message_ids reference, though no FK enforced there)
	m := &memory.Message{
		ConversationID: uuid.New(),
		SenderID:       e.ID,
		MediaType:      "text",
		RawText:        strPtr("I like coffee"),
		Processed:      false,
		Timestamp:      nowUTC(),
	}
	if err := messageStore.Create(ctx, m); err != nil {
		log.Fatalf("message create: %v", err)
	}
	fmt.Printf("message created: id=%s\n", m.ID)

	gotMsg, err := messageStore.GetByID(ctx, m.ID)
	if err != nil {
		log.Fatalf("message get: %v", err)
	}
	fmt.Printf("message fetched: %+v raw_text=%s\n", gotMsg, *gotMsg.RawText)

	gotMsg.Processed = true
	if err := messageStore.Update(ctx, gotMsg); err != nil {
		log.Fatalf("message update: %v", err)
	}
	fmt.Println("message updated: processed=true")

	// Edge CRUD with object_id set (entity->entity)
	edge1 := &memory.Edge{
		SubjectID:        e.ID,
		Predicate:        "likes",
		ObjectID:         &e2.ID,
		Confidence:       0.9,
		Importance:       0.5,
		SourceType:       "whatsapp_text",
		SourceWeight:     1.0,
		SourceMessageIDs: []uuid.UUID{m.ID},
		DecayRate:        0.01,
		DecayLocked:      false,
		IsCorrection:     false,
	}
	if err := edgeStore.Create(ctx, edge1); err != nil {
		log.Fatalf("edge1 create: %v", err)
	}
	fmt.Printf("edge1 created: id=%s first_seen=%s source_message_ids=%v\n", edge1.ID, edge1.FirstSeen, edge1.SourceMessageIDs)

	// Edge CRUD with object_literal set (entity->literal)
	edge2 := &memory.Edge{
		SubjectID:        e.ID,
		Predicate:        "likes",
		ObjectLiteral:    strPtr("hiking"),
		Confidence:       0.7,
		Importance:       0.3,
		SourceType:       "voice_transcript",
		SourceWeight:     0.8,
		SourceMessageIDs: []uuid.UUID{},
		DecayRate:        0.02,
	}
	if err := edgeStore.Create(ctx, edge2); err != nil {
		log.Fatalf("edge2 create: %v", err)
	}
	fmt.Printf("edge2 created: id=%s object_literal=%s\n", edge2.ID, *edge2.ObjectLiteral)

	gotEdge, err := edgeStore.GetByID(ctx, edge1.ID)
	if err != nil {
		log.Fatalf("edge get: %v", err)
	}
	fmt.Printf("edge1 fetched: object_id=%s source_message_ids=%v\n", gotEdge.ObjectID, gotEdge.SourceMessageIDs)

	gotEdge.Confidence = 0.95
	gotEdge.LastReinforced = nowUTC()
	if err := edgeStore.Update(ctx, gotEdge); err != nil {
		log.Fatalf("edge update: %v", err)
	}
	fmt.Println("edge1 updated: confidence=0.95")

	// GetBySubjectAndPredicate (uses the (subject_id, predicate) index)
	edges, err := edgeStore.GetBySubjectAndPredicate(ctx, e.ID, "likes")
	if err != nil {
		log.Fatalf("get by subject/predicate: %v", err)
	}
	fmt.Printf("GetBySubjectAndPredicate(%s, likes) returned %d edges\n", e.ID, len(edges))
	for _, ed := range edges {
		fmt.Printf("  edge id=%s object_id=%v object_literal=%v\n", ed.ID, ed.ObjectID, ed.ObjectLiteral)
	}

	// verify the XOR check constraint still rejects bad rows through the store
	bad := &memory.Edge{
		SubjectID:    e.ID,
		Predicate:    "bad",
		Confidence:   0.1,
		Importance:   0.1,
		SourceType:   "whatsapp_text",
		SourceWeight: 1.0,
		DecayRate:    0.01,
	}
	if err := edgeStore.Create(ctx, bad); err == nil {
		log.Fatalf("expected xor constraint violation, got none")
	} else {
		fmt.Printf("xor constraint correctly rejected edge with neither object set: %v\n", err)
	}

	// Entity delete
	if err := entityStore.Delete(ctx, e2.ID); err != nil {
		log.Fatalf("entity2 delete: %v", err)
	}
	fmt.Println("entity2 deleted")

	fmt.Println("ALL CHECKS PASSED")
}

func strPtr(s string) *string { return &s }

func nowUTC() time.Time { return time.Now().UTC() }
