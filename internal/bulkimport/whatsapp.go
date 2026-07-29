package bulkimport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp"
)

// ImportWhatsAppDir walks dir for .txt export files, one per conversation —
// matching WhatsApp's own "Export Chat" granularity. There is no
// "export all chats" option in WhatsApp itself, so bulk-importing WhatsApp
// history means exporting each chat individually first (same flow, done
// once per chat) and pointing this at the directory they land in.
//
// conversation_id for each file is derived from its filename (with the
// .txt extension stripped), since a WhatsApp .txt export carries no
// self-describing chat ID — see whatsapp.ExportParser.Parse's own
// conversationID parameter, which this fills in per file.
func ImportWhatsAppDir(ctx context.Context, dir string, extractor WindowExtractor, sink TripleSink) (*Result, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}

	result := &Result{Errors: map[string]error{}}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}

		conversationID := strings.TrimSuffix(entry.Name(), ".txt")
		cr, err := importWhatsAppFile(ctx, filepath.Join(dir, entry.Name()), conversationID, extractor, sink)
		if err != nil {
			result.Errors[entry.Name()] = err
			continue
		}
		result.Conversations = append(result.Conversations, cr)
	}

	return result, nil
}

func importWhatsAppFile(ctx context.Context, path, conversationID string, extractor WindowExtractor, sink TripleSink) (ConversationResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return ConversationResult{}, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	raws, err := whatsapp.NewExportParser().Parse(f, conversationID)
	if err != nil {
		return ConversationResult{}, fmt.Errorf("parse: %w", err)
	}

	return processConversation(ctx, conversationID, raws, extractor, sink)
}
