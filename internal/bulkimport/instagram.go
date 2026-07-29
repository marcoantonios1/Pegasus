package bulkimport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcoantonios1/Pegasus/internal/ingestion/instagram"
)

// ImportInstagramDir walks an extracted Instagram export's
// .../messages/inbox directory for message_*.json files. Unlike WhatsApp,
// Instagram's "Download Your Information" export already bundles every
// conversation into one tree, one folder per thread — so this walks the
// whole tree in a single call rather than requiring one invocation per
// conversation.
//
// conversation_id for each file comes from that file's own thread_path —
// Instagram's JSON is self-describing (see instagram.ExportParser.Parse,
// which takes no conversationID parameter for exactly this reason, unlike
// the WhatsApp path).
//
// Known limitation: a large thread split across message_1.json,
// message_2.json, ... (Instagram's pagination) is processed as separate
// ConversationResult entries sharing the same ConversationID, one per
// page. Windowing runs independently within each page — a window never
// spans a pagination boundary, the same "signal split across a boundary"
// tradeoff already documented for windows in general (see
// extraction_review.md), just at the page level instead of the
// message-count level. Not observed in Marco's own export (single-page
// threads throughout), not stitched across pages here.
func ImportInstagramDir(ctx context.Context, dir string, extractor WindowExtractor, sink TripleSink) (*Result, error) {
	result := &Result{Errors: map[string]error{}}

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasPrefix(d.Name(), "message_") || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		cr, ferr := importInstagramFile(ctx, path, extractor, sink)
		if ferr != nil {
			result.Errors[path] = ferr
			return nil
		}
		result.Conversations = append(result.Conversations, cr)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk directory: %w", err)
	}

	return result, nil
}

func importInstagramFile(ctx context.Context, path string, extractor WindowExtractor, sink TripleSink) (ConversationResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return ConversationResult{}, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	raws, err := instagram.NewExportParser().Parse(f)
	if err != nil {
		return ConversationResult{}, fmt.Errorf("parse: %w", err)
	}

	conversationID := ""
	if len(raws) > 0 {
		conversationID = raws[0].ConversationID
	}

	return processConversation(ctx, conversationID, raws, extractor, sink)
}
