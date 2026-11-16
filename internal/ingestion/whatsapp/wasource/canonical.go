package wasource

import (
	"context"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// canonicalizer rewrites sender/chat JIDs to one stable form per person
// before events reach the EventHandler, so resolveSender's exact-string
// match sees the same ID for the same person regardless of which device
// or addressing mode a message came through:
//
//   - device suffixes are stripped (ToNonAD): "123:7@s.whatsapp.net" ->
//     "123@s.whatsapp.net";
//   - @lid JIDs are mapped to the phone-number JID when known (the
//     message's own SenderAlt/RecipientAlt first, then the store's LID
//     map); an unmappable LID is kept as-is and counted;
//   - messages from the account itself always get the account's own
//     canonical phone-number JID as sender, which Run also adds to the
//     pipeline's SelfExternalIDs.
type canonicalizer struct {
	self  types.JID // canonical own PN JID
	pnFor func(ctx context.Context, lid types.JID) (types.JID, error)
	stats *Stats
}

func (c *canonicalizer) canonical(ctx context.Context, j, alt types.JID) types.JID {
	j = j.ToNonAD()
	if j.Server != types.HiddenUserServer {
		return j
	}
	if !alt.IsEmpty() && alt.Server == types.DefaultUserServer {
		return alt.ToNonAD()
	}
	if c.pnFor != nil {
		if pn, err := c.pnFor(ctx, j); err == nil && !pn.IsEmpty() {
			return pn.ToNonAD()
		}
	}
	c.stats.add(&c.stats.lidUnmapped)
	return j
}

// apply returns a canonicalized copy of evt (the original is shared with
// any other whatsmeow handler and must not be mutated).
func (c *canonicalizer) apply(ctx context.Context, evt *events.Message) *events.Message {
	out := *evt
	info := evt.Info

	if info.IsFromMe {
		info.Sender = c.self
	} else {
		info.Sender = c.canonical(ctx, info.Sender, info.SenderAlt)
	}

	// In a DM the chat JID is the other party: the sender when incoming,
	// the recipient when outgoing.
	chatAlt := info.SenderAlt
	if info.IsFromMe {
		chatAlt = info.RecipientAlt
	}
	info.Chat = c.canonical(ctx, info.Chat, chatAlt)

	out.Info = info
	return &out
}
