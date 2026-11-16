# Identity: who sent a message, and which entity is Marco

How Pegasus decides that a message came from Marco, and what Instagram's
export does and doesn't let us know about senders. Configuration is in the
README ("Self identity"); this file explains the reasoning and the risks.

## The rule

A message's sender is Marco when `(platform, sender_external_id)` matches a
configured self identity for **that platform**:

| Platform | `sender_external_id` | Configured as | Comparison |
|---|---|---|---|
| WhatsApp (live) | the sender JID, canonicalized by the live source (no device suffix; `@lid` mapped to the phone-number JID when known) | `PEGASUS_SELF_WHATSAPP_IDS`: phone number or JID | exact, after the same canonicalization |
| WhatsApp (`.txt` export) | the label the export prints before each message | `PEGASUS_SELF_WHATSAPP_IDS`: that label | exact, after NFC + trim |
| Instagram (export) | `sender_name`, mojibake-repaired | `PEGASUS_SELF_INSTAGRAM_NAMES` | exact, after NFC + trim |

A match resolves to the single `is_self` entity created at startup
(`API.BootstrapSelf`). Anything else resolves to a contact entity named after
the sender ID. Separately, an extracted subject or object equal to the self
name, an alias, or any configured identity string (case-insensitive)
resolves to Marco.

**WhatsApp export label, checked:** in two real exports (kept outside the
repo), every one of Marco's own messages is labelled `Marco`: his WhatsApp
profile name, not `You` and not a number. Configure the label exactly as it
appears in the export. If he changes his profile name, newer exports use the
new one, so add it too.

## Instagram: is `sender_external_id` stable? (addendum §A2.5)

**Short answer: no.** Treat it as a display name that can change, not an
identifier.

What the code and its fixtures show:

- **It is the display name, not a username and not a numeric ID.** The
  adapter (`internal/ingestion/instagram/export.go`) sets
  `SenderExternalID = fixMojibake(sender_name)`. The adapter's own comments
  record that, in a real export, `sender_name` held `Yasmine 🌸`, which came
  out mojibaked. A username can't contain spaces or emoji, so that field is
  the profile's display name. The addendum's "usernames" assumption doesn't
  hold for this export format. (Some test fixtures use handle-like strings
  such as `marco.antonios`; those are synthetic and don't show the real
  format.)
- **Message entries carry no stable identifier.** The schema the adapter
  decodes has `sender_name`, `timestamp_ms`, `content`, media and reaction
  fields: no user ID, no message ID. The adapter synthesizes `ExternalID` by
  hashing `(conversation, sender_name, timestamp, content)`.
- **Display names are mutable, not unique, and messy.** People rename
  themselves. Two people can share a name. Names carry emoji and, in the raw
  export, mojibake (repaired by the adapter before anything else sees it).

**Consequences:**

1. A contact who renames between two exports becomes a **second entity**:
   the old name and the new name don't match.
2. The same rename also changes the synthesized `ExternalID` of every message
   they sent. So re-importing a newer export does **not** de-duplicate their
   old messages: they are stored again under the new name.
3. Marco's own messages stop being attributed to him after he renames, until
   the old name is configured. That is why `PEGASUS_SELF_INSTAGRAM_NAMES`
   takes one entry per name he has used.

**Not yet verified against two real exports.** The conclusion above comes
from the adapter, its comments and its fixtures, plus one real export's
`sender_name`. It has not been confirmed by comparing two exports taken at
different times. To check:

1. Download two Instagram exports (JSON) some time apart, and keep them
   outside the repo.
2. Pick a contact known to have changed their display name in between.
3. In the same thread in both, compare `sender_name` on that contact's
   messages, and compare `thread_path` (see below).
4. If `sender_name` differs, the conclusion is confirmed. If some other field
   carried a stable ID, it would be worth capturing for future cross-platform
   identity resolution (out of scope here).

### `thread_path` may be unstable too (flagged, not changed)

The conversation ID comes from `thread_path` (falling back to the title). In
the fixtures it looks like `inbox/marco_bugs_1234567890`: a slug plus a
number. If the slug follows the other participant's username or name, a
rename may also change `thread_path`, and with it `conversation_id` and
message de-duplication. This needs the same two-export check. This change
deliberately leaves the derivation alone.

## Risks of matching Instagram by display name

- **A contact who shares one of Marco's configured names**, in a 1:1 thread,
  has their messages attributed to Marco. Because only the self entity's
  messages may correct facts, that also gives them correction authority.
  Configure only names that are genuinely, distinctively his.
- **Group threads with duplicate names** are ambiguous in the same way.
  Group chats are out of scope today, but the risk carries over when they
  aren't.
- **Self-attribution is lost silently after a rename** until the new name is
  added (and old names must stay configured for old exports).
- An **unconfigured** sender whose name collides with the self entity's
  canonical name is refused with an error, never attributed to Marco.
