# Message Workflows

For local agent-to-agent workflows:

- `hadron_message_send` stores an envelope
- `hadron_messages_inbox` destructively reads a recipient inbox
- `hadron_messages_list` is the non-destructive list surface
- `hadron_messages_thread` loads a thread or correlation group
- `hadron_message_get` and `hadron_message_consume` target a single message

Prefer recipient and thread based reads over id-only polling when the workflow already has a stable thread or correlation id.
