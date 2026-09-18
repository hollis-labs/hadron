package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/persistence"
)

func (a *Adapter) handleMessageSend(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeMessageWrite); err != nil {
		return nil, err
	}
	substrate := strings.TrimSpace(argString(args, "substrate", ""))
	if substrate == "" {
		return nil, budget.NewToolError("validation_error", "substrate is required").WithField("substrate")
	}
	kind := messaging.Kind(strings.TrimSpace(argString(args, "kind", "")))
	if _, ok := validMCPMessageKinds[kind]; !ok {
		return nil, budget.NewToolError("validation_error", "invalid message kind").WithField("kind")
	}
	fromRaw := strings.TrimSpace(argString(args, "from", ""))
	toRaw := strings.TrimSpace(argString(args, "to", ""))
	from, ok := parseURNOK(fromRaw)
	if !ok {
		return nil, budget.NewToolError("validation_error", "invalid from URN").WithField("from")
	}
	to, ok := parseURNOK(toRaw)
	if !ok {
		return nil, budget.NewToolError("validation_error", "invalid to URN").WithField("to")
	}
	payloadJSON := strings.TrimSpace(argString(args, "payload_json", ""))
	if payloadJSON == "" {
		payloadJSON = "null"
	}
	if !json.Valid([]byte(payloadJSON)) {
		return nil, budget.NewToolError("validation_error", "payload_json must be valid JSON").WithField("payload_json")
	}
	metadataJSON := strings.TrimSpace(argString(args, "metadata_json", ""))
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	metadata, ok := parseMetadataJSONMap(metadataJSON)
	if !ok {
		return nil, budget.NewToolError("validation_error", "metadata_json must be a JSON object").WithField("metadata_json")
	}
	threadID := strings.TrimSpace(argString(args, "thread_id", ""))
	inReplyTo := strings.TrimSpace(argString(args, "in_reply_to", ""))
	correlationID := firstNonEmpty(metadata["correlation_id"], threadID, inReplyTo)
	messageID := fmt.Sprintf("msg-%s-%04d", time.Now().UTC().Format("20060102-150405"), messageSeqAdd())
	rec := persistence.MessageRecord{
		ID:            messageID,
		Substrate:     substrate,
		Kind:          string(kind),
		Channel:       strings.TrimSpace(argString(args, "channel", "")),
		FromURN:       from.URN(),
		ToURN:         to.URN(),
		ThreadID:      threadID,
		InReplyTo:     inReplyTo,
		CorrelationID: correlationID,
		PayloadJSON:   payloadJSON,
		ContentType:   defaultString(strings.TrimSpace(argString(args, "content_type", "")), "application/json"),
		MetadataJSON:  metadataJSON,
		CreatedAt:     time.Now().UTC(),
	}
	if createErr := a.store.CreateMessage(ctx, rec); createErr != nil {
		return nil, budget.NewToolError("internal_error", createErr.Error())
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return env, nil
}

func (a *Adapter) handleMessagesInbox(ctx context.Context, args map[string]any) (any, error) {
	substrate := strings.TrimSpace(argString(args, "substrate", ""))
	toURN := strings.TrimSpace(argString(args, "to", ""))
	if substrate == "" || toURN == "" {
		return nil, budget.NewToolError("validation_error", "substrate and to are required")
	}
	if _, ok := parseURNOK(toURN); !ok {
		return nil, budget.NewToolError("validation_error", "invalid to URN").WithField("to")
	}
	limit := int(argFloat(args, "limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByRecipient(ctx, substrate, toURN, strings.TrimSpace(argString(args, "correlation_id", "")), limit)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return nil, budget.NewToolError("internal_error", err.Error())
		}
		items = append(items, env)
	}
	return map[string]any{"messages": items, "count": len(items)}, nil
}

func (a *Adapter) handleMessagesList(ctx context.Context, args map[string]any) (any, error) {
	substrate := strings.TrimSpace(argString(args, "substrate", ""))
	toURN := strings.TrimSpace(argString(args, "to", ""))
	if substrate == "" || toURN == "" {
		return nil, budget.NewToolError("validation_error", "substrate and to are required")
	}
	if _, ok := parseURNOK(toURN); !ok {
		return nil, budget.NewToolError("validation_error", "invalid to URN").WithField("to")
	}
	limit := int(argFloat(args, "limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByRecipientNonDestructive(ctx, substrate, toURN, strings.TrimSpace(argString(args, "correlation_id", "")), limit)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return nil, budget.NewToolError("internal_error", err.Error())
		}
		items = append(items, env)
	}
	return map[string]any{"messages": items, "count": len(items)}, nil
}

func (a *Adapter) handleMessagesThread(ctx context.Context, args map[string]any) (any, error) {
	substrate := strings.TrimSpace(argString(args, "substrate", ""))
	threadID := strings.TrimSpace(argString(args, "thread_id", ""))
	if substrate == "" || threadID == "" {
		return nil, budget.NewToolError("validation_error", "substrate and thread_id are required")
	}
	limit := int(argFloat(args, "limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByThread(ctx, substrate, threadID, limit)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return nil, budget.NewToolError("internal_error", err.Error())
		}
		items = append(items, env)
	}
	return map[string]any{"messages": items, "count": len(items)}, nil
}

func (a *Adapter) handleMessageGet(ctx context.Context, args map[string]any) (any, error) {
	messageID := strings.TrimSpace(argString(args, "message_id", ""))
	if messageID == "" {
		return nil, budget.NewToolError("validation_error", "message_id is required").WithField("message_id")
	}
	substrate := strings.TrimSpace(argString(args, "substrate", ""))
	if substrate != "" {
		rec, err := a.store.GetMessage(ctx, messageID)
		if err == nil && rec.Substrate != substrate {
			return nil, budget.NewToolError("not_found", "message not found").WithField("message_id")
		}
		if err != nil {
			if isNotFound(err) {
				return nil, budget.NewToolError("not_found", "message not found").WithField("message_id")
			}
			return nil, budget.NewToolError("internal_error", err.Error())
		}
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return nil, budget.NewToolError("internal_error", err.Error())
		}
		return env, nil
	}
	rec, err := a.store.GetMessage(ctx, messageID)
	if err != nil {
		if isNotFound(err) {
			return nil, budget.NewToolError("not_found", "message not found").WithField("message_id")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return env, nil
}

func (a *Adapter) handleMessageConsume(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeMessageWrite); err != nil {
		return nil, err
	}
	messageID := strings.TrimSpace(argString(args, "message_id", ""))
	if messageID == "" {
		return nil, budget.NewToolError("validation_error", "message_id is required").WithField("message_id")
	}
	substrate := strings.TrimSpace(argString(args, "substrate", ""))
	if substrate != "" {
		rec, err := a.store.GetMessage(ctx, messageID)
		if err == nil && rec.Substrate != substrate {
			return nil, budget.NewToolError("not_found", "message not found").WithField("message_id")
		}
		if err != nil {
			if isNotFound(err) {
				return nil, budget.NewToolError("not_found", "message not found").WithField("message_id")
			}
			return nil, budget.NewToolError("internal_error", err.Error())
		}
	}
	if err := a.store.ConsumeMessage(ctx, messageID, time.Now().UTC()); err != nil {
		if isNotFound(err) {
			return nil, budget.NewToolError("not_found", "message not found").WithField("message_id")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	rec, err := a.store.GetMessage(ctx, messageID)
	if err != nil {
		if isNotFound(err) {
			return map[string]any{"message_id": messageID, "status": "consumed"}, nil
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return env, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

var validMCPMessageKinds = map[messaging.Kind]struct{}{
	messaging.MsgKindRequest:      {},
	messaging.MsgKindResponse:     {},
	messaging.MsgKindNotice:       {},
	messaging.MsgKindStatusUpdate: {},
	messaging.MsgKindHandoff:      {},
	messaging.MsgKindEscalation:   {},
}

func messageSeqAdd() uint64 {
	return atomic.AddUint64(&messageSeq, 1)
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseURNOK(raw string) (messaging.Address, bool) {
	addr, err := messaging.ParseURN(raw)
	if err != nil {
		return messaging.Address{}, false
	}
	return addr, true
}

func parseMetadataJSONMap(raw string) (map[string]string, bool) {
	metadata := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return nil, false
	}
	return metadata, true
}
