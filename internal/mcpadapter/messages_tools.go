package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) handleMessageSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeMessageWrite); deny != nil {
		return deny, nil
	}
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	if substrate == "" {
		return toolError("validation_error", "substrate is required"), nil
	}
	kind := messaging.Kind(strings.TrimSpace(req.GetString("kind", "")))
	if _, ok := validMCPMessageKinds[kind]; !ok {
		return toolError("validation_error", "invalid message kind"), nil
	}
	fromRaw := strings.TrimSpace(req.GetString("from", ""))
	toRaw := strings.TrimSpace(req.GetString("to", ""))
	from, ok := parseURNOK(fromRaw)
	if !ok {
		return toolError("validation_error", "invalid from URN"), nil
	}
	to, ok := parseURNOK(toRaw)
	if !ok {
		return toolError("validation_error", "invalid to URN"), nil
	}
	payloadJSON := strings.TrimSpace(req.GetString("payload_json", ""))
	if payloadJSON == "" {
		payloadJSON = "null"
	}
	if !json.Valid([]byte(payloadJSON)) {
		return toolError("validation_error", "payload_json must be valid JSON"), nil
	}
	metadataJSON := strings.TrimSpace(req.GetString("metadata_json", ""))
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	metadata, ok := parseMetadataJSONMap(metadataJSON)
	if !ok {
		return toolError("validation_error", "metadata_json must be a JSON object"), nil
	}
	threadID := strings.TrimSpace(req.GetString("thread_id", ""))
	inReplyTo := strings.TrimSpace(req.GetString("in_reply_to", ""))
	correlationID := firstNonEmpty(metadata["correlation_id"], threadID, inReplyTo)
	messageID := fmt.Sprintf("msg-%s-%04d", time.Now().UTC().Format("20060102-150405"), messageSeqAdd())
	rec := persistence.MessageRecord{
		ID:            messageID,
		Substrate:     substrate,
		Kind:          string(kind),
		Channel:       strings.TrimSpace(req.GetString("channel", "")),
		FromURN:       from.URN(),
		ToURN:         to.URN(),
		ThreadID:      threadID,
		InReplyTo:     inReplyTo,
		CorrelationID: correlationID,
		PayloadJSON:   payloadJSON,
		ContentType:   defaultString(strings.TrimSpace(req.GetString("content_type", "")), "application/json"),
		MetadataJSON:  metadataJSON,
		CreatedAt:     time.Now().UTC(),
	}
	if createErr := a.store.CreateMessage(ctx, rec); createErr != nil {
		return toolError("internal_error", createErr.Error()), nil
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(env), nil
}

func (a *Adapter) handleMessagesInbox(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	toURN := strings.TrimSpace(req.GetString("to", ""))
	if substrate == "" || toURN == "" {
		return toolError("validation_error", "substrate and to are required"), nil
	}
	if _, ok := parseURNOK(toURN); !ok {
		return toolError("validation_error", "invalid to URN"), nil
	}
	limit := int(req.GetFloat("limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByRecipient(ctx, substrate, toURN, strings.TrimSpace(req.GetString("correlation_id", "")), limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		items = append(items, env)
	}
	return toolJSON(map[string]any{"messages": items, "count": len(items)}), nil
}

func (a *Adapter) handleMessagesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	toURN := strings.TrimSpace(req.GetString("to", ""))
	if substrate == "" || toURN == "" {
		return toolError("validation_error", "substrate and to are required"), nil
	}
	if _, ok := parseURNOK(toURN); !ok {
		return toolError("validation_error", "invalid to URN"), nil
	}
	limit := int(req.GetFloat("limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByRecipientNonDestructive(ctx, substrate, toURN, strings.TrimSpace(req.GetString("correlation_id", "")), limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		items = append(items, env)
	}
	return toolJSON(map[string]any{"messages": items, "count": len(items)}), nil
}

func (a *Adapter) handleMessagesThread(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	threadID := strings.TrimSpace(req.GetString("thread_id", ""))
	if substrate == "" || threadID == "" {
		return toolError("validation_error", "substrate and thread_id are required"), nil
	}
	limit := int(req.GetFloat("limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByThread(ctx, substrate, threadID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		items = append(items, env)
	}
	return toolJSON(map[string]any{"messages": items, "count": len(items)}), nil
}

func (a *Adapter) handleMessageGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	messageID := strings.TrimSpace(req.GetString("message_id", ""))
	if messageID == "" {
		return toolError("validation_error", "message_id is required"), nil
	}
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	if substrate != "" {
		rec, err := a.store.GetMessage(ctx, messageID)
		if err == nil && rec.Substrate != substrate {
			return toolError("not_found", "message not found"), nil
		}
		if err != nil {
			if isNotFound(err) {
				return toolError("not_found", "message not found"), nil
			}
			return toolError("internal_error", err.Error()), nil
		}
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		return toolJSON(env), nil
	}
	rec, err := a.store.GetMessage(ctx, messageID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "message not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(env), nil
}

func (a *Adapter) handleMessageConsume(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeMessageWrite); deny != nil {
		return deny, nil
	}
	messageID := strings.TrimSpace(req.GetString("message_id", ""))
	if messageID == "" {
		return toolError("validation_error", "message_id is required"), nil
	}
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	if substrate != "" {
		rec, err := a.store.GetMessage(ctx, messageID)
		if err == nil && rec.Substrate != substrate {
			return toolError("not_found", "message not found"), nil
		}
		if err != nil {
			if isNotFound(err) {
				return toolError("not_found", "message not found"), nil
			}
			return toolError("internal_error", err.Error()), nil
		}
	}
	if err := a.store.ConsumeMessage(ctx, messageID, time.Now().UTC()); err != nil {
		if isNotFound(err) {
			return toolError("not_found", "message not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	rec, err := a.store.GetMessage(ctx, messageID)
	if err != nil {
		if isNotFound(err) {
			return toolJSON(map[string]any{"message_id": messageID, "status": "consumed"}), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(env), nil
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
