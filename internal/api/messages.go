package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/hollis-labs/go-messaging"
)

var validMessageKinds = map[messaging.Kind]struct{}{
	messaging.MsgKindRequest:      {},
	messaging.MsgKindResponse:     {},
	messaging.MsgKindNotice:       {},
	messaging.MsgKindStatusUpdate: {},
	messaging.MsgKindHandoff:      {},
	messaging.MsgKindEscalation:   {},
}

func (s *Server) handleMessagesCollection(w http.ResponseWriter, r *http.Request) {
	if s.deps.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages unavailable")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		Substrate   string            `json:"substrate"`
		Kind        messaging.Kind    `json:"kind"`
		Channel     messaging.Channel `json:"channel,omitempty"`
		From        messaging.Address `json:"from"`
		To          messaging.Address `json:"to"`
		ThreadID    string            `json:"thread_id,omitempty"`
		InReplyTo   string            `json:"in_reply_to,omitempty"`
		Payload     json.RawMessage   `json:"payload,omitempty"`
		ContentType string            `json:"content_type,omitempty"`
		Metadata    map[string]string `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if strings.TrimSpace(body.Substrate) == "" {
		writeError(w, http.StatusBadRequest, "substrate is required")
		return
	}
	if _, ok := validMessageKinds[body.Kind]; !ok {
		writeError(w, http.StatusBadRequest, "invalid message kind")
		return
	}
	env, err := s.deps.Messages.Send(r.Context(), body.Substrate, messaging.Envelope{
		Kind:        body.Kind,
		Channel:     body.Channel,
		From:        body.From,
		To:          body.To,
		ThreadID:    body.ThreadID,
		InReplyTo:   body.InReplyTo,
		Payload:     body.Payload,
		ContentType: body.ContentType,
		Metadata:    body.Metadata,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, env)
}

func (s *Server) handleMessagesInbox(w http.ResponseWriter, r *http.Request) {
	if s.deps.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages unavailable")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	substrate := strings.TrimSpace(q.Get("substrate"))
	toURN := strings.TrimSpace(q.Get("to"))
	if substrate == "" || toURN == "" {
		writeError(w, http.StatusBadRequest, "substrate and to are required")
		return
	}
	limit := 10
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 100 {
			n = 100
		}
		limit = n
	}
	envs, err := s.deps.Messages.Inbox(r.Context(), substrate, toURN, strings.TrimSpace(q.Get("correlation_id")), limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": envs, "count": len(envs)})
}

func (s *Server) handleMessagesList(w http.ResponseWriter, r *http.Request) {
	if s.deps.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages unavailable")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	substrate := strings.TrimSpace(q.Get("substrate"))
	toURN := strings.TrimSpace(q.Get("to"))
	if substrate == "" || toURN == "" {
		writeError(w, http.StatusBadRequest, "substrate and to are required")
		return
	}
	limit := 10
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 100 {
			n = 100
		}
		limit = n
	}
	envs, err := s.deps.Messages.List(r.Context(), substrate, toURN, strings.TrimSpace(q.Get("correlation_id")), limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": envs, "count": len(envs)})
}

func (s *Server) handleMessagesThread(w http.ResponseWriter, r *http.Request) {
	if s.deps.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages unavailable")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	threadID := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, "/v1/messages/thread/"))
	substrate := strings.TrimSpace(r.URL.Query().Get("substrate"))
	if substrate == "" || threadID == "" {
		writeError(w, http.StatusBadRequest, "substrate and thread id are required")
		return
	}
	limit := 10
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		if n > 100 {
			n = 100
		}
		limit = n
	}
	envs, err := s.deps.Messages.Thread(r.Context(), substrate, threadID, limit)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": envs, "count": len(envs)})
}

func (s *Server) handleMessageByID(w http.ResponseWriter, r *http.Request) {
	if s.deps.Messages == nil {
		writeError(w, http.StatusServiceUnavailable, "messages unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/messages/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "message id is required")
		return
	}
	id := parts[0]
	substrate := strings.TrimSpace(r.URL.Query().Get("substrate"))
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		env, err := s.deps.Messages.Get(r.Context(), substrate, id)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no rows") {
				writeError(w, http.StatusNotFound, "message not found")
				return
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, env)
		return
	}
	if len(parts) == 2 && parts[1] == "consume" {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := s.deps.Messages.Consume(r.Context(), substrate, id); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "consumed"})
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}
