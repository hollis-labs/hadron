package messagesubstrate

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/go-messaging"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/settings"
)

const (
	kindGoMessagingHTTP = "go_messaging_http"
	kindTetherHTTP      = "tether_http"
)

type remoteMessageClient struct {
	base       *url.URL
	httpClient *http.Client
	headers    map[string]string
	notifyWake bool
	authority  string
}

func newRemoteMessageClient(cfg settings.MessageSubstrateSetting) (*remoteMessageClient, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("base_url is required")
	}
	base, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base_url: %w", err)
	}
	client := &http.Client{}
	if cfg.TimeoutSeconds > 0 {
		client.Timeout = time.Duration(cfg.TimeoutSeconds) * time.Second
	}
	headers := make(map[string]string, len(cfg.Headers))
	for k, v := range cfg.Headers {
		headers[k] = v
	}
	return &remoteMessageClient{
		base:       base,
		httpClient: client,
		headers:    headers,
		notifyWake: cfg.NotifyWake,
		authority:  cfg.Authority,
	}, nil
}

func (c *remoteMessageClient) PollMessage(ctx context.Context, query execution.MessageQuery) (*execution.Message, error) {
	to, err := messaging.ParseURN(query.To)
	if err != nil {
		return nil, err
	}
	if validateErr := c.validateAddress(to); validateErr != nil {
		return nil, validateErr
	}
	envs, err := c.Inbox(ctx, to, query.CorrelationID, 1)
	if err != nil {
		return nil, err
	}
	if len(envs) == 0 {
		return nil, nil
	}
	return envelopeToExecutionMessage(envs[0])
}

func (c *remoteMessageClient) Send(ctx context.Context, env messaging.Envelope) (messaging.Envelope, error) {
	if err := c.validateAddress(env.From); err != nil {
		return messaging.Envelope{}, fmt.Errorf("invalid from address: %w", err)
	}
	if err := c.validateAddress(env.To); err != nil {
		return messaging.Envelope{}, fmt.Errorf("invalid to address: %w", err)
	}
	body, err := json.Marshal(envelopeToRemoteRequest(env))
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("marshal envelope: %w", err)
	}
	path := "messages"
	if c.notifyWake {
		path = "messages/notify"
	}
	req, err := c.newRequest(ctx, http.MethodPost, path, nil, bytes.NewReader(body))
	if err != nil {
		return messaging.Envelope{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if c.notifyWake {
		var wrapped struct {
			Message messaging.Envelope `json:"message"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&wrapped); err != nil {
			return messaging.Envelope{}, fmt.Errorf("decode notify response: %w", err)
		}
		return wrapped.Message, nil
	}
	var out messaging.Envelope
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return messaging.Envelope{}, fmt.Errorf("decode send response: %w", err)
	}
	return out, nil
}

func (c *remoteMessageClient) Get(ctx context.Context, id string) (messaging.Envelope, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "messages/"+url.PathEscape(id), nil, nil)
	if err != nil {
		return messaging.Envelope{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out messaging.Envelope
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return messaging.Envelope{}, fmt.Errorf("decode get response: %w", err)
	}
	return out, nil
}

func (c *remoteMessageClient) Inbox(ctx context.Context, to messaging.Address, correlationID string, limit int) ([]messaging.Envelope, error) {
	return c.fetchRecipientMessages(ctx, "messages/inbox", to, correlationID, limit)
}

func (c *remoteMessageClient) List(ctx context.Context, to messaging.Address, correlationID string, limit int) ([]messaging.Envelope, error) {
	return c.fetchRecipientMessages(ctx, "messages/list", to, correlationID, limit)
}

func (c *remoteMessageClient) Thread(ctx context.Context, threadID string, limit int) ([]messaging.Envelope, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	req, err := c.newRequest(ctx, http.MethodGet, "messages/thread/"+url.PathEscape(threadID), q, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Messages []messaging.Envelope `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode thread response: %w", err)
	}
	return out.Messages, nil
}

func (c *remoteMessageClient) fetchRecipientMessages(ctx context.Context, path string, to messaging.Address, correlationID string, limit int) ([]messaging.Envelope, error) {
	q := url.Values{}
	q.Set("to", to.URN())
	if strings.TrimSpace(correlationID) != "" {
		q.Set("thread_id", correlationID)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	req, err := c.newRequest(ctx, http.MethodGet, path, q, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Messages []messaging.Envelope `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode inbox response: %w", err)
	}
	return out.Messages, nil
}

func (c *remoteMessageClient) Consume(ctx context.Context, id string) error {
	env, err := c.Get(ctx, id)
	if err != nil {
		return err
	}
	q := url.Values{}
	q.Set("as", env.To.URN())
	req, err := c.newRequest(ctx, http.MethodPost, "messages/"+url.PathEscape(id)+"/consume", q, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	return nil
}

func (c *remoteMessageClient) validateAddress(addr messaging.Address) error {
	if addr.IsZero() {
		return fmt.Errorf("address is required")
	}
	if c.authority != "" && addr.Authority != c.authority {
		return fmt.Errorf("address authority %q does not match substrate authority %q", addr.Authority, c.authority)
	}
	return nil
}

func (c *remoteMessageClient) newRequest(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Request, error) {
	u := c.base.JoinPath(path)
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (c *remoteMessageClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.httpClient.Do(req) //nolint:gosec // base URL is operator-configured substrate config.
	if err != nil {
		return nil, fmt.Errorf("remote message substrate unreachable: %w", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, sql.ErrNoRows
	}
	return nil, fmt.Errorf("remote message substrate returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
}

func envelopeToRemoteRequest(env messaging.Envelope) map[string]any {
	return map[string]any{
		"kind":         env.Kind,
		"channel":      env.Channel,
		"from":         env.From.URN(),
		"to":           env.To.URN(),
		"thread_id":    env.ThreadID,
		"in_reply_to":  env.InReplyTo,
		"payload":      env.Payload,
		"content_type": env.ContentType,
		"metadata":     env.Metadata,
	}
}

func envelopeToExecutionMessage(env messaging.Envelope) (*execution.Message, error) {
	msg := &execution.Message{
		ID:   env.ID,
		Body: string(env.Payload),
	}
	if len(env.Payload) > 0 && string(env.Payload) != "null" {
		var bodyJSON any
		if err := json.Unmarshal(env.Payload, &bodyJSON); err == nil {
			msg.BodyJSON = bodyJSON
		}
	}
	return msg, nil
}
