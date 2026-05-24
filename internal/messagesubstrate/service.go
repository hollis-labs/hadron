package messagesubstrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
)

const kindGoMessaging = "go_messaging"

type Store interface {
	CreateMessage(ctx context.Context, rec persistence.MessageRecord) error
	GetMessage(ctx context.Context, id string) (persistence.MessageRecord, error)
	ListMessagesByRecipient(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]persistence.MessageRecord, error)
	ListMessagesByRecipientNonDestructive(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]persistence.MessageRecord, error)
	ListMessagesByThread(ctx context.Context, substrate, threadID string, limit int) ([]persistence.MessageRecord, error)
	ClaimNextMessage(ctx context.Context, substrate, toURN, correlationID string, claimedAt time.Time) (persistence.MessageRecord, error)
	ConsumeMessage(ctx context.Context, id string, consumedAt time.Time) error
}

type Service struct {
	store      Store
	substrates map[string]settings.MessageSubstrateSetting
	seq        atomic.Uint64
}

func New(store Store, substrates map[string]settings.MessageSubstrateSetting) *Service {
	cloned := make(map[string]settings.MessageSubstrateSetting, len(substrates))
	for name, cfg := range substrates {
		cloned[name] = cfg
	}
	return &Service{store: store, substrates: cloned}
}

func (s *Service) PollMessage(ctx context.Context, query execution.MessageQuery) (*execution.Message, error) {
	cfg, err := s.requireSubstrate(query.Substrate)
	if err != nil {
		return nil, err
	}
	switch cfg.Kind {
	case kindGoMessaging:
		if err := validateRecipientURN(query.To, cfg); err != nil {
			return nil, err
		}
		rec, err := s.store.ClaimNextMessage(ctx, query.Substrate, query.To, query.CorrelationID, time.Now().UTC())
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			return nil, err
		}
		return toExecutionMessage(rec)
	case kindGoMessagingHTTP, kindTetherHTTP:
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return nil, err
		}
		return client.PollMessage(ctx, query)
	default:
		return nil, fmt.Errorf("message substrate %q kind %q is not supported", query.Substrate, cfg.Kind)
	}
}

func (s *Service) Send(ctx context.Context, substrate string, env messaging.Envelope) (messaging.Envelope, error) {
	cfg, err := s.requireSubstrate(substrate)
	if err != nil {
		return messaging.Envelope{}, err
	}
	if env.Kind == "" {
		env.Kind = messaging.MsgKindNotice
	}
	if env.ContentType == "" {
		env.ContentType = "application/json"
	}
	if env.Metadata == nil {
		env.Metadata = map[string]string{}
	}
	switch cfg.Kind {
	case kindGoMessaging:
		if err := validateAddress(env.From, cfg); err != nil {
			return messaging.Envelope{}, fmt.Errorf("invalid from address: %w", err)
		}
		if err := validateAddress(env.To, cfg); err != nil {
			return messaging.Envelope{}, fmt.Errorf("invalid to address: %w", err)
		}
		id := s.nextMessageID()
		now := time.Now().UTC()
		correlationID := firstNonEmpty(env.Metadata["correlation_id"], env.ThreadID, env.InReplyTo)
		rec := persistence.MessageRecord{
			ID:            id,
			Substrate:     substrate,
			Kind:          string(env.Kind),
			Channel:       string(env.Channel),
			FromURN:       env.From.URN(),
			ToURN:         env.To.URN(),
			ThreadID:      env.ThreadID,
			InReplyTo:     env.InReplyTo,
			CorrelationID: correlationID,
			PayloadJSON:   payloadJSON(env.Payload),
			ContentType:   env.ContentType,
			MetadataJSON:  metadataJSON(env.Metadata),
			CreatedAt:     now,
		}
		if err := s.store.CreateMessage(ctx, rec); err != nil {
			return messaging.Envelope{}, err
		}
		return toEnvelope(rec)
	case kindGoMessagingHTTP, kindTetherHTTP:
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return messaging.Envelope{}, err
		}
		return client.Send(ctx, env)
	default:
		return messaging.Envelope{}, fmt.Errorf("message substrate %q kind %q is not supported", substrate, cfg.Kind)
	}
}

func (s *Service) Get(ctx context.Context, substrate, id string) (messaging.Envelope, error) {
	if strings.TrimSpace(substrate) == "" && s.store != nil {
		rec, err := s.store.GetMessage(ctx, id)
		if err == nil {
			return toEnvelope(rec)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return messaging.Envelope{}, err
		}
	}
	for name, cfg := range s.substrates {
		if substrate != "" && name != substrate {
			continue
		}
		if cfg.Kind != kindGoMessagingHTTP && cfg.Kind != kindTetherHTTP {
			continue
		}
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return messaging.Envelope{}, err
		}
		env, err := client.Get(ctx, id)
		if err == nil {
			return env, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return messaging.Envelope{}, err
		}
	}
	return messaging.Envelope{}, sql.ErrNoRows
}

func (s *Service) Inbox(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]messaging.Envelope, error) {
	cfg, err := s.requireSubstrate(substrate)
	if err != nil {
		return nil, err
	}
	switch cfg.Kind {
	case kindGoMessaging:
		if err := validateRecipientURN(toURN, cfg); err != nil {
			return nil, err
		}
		recs, err := s.store.ListMessagesByRecipient(ctx, substrate, toURN, correlationID, limit)
		if err != nil {
			return nil, err
		}
		out := make([]messaging.Envelope, 0, len(recs))
		for _, rec := range recs {
			env, err := toEnvelope(rec)
			if err != nil {
				return nil, err
			}
			out = append(out, env)
		}
		return out, nil
	case kindGoMessagingHTTP, kindTetherHTTP:
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return nil, err
		}
		to, err := messaging.ParseURN(toURN)
		if err != nil {
			return nil, err
		}
		return client.Inbox(ctx, to, correlationID, limit)
	default:
		return nil, fmt.Errorf("message substrate %q kind %q is not supported", substrate, cfg.Kind)
	}
}

func (s *Service) List(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]messaging.Envelope, error) {
	cfg, err := s.requireSubstrate(substrate)
	if err != nil {
		return nil, err
	}
	switch cfg.Kind {
	case kindGoMessaging:
		if err := validateRecipientURN(toURN, cfg); err != nil {
			return nil, err
		}
		recs, err := s.store.ListMessagesByRecipientNonDestructive(ctx, substrate, toURN, correlationID, limit)
		if err != nil {
			return nil, err
		}
		out := make([]messaging.Envelope, 0, len(recs))
		for _, rec := range recs {
			env, err := toEnvelope(rec)
			if err != nil {
				return nil, err
			}
			out = append(out, env)
		}
		return out, nil
	case kindGoMessagingHTTP, kindTetherHTTP:
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return nil, err
		}
		to, err := messaging.ParseURN(toURN)
		if err != nil {
			return nil, err
		}
		return client.List(ctx, to, correlationID, limit)
	default:
		return nil, fmt.Errorf("message substrate %q kind %q is not supported", substrate, cfg.Kind)
	}
}

func (s *Service) Thread(ctx context.Context, substrate, threadID string, limit int) ([]messaging.Envelope, error) {
	cfg, err := s.requireSubstrate(substrate)
	if err != nil {
		return nil, err
	}
	switch cfg.Kind {
	case kindGoMessaging:
		recs, err := s.store.ListMessagesByThread(ctx, substrate, threadID, limit)
		if err != nil {
			return nil, err
		}
		out := make([]messaging.Envelope, 0, len(recs))
		for _, rec := range recs {
			env, err := toEnvelope(rec)
			if err != nil {
				return nil, err
			}
			out = append(out, env)
		}
		return out, nil
	case kindGoMessagingHTTP, kindTetherHTTP:
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return nil, err
		}
		return client.Thread(ctx, threadID, limit)
	default:
		return nil, fmt.Errorf("message substrate %q kind %q is not supported", substrate, cfg.Kind)
	}
}

func (s *Service) Consume(ctx context.Context, substrate, id string) error {
	if strings.TrimSpace(substrate) == "" && s.store != nil {
		if err := s.store.ConsumeMessage(ctx, id, time.Now().UTC()); err == nil {
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	for name, cfg := range s.substrates {
		if substrate != "" && name != substrate {
			continue
		}
		if cfg.Kind != kindGoMessagingHTTP && cfg.Kind != kindTetherHTTP {
			continue
		}
		client, err := newRemoteMessageClient(cfg)
		if err != nil {
			return err
		}
		err = client.Consume(ctx, id)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	return sql.ErrNoRows
}

func (s *Service) requireSubstrate(name string) (settings.MessageSubstrateSetting, error) {
	cfg, ok := s.substrates[name]
	if !ok {
		return settings.MessageSubstrateSetting{}, fmt.Errorf("message substrate %q is not configured", name)
	}
	return cfg, nil
}

func validateAddress(addr messaging.Address, cfg settings.MessageSubstrateSetting) error {
	if addr.IsZero() {
		return fmt.Errorf("address is required")
	}
	return validateRecipientURN(addr.URN(), cfg)
}

func validateRecipientURN(urn string, cfg settings.MessageSubstrateSetting) error {
	addr, err := messaging.ParseURN(urn)
	if err != nil {
		return err
	}
	if cfg.Authority != "" && addr.Authority != cfg.Authority {
		return fmt.Errorf("address authority %q does not match substrate authority %q", addr.Authority, cfg.Authority)
	}
	return nil
}

func toExecutionMessage(rec persistence.MessageRecord) (*execution.Message, error) {
	env, err := toEnvelope(rec)
	if err != nil {
		return nil, err
	}
	return envelopeToExecutionMessage(env)
}

func toEnvelope(rec persistence.MessageRecord) (messaging.Envelope, error) {
	from, err := messaging.ParseURN(rec.FromURN)
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("parse from URN: %w", err)
	}
	to, err := messaging.ParseURN(rec.ToURN)
	if err != nil {
		return messaging.Envelope{}, fmt.Errorf("parse to URN: %w", err)
	}
	metadata := map[string]string{}
	if strings.TrimSpace(rec.MetadataJSON) != "" {
		if err := json.Unmarshal([]byte(rec.MetadataJSON), &metadata); err != nil {
			return messaging.Envelope{}, fmt.Errorf("parse metadata json: %w", err)
		}
	}
	env := messaging.Envelope{
		ID:          rec.ID,
		Kind:        messaging.Kind(rec.Kind),
		Channel:     messaging.Channel(rec.Channel),
		From:        from,
		To:          to,
		ThreadID:    rec.ThreadID,
		InReplyTo:   rec.InReplyTo,
		Payload:     json.RawMessage(rec.PayloadJSON),
		ContentType: rec.ContentType,
		Metadata:    metadata,
		CreatedAt:   rec.CreatedAt,
	}
	if rec.DeliveredAt.Valid {
		tm, err := time.Parse(time.RFC3339Nano, rec.DeliveredAt.String)
		if err != nil {
			return messaging.Envelope{}, fmt.Errorf("parse delivered_at: %w", err)
		}
		env.DeliveredAt = &tm
	}
	if rec.ConsumedAt.Valid {
		tm, err := time.Parse(time.RFC3339Nano, rec.ConsumedAt.String)
		if err != nil {
			return messaging.Envelope{}, fmt.Errorf("parse consumed_at: %w", err)
		}
		env.ConsumedAt = &tm
	}
	return env, nil
}

func payloadJSON(payload json.RawMessage) string {
	if len(payload) == 0 {
		return "null"
	}
	return string(payload)
}

func metadataJSON(metadata map[string]string) string {
	if metadata == nil {
		return "{}"
	}
	b, err := json.Marshal(metadata)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func (s *Service) nextMessageID() string {
	n := s.seq.Add(1)
	return fmt.Sprintf("msg-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}
