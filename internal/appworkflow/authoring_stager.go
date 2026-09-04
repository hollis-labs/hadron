package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
)

var ErrAuthoringStageConflict = errors.New("workflow authoring stage conflict")

// AuthoringSourceStager is the ephemeral exact-source bridge used only while
// ordinary validation and contract qualification run. It is not a registry or
// definition authority.
type AuthoringSourceStager struct {
	mu      sync.RWMutex
	sources map[string]stagedAuthoringSource
}

type stagedAuthoringSource struct {
	source     ResolvedSource
	references uint64
}

func NewAuthoringSourceStager() *AuthoringSourceStager {
	return &AuthoringSourceStager{sources: make(map[string]stagedAuthoringSource)}
}

// Stage stores one defensively owned exact source. Replays must be identical.
func (s *AuthoringSourceStager) Stage(ctx context.Context, source ResolvedSource) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrAuthoringStageConflict)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("%w: stager is unavailable", ErrAuthoringStageConflict)
	}
	source = normalizeResolvedSourceSchema(source)
	if source.Movable || source.Definition.Digest == "" || source.Digest != source.Definition.Digest ||
		source.Digest != values.SHA256Digest(source.Bytes) || validateResolvedSourceTransport(source) != nil {
		return fmt.Errorf("%w: exact immutable source is required", ErrAuthoringStageConflict)
	}
	cloned, err := cloneResolvedSource(source)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAuthoringStageConflict, err)
	}
	key, err := exactSourceKey(source.Definition)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sources == nil {
		s.sources = make(map[string]stagedAuthoringSource)
	}
	if prior, exists := s.sources[key]; exists {
		if !equalResolvedSourceIdentity(prior.source, cloned) {
			return ErrAuthoringStageConflict
		}
		prior.references++
		s.sources[key] = prior
		return nil
	}
	s.sources[key] = stagedAuthoringSource{source: cloned, references: 1}
	return nil
}

// ResolveAuthoringSource implements the exact source resolver consumed by
// DefinitionResolver. Movable or digest-less lookup is never supported.
func (s *AuthoringSourceStager) ResolveAuthoringSource(ctx context.Context, ref graph.DefinitionRef) (ResolvedSource, error) {
	if ctx == nil || s == nil || ref.Digest == "" || ref.Version == "" {
		return ResolvedSource{}, ErrDefinitionUnresolved
	}
	if err := ctx.Err(); err != nil {
		return ResolvedSource{}, err
	}
	key, err := exactSourceKey(ref)
	if err != nil {
		return ResolvedSource{}, err
	}
	s.mu.RLock()
	staged, exists := s.sources[key]
	s.mu.RUnlock()
	if !exists || staged.source.Definition.ID != ref.ID || staged.source.Definition.Version != ref.Version || staged.source.Definition.Digest != ref.Digest {
		return ResolvedSource{}, ErrDefinitionUnresolved
	}
	source := staged.source
	source.Requested = ref
	return cloneResolvedSource(source)
}

// Remove forgets only the exact staged source after qualification finishes.
func (s *AuthoringSourceStager) Remove(ref graph.DefinitionRef) {
	if s == nil {
		return
	}
	key, err := exactSourceKey(ref)
	if err != nil {
		return
	}
	s.mu.Lock()
	staged, exists := s.sources[key]
	if exists && staged.references > 1 {
		staged.references--
		s.sources[key] = staged
	} else {
		delete(s.sources, key)
	}
	s.mu.Unlock()
}

var _ AuthoringSourceResolver = (*AuthoringSourceStager)(nil)
