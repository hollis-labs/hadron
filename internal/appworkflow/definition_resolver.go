package appworkflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
	"gopkg.in/yaml.v3"
)

type definitionCacheKey struct {
	source   string
	semantic string
}

type definitionFlight struct {
	done chan struct{}
	plan *compile.ExecutionPlan
	err  error
}

type planVariants map[string]*compile.ExecutionPlan

type frozenNodeExpander struct {
	name     string
	expander compile.NodeExpander
}

func (e frozenNodeExpander) Name() string { return e.name }

func (e frozenNodeExpander) ExpandNode(request compile.NodeExpansionRequest) (compile.NodeExpansion, bool, []diagnostic.Diagnostic) {
	return e.expander.ExpandNode(request)
}

// DefinitionCacheStats exposes non-semantic cache telemetry for tests and
// operations. It contains no source identity or caller information.
type DefinitionCacheStats struct {
	Plans        int
	ExactSources int
	Compilations uint64
}

// DefinitionResolver is Hadron's sole graph-native source/compiler boundary.
// Its options and collaborator semantics are immutable for its lifetime.
type DefinitionResolver struct {
	sources      definitionSourceOptions
	authorizer   DefinitionAuthorizer
	bundles      hoststate.BundledDefinitionSource
	kinds        *frozenKindLookup
	verifiers    *verification.MemoryRegistry
	policyHooks  []compile.PolicyHook
	dependencies compile.DependencyOptions
	expanders    []compile.NodeExpander
	maxCallDepth int
	semanticKey  string

	mu           sync.Mutex
	plans        map[definitionCacheKey]*compile.ExecutionPlan
	planByDigest map[string]planVariants
	exactSources map[string]ResolvedSource
	flights      map[definitionCacheKey]*definitionFlight
	compilations atomic.Uint64
}

func NewDefinitionResolver(options DefinitionResolverOptions) (*DefinitionResolver, error) {
	if nilInterface(options.Authorizer) {
		return nil, invalidDefinitionOptions("definition authorizer is required")
	}
	if strings.TrimSpace(options.Compile.SemanticRevision) == "" || !utf8.ValidString(options.Compile.SemanticRevision) {
		return nil, invalidDefinitionOptions("semantic revision is required and must be valid UTF-8")
	}
	sources, err := normalizeDefinitionSourceOptions(options)
	if err != nil {
		return nil, err
	}
	kinds, specs, err := freezeKindLookup(options.Compile.StepKinds)
	if err != nil {
		return nil, invalidDefinitionOptions(err.Error())
	}
	verifierSource := options.Compile.Verifiers
	if verifierSource == nil {
		verifierSource = verification.NewDefaultRegistry()
	} else if nilInterface(verifierSource) {
		return nil, invalidDefinitionOptions("verification registry must not be typed nil")
	}
	verifiers, err := verification.SnapshotRegistry(verifierSource)
	if err != nil {
		return nil, invalidDefinitionOptions(fmt.Sprintf("snapshot verification registry: %v", err))
	}
	verifierSpecs, err := canonicalVerifierSpecs(verifiers.List())
	if err != nil {
		return nil, invalidDefinitionOptions(fmt.Sprintf("canonical verification registry: %v", err))
	}
	maxDepth := options.Compile.MaxCallDepth
	if maxDepth == 0 {
		maxDepth = compile.DefaultMaxCallDepth
	}
	if maxDepth < 1 {
		return nil, invalidDefinitionOptions("maximum call depth must be positive")
	}
	hooks := append([]compile.PolicyHook(nil), options.Compile.PolicyHooks...)
	for index, hook := range hooks {
		if nilInterface(hook) {
			return nil, invalidDefinitionOptions(fmt.Sprintf("policy hook[%d] is nil", index))
		}
	}
	extractors := make(map[string]compile.VerificationExpressionExtractor, len(options.Compile.DependencyOptions.VerificationExtractors))
	extractorKeys := make([]string, 0, len(options.Compile.DependencyOptions.VerificationExtractors))
	for key, extractor := range options.Compile.DependencyOptions.VerificationExtractors {
		if strings.TrimSpace(key) == "" || nilInterface(extractor) {
			return nil, invalidDefinitionOptions("verification extractors require non-empty names and non-nil implementations")
		}
		extractors[key] = extractor
		extractorKeys = append(extractorKeys, key)
	}
	sort.Strings(extractorKeys)
	expanders, expanderNames, err := normalizeNodeExpanders(options.Compile.NodeExpanders)
	if err != nil {
		return nil, invalidDefinitionOptions(err.Error())
	}
	if options.BundledDefinitions != nil && nilInterface(options.BundledDefinitions) {
		return nil, invalidDefinitionOptions("bundled definition source must not be typed nil")
	}
	semanticKey, err := semanticDefinitionKey(options.Compile.SemanticRevision, maxDepth, specs, verifierSpecs, len(hooks), extractorKeys, expanderNames)
	if err != nil {
		return nil, invalidDefinitionOptions(err.Error())
	}
	return &DefinitionResolver{
		sources: sources, authorizer: options.Authorizer, bundles: options.BundledDefinitions, kinds: kinds, verifiers: verifiers,
		policyHooks: hooks, dependencies: compile.DependencyOptions{VerificationExtractors: extractors},
		expanders: expanders, maxCallDepth: maxDepth, semanticKey: semanticKey,
		plans:        make(map[definitionCacheKey]*compile.ExecutionPlan),
		planByDigest: make(map[string]planVariants),
		exactSources: make(map[string]ResolvedSource), flights: make(map[definitionCacheKey]*definitionFlight),
	}, nil
}

// Verifiers returns the resolver's frozen verifier catalog. The read-only
// interface lets Hadron Host and worker composition consume the exact same
// implementations and specs used by cached definition validation.
func (r *DefinitionResolver) Verifiers() verification.Registry {
	if r == nil {
		return nil
	}
	return r.verifiers
}

// RecoveryDependencyOptions returns the resolver's frozen dependency
// extractor catalog. The map is copied so recovery and callers cannot mutate
// the compiler semantics retained by the resolver.
func (r *DefinitionResolver) RecoveryDependencyOptions() compile.DependencyOptions {
	if r == nil {
		return compile.DependencyOptions{}
	}
	extractors := make(map[string]compile.VerificationExpressionExtractor, len(r.dependencies.VerificationExtractors))
	for name, extractor := range r.dependencies.VerificationExtractors {
		extractors[name] = extractor
	}
	return compile.DependencyOptions{VerificationExtractors: extractors}
}

// ResolveSource authorizes and resolves exact source bytes without compiling.
func (r *DefinitionResolver) ResolveSource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	requested, err := r.authorizeRequested(ctx, requested)
	if err != nil {
		return ResolvedSource{}, err
	}
	return r.resolveAuthorizedSource(ctx, requested)
}

func (r *DefinitionResolver) authorizeRequested(ctx context.Context, requested graph.DefinitionRef) (graph.DefinitionRef, error) {
	if ctx == nil {
		return graph.DefinitionRef{}, definitionError(CodeDefinitionInvalid, ErrDefinitionUnresolved, requested.Locator, "workflow definition context is required", "Supply a live request context.")
	}
	if err := ctx.Err(); err != nil {
		return graph.DefinitionRef{}, err
	}
	if r == nil || nilInterface(r.authorizer) || r.kinds == nil {
		return graph.DefinitionRef{}, definitionError(CodeDefinitionInvalid, ErrInvalidDefinitionOptions, requested.Locator, "workflow definition resolver is not initialized", "Construct the Hadron definition resolver with all required collaborators.")
	}
	if err := validateDefinitionTransport(requested); err != nil {
		return graph.DefinitionRef{}, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, err), requested.Locator, "workflow definition reference contains invalid transported text or metadata", "Use valid UTF-8 and JSON-compatible reference provenance.")
	}
	cloned, cloneErr := cloneDefinitionReference(requested)
	if cloneErr != nil {
		return graph.DefinitionRef{}, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, cloneErr), requested.Locator, "workflow definition reference is not JSON-compatible", "Use an application-neutral DefinitionRef envelope.")
	}
	requested = normalizeRequestedDefinition(cloned)
	if validationErr := validateRequestedDefinitionReference(requested); validationErr != nil {
		return graph.DefinitionRef{}, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, validationErr), requested.Locator, "workflow definition reference is invalid", "Provide one supported, unambiguous definition reference.")
	}
	if authorizationErr := r.callAuthorizer(ctx, DefinitionAuthorization{Stage: AuthorizationRequested, Requested: requested}); authorizationErr != nil {
		return graph.DefinitionRef{}, definitionError(CodeDefinitionUnauthorized, errors.Join(ErrDefinitionUnauthorized, authorizationErr), "", "workflow definition resolution is not authorized", "Use a definition allowed for the current principal, authority, and scope.")
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return graph.DefinitionRef{}, contextErr
	}
	return requested, nil
}

func (r *DefinitionResolver) resolveAuthorizedSource(ctx context.Context, requested graph.DefinitionRef) (ResolvedSource, error) {
	exactKey := ""
	if exactDefinitionReference(requested) {
		var cloneErr error
		exactKey, cloneErr = exactSourceKey(requested)
		if cloneErr != nil {
			return ResolvedSource{}, cloneErr
		}
		r.mu.Lock()
		cached, exists := r.exactSources[exactKey]
		r.mu.Unlock()
		if exists {
			if authorizationErr := r.authorizeResolved(ctx, requested, cached); authorizationErr != nil {
				return ResolvedSource{}, authorizationErr
			}
			cached.Requested = requested
			return cloneResolvedSource(cached)
		}
	}

	resolved, resolutionErr := r.resolveFreshSource(ctx, requested)
	if resolutionErr != nil {
		return ResolvedSource{}, resolutionErr
	}
	if validationErr := validateResolvedSourceTransport(resolved); validationErr != nil {
		return ResolvedSource{}, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, validationErr), requested.Locator, "resolved workflow source contains invalid transported identity or provenance", "Repair the source provider so all identities are valid UTF-8 and metadata is JSON-compatible.")
	}
	resolved, cloneErr := cloneResolvedSource(resolved)
	if cloneErr != nil {
		return ResolvedSource{}, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, cloneErr), requested.Locator, "resolved workflow source is not JSON-compatible", "Use JSON-compatible provenance metadata.")
	}
	if resolved.Digest != values.SHA256Digest(resolved.Bytes) || resolved.Definition.Digest != resolved.Digest || resolved.Definition.Provenance == nil || resolved.Definition.Provenance.Digest != resolved.Digest {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "resolved workflow identity does not match exact source bytes", "Repair the source provider so source digest and provenance are coherent.")
	}
	if exactKey != "" && resolved.Movable {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "exact workflow reference resolved as a movable source", "Repair the source provider so pinned registry versions and digests are immutable.")
	}
	if requested.Authority != "" && requested.Authority != resolved.Definition.Authority {
		return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "resolved workflow authority differs from the requested authority", "Use the authoritative source reference returned by the registry or host.")
	}
	if err := r.authorizeResolved(ctx, requested, resolved); err != nil {
		return ResolvedSource{}, err
	}
	if exactKey != "" {
		r.mu.Lock()
		if prior, exists := r.exactSources[exactKey]; exists {
			r.mu.Unlock()
			if !equalResolvedSourceIdentity(prior, resolved) {
				return ResolvedSource{}, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "exact workflow source cache conflicts with fresh resolution identity", "Do not reuse a pinned reference for different source bytes, authority, trust, or provenance.")
			}
			prior.Requested = requested
			return cloneResolvedSource(prior)
		}
		cached := resolved
		cached.Requested = graph.DefinitionRef{}
		r.exactSources[exactKey] = cached
		r.mu.Unlock()
	}
	return cloneResolvedSource(resolved)
}

func (r *DefinitionResolver) authorizeResolved(ctx context.Context, requested graph.DefinitionRef, resolved ResolvedSource) error {
	definition, err := cloneDefinitionReference(resolved.Definition)
	if err != nil {
		return err
	}
	if err := r.callAuthorizer(ctx, DefinitionAuthorization{Stage: AuthorizationResolved, Requested: requested, Resolved: &definition, TrustClass: resolved.TrustClass}); err != nil {
		return definitionError(CodeDefinitionUnauthorized, errors.Join(ErrDefinitionUnauthorized, err), "", "resolved workflow definition is not authorized", "Use a source authority and trust class allowed for the current principal and scope.")
	}
	return ctx.Err()
}

func (r *DefinitionResolver) callAuthorizer(ctx context.Context, input DefinitionAuthorization) error {
	cloned, err := cloneDefinitionAuthorization(input)
	if err != nil {
		return err
	}
	return r.authorizer.AuthorizeDefinition(ctx, cloned)
}

// ResolvePlan resolves, compiles, infers, and recursively validates one root
// definition. Full call validation is intentionally not cached because child
// references may be movable aliases.
func (r *DefinitionResolver) ResolvePlan(ctx context.Context, requested graph.DefinitionRef) (*compile.ExecutionPlan, error) {
	resolved, err := r.ResolveSource(ctx, requested)
	if err != nil {
		return nil, err
	}
	plan, err := r.localPlan(ctx, resolved)
	if err != nil {
		return nil, err
	}
	findings := compile.ValidatePlan(ctx, plan, compile.ValidationOptions{
		StepKinds: r.kinds, Verifiers: r.verifiers, PolicyHooks: r.policyHooks, Definitions: r,
		MaxCallDepth: r.maxCallDepth,
	})
	if len(findings) != 0 {
		return nil, diagnosticsError(ErrDefinitionUnresolved, findings)
	}
	cloned, err := cloneExecutionPlan(plan)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	variants := r.planByDigest[cloned.Digest]
	for _, prior := range variants {
		if !sameSemanticPlan(prior, cloned) {
			r.mu.Unlock()
			return nil, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, resolved.Definition.Locator, "plan digest collides with different compiled content", "Change the semantic revision or repair non-deterministic compiler collaborators.")
		}
	}
	variantKey, keyErr := exactPlanVariantKey(cloned)
	if keyErr != nil {
		r.mu.Unlock()
		return nil, keyErr
	}
	if variants == nil {
		variants = make(planVariants)
		r.planByDigest[cloned.Digest] = variants
	}
	if prior := variants[variantKey]; prior != nil {
		priorJSON, priorErr := json.Marshal(prior)
		currentJSON, currentErr := json.Marshal(cloned)
		if priorErr != nil || currentErr != nil || !bytes.Equal(priorJSON, currentJSON) {
			r.mu.Unlock()
			return nil, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, resolved.Definition.Locator, "plan provenance cache key collides with different content", "Repair non-deterministic compiler or provenance collaborators.")
		}
	} else {
		variants[variantKey] = cloned
	}
	r.mu.Unlock()
	return cloneExecutionPlan(cloned)
}

// ResolveDefinition implements compile.DefinitionResolver for call traversal
// and call@v1. It performs local validation; the caller's traversal owns cycle
// and depth validation. Its digest/provenance use the semantic graph digest as
// required by the call executor, while selected source digest remains in the
// graph provenance metadata.
func (r *DefinitionResolver) ResolveDefinition(ctx context.Context, requested graph.DefinitionRef) (compile.ResolvedDefinition, error) {
	requested, err := r.authorizeRequested(ctx, requested)
	if err != nil {
		return compile.ResolvedDefinition{}, err
	}
	if bundled, found, bundleErr := r.resolveAuthorizedBundle(ctx, requested); bundleErr != nil {
		return compile.ResolvedDefinition{}, bundleErr
	} else if found {
		return bundled, nil
	}
	resolved, err := r.resolveAuthorizedSource(ctx, requested)
	if err != nil {
		return compile.ResolvedDefinition{}, err
	}
	plan, err := r.localPlan(ctx, resolved)
	if err != nil {
		return compile.ResolvedDefinition{}, err
	}
	cloned, err := cloneExecutionPlan(plan)
	if err != nil {
		return compile.ResolvedDefinition{}, err
	}
	provenance := cloned.Graph.Provenance
	if provenance.Metadata == nil {
		provenance.Metadata = make(graph.Metadata)
	}
	provenance.Metadata["source_digest"] = resolved.Digest
	provenance.Digest = cloned.Graph.Digest
	cloned.Graph.Provenance = provenance
	definition := cloned.Definition
	definition.Digest = cloned.Graph.Digest
	definition.Provenance = &provenance
	return compile.ResolvedDefinition{Definition: definition, Graph: cloned.Graph}, nil
}

func (r *DefinitionResolver) resolveAuthorizedBundle(ctx context.Context, requested graph.DefinitionRef) (compile.ResolvedDefinition, bool, error) {
	if !exactDefinitionReference(requested) || requested.Digest == "" {
		return compile.ResolvedDefinition{}, false, nil
	}
	candidates, err := r.localBundledDefinitions(ctx, requested)
	if err != nil {
		return compile.ResolvedDefinition{}, false, err
	}
	if r.bundles != nil {
		persisted, sourceErr := r.bundles.FindBundledDefinitions(ctx, requested)
		if sourceErr != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return compile.ResolvedDefinition{}, false, contextErr
			}
			return compile.ResolvedDefinition{}, false, definitionError(CodeDefinitionUnresolved, errors.Join(ErrDefinitionUnresolved, sourceErr), requested.Locator, "durable bundled workflow definitions could not be inspected", "Repair the durable workflow plan journal before retrying the exact call.")
		}
		candidates = append(candidates, persisted...)
	}
	if len(candidates) == 0 {
		return compile.ResolvedDefinition{}, false, nil
	}
	normalized, err := normalizeBundledCandidates(ctx, requested, candidates)
	if err != nil {
		return compile.ResolvedDefinition{}, false, err
	}
	var authorized *compile.ResolvedDefinition
	var authorizationErr error
	for _, candidate := range normalized {
		definition, cloneErr := cloneDefinitionReference(candidate.Definition.Definition)
		if cloneErr != nil {
			return compile.ResolvedDefinition{}, false, cloneErr
		}
		if authErr := r.callAuthorizer(ctx, DefinitionAuthorization{
			Stage: AuthorizationResolved, Requested: requested, Resolved: &definition,
			Container: &candidate.Container, TrustClass: candidate.TrustClass,
		}); authErr != nil {
			authorizationErr = authErr
			continue
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return compile.ResolvedDefinition{}, false, contextErr
		}
		resolved := candidate.Definition
		if authorized == nil {
			authorized = &resolved
			continue
		}
		if !equalResolvedDefinitions(*authorized, resolved) {
			return compile.ResolvedDefinition{}, false, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, requested.Locator, "authorized bundled workflow plans disagree on one immutable definition tuple", "Repair conflicting durable plan snapshots before retrying the call.")
		}
	}
	if authorized == nil {
		return compile.ResolvedDefinition{}, false, definitionError(CodeDefinitionUnauthorized, errors.Join(ErrDefinitionUnauthorized, authorizationErr), "", "resolved bundled workflow definition is not authorized", "Use a plan and generated child allowed for the current principal, authority, and trust class.")
	}
	cloned, cloneErr := cloneResolvedDefinition(*authorized)
	if cloneErr != nil {
		return compile.ResolvedDefinition{}, false, cloneErr
	}
	return cloned, true, nil
}

func (r *DefinitionResolver) localBundledDefinitions(ctx context.Context, requested graph.DefinitionRef) ([]hoststate.BundledDefinitionCandidate, error) {
	r.mu.Lock()
	plans := make([]*compile.ExecutionPlan, 0, len(r.plans))
	for _, plan := range r.plans {
		plans = append(plans, plan)
	}
	r.mu.Unlock()
	sort.Slice(plans, func(left, right int) bool {
		leftKey, _ := exactPlanVariantKey(plans[left])
		rightKey, _ := exactPlanVariantKey(plans[right])
		return leftKey < rightKey
	})
	result := make([]hoststate.BundledDefinitionCandidate, 0)
	for _, plan := range plans {
		resolver, err := compile.NewBundledDefinitionResolver(plan)
		if err != nil {
			return nil, definitionError(CodeDefinitionPinConflict, errors.Join(ErrDefinitionPinConflict, err), requested.Locator, "cached workflow plan contains invalid bundled definitions", "Repair the deterministic source expander or change its semantic revision.")
		}
		resolved, resolveErr := resolver.ResolveDefinition(ctx, requested)
		if errors.Is(resolveErr, compile.ErrBundledDefinitionNotFound) {
			continue
		}
		if resolveErr != nil {
			return nil, resolveErr
		}
		trustClass, _ := plan.Provenance.Metadata["trust_class"].(string)
		result = append(result, hoststate.BundledDefinitionCandidate{
			Definition: resolved, Container: planReference(plan), TrustClass: trustClass,
		})
	}
	return result, nil
}

// LoadPlan returns an authorized defensive copy of a plan previously resolved
// successfully through the full root pipeline.
func (r *DefinitionResolver) LoadPlan(ctx context.Context, digest string) (*compile.ExecutionPlan, error) {
	if ctx == nil {
		return nil, definitionError(CodeDefinitionInvalid, ErrDefinitionUnresolved, "", "workflow plan context is required", "Supply a live request context.")
	}
	if err := values.ValidateDigest(digest); err != nil {
		return nil, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, err), "", "workflow plan digest is invalid", "Supply an exact SHA-256 plan digest.")
	}
	if err := r.callAuthorizer(ctx, DefinitionAuthorization{Stage: AuthorizationPlanLoad, Requested: graph.DefinitionRef{Kind: "workflow", Digest: digest}}); err != nil {
		return nil, definitionError(CodeDefinitionUnauthorized, errors.Join(ErrDefinitionUnauthorized, err), "", "workflow plan load is not authorized", "Use a plan allowed for the current principal and scope.")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	stored := r.planByDigest[digest]
	variants := make([]*compile.ExecutionPlan, 0, len(stored))
	variantKeys := make([]string, 0, len(stored))
	for key := range stored {
		variantKeys = append(variantKeys, key)
	}
	sort.Strings(variantKeys)
	for _, key := range variantKeys {
		variants = append(variants, stored[key])
	}
	r.mu.Unlock()
	if len(variants) == 0 {
		return nil, definitionError(CodeDefinitionUnresolved, ErrDefinitionUnresolved, "", "workflow plan digest is not available in the resolver cache", "Resolve the exact definition before loading its plan digest.")
	}
	authorized := make([]*compile.ExecutionPlan, 0, 1)
	for _, plan := range variants {
		cloned, cloneErr := cloneExecutionPlan(plan)
		if cloneErr != nil {
			return nil, cloneErr
		}
		trustClass, _ := cloned.Provenance.Metadata["trust_class"].(string)
		resolved := cloned.Definition
		if authorizationErr := r.callAuthorizer(ctx, DefinitionAuthorization{
			Stage: AuthorizationResolved, Requested: graph.DefinitionRef{Kind: "workflow", Digest: digest},
			Resolved: &resolved, TrustClass: trustClass,
		}); authorizationErr == nil {
			authorized = append(authorized, cloned)
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
	}
	switch len(authorized) {
	case 0:
		return nil, definitionError(CodeDefinitionUnauthorized, ErrDefinitionUnauthorized, "", "workflow plan load is not authorized for its resolved authority", "Use a plan allowed for the current principal, authority, trust class, and scope.")
	case 1:
		return authorized[0], nil
	default:
		return nil, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, "", "workflow plan digest has multiple authorized provenance variants", "Resolve and retain the exact DefinitionRef instead of loading an ambiguous digest-only plan.")
	}
}

func (r *DefinitionResolver) CacheStats() DefinitionCacheStats {
	if r == nil {
		return DefinitionCacheStats{}
	}
	r.mu.Lock()
	stats := DefinitionCacheStats{Plans: len(r.plans), ExactSources: len(r.exactSources), Compilations: r.compilations.Load()}
	r.mu.Unlock()
	return stats
}

func (r *DefinitionResolver) localPlan(ctx context.Context, source ResolvedSource) (*compile.ExecutionPlan, error) {
	contextKey, err := resolvedSourceContextKey(source)
	if err != nil {
		return nil, err
	}
	key := definitionCacheKey{source: contextKey, semantic: r.semanticKey}
	r.mu.Lock()
	if plan := r.plans[key]; plan != nil {
		r.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return cloneExecutionPlan(plan)
	}
	if flight := r.flights[key]; flight != nil {
		r.mu.Unlock()
		select {
		case <-flight.done:
			if flight.err != nil {
				return nil, flight.err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return cloneExecutionPlan(flight.plan)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &definitionFlight{done: make(chan struct{})}
	r.flights[key] = flight
	r.mu.Unlock()

	// Compilation is deterministic source work shared by authorized callers.
	// A first caller's cancellation must not poison the shared flight for other
	// callers, while every caller still observes its own cancellation below.
	plan, buildErr := r.compileLocalPlan(context.WithoutCancel(ctx), source)
	r.mu.Lock()
	if buildErr == nil {
		cached, cloneErr := cloneExecutionPlan(plan)
		if cloneErr != nil {
			buildErr = cloneErr
		} else {
			r.plans[key] = cached
			flight.plan = cached
		}
	}
	flight.err = buildErr
	delete(r.flights, key)
	close(flight.done)
	r.mu.Unlock()
	if buildErr != nil {
		return nil, buildErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cloneExecutionPlan(plan)
}

func (r *DefinitionResolver) compileLocalPlan(ctx context.Context, source ResolvedSource) (*compile.ExecutionPlan, error) {
	r.compilations.Add(1)
	loaded := compile.LoadBytes(source.Definition.Locator, source.Bytes)
	if len(loaded.Diagnostics) != 0 {
		return nil, diagnosticsError(ErrDefinitionUnresolved, loaded.Diagnostics)
	}
	if loaded.Source == nil {
		return nil, definitionError(CodeDefinitionUnresolved, ErrDefinitionUnresolved, source.Definition.Locator, "workflow source loader returned no source", "Repair the graph-native workflow source.")
	}
	if err := bindResolvedProvenance(loaded.Source, source); err != nil {
		return nil, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, err), source.Definition.Locator, "workflow source provenance could not be bound", "Use JSON-compatible host provenance and graph-native workflow syntax.")
	}
	compiled := compile.CompileWithOptions(loaded.Source, compile.CompileOptions{NodeExpanders: r.expanders})
	if len(compiled.Diagnostics) != 0 {
		return nil, diagnosticsError(ErrDefinitionUnresolved, compiled.Diagnostics)
	}
	if compiled.Plan == nil {
		return nil, definitionError(CodeDefinitionUnresolved, ErrDefinitionUnresolved, source.Definition.Locator, "workflow compiler returned no execution plan", "Repair the graph-native workflow source.")
	}
	if len(compiled.Plan.SourceDigests) != 1 || compiled.Plan.SourceDigests[0].Digest != source.Digest {
		return nil, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, source.Definition.Locator, "compiler source digest differs from resolved source bytes", "Retry with stable exact workflow source bytes.")
	}
	if source.Definition.Version != "" && compiled.Plan.Graph.Version != source.Definition.Version {
		return nil, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, source.Definition.Locator, "workflow source version differs from its pinned reference", "Select the source version declared by the workflow or update the immutable registry record.")
	}
	if source.Definition.ID != "" && compiled.Plan.Graph.ID != source.Definition.ID {
		return nil, definitionError(CodeDefinitionPinConflict, ErrDefinitionPinConflict, source.Definition.Locator, "workflow source identity differs from its resolved reference", "Select a workflow whose declared id matches the file, package, or immutable registry record.")
	}
	inferred := compile.InferValueDependencies(compiled.Plan, r.dependencies)
	if len(inferred.Diagnostics) != 0 {
		return nil, diagnosticsError(ErrDefinitionUnresolved, inferred.Diagnostics)
	}
	if inferred.Plan == nil {
		return nil, definitionError(CodeDefinitionUnresolved, ErrDefinitionUnresolved, source.Definition.Locator, "workflow dependency inference returned no execution plan", "Repair invalid value references and graph dependencies.")
	}
	plan, err := cloneExecutionPlan(inferred.Plan)
	if err != nil {
		return nil, err
	}
	plan.Definition.Locator = source.Definition.Locator
	provenance := plan.Provenance
	plan.Definition.Provenance = &provenance
	findings := compile.ValidatePlan(ctx, plan, compile.ValidationOptions{
		StepKinds: r.kinds, Verifiers: r.verifiers, MaxCallDepth: r.maxCallDepth,
	})
	if len(findings) != 0 {
		return nil, diagnosticsError(ErrDefinitionUnresolved, findings)
	}
	return plan, nil
}

func bindResolvedProvenance(source *compile.Source, resolved ResolvedSource) error {
	if source == nil || source.Document == nil || len(source.Document.Content) != 1 {
		return errors.New("loaded source document is unavailable")
	}
	root := source.Document.Content[0]
	workflowNode := mappingValue(root, "workflow")
	if workflowNode == nil || workflowNode.Kind != yaml.MappingNode {
		return errors.New("workflow header is unavailable")
	}
	provenance := resolved.Definition.Provenance
	if provenance == nil {
		return errors.New("resolved provenance is required")
	}
	// Host resolution is authoritative. Replacing the authored mapping prevents
	// omitted host fields from inheriting stale or forged revision, parent, or
	// metadata claims. Locator and digest remain compiler-derived from the exact
	// selected source and loader locator.
	provenanceNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	upsertScalar(provenanceNode, "authority", provenance.Authority)
	upsertScalar(provenanceNode, "origin", provenance.Origin)
	if provenance.Revision != "" {
		upsertScalar(provenanceNode, "revision", provenance.Revision)
	}
	if len(provenance.Parents) != 0 {
		parentsNode := &yaml.Node{}
		if err := parentsNode.Encode(provenance.Parents); err != nil {
			return fmt.Errorf("encode host provenance parents: %w", err)
		}
		upsertNode(provenanceNode, "parents", parentsNode)
	}
	if len(provenance.Metadata) != 0 {
		metadataNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		keys := make([]string, 0, len(provenance.Metadata))
		for key := range provenance.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			valueNode := &yaml.Node{}
			if err := valueNode.Encode(provenance.Metadata[key]); err != nil {
				return fmt.Errorf("encode host provenance metadata %q: %w", key, err)
			}
			upsertNode(metadataNode, key, valueNode)
		}
		upsertNode(provenanceNode, "metadata", metadataNode)
	}
	upsertNode(workflowNode, "provenance", provenanceNode)
	return nil
}

func mappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func appendMapping(mapping *yaml.Node, key string, value *yaml.Node) {
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value,
	)
}

func upsertScalar(mapping *yaml.Node, key, value string) {
	upsertNode(mapping, key, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
}

func upsertNode(mapping *yaml.Node, key string, value *yaml.Node) {
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			mapping.Content[index+1] = value
			return
		}
	}
	appendMapping(mapping, key, value)
}

func normalizeRequestedDefinition(input graph.DefinitionRef) graph.DefinitionRef {
	input.Authority = strings.TrimSpace(input.Authority)
	input.Kind = strings.TrimSpace(input.Kind)
	input.ID = strings.TrimSpace(input.ID)
	input.Locator = strings.TrimSpace(input.Locator)
	input.Version = strings.TrimSpace(input.Version)
	input.Digest = strings.TrimSpace(input.Digest)
	return input
}

func validateRequestedDefinitionReference(input graph.DefinitionRef) error {
	for _, field := range []struct{ name, value string }{
		{"authority", input.Authority}, {"kind", input.Kind}, {"id", input.ID},
		{"locator", input.Locator}, {"version", input.Version}, {"digest", input.Digest},
	} {
		if !utf8.ValidString(field.value) || containsDefinitionControl(field.value) {
			return fmt.Errorf("definition %s must be valid UTF-8 without control characters", field.name)
		}
	}
	if input.ID == "" && input.Locator == "" {
		return errors.New("definition id or locator is required")
	}
	switch input.Kind {
	case "", "workflow":
		if (input.ID == "") == (input.Locator == "") && !exactBundledDefinitionReference(input) {
			return errors.New("generic workflow reference requires exactly one of id or locator")
		}
	case DefinitionKindFile:
		if input.Locator == "" {
			return errors.New("file workflow reference requires locator")
		}
	case DefinitionKindRegistry:
		if input.ID == "" || input.Locator != "" {
			return errors.New("registry workflow reference requires id and forbids locator")
		}
	case DefinitionKindPackage:
		if input.Locator == "" {
			return errors.New("package workflow reference requires locator")
		}
	default:
		return fmt.Errorf("unsupported definition kind %q", input.Kind)
	}
	if input.Digest != "" {
		if err := values.ValidateDigest(input.Digest); err != nil {
			return err
		}
	}
	if input.Provenance != nil && input.Authority != "" && input.Provenance.Authority != "" && input.Authority != input.Provenance.Authority {
		return errors.New("definition authority differs from supplied provenance")
	}
	return nil
}

func exactBundledDefinitionReference(input graph.DefinitionRef) bool {
	return input.Kind == "workflow" && input.Authority != "" && input.ID != "" && input.Locator != "" &&
		input.Version != "" && input.Digest != "" && input.Provenance != nil && input.Provenance.Digest == input.Digest
}

func validateDefinitionTransport(input graph.DefinitionRef) error {
	for _, field := range []struct{ name, value string }{
		{"authority", input.Authority}, {"kind", input.Kind}, {"id", input.ID},
		{"locator", input.Locator}, {"version", input.Version}, {"digest", input.Digest},
	} {
		if !utf8.ValidString(field.value) || containsDefinitionControl(field.value) {
			return fmt.Errorf("definition %s must be valid UTF-8 without control characters", field.name)
		}
	}
	if input.Provenance == nil {
		return nil
	}
	provenance := input.Provenance
	for _, field := range []struct{ name, value string }{
		{"provenance authority", provenance.Authority}, {"provenance origin", provenance.Origin},
		{"provenance locator", provenance.Locator}, {"provenance revision", provenance.Revision},
		{"provenance digest", provenance.Digest},
	} {
		if !utf8.ValidString(field.value) || containsDefinitionControl(field.value) {
			return fmt.Errorf("definition %s must be valid UTF-8 without control characters", field.name)
		}
	}
	if provenance.Digest != "" {
		if err := values.ValidateDigest(provenance.Digest); err != nil {
			return fmt.Errorf("definition provenance digest: %w", err)
		}
	}
	for index, parent := range provenance.Parents {
		for _, field := range []struct{ name, value string }{
			{"authority", parent.Authority}, {"locator", parent.Locator}, {"digest", parent.Digest},
		} {
			if !utf8.ValidString(field.value) || containsDefinitionControl(field.value) {
				return fmt.Errorf("definition provenance parent[%d] %s is invalid", index, field.name)
			}
		}
		if parent.Digest != "" {
			if err := values.ValidateDigest(parent.Digest); err != nil {
				return fmt.Errorf("definition provenance parent[%d] digest: %w", index, err)
			}
		}
	}
	if provenance.Metadata != nil {
		if _, err := values.DigestInline(map[string]any(provenance.Metadata)); err != nil {
			return fmt.Errorf("definition provenance metadata: %w", err)
		}
	}
	return nil
}

func validateResolvedSourceTransport(input ResolvedSource) error {
	if err := validateDefinitionTransport(input.Requested); err != nil {
		return err
	}
	if err := validateDefinitionTransport(input.Definition); err != nil {
		return err
	}
	if !utf8.ValidString(input.TrustClass) || containsDefinitionControl(input.TrustClass) {
		return errors.New("resolved trust class must be valid UTF-8 without control characters")
	}
	return nil
}

func containsDefinitionControl(input string) bool {
	return strings.IndexFunc(input, unicode.IsControl) >= 0
}

func exactSourceKey(input graph.DefinitionRef) (string, error) {
	kind := input.Kind
	if kind == "" || kind == "workflow" {
		if input.Locator != "" {
			kind = DefinitionKindFile
		} else {
			kind = DefinitionKindRegistry
		}
	}
	encoded, err := json.Marshal(struct {
		Kind      string `json:"kind"`
		Authority string `json:"authority,omitempty"`
		ID        string `json:"id,omitempty"`
		Locator   string `json:"locator,omitempty"`
		Version   string `json:"version,omitempty"`
		Digest    string `json:"digest,omitempty"`
	}{kind, input.Authority, input.ID, input.Locator, input.Version, input.Digest})
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func exactDefinitionReference(input graph.DefinitionRef) bool {
	if input.Digest != "" {
		return true
	}
	if input.Version == "" {
		return false
	}
	switch input.Kind {
	case DefinitionKindRegistry:
		return true
	case "", "workflow":
		return input.ID != "" && input.Locator == ""
	default:
		return false
	}
}

func equalResolvedSourceIdentity(left, right ResolvedSource) bool {
	identity := func(input ResolvedSource) any {
		return struct {
			Definition graph.DefinitionRef `json:"definition"`
			Bytes      []byte              `json:"bytes"`
			Digest     string              `json:"digest"`
			TrustClass string              `json:"trust_class"`
			Movable    bool                `json:"movable"`
		}{input.Definition, input.Bytes, input.Digest, input.TrustClass, input.Movable}
	}
	leftJSON, leftErr := json.Marshal(identity(left))
	rightJSON, rightErr := json.Marshal(identity(right))
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func resolvedSourceContextKey(input ResolvedSource) (string, error) {
	encoded, err := json.Marshal(struct {
		Definition graph.DefinitionRef `json:"definition"`
		Digest     string              `json:"digest"`
		Trust      string              `json:"trust"`
	}{input.Definition, input.Digest, input.TrustClass})
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func normalizeNodeExpanders(input []compile.NodeExpander) ([]compile.NodeExpander, []string, error) {
	type named struct {
		name     string
		expander compile.NodeExpander
	}
	normalized := make([]named, 0, len(input))
	for index, expander := range input {
		if nilInterface(expander) {
			return nil, nil, fmt.Errorf("node expander[%d] is nil", index)
		}
		name := expander.Name()
		if strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) || !utf8.ValidString(name) || containsDefinitionControl(name) {
			return nil, nil, fmt.Errorf("node expander[%d] requires a stable non-empty name", index)
		}
		normalized = append(normalized, named{name: name, expander: expander})
	}
	sort.Slice(normalized, func(left, right int) bool { return normalized[left].name < normalized[right].name })
	expanders := make([]compile.NodeExpander, 0, len(normalized))
	names := make([]string, 0, len(normalized))
	for index, item := range normalized {
		if index > 0 && item.name == normalized[index-1].name {
			return nil, nil, fmt.Errorf("node expander name %q is duplicated", item.name)
		}
		expanders = append(expanders, frozenNodeExpander(item))
		names = append(names, item.name)
	}
	return expanders, names, nil
}

func normalizeBundledCandidates(ctx context.Context, requested graph.DefinitionRef, input []hoststate.BundledDefinitionCandidate) ([]hoststate.BundledDefinitionCandidate, error) {
	result := make([]hoststate.BundledDefinitionCandidate, 0, len(input))
	for index, candidate := range input {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if strings.TrimSpace(candidate.TrustClass) == "" || candidate.TrustClass != strings.TrimSpace(candidate.TrustClass) ||
			!utf8.ValidString(candidate.TrustClass) || containsDefinitionControl(candidate.TrustClass) {
			return nil, definitionError(CodeDefinitionInvalid, ErrDefinitionUnresolved, requested.Locator, fmt.Sprintf("bundled workflow candidate[%d] has invalid trust metadata", index), "Repair the durable containing plan provenance.")
		}
		if err := candidate.Container.Validate(); err != nil {
			return nil, definitionError(CodeDefinitionInvalid, errors.Join(ErrDefinitionUnresolved, err), requested.Locator, fmt.Sprintf("bundled workflow candidate[%d] has invalid container identity", index), "Repair the durable containing plan identity.")
		}
		resolver, err := compile.NewBundledDefinitionResolver(&compile.ExecutionPlan{BundledDefinitions: []compile.ResolvedDefinition{candidate.Definition}})
		if err != nil {
			return nil, definitionError(CodeDefinitionPinConflict, errors.Join(ErrDefinitionPinConflict, err), requested.Locator, fmt.Sprintf("bundled workflow candidate[%d] is invalid", index), "Repair the immutable bundled definition in the durable plan.")
		}
		resolved, err := resolver.ResolveDefinition(ctx, requested)
		if err != nil {
			return nil, definitionError(CodeDefinitionPinConflict, errors.Join(ErrDefinitionPinConflict, err), requested.Locator, fmt.Sprintf("bundled workflow candidate[%d] does not match the requested tuple", index), "Repair the durable plan bundle index.")
		}
		result = append(result, hoststate.BundledDefinitionCandidate{Definition: resolved, Container: candidate.Container, TrustClass: candidate.TrustClass})
	}
	sort.Slice(result, func(left, right int) bool {
		return bundledCandidateKey(result[left]) < bundledCandidateKey(result[right])
	})
	deduplicated := result[:0]
	for _, candidate := range result {
		if len(deduplicated) == 0 || bundledCandidateKey(deduplicated[len(deduplicated)-1]) != bundledCandidateKey(candidate) {
			deduplicated = append(deduplicated, candidate)
		}
	}
	return deduplicated, nil
}

func bundledCandidateKey(candidate hoststate.BundledDefinitionCandidate) string {
	encoded, _ := json.Marshal(candidate)
	return string(encoded)
}

func planReference(plan *compile.ExecutionPlan) runtime.PlanRef {
	return runtime.PlanRef{
		ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest,
		SchemaVersion: plan.SchemaVersion,
	}
}

func cloneResolvedDefinition(input compile.ResolvedDefinition) (compile.ResolvedDefinition, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return compile.ResolvedDefinition{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var output compile.ResolvedDefinition
	if err := decoder.Decode(&output); err != nil {
		return compile.ResolvedDefinition{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return compile.ResolvedDefinition{}, errors.New("resolved definition contains trailing JSON")
	}
	return output, nil
}

func equalResolvedDefinitions(left, right compile.ResolvedDefinition) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func semanticDefinitionKey(revision string, maxDepth int, specs []stepkind.StepKindSpec, verifierSpecs []verification.VerifierSpec, hookCount int, extractorKeys, expanderNames []string) (string, error) {
	canonicalVerifiers, err := canonicalVerifierSpecs(verifierSpecs)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(struct {
		Revision     string                      `json:"revision"`
		MaxCallDepth int                         `json:"max_call_depth"`
		StepKinds    []stepkind.StepKindSpec     `json:"step_kinds"`
		Verifiers    []verification.VerifierSpec `json:"verifiers"`
		PolicyHooks  int                         `json:"policy_hooks"`
		Extractors   []string                    `json:"verification_extractors"`
		Expanders    []string                    `json:"node_expanders"`
	}{revision, maxDepth, specs, canonicalVerifiers, hookCount, extractorKeys, expanderNames})
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func canonicalVerifierSpecs(input []verification.VerifierSpec) ([]verification.VerifierSpec, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	result := make([]verification.VerifierSpec, 0, len(input))
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	if result == nil {
		result = []verification.VerifierSpec{}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Kind == result[right].Kind {
			return result[left].Version < result[right].Version
		}
		return result[left].Kind < result[right].Kind
	})
	for index, spec := range result {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		if index > 0 && spec.Kind == result[index-1].Kind {
			return nil, fmt.Errorf("duplicate verifier spec %q", spec.Kind)
		}
	}
	return result, nil
}

func exactPlanVariantKey(plan *compile.ExecutionPlan) (string, error) {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func sameSemanticPlan(left, right *compile.ExecutionPlan) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftIdentity := struct {
		SchemaVersion string
		ID            string
		Digest        string
		Definition    struct {
			Authority string
			Kind      string
			ID        string
			Version   string
			Digest    string
		}
		GraphID      string
		GraphVersion string
		GraphDigest  string
		Sources      []compile.SourceDigest
	}{
		SchemaVersion: left.SchemaVersion, ID: left.ID, Digest: left.Digest,
		GraphID: left.Graph.ID, GraphVersion: left.Graph.Version, GraphDigest: left.Graph.Digest,
		Sources: append([]compile.SourceDigest(nil), left.SourceDigests...),
	}
	leftIdentity.Definition.Authority = left.Definition.Authority
	leftIdentity.Definition.Kind = left.Definition.Kind
	leftIdentity.Definition.ID = left.Definition.ID
	leftIdentity.Definition.Version = left.Definition.Version
	leftIdentity.Definition.Digest = left.Definition.Digest
	rightIdentity := leftIdentity
	rightIdentity.SchemaVersion = right.SchemaVersion
	rightIdentity.ID = right.ID
	rightIdentity.Digest = right.Digest
	rightIdentity.Definition.Authority = right.Definition.Authority
	rightIdentity.Definition.Kind = right.Definition.Kind
	rightIdentity.Definition.ID = right.Definition.ID
	rightIdentity.Definition.Version = right.Definition.Version
	rightIdentity.Definition.Digest = right.Definition.Digest
	rightIdentity.GraphID = right.Graph.ID
	rightIdentity.GraphVersion = right.Graph.Version
	rightIdentity.GraphDigest = right.Graph.Digest
	rightIdentity.Sources = append([]compile.SourceDigest(nil), right.SourceDigests...)
	return reflect.DeepEqual(leftIdentity, rightIdentity)
}

type frozenKindLookup struct {
	kinds map[string]stepkind.StepKind
	specs []stepkind.StepKindSpec
}

func freezeKindLookup(input compile.StepKindLookup) (*frozenKindLookup, []stepkind.StepKindSpec, error) {
	if nilInterface(input) {
		return nil, nil, errors.New("exact step-kind lookup is required")
	}
	specs := input.List()
	if len(specs) == 0 {
		return nil, nil, errors.New("at least one exact step-kind registration is required")
	}
	encoded, err := json.Marshal(specs)
	if err != nil {
		return nil, nil, fmt.Errorf("encode step-kind specs: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned []stepkind.StepKindSpec
	if err := decoder.Decode(&cloned); err != nil {
		return nil, nil, err
	}
	sort.Slice(cloned, func(left, right int) bool {
		if cloned[left].Name == cloned[right].Name {
			return cloned[left].Version < cloned[right].Version
		}
		return cloned[left].Name < cloned[right].Name
	})
	frozen := &frozenKindLookup{kinds: make(map[string]stepkind.StepKind), specs: cloned}
	for index, spec := range cloned {
		if index > 0 && spec.Name == cloned[index-1].Name && spec.Version == cloned[index-1].Version {
			return nil, nil, fmt.Errorf("duplicate step-kind spec %s@%s", spec.Name, spec.Version)
		}
		kind, exists := input.Lookup(spec.Name, spec.Version)
		if !exists || nilInterface(kind) {
			return nil, nil, fmt.Errorf("step-kind lookup/list mismatch for %s@%s", spec.Name, spec.Version)
		}
		frozen.kinds[spec.Name+"\x00"+spec.Version] = kind
	}
	return frozen, append([]stepkind.StepKindSpec(nil), cloned...), nil
}

func (f *frozenKindLookup) Lookup(name, version string) (stepkind.StepKind, bool) {
	if f == nil {
		return nil, false
	}
	kind, exists := f.kinds[name+"\x00"+version]
	return kind, exists
}

func (f *frozenKindLookup) List() []stepkind.StepKindSpec {
	if f == nil {
		return nil
	}
	encoded, _ := json.Marshal(f.specs)
	var result []stepkind.StepKindSpec
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	_ = decoder.Decode(&result)
	return result
}

var (
	_ DefinitionProvider         = (*DefinitionResolver)(nil)
	_ compile.DefinitionResolver = (*DefinitionResolver)(nil)
)
