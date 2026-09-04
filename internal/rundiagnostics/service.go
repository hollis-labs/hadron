package rundiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

// Inspect reconstructs an operator-facing explanation only from durable
// snapshots, immutable events, and the exact pinned plan.
func (s Service) Inspect(ctx context.Context, request Query) (Result, error) {
	query, normalizeErr := normalizeQuery(request)
	if normalizeErr != nil {
		return Result{}, normalizeErr
	}
	if ctx == nil || typedNil(s.State) || typedNil(s.Plans) {
		return Result{}, fmt.Errorf("%w: context, state reader, and plan source are required", ErrInvalidGraphQuery)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return Result{}, contextErr
	}
	run, loadRunErr := s.State.LoadRun(ctx, query.RunID)
	if loadRunErr != nil {
		return Result{}, loadRunErr
	}
	if validationErr := run.Validate(); validationErr != nil {
		return Result{}, corrupt("run", validationErr)
	}
	pinned, loadPlanErr := s.Plans.LoadRecoveryPlan(ctx, run)
	if loadPlanErr != nil {
		if errors.Is(loadPlanErr, workflowruntime.ErrNotFound) {
			return Result{}, loadPlanErr
		}
		return Result{}, corrupt("pinned plan", loadPlanErr)
	}
	if validationErr := pinned.Validate(); validationErr != nil {
		return Result{}, corrupt("pinned plan", validationErr)
	}
	if pinned.Ref != run.Plan {
		return Result{}, corrupt("pinned plan", errors.New("identity differs from run"))
	}

	projectedPlan, edgesTruncated := projectPlan(pinned.Plan, query.NodeLimit)
	result := Result{SchemaVersion: graphDiagnosticSchemaVersion, Run: projectRun(run), Plan: projectedPlan}
	result.Capabilities = Capabilities{
		ControlDecisions: !typedNil(s.Control), ReplayProvenance: !typedNil(s.Replay),
		PinBindings: !typedNil(s.Pins), ConcurrencyState: !typedNil(s.Resources),
		StartBinding: !typedNil(s.Starts), ActivationAttempts: !typedNil(s.Activations),
	}
	result.Omissions = capabilityOmissions(result.Capabilities)
	if len(pinned.Plan.Graph.Nodes) > query.NodeLimit {
		result.Truncated.Nodes = true
	}
	result.Truncated.Edges = edgesTruncated

	events, eventsTruncated, loadEventsErr := s.loadEvents(ctx, query)
	if loadEventsErr != nil {
		return Result{}, loadEventsErr
	}
	result.Truncated.Events = eventsTruncated
	rawEvents := events
	for _, event := range rawEvents {
		rendered, renderErr := workflowruntime.RenderEvent(event, query.Display)
		if renderErr != nil {
			return Result{}, corrupt("event", renderErr)
		}
		result.Events = append(result.Events, rendered)
	}

	nodes, nodesTruncated, loadNodesErr := s.loadNodes(ctx, query)
	if loadNodesErr != nil {
		return Result{}, loadNodesErr
	}
	sort.Slice(nodes, func(i, j int) bool { return invocationLess(nodes[i].ID, nodes[j].ID) })
	result.Truncated.Nodes = result.Truncated.Nodes || nodesTruncated
	definitions := make(map[string]graph.Node, len(pinned.Plan.Graph.Nodes))
	for _, definition := range pinned.Plan.Graph.Nodes {
		definitions[definition.ID] = definition
	}
	byDefinition := make(map[string][]workflowruntime.NodeInvocationSnapshot)
	for _, node := range nodes {
		if validationErr := node.Validate(); validationErr != nil {
			return Result{}, corrupt("node invocation", validationErr)
		}
		if _, exists := definitions[node.ID.NodeID]; !exists {
			return Result{}, corrupt("node invocation", fmt.Errorf("node %q is absent from pinned plan", node.ID.NodeID))
		}
		byDefinition[node.ID.NodeID] = append(byDefinition[node.ID.NodeID], node)
	}

	collector := newValueCollector(query.ValueLimit)
	collector.add(run.Inputs, "run.inputs")
	collector.add(run.Outputs, "run.outputs")
	control, loadControlErr := s.loadControl(ctx, query, nodes, collector)
	if loadControlErr != nil {
		return Result{}, loadControlErr
	}
	result.Control = control
	replay, loadReplayErr := s.loadReplay(ctx, query.RunID)
	if loadReplayErr != nil {
		return Result{}, loadReplayErr
	}
	result.Replay = replay
	resources, loadResourcesErr := s.loadResources(ctx, query)
	if loadResourcesErr != nil {
		return Result{}, loadResourcesErr
	}
	result.Resources = resources
	if resources != nil && len(resources.Holders)+len(resources.Waiters) >= query.ResourceLimit {
		result.Truncated.Resources = true
	}

	for _, node := range nodes {
		definition := definitions[node.ID.NodeID]
		diagnostic, nodeErr := s.projectNode(ctx, query, node, definition, pinned.Plan, byDefinition, rawEvents, resources, collector)
		if nodeErr != nil {
			return Result{}, nodeErr
		}
		result.Truncated.Attempts = result.Truncated.Attempts || diagnostic.AttemptsTruncated
		result.Nodes = append(result.Nodes, diagnostic)
	}
	for _, event := range rawEvents {
		collector.add(event.Values, "event."+strconv.FormatUint(event.Sequence, 10))
	}

	var renderValuesErr error
	result.Values, result.Truncated.Values, renderValuesErr = collector.render(ctx, s.State, query.Display)
	if renderValuesErr != nil {
		return Result{}, renderValuesErr
	}
	result.Plan.Edges = projectEdgeValueFlows(result.Plan.Edges, result.Nodes, result.Values)
	var startActivationErr error
	result.StartActivation, result.StartPolicy, startActivationErr = s.loadStartDiagnostics(ctx, query.RunID)
	if startActivationErr != nil {
		return Result{}, startActivationErr
	}
	var activationErr error
	result.Activations, result.Truncated.Activations, activationErr = s.loadActivations(ctx, query)
	if activationErr != nil {
		return Result{}, activationErr
	}
	return result, nil
}

func (s Service) loadNodes(ctx context.Context, query normalizedQuery) ([]workflowruntime.NodeInvocationSnapshot, bool, error) {
	if bounded, ok := s.State.(BoundedStateReader); ok && !typedNil(bounded) {
		return bounded.ListRunInvocationsForDiagnostics(ctx, query.RunID, query.NodeLimit)
	}
	nodes, err := s.State.ListRunInvocations(ctx, query.RunID)
	if err != nil {
		return nil, false, err
	}
	truncated := len(nodes) > query.NodeLimit
	if truncated {
		nodes = nodes[:query.NodeLimit]
	}
	return nodes, truncated, nil
}

func projectRun(run workflowruntime.RunSnapshot) RunDiagnostic {
	return RunDiagnostic{ID: run.ID, Plan: run.Plan, Status: run.Status, Inputs: cloneRef(run.Inputs), Outputs: cloneRef(run.Outputs), Generation: run.Generation, CreatedAt: run.CreatedAt.UTC(), UpdatedAt: run.UpdatedAt.UTC()}
}

func projectPlan(plan compile.ExecutionPlan, limit int) (PlanDiagnostic, bool) {
	result := PlanDiagnostic{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion, GraphDigest: plan.Graph.Digest,
		Definition: DefinitionDiagnostic{Authority: plan.Definition.Authority, Kind: plan.Definition.Kind, ID: plan.Definition.ID, Locator: safeLocator(plan.Definition.Locator), Version: plan.Definition.Version, Digest: plan.Definition.Digest},
		Provenance: safeProvenance(plan.Provenance), Source: safeSource(plan.SourceMap.Graph)}
	for _, digest := range plan.SourceDigests {
		result.SourceDigests = append(result.SourceDigests, SourceDigestDiagnostic{Format: digest.Format, Digest: digest.Digest})
	}
	visibleNodes := make(map[string]struct{}, min(len(plan.Graph.Nodes), limit))
	for index, node := range plan.Graph.Nodes {
		if index >= limit {
			break
		}
		visibleNodes[node.ID] = struct{}{}
		result.Nodes = append(result.Nodes, projectPlanNode(node, plan.SourceMap.Nodes[node.ID]))
	}
	edgesOmittedByNodes := false
	for _, edge := range plan.Graph.Edges {
		if _, visible := visibleNodes[edge.From]; !visible {
			edgesOmittedByNodes = true
			continue
		}
		if _, visible := visibleNodes[edge.To]; !visible {
			edgesOmittedByNodes = true
			continue
		}
		source := edge.Source
		if mapped, ok := plan.SourceMap.Edges[compile.EdgeSourceKey(edge.From, edge.To, edge.Kind)]; ok {
			source = &mapped
		}
		result.Edges = append(result.Edges, PlanEdgeDiagnostic{From: edge.From, To: edge.To, Kind: edge.Kind, Source: safeSource(source)})
	}
	sort.Slice(result.Edges, func(i, j int) bool {
		if result.Edges[i].From != result.Edges[j].From {
			return result.Edges[i].From < result.Edges[j].From
		}
		if result.Edges[i].To != result.Edges[j].To {
			return result.Edges[i].To < result.Edges[j].To
		}
		return result.Edges[i].Kind < result.Edges[j].Kind
	})
	edgesTruncated := edgesOmittedByNodes || len(result.Edges) > limit
	if len(result.Edges) > limit {
		result.Edges = result.Edges[:limit]
	}
	for _, activation := range plan.Graph.Activations {
		source, ok := plan.SourceMap.Activations[activation.ID]
		if !ok && activation.Source != nil {
			source = *activation.Source
		}
		result.Activations = append(result.Activations, PlanActivationDiagnostic{ID: activation.ID, Kind: activation.Kind, Source: safeSource(optionalSource(source))})
	}
	sort.Slice(result.Activations, func(i, j int) bool { return result.Activations[i].ID < result.Activations[j].ID })
	return result, edgesTruncated
}

func projectEdgeValueFlows(edges []PlanEdgeDiagnostic, nodes []NodeDiagnostic, rendered []ValueSetDiagnostic) []PlanEdgeDiagnostic {
	available := make(map[string]struct{}, len(rendered))
	for _, set := range rendered {
		available[valueRefKey(set.Ref)] = struct{}{}
	}
	result := append([]PlanEdgeDiagnostic(nil), edges...)
	for index := range result {
		edge := &result[index]
		if edge.Kind != graph.EdgeData {
			continue
		}
		flow := &EdgeValueFlowDiagnostic{}
		for _, node := range nodes {
			if node.ID.NodeID == edge.From && node.Outputs != nil {
				if _, ok := available[valueRefKey(*node.Outputs)]; ok {
					flow.SourceOutputs = append(flow.SourceOutputs, InvocationValueDiagnostic{Invocation: node.ID, Values: *node.Outputs})
				} else {
					flow.ValuesOmitted = true
				}
			}
			if node.ID.NodeID == edge.To && node.Inputs != nil {
				if _, ok := available[valueRefKey(*node.Inputs)]; ok {
					flow.TargetInputs = append(flow.TargetInputs, InvocationValueDiagnostic{Invocation: node.ID, Values: *node.Inputs})
				} else {
					flow.ValuesOmitted = true
				}
			}
		}
		sort.Slice(flow.SourceOutputs, func(i, j int) bool {
			return invocationLess(flow.SourceOutputs[i].Invocation, flow.SourceOutputs[j].Invocation)
		})
		sort.Slice(flow.TargetInputs, func(i, j int) bool {
			return invocationLess(flow.TargetInputs[i].Invocation, flow.TargetInputs[j].Invocation)
		})
		edge.ValueFlow = flow
	}
	return result
}

func valueRefKey(ref values.ValueSetRef) string { return ref.ID + "\x00" + ref.Digest }

func projectPlanNode(node graph.Node, mapped graph.SourceRef) PlanNodeDiagnostic {
	ready := node.ReadyWhen
	if ready == "" {
		ready = graph.ReadyAllSuccess
	}
	needs := make([]string, 0, len(node.Needs))
	for _, need := range node.Needs {
		needs = append(needs, need.Node)
	}
	catchTargets := make([]string, 0)
	for _, rule := range node.Catch {
		catchTargets = append(catchTargets, rule.Targets...)
	}
	switchTargets := make([]string, 0)
	if node.Switch != nil {
		for _, arm := range node.Switch.Arms {
			switchTargets = append(switchTargets, arm.Targets...)
		}
		switchTargets = append(switchTargets, node.Switch.Default...)
	}
	source := node.Source
	if mapped.Locator != "" {
		source = &mapped
	}
	result := PlanNodeDiagnostic{ID: node.ID, DisplayName: node.DisplayName, Kind: node.Kind, KindVersion: node.KindVersion, ReadyWhen: ready,
		Needs: canonicalIDs(needs), Effects: append(graph.EffectSet(nil), node.Effects...), Finally: node.Finally != nil,
		CatchTargets: canonicalIDs(catchTargets), SwitchTargets: canonicalIDs(switchTargets), Position: safePosition(node.Metadata), Source: safeSource(source)}
	if node.Retry != nil {
		result.Retry = &RetryDiagnostic{Attempts: node.Retry.Attempts, Strategy: node.Retry.Backoff.Strategy, InitialDelay: node.Retry.Backoff.InitialDelay, MaxDelay: node.Retry.Backoff.MaxDelay}
	}
	return result
}

func optionalSource(source graph.SourceRef) *graph.SourceRef {
	if source.Locator == "" {
		return nil
	}
	return &source
}

func (s Service) loadEvents(ctx context.Context, query normalizedQuery) ([]workflowruntime.Event, bool, error) {
	events, err := s.State.ListEvents(ctx, workflowruntime.EventQuery{RunID: query.RunID, Limit: query.EventLimit + 1})
	if err != nil {
		if errors.Is(err, workflowruntime.ErrInvalidRecord) {
			return nil, false, corrupt("events", err)
		}
		return nil, false, err
	}
	for index, event := range events {
		if err := event.Validate(); err != nil || event.RunID != query.RunID {
			if err == nil {
				err = errors.New("event belongs to another run")
			}
			return nil, false, corrupt(fmt.Sprintf("event[%d]", index), err)
		}
		if index > 0 && events[index-1].Sequence >= event.Sequence {
			return nil, false, corrupt("events", errors.New("sequence is not strictly increasing"))
		}
		if event.Type == workflowruntime.EventNodeStatusChanged && event.Attributes["to_status"] == string(workflowruntime.NodeSkipped) {
			if event.Attributes["explanation"] == "" {
				return nil, false, corrupt(fmt.Sprintf("event[%d]", index), errors.New("skipped status event is missing its explanation"))
			}
			var reason workflowruntime.BlockedReason
			if decodeErr := json.Unmarshal([]byte(event.Attributes["explanation"]), &reason); decodeErr != nil {
				return nil, false, corrupt(fmt.Sprintf("event[%d]", index), decodeErr)
			}
			if validationErr := reason.Validate(); validationErr != nil {
				return nil, false, corrupt(fmt.Sprintf("event[%d]", index), validationErr)
			}
			for _, dependency := range reason.Dependencies {
				if dependency.RunID != event.RunID {
					return nil, false, corrupt(fmt.Sprintf("event[%d]", index), errors.New("skip explanation dependency belongs to another run"))
				}
			}
		}
	}
	truncated := len(events) > query.EventLimit
	if truncated {
		events = events[:query.EventLimit]
	}
	return events, truncated, nil
}

func (s Service) projectNode(ctx context.Context, query normalizedQuery, node workflowruntime.NodeInvocationSnapshot, definition graph.Node, plan compile.ExecutionPlan, byDefinition map[string][]workflowruntime.NodeInvocationSnapshot, events []workflowruntime.Event, resources *ResourceDiagnostic, collector *valueCollector) (NodeDiagnostic, error) {
	result := NodeDiagnostic{ID: node.ID, Status: node.Status, Origin: node.Origin, MemoKeyDigest: node.MemoKeyDigest, Inputs: cloneRef(node.Inputs), Outputs: cloneRef(node.Outputs), LatestAttempt: node.LatestAttempt, Priority: node.Priority, ClaimGeneration: node.ClaimGeneration, Generation: node.Generation, CreatedAt: node.CreatedAt.UTC(), UpdatedAt: node.UpdatedAt.UTC()}
	result.Definition = projectPlanNode(definition, plan.SourceMap.Nodes[definition.ID])
	result.Source = result.Definition.Source
	if node.Lease != nil {
		owner, masked := safeDiagnosticText(node.Lease.Owner)
		result.Lease = &LeaseDiagnostic{Owner: owner, Generation: node.Lease.Generation, ExpiresAt: node.Lease.ExpiresAt.UTC(), Masked: masked}
	}
	collector.add(node.Inputs, "node."+node.ID.NodeID+".inputs")
	collector.add(node.Outputs, "node."+node.ID.NodeID+".outputs")
	attempts, attemptsTruncated, err := s.loadAttempts(ctx, node.ID, query.AttemptLimit)
	if err != nil {
		return NodeDiagnostic{}, err
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].ID.Number < attempts[j].ID.Number })
	result.AttemptsTruncated = attemptsTruncated
	for index, attempt := range attempts {
		if err := attempt.Validate(); err != nil || attempt.ID.Invocation != node.ID || attempt.ID.Number != index+1 {
			if err == nil {
				err = errors.New("attempt identity/order differs from invocation")
			}
			return NodeDiagnostic{}, corrupt("attempt", err)
		}
		attributes, masked := maskedMap(attempt.Executor.Attributes, query.Display)
		result.Attempts = append(result.Attempts, AttemptDiagnostic{Number: attempt.ID.Number, Status: attempt.Status,
			Executor: ExecutorDiagnostic{Kind: attempt.Executor.Kind, Version: attempt.Executor.Version, Target: safeLocator(attempt.Executor.Target), Attributes: attributes, Masked: masked},
			Inputs:   cloneRef(attempt.Inputs), Outputs: cloneRef(attempt.Outputs), Failure: failureDiagnostic(attempt.Failure, query.Display), StartedAt: attempt.StartedAt.UTC(), FinishedAt: attempt.FinishedAt.UTC(), Generation: attempt.Generation})
		collector.add(attempt.Inputs, fmt.Sprintf("node.%s.attempt.%d.inputs", node.ID.NodeID, attempt.ID.Number))
		collector.add(attempt.Outputs, fmt.Sprintf("node.%s.attempt.%d.outputs", node.ID.NodeID, attempt.ID.Number))
	}
	if node.Wait != nil {
		wait, loadErr := s.State.LoadWait(ctx, node.Wait.ID)
		if loadErr != nil {
			if errors.Is(loadErr, workflowruntime.ErrNotFound) {
				return NodeDiagnostic{}, corrupt("wait", errors.New("node references a missing wait"))
			}
			return NodeDiagnostic{}, loadErr
		}
		if err := wait.Validate(); err != nil || wait.Invocation != node.ID || wait.Ref != *node.Wait {
			if err == nil {
				err = errors.New("wait identity differs from node")
			}
			return NodeDiagnostic{}, corrupt("wait", err)
		}
		result.Wait = projectWait(wait)
		collector.add(wait.Payload, "node."+node.ID.NodeID+".wait.payload")
		collector.add(wait.ResumeValues, "node."+node.ID.NodeID+".wait.resume")
	}
	result.Upstream = upstreamDiagnostics(definition, plan.Graph.Edges, byDefinition)
	result.Downstream = downstreamDiagnostics(definition.ID, plan, byDefinition)
	result.Explanation = explainNode(node, definition, result.Attempts, result.Wait, result.Upstream, events, query.Display)
	if !typedNil(s.Pins) {
		binding, loadErr := s.Pins.LoadPin(ctx, node.ID)
		if loadErr == nil {
			if err := binding.Validate(); err != nil || binding.Target != node.ID {
				if err == nil {
					err = errors.New("pin target differs from invocation")
				}
				return NodeDiagnostic{}, corrupt("pin binding", err)
			}
			policyReason, _ := safeDiagnosticText(binding.Policy.Reason)
			result.Pin = &PinDiagnostic{Outputs: binding.Outputs, Source: binding.Source, SourcePlanDigest: binding.SourcePlanDigest, SourceOrigin: binding.SourceOrigin, OutputSchemaDigest: binding.OutputSchemaDigest, PolicyCode: binding.Policy.Code, PolicyReason: policyReason, BoundAt: binding.BoundAt.UTC()}
			collector.add(&binding.Outputs, "node."+node.ID.NodeID+".pin.outputs")
		} else if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return NodeDiagnostic{}, loadErr
		}
	}
	result.Resources = resourcesForNode(resources, node.ID)
	return result, nil
}

func (s Service) loadAttempts(ctx context.Context, id workflowruntime.NodeInvocationID, limit int) ([]workflowruntime.AttemptSnapshot, bool, error) {
	if bounded, ok := s.State.(BoundedStateReader); ok && !typedNil(bounded) {
		return bounded.ListAttemptsForDiagnostics(ctx, id, limit)
	}
	attempts, err := s.State.ListAttempts(ctx, id)
	if err != nil {
		return nil, false, err
	}
	truncated := len(attempts) > limit
	if truncated {
		attempts = attempts[:limit]
	}
	return attempts, truncated, nil
}

func projectWait(wait workflowruntime.WaitSnapshot) *WaitDiagnostic {
	result := &WaitDiagnostic{ID: wait.Ref.ID, Kind: wait.Kind, Status: wait.Status, WakeSource: wait.WakeSource, Visibility: wait.Visibility, WakeAt: wait.WakeAt.UTC(), Deadline: wait.Deadline.UTC(), Payload: cloneRef(wait.Payload), ResumeValues: cloneRef(wait.ResumeValues), Generation: wait.Generation, CreatedAt: wait.CreatedAt.UTC(), UpdatedAt: wait.UpdatedAt.UTC(), ResolvedAt: wait.ResolvedAt.UTC()}
	if wait.Resolution != nil {
		responderKind, _ := safeDiagnosticText(wait.Resolution.Responder.Kind)
		result.Resolution = &WaitResolutionDiagnostic{Source: wait.Resolution.Source, ResponderKind: responderKind, PayloadDigest: wait.Resolution.PayloadDigest, ResolvedAt: wait.Resolution.ResolvedAt.UTC()}
	}
	return result
}

func upstreamDiagnostics(node graph.Node, edges []graph.Edge, invocations map[string][]workflowruntime.NodeInvocationSnapshot) []DependencyDiagnostic {
	ids := make([]string, 0, len(node.Needs))
	for _, need := range node.Needs {
		ids = append(ids, need.Node)
	}
	for _, edge := range edges {
		if edge.To == node.ID {
			ids = append(ids, edge.From)
		}
	}
	ids = canonicalIDs(ids)
	result := make([]DependencyDiagnostic, 0, len(ids))
	for _, id := range ids {
		dependency := DependencyDiagnostic{NodeID: id}
		for _, invocation := range invocations[id] {
			dependency.Invocations = append(dependency.Invocations, DependencyInvocationDiagnostic{ID: invocation.ID, Status: invocation.Status})
		}
		result = append(result, dependency)
	}
	return result
}

func downstreamDiagnostics(nodeID string, plan compile.ExecutionPlan, invocations map[string][]workflowruntime.NodeInvocationSnapshot) []DownstreamDiagnostic {
	ids := make([]string, 0)
	for _, edge := range plan.Graph.Edges {
		if edge.From == nodeID {
			ids = append(ids, edge.To)
		}
	}
	for _, candidate := range plan.Graph.Nodes {
		for _, need := range candidate.Needs {
			if need.Node == nodeID {
				ids = append(ids, candidate.ID)
			}
		}
		if candidate.Finally != nil && (len(candidate.Finally.Scope) == 0 || containsString(candidate.Finally.Scope, nodeID)) {
			ids = append(ids, candidate.ID)
		}
	}
	for _, source := range plan.Graph.Nodes {
		if source.ID != nodeID {
			continue
		}
		for _, rule := range source.Catch {
			ids = append(ids, rule.Targets...)
		}
		if source.Switch != nil {
			for _, arm := range source.Switch.Arms {
				ids = append(ids, arm.Targets...)
			}
			ids = append(ids, source.Switch.Default...)
		}
	}
	ids = canonicalIDs(ids)
	definitions := make(map[string]graph.Node, len(plan.Graph.Nodes))
	for _, node := range plan.Graph.Nodes {
		definitions[node.ID] = node
	}
	result := make([]DownstreamDiagnostic, 0, len(ids))
	for _, id := range ids {
		definition, exists := definitions[id]
		if !exists {
			continue
		}
		_ = invocations // reserved for fan-out-aware transport enrichments.
		result = append(result, DownstreamDiagnostic{NodeID: id, Effects: append(graph.EffectSet(nil), definition.Effects...), Source: safeSource(sourceForNode(plan, definition))})
	}
	return result
}

func sourceForNode(plan compile.ExecutionPlan, node graph.Node) *graph.SourceRef {
	if source, exists := plan.SourceMap.Nodes[node.ID]; exists {
		return &source
	}
	return node.Source
}

func explainNode(node workflowruntime.NodeInvocationSnapshot, definition graph.Node, attempts []AttemptDiagnostic, wait *WaitDiagnostic, upstream []DependencyDiagnostic, events []workflowruntime.Event, policy values.DisplayPolicy) NodeExplanation {
	switch node.Status {
	case workflowruntime.NodeBlocked:
		if node.Blocked != nil {
			message, messageMasked := safeDiagnosticText(node.Blocked.Message)
			details, detailsMasked := maskedMap(node.Blocked.Details, policy)
			return NodeExplanation{Code: node.Blocked.Code, Message: message, Dependencies: append([]workflowruntime.NodeInvocationID(nil), node.Blocked.Dependencies...), Details: details, Masked: messageMasked || detailsMasked}
		}
	case workflowruntime.NodeSkipped:
		if reason := skippedReason(node.ID, events, policy); reason != nil {
			return *reason
		}
		return NodeExplanation{Code: "node_skipped", Message: "the node was durably skipped"}
	case workflowruntime.NodeFailed, workflowruntime.NodeTimedOut, workflowruntime.NodeCanceled, workflowruntime.NodeCrashed:
		failure := latestFailure(attempts)
		return NodeExplanation{Code: "node_" + string(node.Status), Message: "the latest durable attempt ended " + string(node.Status), Failure: failure}
	case workflowruntime.NodeWaiting:
		if wait != nil {
			return NodeExplanation{Code: "wait_" + string(wait.Status), Message: "the node is suspended on a durable " + string(wait.Kind) + " wait", Details: map[string]string{"wake_source": string(wait.WakeSource)}}
		}
		return NodeExplanation{Code: "retry_or_external_wait", Message: "the node is waiting for a persisted retry or external operation"}
	case workflowruntime.NodePending:
		if definition.Finally != nil {
			return NodeExplanation{Code: "finalizer_pending", Message: "the finalizer is pending terminal completion of its declared scope"}
		}
		if nonTerminal := nonTerminalDependencies(upstream); len(nonTerminal) > 0 {
			return NodeExplanation{Code: "upstream_pending", Message: "the node is waiting for upstream dependencies", Dependencies: nonTerminal}
		}
		if dependencies := allDependencies(upstream); len(dependencies) > 0 {
			return NodeExplanation{Code: "upstream_terminal", Message: "upstream dependencies are terminal and the node is awaiting a readiness or control-flow decision", Dependencies: dependencies}
		}
		return NodeExplanation{Code: "admission_pending", Message: "the node is pending readiness or control-flow admission"}
	case workflowruntime.NodeReady:
		return NodeExplanation{Code: "ready", Message: "the node is ready for a durable claim"}
	case workflowruntime.NodeRunning:
		return NodeExplanation{Code: "running", Message: "the node has an active durable attempt"}
	case workflowruntime.NodeSucceeded:
		origin := node.Origin
		if origin == "" {
			origin = workflowruntime.OriginExecuted
		}
		return NodeExplanation{Code: "succeeded_" + string(origin), Message: "the node completed successfully with outcome origin " + string(origin)}
	}
	return NodeExplanation{Code: "unknown", Message: "the persisted node status has no diagnostic projection"}
}

func skippedReason(id workflowruntime.NodeInvocationID, events []workflowruntime.Event, policy values.DisplayPolicy) *NodeExplanation {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.Type != workflowruntime.EventNodeStatusChanged || event.Invocation == nil || *event.Invocation != id || event.Attributes["to_status"] != string(workflowruntime.NodeSkipped) || event.Attributes["explanation"] == "" {
			continue
		}
		var reason workflowruntime.BlockedReason
		if err := json.Unmarshal([]byte(event.Attributes["explanation"]), &reason); err != nil || reason.Validate() != nil {
			return &NodeExplanation{Code: "skipped_explanation_corrupt", Message: "the persisted skip explanation is malformed"}
		}
		if event.Redaction == values.RedactionSecret || event.Redaction == values.RedactionPrivate && !policy.RevealsPrivate() {
			return &NodeExplanation{Code: reason.Code, Message: values.RedactedMarker, Masked: true}
		}
		return &NodeExplanation{Code: reason.Code, Message: reason.Message, Dependencies: append([]workflowruntime.NodeInvocationID(nil), reason.Dependencies...), Details: cloneStringMap(reason.Details)}
	}
	return nil
}

func latestFailure(attempts []AttemptDiagnostic) *FailureDiagnostic {
	for index := len(attempts) - 1; index >= 0; index-- {
		if attempts[index].Failure != nil {
			copyFailure := *attempts[index].Failure
			copyFailure.Details = cloneStringMap(attempts[index].Failure.Details)
			return &copyFailure
		}
	}
	return nil
}

func nonTerminalDependencies(input []DependencyDiagnostic) []workflowruntime.NodeInvocationID {
	result := make([]workflowruntime.NodeInvocationID, 0)
	for _, dependency := range input {
		for _, invocation := range dependency.Invocations {
			if !invocation.Status.Terminal() {
				result = append(result, invocation.ID)
			}
		}
	}
	return result
}

func allDependencies(input []DependencyDiagnostic) []workflowruntime.NodeInvocationID {
	result := make([]workflowruntime.NodeInvocationID, 0)
	for _, dependency := range input {
		for _, invocation := range dependency.Invocations {
			result = append(result, invocation.ID)
		}
	}
	return result
}

func (s Service) loadControl(ctx context.Context, query normalizedQuery, nodes []workflowruntime.NodeInvocationSnapshot, collector *valueCollector) (ControlDiagnostic, error) {
	if typedNil(s.Control) {
		return ControlDiagnostic{}, nil
	}
	var result ControlDiagnostic
	for _, node := range nodes {
		for _, kind := range []workflowruntime.ControlDecisionKind{workflowruntime.ControlSwitch, workflowruntime.ControlCatch} {
			decision, err := s.Control.LoadControlDecision(ctx, workflowruntime.ControlDecisionID{Source: node.ID, Kind: kind})
			if errors.Is(err, workflowruntime.ErrNotFound) {
				continue
			}
			if err != nil {
				return ControlDiagnostic{}, err
			}
			if err := decision.Validate(); err != nil || decision.ID.Source.RunID != query.RunID {
				if err == nil {
					err = errors.New("control decision belongs to another run")
				}
				return ControlDiagnostic{}, corrupt("control decision", err)
			}
			projected := ControlDecisionDiagnostic{Source: decision.ID.Source, Kind: decision.ID.Kind, Outcome: decision.Outcome, Targets: append([]workflowruntime.NodeInvocationID(nil), decision.Targets...), BindAs: decision.BindAs, Error: cloneRef(decision.Error), Generation: decision.Generation, CreatedAt: decision.CreatedAt.UTC()}
			if decision.RuleIndex != nil {
				value := *decision.RuleIndex
				projected.RuleIndex = &value
			}
			result.Decisions = append(result.Decisions, projected)
			collector.add(decision.Error, "control."+string(kind)+"."+node.ID.NodeID+".error")
		}
	}
	sort.Slice(result.Decisions, func(i, j int) bool {
		if result.Decisions[i].Source.NodeID != result.Decisions[j].Source.NodeID {
			return result.Decisions[i].Source.NodeID < result.Decisions[j].Source.NodeID
		}
		if result.Decisions[i].Source.Iteration != result.Decisions[j].Source.Iteration {
			return result.Decisions[i].Source.Iteration < result.Decisions[j].Source.Iteration
		}
		return result.Decisions[i].Kind < result.Decisions[j].Kind
	})
	intent, err := s.Control.LoadTerminalIntent(ctx, query.RunID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return result, nil
	}
	if err != nil {
		return ControlDiagnostic{}, err
	}
	if err := intent.Validate(); err != nil || intent.RunID != query.RunID {
		if err == nil {
			err = errors.New("terminal intent belongs to another run")
		}
		return ControlDiagnostic{}, corrupt("terminal intent", err)
	}
	finalizers := make([]workflowruntime.FinalizerScope, len(intent.Finalizers))
	for index, finalizer := range intent.Finalizers {
		finalizers[index] = finalizer
		finalizers[index].Scope = append([]workflowruntime.NodeInvocationID(nil), finalizer.Scope...)
	}
	result.TerminalIntent = &TerminalIntentDiagnostic{IntendedStatus: intent.IntendedStatus, SuccessOutputsRequired: intent.SuccessOutputsRequired, Reason: failureDiagnostic(intent.Reason, query.Display), Error: cloneRef(intent.Error), Finalizers: finalizers, Status: intent.Status, Generation: intent.Generation, CreatedAt: intent.CreatedAt.UTC(), UpdatedAt: intent.UpdatedAt.UTC(), CompletedAt: intent.CompletedAt.UTC()}
	collector.add(intent.Error, "control.terminal.error")
	return result, nil
}

func (s Service) loadReplay(ctx context.Context, runID workflowruntime.RunID) (*ReplayDiagnostic, error) {
	if typedNil(s.Replay) {
		return nil, nil
	}
	provenance, err := s.Replay.LoadReplayProvenance(ctx, runID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := provenance.Validate(); err != nil || provenance.RunID != runID {
		if err == nil {
			err = errors.New("replay provenance belongs to another run")
		}
		return nil, corrupt("replay provenance", err)
	}
	result := &ReplayDiagnostic{SourceRunID: provenance.SourceRunID, FromNodeID: provenance.FromNodeID, PlanDigest: provenance.PlanDigest, CreatedAt: provenance.CreatedAt.UTC()}
	for _, policy := range provenance.Policy {
		var attempt *workflowruntime.AttemptID
		if policy.Attempt != nil {
			value := *policy.Attempt
			attempt = &value
		}
		reason, _ := safeDiagnosticText(policy.Decision.Reason)
		result.Policy = append(result.Policy, ReplayPolicyDiagnostic{Invocation: policy.Invocation, Attempt: attempt, Allow: policy.Decision.Allow, Code: policy.Decision.Code, Reason: reason})
	}
	return result, nil
}

func (s Service) loadResources(ctx context.Context, query normalizedQuery) (*ResourceDiagnostic, error) {
	if typedNil(s.Resources) {
		return nil, nil
	}
	state, err := s.Resources.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: query.RunID, Now: query.Now, Limit: query.ResourceLimit})
	if err != nil {
		return nil, err
	}
	for _, holder := range state.Holders {
		if err := holder.Validate(); err != nil || holder.Invocation.RunID != query.RunID {
			if err == nil {
				err = errors.New("resource holder belongs to another run")
			}
			return nil, corrupt("resource holder", err)
		}
		if err := validateResourceMetadata(holder.Resource, holder.Owner); err != nil {
			return nil, corrupt("resource holder", err)
		}
	}
	for _, waiter := range state.Waiters {
		if err := waiter.Validate(); err != nil || waiter.Invocation.RunID != query.RunID {
			if err == nil {
				err = errors.New("resource waiter belongs to another run")
			}
			return nil, corrupt("resource waiter", err)
		}
		for _, requirement := range waiter.Requirements {
			if err := validateResourceMetadata(requirement.Resource, ""); err != nil {
				return nil, corrupt("resource waiter", err)
			}
		}
		for _, blocked := range waiter.Blocked {
			if err := validateResourceMetadata(blocked, ""); err != nil {
				return nil, corrupt("resource waiter", err)
			}
		}
	}
	return &ResourceDiagnostic{Holders: append([]workflowruntime.SchedulerResourceHolder(nil), state.Holders...), Waiters: cloneWaiters(state.Waiters)}, nil
}

func validateResourceMetadata(resource workflowruntime.SchedulerResourceID, owner string) error {
	for _, value := range []string{resource.Name, owner} {
		if value == "" {
			continue
		}
		if _, sensitive := safeDiagnosticText(value); sensitive {
			return errors.New("resource metadata contains credential-shaped data")
		}
	}
	return nil
}

func resourcesForNode(resources *ResourceDiagnostic, id workflowruntime.NodeInvocationID) NodeResourceDiagnostic {
	var result NodeResourceDiagnostic
	if resources == nil {
		return result
	}
	for _, holder := range resources.Holders {
		if holder.Invocation == id {
			result.Holders = append(result.Holders, holder)
		}
	}
	for _, waiter := range resources.Waiters {
		if waiter.Invocation == id {
			copyWaiter := waiter
			copyWaiter.Requirements = append([]workflowruntime.SchedulerResourceRequirement(nil), waiter.Requirements...)
			copyWaiter.Blocked = append([]workflowruntime.SchedulerResourceID(nil), waiter.Blocked...)
			result.Waiter = &copyWaiter
			break
		}
	}
	return result
}

func (s Service) loadActivations(ctx context.Context, query normalizedQuery) ([]ActivationFireAttempt, bool, error) {
	result := make([]ActivationFireAttempt, 0)
	truncated := false
	if !typedNil(s.Activations) {
		attempts, sourceTruncated, err := s.Activations.ListRunActivationAttempts(ctx, query.RunID, query.ActivationLimit)
		if err != nil {
			if errors.Is(err, workflowruntime.ErrInvalidRecord) {
				return nil, false, corrupt("activation attempts", err)
			}
			return nil, false, err
		}
		for index, attempt := range attempts {
			attempt.ScheduledAt = attempt.ScheduledAt.UTC()
			attempt.FiredAt = attempt.FiredAt.UTC()
			if err := attempt.validate(query.RunID); err != nil {
				return nil, false, corrupt(fmt.Sprintf("activation attempt[%d]", index), err)
			}
			result = append(result, attempt)
		}
		truncated = sourceTruncated || len(result) > query.ActivationLimit
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].ScheduledAt.Equal(result[j].ScheduledAt) {
			return result[i].ScheduledAt.Before(result[j].ScheduledAt)
		}
		if result[i].FireID != result[j].FireID {
			return result[i].FireID < result[j].FireID
		}
		return result[i].Attempt < result[j].Attempt
	})
	if len(result) > query.ActivationLimit {
		result = result[:query.ActivationLimit]
	}
	return result, truncated, nil
}

func (s Service) loadStartDiagnostics(ctx context.Context, runID workflowruntime.RunID) (*StartActivationDiagnostic, *StartPolicyDiagnostic, error) {
	if typedNil(s.Starts) {
		return nil, nil, nil
	}
	start, err := s.Starts.LoadStart(ctx, runID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if err := start.Validate(); err != nil || start.Record.Run.ID != runID {
		if err == nil {
			err = errors.New("start binding belongs to another run")
		}
		return nil, nil, corrupt("start binding", err)
	}
	policy, policyErr := projectStartPolicy(start.Record)
	if policyErr != nil {
		return nil, nil, corrupt("start policy", policyErr)
	}
	if start.Record.Activation == nil {
		return nil, policy, nil
	}
	binding := start.Record.Activation
	if _, sensitive := safeDiagnosticText(binding.ActivationID); sensitive {
		return nil, nil, corrupt("start activation", errors.New("activation id contains credential-shaped metadata"))
	}
	return &StartActivationDiagnostic{ActivationID: binding.ActivationID, FireIdentityDigest: values.SHA256Digest([]byte(binding.ActivationID + "\x00" + binding.IdempotencyKey)), OccurredAt: binding.OccurredAt.UTC()}, policy, nil
}

func projectStartPolicy(record hoststate.StartRecord) (*StartPolicyDiagnostic, error) {
	facts := record.Facts
	result := &StartPolicyDiagnostic{
		Effects: append(graph.EffectSet(nil), facts.Effects...), RequiredCapabilities: append([]string(nil), facts.RequiredCapabilities...),
		BlastRadius: make(map[string]int, len(facts.BlastRadius)), NodeCount: facts.NodeCount,
		DryRunAvailable: facts.DryRunAvailable, ConfirmationAdvised: facts.ConfirmationAdvised, Decision: record.Decision.Outcome,
	}
	for _, effect := range result.Effects {
		if !effect.Valid() {
			return nil, errors.New("start effects contain an invalid declaration")
		}
	}
	for _, capability := range result.RequiredCapabilities {
		if hoststate.ValidatePublicText(capability, 128, true) != nil {
			return nil, errors.New("required capabilities contain unsafe metadata")
		}
	}
	for key, count := range facts.BlastRadius {
		if hoststate.ValidatePublicText(key, 128, true) != nil || count < 0 {
			return nil, errors.New("blast radius contains unsafe metadata")
		}
		result.BlastRadius[key] = count
	}
	if exposure := strings.TrimSpace(record.Identity.Extension["exposure_ref"]); exposure != "" {
		if hoststate.ValidatePublicText(exposure, hoststate.MaximumActivationTextBytes, true) != nil {
			return nil, errors.New("exposure reference contains unsafe metadata")
		}
		result.ExposureRef, result.ExposureMasked = safeDiagnosticText(exposure)
	}
	return result, nil
}

func capabilityOmissions(capabilities Capabilities) []string {
	var result []string
	for _, item := range []struct {
		available bool
		name      string
	}{{capabilities.ControlDecisions, "control_decisions"}, {capabilities.ReplayProvenance, "replay_provenance"}, {capabilities.PinBindings, "pin_bindings"}, {capabilities.ConcurrencyState, "concurrency_state"}, {capabilities.StartBinding, "start_binding"}, {capabilities.ActivationAttempts, "activation_attempts"}} {
		if !item.available {
			result = append(result, item.name)
		}
	}
	return result
}

type valueCollector struct {
	limit     int
	entries   map[string]*valueCollection
	truncated bool
}

type valueCollection struct {
	ref   values.ValueSetRef
	roles map[string]struct{}
}

func newValueCollector(limit int) *valueCollector {
	return &valueCollector{limit: limit, entries: make(map[string]*valueCollection)}
}

func (c *valueCollector) add(ref *values.ValueSetRef, role string) {
	if ref == nil {
		return
	}
	key := ref.ID + "\x00" + ref.Digest
	entry, exists := c.entries[key]
	if !exists {
		if len(c.entries) >= c.limit {
			c.truncated = true
			return
		}
		entry = &valueCollection{ref: *ref, roles: make(map[string]struct{})}
		c.entries[key] = entry
	}
	entry.roles[role] = struct{}{}
}

func (c *valueCollector) render(ctx context.Context, store StateReader, policy values.DisplayPolicy) ([]ValueSetDiagnostic, bool, error) {
	keys := make([]string, 0, len(c.entries))
	for key := range c.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ValueSetDiagnostic, 0, len(keys))
	for _, key := range keys {
		entry := c.entries[key]
		set, err := store.LoadValues(ctx, entry.ref)
		if err != nil {
			if errors.Is(err, workflowruntime.ErrNotFound) {
				return nil, false, corrupt("value set", errors.New("referenced value set is missing"))
			}
			if errors.Is(err, workflowruntime.ErrInvalidRecord) {
				return nil, false, corrupt("value set", err)
			}
			return nil, false, err
		}
		digest, err := values.DigestValueSet(set)
		if err != nil || digest != entry.ref.Digest {
			if err == nil {
				err = errors.New("value-set content digest differs from reference")
			}
			return nil, false, corrupt("value set", err)
		}
		rendered, err := values.RenderValueSet(set, policy)
		if err != nil {
			return nil, false, corrupt("value set", err)
		}
		roles := make([]string, 0, len(entry.roles))
		for role := range entry.roles {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		result = append(result, ValueSetDiagnostic{Ref: entry.ref, Roles: roles, Values: rendered})
	}
	return result, c.truncated, nil
}

func invocationLess(left, right workflowruntime.NodeInvocationID) bool {
	if left.NodeID != right.NodeID {
		return left.NodeID < right.NodeID
	}
	return left.Iteration < right.Iteration
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneWaiters(input []workflowruntime.SchedulerResourceWaiter) []workflowruntime.SchedulerResourceWaiter {
	result := make([]workflowruntime.SchedulerResourceWaiter, len(input))
	for index, waiter := range input {
		result[index] = waiter
		result[index].Requirements = append([]workflowruntime.SchedulerResourceRequirement(nil), waiter.Requirements...)
		result[index].Blocked = append([]workflowruntime.SchedulerResourceID(nil), waiter.Blocked...)
	}
	return result
}
