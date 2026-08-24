package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// InvocationExplanation combines one authoritative node snapshot with its
// complete durable attempt history. It never infers state from logs.
type InvocationExplanation struct {
	Node     NodeInvocationSnapshot `json:"node"`
	Attempts []AttemptSnapshot      `json:"attempts"`
}

// RunExplanation is a restart-stable projection of state plus append-only
// facts. Events explain chronology; snapshots remain lifecycle authority.
type RunExplanation struct {
	Run            RunSnapshot               `json:"run"`
	Invocations    []InvocationExplanation   `json:"invocations"`
	Events         []Event                   `json:"events"`
	Decisions      []ControlDecisionSnapshot `json:"decisions,omitempty"`
	TerminalIntent *TerminalIntentSnapshot   `json:"terminal_intent,omitempty"`
	Replay         *ReplayProvenance         `json:"replay,omitempty"`
	Recovery       RecoverySnapshot          `json:"recovery"`
}

type ExplainService struct {
	Store   StateStore
	Control ControlFlowStore
	Replay  ReplayStore
	Plans   RecoveryPlanSource
}

func (s ExplainService) Explain(ctx context.Context, runID RunID, now time.Time) (RunExplanation, error) {
	if ctx == nil || nilStateStore(s.Store) || nilReflect(s.Replay) || nilRecoveryPlanSource(s.Plans) || now.IsZero() {
		return RunExplanation{}, fmt.Errorf("%w: explain requires context, store, plan source, and time", ErrInvalidRecovery)
	}
	run, loadErr := s.Store.LoadRun(ctx, runID)
	if loadErr != nil {
		return RunExplanation{}, loadErr
	}
	plan, loadErr := s.Plans.LoadRecoveryPlan(ctx, run)
	if loadErr != nil {
		return RunExplanation{}, loadErr
	}
	if validationErr := plan.Validate(); validationErr != nil {
		return RunExplanation{}, fmt.Errorf("%w: invalid pinned explain plan: %w", ErrInvalidRecovery, validationErr)
	}
	if plan.Ref != run.Plan {
		return RunExplanation{}, fmt.Errorf("%w: pinned explain plan mismatch", ErrInvalidRecovery)
	}
	result := RunExplanation{Run: run}
	definitions := make(map[string]struct{}, len(plan.Plan.Graph.Nodes))
	for _, definition := range plan.Plan.Graph.Nodes {
		definitions[definition.ID] = struct{}{}
	}
	nodes, loadErr := s.Replay.ListRunInvocations(ctx, runID)
	if loadErr != nil {
		return RunExplanation{}, loadErr
	}
	for _, node := range nodes {
		if _, exists := definitions[node.ID.NodeID]; !exists {
			return RunExplanation{}, fmt.Errorf("%w: invocation is absent from pinned graph", ErrInvalidRecovery)
		}
		id := node.ID
		attempts, listErr := s.Store.ListAttempts(ctx, id)
		if listErr != nil {
			return RunExplanation{}, listErr
		}
		result.Invocations = append(result.Invocations, InvocationExplanation{Node: node, Attempts: attempts})
		if !nilControlFlowStore(s.Control) {
			for _, kind := range []ControlDecisionKind{ControlSwitch, ControlCatch} {
				decision, decisionErr := s.Control.LoadControlDecision(ctx, ControlDecisionID{Source: id, Kind: kind})
				if decisionErr == nil {
					result.Decisions = append(result.Decisions, decision)
					continue
				}
				if !errors.Is(decisionErr, ErrNotFound) {
					return RunExplanation{}, decisionErr
				}
			}
		}
	}
	result.Events, loadErr = s.Store.ListEvents(ctx, EventQuery{RunID: runID})
	if loadErr != nil {
		return RunExplanation{}, loadErr
	}
	if !nilControlFlowStore(s.Control) {
		intent, intentErr := s.Control.LoadTerminalIntent(ctx, runID)
		if intentErr == nil {
			result.TerminalIntent = &intent
		} else if !errors.Is(intentErr, ErrNotFound) {
			return RunExplanation{}, intentErr
		}
	}
	if !nilReflect(s.Replay) {
		provenance, replayErr := s.Replay.LoadReplayProvenance(ctx, runID)
		if replayErr == nil {
			result.Replay = &provenance
		} else if !errors.Is(replayErr, ErrNotFound) {
			return RunExplanation{}, replayErr
		}
	}
	result.Recovery, loadErr = s.Store.Recovery(ctx, RecoveryQuery{RunID: runID, Now: now})
	if loadErr != nil {
		return RunExplanation{}, loadErr
	}
	sort.Slice(result.Decisions, func(i, j int) bool {
		left, right := result.Decisions[i].ID, result.Decisions[j].ID
		if left.Source.NodeID != right.Source.NodeID {
			return left.Source.NodeID < right.Source.NodeID
		}
		return left.Kind < right.Kind
	})
	return result, nil
}
