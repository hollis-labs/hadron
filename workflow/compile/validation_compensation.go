package compile

import (
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func (v *validator) validateCompensation() {
	handlers := make(map[string]string)
	handlerNodes := make(map[string]graph.Node)
	for _, node := range v.graph.Nodes {
		if node.Compensation == nil {
			continue
		}
		if err := graph.ValidateID(node.Compensation.Handler); err != nil {
			v.invalidCompensation(node, fmt.Sprintf("handler id %q is not canonical: %v", node.Compensation.Handler, err))
			continue
		}
		handler := graph.NormalizeID(node.Compensation.Handler)
		handlers[handler] = node.ID
		if handler == graph.NormalizeID(node.ID) {
			v.invalidCompensation(node, "cannot compensate itself")
		}
		found, ok := compensationNode(v.graph, handler)
		if !ok {
			v.invalidCompensation(node, fmt.Sprintf("references unknown handler %q", node.Compensation.Handler))
			continue
		}
		handlerNodes[handler] = found
		if found.Compensation != nil {
			v.invalidCompensation(found, "handler nodes cannot register their own compensation")
		}
		if found.Finally != nil || found.ForEach != nil || found.Switch != nil || len(found.Catch) != 0 || found.Service != nil || found.Memoization != nil {
			v.invalidCompensation(found, "handler must be one dormant ordinary node without finally, fan-out, routing, service, or memoization semantics")
		}
		if len(found.Needs) != 0 {
			v.invalidCompensation(found, "handler ordering is ledger-owned and cannot declare forward needs")
		}
		if found.Call != nil && found.Call.Mode != graph.CallInline {
			v.invalidCompensation(found, "call handler must use inline mode so rollback completion is terminal before the ledger succeeds")
		}
		v.validateCompensationHandlerBindings(found)
		v.validateCompensableOperation(node)
	}

	if len(handlers) == 0 {
		if v.graph.Compensation != nil {
			v.add(CodeInvalidCompensation, graphSource(v.graph), "workflow declares compensation policy without any compensable node", "Remove the policy or register at least one dormant handler.")
		}
		return
	}
	if v.graph.Compensation == nil || len(v.graph.Compensation.Triggers) == 0 {
		v.add(CodeInvalidCompensation, graphSource(v.graph), "compensable nodes require an explicit workflow compensation trigger", "Declare one or more of failure, cancel, timeout, or manual under workflow compensation.")
	} else {
		seen := make(map[graph.CompensationTrigger]struct{}, len(v.graph.Compensation.Triggers))
		for _, trigger := range v.graph.Compensation.Triggers {
			if !trigger.Valid() {
				v.add(CodeInvalidCompensation, graphSource(v.graph), fmt.Sprintf("workflow uses unsupported compensation trigger %q", trigger), "Use failure, cancel, timeout, or manual.")
			}
			if _, duplicate := seen[trigger]; duplicate {
				v.add(CodeInvalidCompensation, graphSource(v.graph), fmt.Sprintf("workflow repeats compensation trigger %q", trigger), "List each compensation trigger once.")
			}
			seen[trigger] = struct{}{}
		}
		if _, manual := seen[graph.CompensationManual]; manual {
			for _, node := range v.graph.Nodes {
				if node.Finally != nil {
					v.add(CodeInvalidCompensation, graphSource(v.graph), "manual compensation is incompatible with finally nodes", "Use terminal-outcome compensation before finally, or remove manual rollback from this workflow.")
					break
				}
			}
		}
	}

	// A handler is dormant: no forward need, edge, catch, switch, or finally
	// scope may target or source it. Ledger dependencies are the only ordering.
	for _, node := range v.graph.Nodes {
		owner := graph.NormalizeID(node.ID)
		for _, need := range node.Needs {
			if _, sourceHandler := handlers[graph.NormalizeID(need.Node)]; sourceHandler {
				v.invalidCompensation(node, fmt.Sprintf("forward need references dormant handler %q", need.Node))
			}
		}
		if _, handler := handlers[owner]; handler {
			if node.If != nil {
				v.invalidCompensation(node, "handler cannot be conditionally skipped")
			}
		}
		for _, rule := range node.Catch {
			for _, target := range rule.Targets {
				if _, handler := handlers[graph.NormalizeID(target)]; handler {
					v.invalidCompensation(node, fmt.Sprintf("catch targets dormant handler %q", target))
				}
			}
		}
		if node.Switch != nil {
			for _, arm := range node.Switch.Arms {
				for _, target := range arm.Targets {
					if _, handler := handlers[graph.NormalizeID(target)]; handler {
						v.invalidCompensation(node, fmt.Sprintf("switch targets dormant handler %q", target))
					}
				}
			}
		}
	}
	for _, edge := range v.graph.Edges {
		_, fromHandler := handlers[graph.NormalizeID(edge.From)]
		_, toHandler := handlers[graph.NormalizeID(edge.To)]
		if fromHandler || toHandler {
			v.add(CodeInvalidCompensation, firstSource(edge.Source, graphSource(v.graph)), fmt.Sprintf("forward edge %q -> %q crosses a dormant compensation handler", edge.From, edge.To), "Remove the forward edge; rollback ordering is derived from forward dependencies.")
		}
	}
	for _, output := range v.graph.Outputs {
		if output.Value == nil {
			continue
		}
		var references []values.Reference
		var err error
		switch output.Value.Kind {
		case graph.BindingLiteral:
			// Literal outputs cannot reference dormant compensation handlers.
		case graph.BindingExpression:
			if output.Value.Expression != nil {
				references, err = values.ParseReferences(*output.Value.Expression)
			}
		case graph.BindingInterpolation:
			references, err = values.ParseInterpolationReferences(output.Value.Interpolation, output.Value.Source)
		}
		if err != nil {
			continue // ordinary expression validation owns syntax diagnostics
		}
		for _, reference := range references {
			if reference.Root != "steps" {
				continue
			}
			if reference.Dynamic || len(reference.Path) == 0 {
				v.add(CodeInvalidCompensation, firstSource(output.Value.Source, output.Source, graphSource(v.graph)), fmt.Sprintf("workflow output %q dynamically references steps while compensation handlers are dormant", output.Name), "Use a static forward step reference; dormant handler outputs are available only through compensation inspection.")
				continue
			}
			if _, dormant := handlers[graph.NormalizeID(reference.Path[0])]; dormant {
				v.add(CodeInvalidCompensation, firstSource(output.Value.Source, output.Source, graphSource(v.graph)), fmt.Sprintf("workflow output %q references dormant compensation handler %q", output.Name, reference.Path[0]), "Use a forward node output; inspect rollback results through compensation inspection.")
			}
		}
	}
	_ = handlerNodes
}

func (v *validator) validateCompensationHandlerBindings(handler graph.Node) {
	for name, binding := range handler.InputBindings {
		var (
			references []values.Reference
			err        error
		)
		switch binding.Kind {
		case graph.BindingExpression:
			if binding.Expression != nil {
				references, err = values.ParseReferences(*binding.Expression)
			}
		case graph.BindingInterpolation:
			references, err = values.ParseInterpolationReferences(binding.Interpolation, binding.Source)
		default:
			v.invalidCompensation(handler, fmt.Sprintf("handler input %q must map the compensation expression root", name))
			continue
		}
		if err != nil {
			// The general binding validator owns syntax diagnostics.
			continue
		}
		if len(references) == 0 {
			v.invalidCompensation(handler, fmt.Sprintf("handler input %q must reference the compensation expression root", name))
			continue
		}
		for _, reference := range references {
			if reference.Root != "compensation" {
				v.invalidCompensation(handler, fmt.Sprintf("handler input %q references %q; only compensation evidence is in scope", name, reference.Root))
				break
			}
		}
	}
}

func (v *validator) validateCompensableOperation(node graph.Node) {
	kind, spec, _ := v.kinds.resolve(node.Kind, node.KindVersion)
	if kind == nil || spec == nil {
		return
	}
	effects := append(graph.EffectSet(nil), spec.Effects...)
	effects = append(effects, node.Effects...)
	material := false
	for _, effect := range effects {
		material = material || effect == graph.EffectMaterialize || effect == graph.EffectMutate || effect == graph.EffectDestructive
	}
	if !material {
		v.invalidCompensation(node, "has no effect application that can truthfully produce rollback eligibility")
	}
	// External/service completion currently closes attempts through the
	// observation coordinator, which cannot atomically append compensation
	// eligibility. Reject the claim instead of permitting an applied effect to
	// reach terminal success without its ledger entry.
	if spec.CanSuspend || spec.Observation.Mode != stepkind.ObservationNone || spec.Lifecycle.Service || node.Service != nil {
		v.invalidCompensation(node, "uses suspension, external observation, or service lifecycle without an atomic compensation receipt boundary")
		return
	}
	provider, ok := kind.(stepkind.ReversibilityProvider)
	if !ok || spec.Compensation != stepkind.CompensationReceiptRequired {
		v.invalidCompensation(node, "registered kind does not provide operation-specific reversibility evidence")
		return
	}
	config := node.Config
	if config == nil {
		config = graph.Config{}
	}
	request := stepkind.ReversibilityRequest{Config: config, Call: node.Call}
	evidence, err := stepkind.ResolveReversibility(v.ctx, provider, request)
	if err != nil {
		v.invalidCompensation(node, fmt.Sprintf("reversibility evidence failed: %v", err))
		return
	}
	if strings.TrimSpace(evidence.Operation) == "" || values.ValidateSchema(evidence.ReceiptSchema) != nil {
		v.invalidCompensation(node, "registered reversibility evidence is malformed")
	}
	handler, ok := compensationNode(v.graph, graph.NormalizeID(node.Compensation.Handler))
	if !ok {
		return
	}
	_, handlerSpec, _ := v.kinds.resolve(handler.Kind, handler.KindVersion)
	if handlerSpec == nil {
		return
	}
	mode := handlerSpec.Idempotency
	if handler.Idempotency != nil {
		mode = handler.Idempotency.Mode
	}
	if mode != graph.IdempotencyIntrinsic && mode != graph.IdempotencyKeyed {
		v.invalidCompensation(handler, "handler requires intrinsic or keyed idempotency")
	}
	handlerEffects := append(graph.EffectSet(nil), handlerSpec.Effects...)
	handlerEffects = append(handlerEffects, handler.Effects...)
	handlerMaterial := false
	for _, effect := range handlerEffects {
		handlerMaterial = handlerMaterial || effect == graph.EffectMaterialize || effect == graph.EffectMutate || effect == graph.EffectDestructive
	}
	if !handlerMaterial {
		v.invalidCompensation(handler, "handler has no declared material rollback effect")
	}
}

func (v *validator) invalidCompensation(node graph.Node, reason string) {
	v.add(CodeInvalidCompensation, v.nodeSource(node), fmt.Sprintf("node %q compensation %s", node.ID, reason), "Use a dormant, idempotent handler and a registered operation-specific reversibility contract.")
}

func compensationNode(workflow graph.Graph, normalized string) (graph.Node, bool) {
	for _, node := range workflow.Nodes {
		if graph.NormalizeID(node.ID) == normalized {
			return node, true
		}
	}
	return graph.Node{}, false
}
