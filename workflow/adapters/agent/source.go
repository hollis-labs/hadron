package agent

import (
	"fmt"
	"reflect"
	"sort"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const (
	// SugarKindName is the graph-native source kind expanded by SourceExpander.
	SugarKindName = "agent_launch"
	// SourceExpanderName is the deterministic compiler extension identity.
	SourceExpanderName = "workflow.agent-launch.v1"
)

// SourceExpander lowers agent_launch into the ordinary call/wait composition
// implemented by this package. It is stateless and concurrency-safe.
type SourceExpander struct{}

func (SourceExpander) Name() string { return SourceExpanderName }

// ExpandNode implements compile.NodeExpander without consulting a registry,
// daemon, or substrate. Unsupported modifier distribution fails closed rather
// than changing control-flow semantics silently.
func (SourceExpander) ExpandNode(request workflowcompile.NodeExpansionRequest) (workflowcompile.NodeExpansion, bool, []diagnostic.Diagnostic) {
	node := request.Node
	if node.Kind != SugarKindName {
		return workflowcompile.NodeExpansion{}, false, nil
	}
	config, waitPolicy, parentClose, configErr := parseSugarConfig(node)
	if configErr != nil {
		return workflowcompile.NodeExpansion{}, true, []diagnostic.Diagnostic{sugarDiagnostic(node.Source, configErr.Error())}
	}
	if modifierErr := validateSugarModifiers(request.Graph, node); modifierErr != nil {
		return workflowcompile.NodeExpansion{}, true, []diagnostic.Diagnostic{sugarDiagnostic(node.Source, modifierErr.Error())}
	}

	childTimeout := cloneTimeout(node.Timeout)
	if waitPolicy != nil && childTimeout != nil {
		childTimeout.Wait = ""
		if *childTimeout == (graph.TimeoutPolicy{}) {
			childTimeout = nil
		}
	}
	composition, err := Compose(CompositionRequest{
		NodeID: node.ID, DisplayName: node.DisplayName, Config: config,
		ParentClose: parentClose, Timeout: childTimeout, Retry: node.Retry,
		Wait: waitPolicy, InputBindings: node.InputBindings, Source: node.Source,
	})
	if err != nil {
		return workflowcompile.NodeExpansion{}, true, []diagnostic.Diagnostic{sugarDiagnostic(node.Source, err.Error())}
	}

	composition.Launch.Needs = cloneNeeds(node.Needs)
	composition.Launch.ReadyWhen = node.ReadyWhen
	composition.Launch.If = cloneExpression(node.If)
	composition.Launch.Concurrency = cloneConcurrency(node.Concurrency)
	composition.Launch.Source = cloneSource(node.Source)
	if composition.Wait == nil {
		composition.Launch.Metadata = node.Metadata
		composition.Launch.Provenance = node.Provenance
	} else {
		composition.Wait.Metadata = node.Metadata
		composition.Wait.Provenance = node.Provenance
		if node.Timeout != nil && waitPolicy != nil {
			composition.Wait.Timeout = &graph.TimeoutPolicy{Wait: waitPolicy.Timeout}
		}
	}

	nodes := []graph.Node{composition.Launch}
	exitID := composition.Launch.ID
	if composition.Wait != nil {
		nodes = append(nodes, *composition.Wait)
		exitID = composition.Wait.ID
	}
	return workflowcompile.NodeExpansion{
		EntryNodeID: composition.Launch.ID,
		ExitNodeID:  exitID,
		Nodes:       nodes,
		Edges:       composition.Edges,
		Definitions: []workflowcompile.ResolvedDefinition{composition.Definition},
	}, true, nil
}

// Compile lowers workflow source with the agent_launch source extension. The
// returned plan contains every generated child definition as serialized,
// restart-safe bundled material.
func Compile(source *workflowcompile.Source) workflowcompile.CompileResult {
	return workflowcompile.CompileWithOptions(source, workflowcompile.CompileOptions{NodeExpanders: []workflowcompile.NodeExpander{SourceExpander{}}})
}

func parseSugarConfig(node graph.Node) (graph.Config, *WaitPolicy, graph.ParentClosePolicy, error) {
	if node.Config == nil {
		return nil, nil, "", fmt.Errorf("agent_launch config must be an object")
	}
	allowed := map[string]struct{}{
		"substrate": {}, "logical_agent_id": {}, "launch_id": {}, "prompt_append": {}, "parent_close": {}, "wait": {},
	}
	keys := make([]string, 0, len(node.Config))
	for key := range node.Config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowed[key]; !ok {
			return nil, nil, "", fmt.Errorf("agent_launch.%s is unsupported", key)
		}
	}
	required := func(name string) (string, error) {
		raw, exists := node.Config[name]
		if !exists {
			return "", fmt.Errorf("agent_launch.%s is required", name)
		}
		value, ok := raw.(string)
		if !ok || !stableText(value, 4096) {
			return "", fmt.Errorf("agent_launch.%s must be stable non-empty text", name)
		}
		return value, nil
	}
	substrate, err := required("substrate")
	if err != nil {
		return nil, nil, "", err
	}
	logicalAgentID, err := required("logical_agent_id")
	if err != nil {
		return nil, nil, "", err
	}
	launchID := node.ID
	if raw, exists := node.Config["launch_id"]; exists {
		var ok bool
		launchID, ok = raw.(string)
		if !ok || !stableText(launchID, 4096) {
			return nil, nil, "", fmt.Errorf("agent_launch.launch_id must be stable non-empty text")
		}
	}
	prompt := ""
	if raw, exists := node.Config["prompt_append"]; exists {
		var ok bool
		prompt, ok = raw.(string)
		if !ok || !optionalText(prompt, 1<<20) {
			return nil, nil, "", fmt.Errorf("agent_launch.prompt_append must be stable text")
		}
	}
	parentClose := graph.ParentCloseCancel
	if raw, exists := node.Config["parent_close"]; exists {
		value, ok := raw.(string)
		parentClose = graph.ParentClosePolicy(value)
		if !ok || !parentClose.Valid() {
			return nil, nil, "", fmt.Errorf("agent_launch.parent_close must be cancel, abandon, or request_cancel")
		}
	}
	wait, err := parseSugarWait(node.Config["wait"], node.Timeout)
	if err != nil {
		return nil, nil, "", err
	}
	direct := graph.Config{"substrate": substrate, "logical_agent_id": logicalAgentID, "launch_id": launchID}
	if prompt != "" {
		direct["prompt"] = prompt
	}
	return direct, wait, parentClose, nil
}

func parseSugarWait(raw any, timeout *graph.TimeoutPolicy) (*WaitPolicy, error) {
	if raw == nil {
		return nil, nil
	}
	switch value := raw.(type) {
	case bool:
		if !value {
			return nil, nil
		}
		if timeout == nil || timeout.Wait == "" {
			return nil, fmt.Errorf("agent_launch.wait: true requires timeout.wait")
		}
		if err := positiveDuration("timeout.wait", timeout.Wait); err != nil {
			return nil, err
		}
		return &WaitPolicy{Timeout: timeout.Wait}, nil
	case map[string]any:
		if len(value) != 1 {
			return nil, fmt.Errorf("agent_launch.wait object must contain only timeout")
		}
		rawTimeout, exists := value["timeout"]
		text, ok := rawTimeout.(string)
		if !exists || !ok {
			return nil, fmt.Errorf("agent_launch.wait.timeout is required as a duration string")
		}
		wait := graph.Duration(text)
		if err := positiveDuration("agent_launch.wait.timeout", wait); err != nil {
			return nil, err
		}
		if timeout != nil && timeout.Wait != "" && timeout.Wait != wait {
			return nil, fmt.Errorf("agent_launch.wait.timeout conflicts with timeout.wait")
		}
		return &WaitPolicy{Timeout: wait}, nil
	default:
		return nil, fmt.Errorf("agent_launch.wait must be false, true, or an object containing timeout")
	}
}

func validateSugarModifiers(value graph.Graph, node graph.Node) error {
	if node.KindVersion != "" && node.KindVersion != KindVersion {
		return fmt.Errorf("agent_launch kind_version must be %q when present", KindVersion)
	}
	if node.ForEach != nil || node.Finally != nil || node.Switch != nil || len(node.Catch) != 0 {
		return fmt.Errorf("agent_launch does not support for_each, catch, switch, finally, or continue_on_error; expand it explicitly")
	}
	if len(node.Outputs) != 0 {
		return fmt.Errorf("agent_launch owns its closed output declarations")
	}
	if len(node.Effects) != 0 || node.Idempotency != nil {
		return fmt.Errorf("agent_launch owns conservative effects and runtime idempotency")
	}
	if node.Call != nil || node.Verification != nil || node.Memoization != nil || node.Durability != nil || node.Service != nil || node.Compensation != nil ||
		len(node.Policy) != 0 || len(node.Extensions) != 0 || !reflect.DeepEqual(node.Target, graph.ExecutionTargetRequirements{}) {
		return fmt.Errorf("agent_launch contains unsupported execution modifiers; expand it explicitly")
	}
	for _, candidate := range value.Nodes {
		if candidate.ID == node.ID {
			continue
		}
		for _, rule := range candidate.Catch {
			if containsID(rule.Targets, node.ID) {
				return fmt.Errorf("agent_launch cannot be a catch target while expanded; target an explicit call node")
			}
		}
		if candidate.Switch != nil {
			for _, arm := range candidate.Switch.Arms {
				if containsID(arm.Targets, node.ID) {
					return fmt.Errorf("agent_launch cannot be a switch target while expanded; target an explicit call node")
				}
			}
			if containsID(candidate.Switch.Default, node.ID) {
				return fmt.Errorf("agent_launch cannot be a switch target while expanded; target an explicit call node")
			}
		}
	}
	return nil
}

func containsID(values []string, target string) bool {
	for _, value := range values {
		if graph.NormalizeID(value) == graph.NormalizeID(target) {
			return true
		}
	}
	return false
}

func sugarDiagnostic(source *graph.SourceRef, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: workflowcompile.CodeNodeExpansion,
		Message: "agent_launch expansion failed: " + message,
		Source:  cloneSource(source),
		Remediation: &diagnostic.Remediation{
			Message: "Use the supported agent_launch shape or author explicit call and wait_for nodes.",
		},
	}
}

func cloneNeeds(input []graph.Need) []graph.Need {
	result := make([]graph.Need, len(input))
	for index := range input {
		result[index] = input[index]
		result[index].Source = cloneSource(input[index].Source)
	}
	return result
}

func cloneExpression(input *graph.Expression) *graph.Expression {
	if input == nil {
		return nil
	}
	result := *input
	result.Source = cloneSource(input.Source)
	return &result
}

func cloneConcurrency(input []graph.ConcurrencyClaim) []graph.ConcurrencyClaim {
	return append([]graph.ConcurrencyClaim(nil), input...)
}

var _ workflowcompile.NodeExpander = SourceExpander{}
