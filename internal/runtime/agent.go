package runtime

import (
	"context"
	"fmt"
	"sort"

	"github.com/hollis-labs/hadron/internal/agentsettings"
	"github.com/hollis-labs/hadron/internal/tetherhost"
	agentadapter "github.com/hollis-labs/libs/workflow/engine/adapters/agent"
	workflowcompile "github.com/hollis-labs/libs/workflow/engine/compile"
	"github.com/hollis-labs/libs/workflow/engine/diagnostic"
	"github.com/hollis-labs/libs/workflow/engine/graph"
)

// tetherSessionSubstrates returns the configured tether_session substrates.
func tetherSessionSubstrates(substrates map[string]agentsettings.Substrate) map[string]agentsettings.Substrate {
	result := make(map[string]agentsettings.Substrate)
	for name, substrate := range substrates {
		if substrate.Kind == agentsettings.KindTetherSession {
			result[name] = substrate
		}
	}
	return result
}

// newTetherSessionHost returns the Tether-backed session host, or nil when
// agent_session@v1 stays gated because no tether_session substrate is
// configured. Without an injected client it builds the go-tether-client
// adapter for the substrates' muxd endpoint; that never dials, so hadrond
// starts while muxd is down and agent steps see it as unreachable.
func newTetherSessionHost(dataDir string, substrates map[string]agentsettings.Substrate, options runtimeOptions) (*tetherhost.Host, error) {
	tether := tetherSessionSubstrates(substrates)
	if len(tether) == 0 {
		return nil, nil
	}
	client := options.tetherClient
	if client == nil {
		endpoint, err := sharedTetherEndpoint(tether)
		if err != nil {
			return nil, err
		}
		adapter, err := tetherhost.NewTetherClient(endpoint)
		if err != nil {
			return nil, err
		}
		client = adapter
	}
	secret, err := tetherhost.LoadOrCreateSecret(dataDir)
	if err != nil {
		return nil, fmt.Errorf("agent result secret: %w", err)
	}
	return tetherhost.New(tetherhost.Options{Client: client, Substrates: tether, Secret: secret, Now: options.tetherNow})
}

// sharedTetherEndpoint returns the one muxd endpoint the tether_session
// substrates name. The session host holds a single client, so substrates
// naming different daemons are refused rather than silently merged.
func sharedTetherEndpoint(substrates map[string]agentsettings.Substrate) (string, error) {
	names := make([]string, 0, len(substrates))
	for name := range substrates {
		names = append(names, name)
	}
	sort.Strings(names)
	endpoint := ""
	for index, name := range names {
		if substrates[name].Tether == nil {
			return "", fmt.Errorf("agent substrate %q has no tether settings", name)
		}
		current := substrates[name].Tether.EffectiveEndpoint()
		if index > 0 && current != endpoint {
			return "", fmt.Errorf("tether_session substrates %q and %q name different muxd endpoints; one hadron talks to one muxd", names[0], name)
		}
		endpoint = current
	}
	return endpoint, nil
}

// tetherAgentLaunchExpander is agentadapter.SourceExpander with one added
// refusal: an agent_launch that binds typed inputs on a tether_session
// substrate. The Tether session host cannot deliver typed inputs yet, and
// bundled child graphs are not policy-validated when the root compiles, so
// the refusal must happen here, before a run exists. Expansion is otherwise
// unchanged.
type tetherAgentLaunchExpander struct {
	inner      agentadapter.SourceExpander
	substrates map[string]agentsettings.Substrate
}

func (e tetherAgentLaunchExpander) Name() string { return e.inner.Name() }

func (e tetherAgentLaunchExpander) ExpandNode(request workflowcompile.NodeExpansionRequest) (workflowcompile.NodeExpansion, bool, []diagnostic.Diagnostic) {
	node := request.Node
	if node.Kind == agentadapter.SugarKindName && len(node.InputBindings) != 0 && e.tetherSubstrate(node.Config) {
		finding := typedInputsDiagnostic("agent_launch node "+node.ID, sortedBindingNames(node.InputBindings), workflowcompile.CodeNodeExpansion)
		if node.Source != nil {
			cloned := *node.Source
			finding.Source = &cloned
		}
		return workflowcompile.NodeExpansion{}, true, []diagnostic.Diagnostic{finding}
	}
	return e.inner.ExpandNode(request)
}

func (e tetherAgentLaunchExpander) tetherSubstrate(config graph.Config) bool {
	name, _ := config["substrate"].(string)
	substrate, ok := e.substrates[name]
	return ok && substrate.Kind == agentsettings.KindTetherSession
}

// tetherAgentInputsPolicy refuses a directly authored agent_session node on a
// tether_session substrate that binds any input other than the reserved
// parent-correlation.
func tetherAgentInputsPolicy(substrates map[string]agentsettings.Substrate) workflowcompile.PolicyHook {
	expander := tetherAgentLaunchExpander{substrates: substrates}
	return workflowcompile.PolicyHookFunc(func(_ context.Context, input workflowcompile.NodeValidation) []diagnostic.Diagnostic {
		if input.Node.Kind != agentadapter.KindName || !expander.tetherSubstrate(input.Node.Config) {
			return nil
		}
		var typed []string
		for _, name := range sortedBindingNames(input.Node.InputBindings) {
			if name != agentadapter.ParentCorrelationInput {
				typed = append(typed, name)
			}
		}
		if len(typed) == 0 {
			return nil
		}
		return []diagnostic.Diagnostic{typedInputsDiagnostic("agent_session node "+input.Node.ID, typed, workflowcompile.CodePolicyViolation)}
	})
}

func typedInputsDiagnostic(subject string, names []string, code diagnostic.Code) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError, Code: code,
		Message: fmt.Sprintf("%s binds inputs %v: %s", subject, names, tetherhost.InputsUnsupportedMessage),
		Remediation: &diagnostic.Remediation{
			Message: "Remove the step's with bindings and put the task in prompt_append; the agent reads context itself.",
		},
	}
}

func sortedBindingNames(bindings map[string]graph.Binding) []string {
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
