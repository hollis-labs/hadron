package compile

import (
	"strconv"

	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

var executorSourceKinds = map[string]string{
	"agent_launch": "agent_launch",
	"checkpoint":   "checkpoint",
	"cmd":          "cmd",
	"emit":         "emit",
	"http":         "http",
	"http_call":    "http",
	"human_gate":   "human_gate",
	"llm":          "llm",
	"mcp":          "mcp",
	"mcp_call":     "mcp",
	"message_wait": "message_wait",
	"script":       "script",
	"sleep":        "sleep",
	"transform":    "transform",
	"wait_for":     "wait_for",
}

var supportedNodeFields = []string{
	"id", "name", "display_name", "kind", "kind_version", "needs", "ready_when",
	"if", "for_each", "concurrency", "config", "with", "outputs", "effects", "retry",
	"idempotency", "timeout", "catch", "finally", "switch", "call", "metadata",
	"agent_launch", "checkpoint", "cmd", "emit", "http", "http_call", "human_gate",
	"llm", "mcp", "mcp_call", "message_wait", "script", "sleep", "transform", "wait_for",
}

func (l *lowerer) lowerNodes(node *yaml.Node, path []string, sourceMap *graph.SourceMap, forceFinally bool) ([]graph.Node, []graph.Edge) {
	items := l.sequence(node, path)
	nodes := make([]graph.Node, 0, len(items))
	var edges []graph.Edge
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		compiled, nodeEdges := l.lowerNode(item, itemPath)
		if forceFinally && compiled.Finally == nil {
			compiled.Finally = &graph.FinallySpec{}
		}
		nodes = append(nodes, compiled)
		edges = append(edges, nodeEdges...)
		if compiled.ID != "" {
			sourceMap.Nodes[compiled.ID] = l.location(item, itemPath)
		}
		for j, edge := range nodeEdges {
			needPath := appendPath(appendPath(itemPath, "needs"), strconv.Itoa(j))
			needNode, ok := l.source.Node(needPath...)
			if !ok {
				needNode = item
			}
			sourceMap.Edges[EdgeSourceKey(edge.From, edge.To, edge.Kind)] = l.location(needNode, needPath)
		}
	}
	return nodes, edges
}

func (l *lowerer) lowerNode(node *yaml.Node, path []string) (graph.Node, []graph.Edge) {
	fields := l.mapping(node, path, supportedNodeFields...)
	identity, ok := fields["id"]
	if !ok {
		identity, ok = fields["name"]
	}
	if !ok {
		l.invalidShape(node, path, "step.id or step.name is required")
	}
	var compiled graph.Node
	nodeRef := l.location(node, path)
	compiled.Source = &nodeRef
	if ok {
		compiled.ID = l.normalizeID(identity.value, identity.path)
	}
	if name, exists := fields["name"]; exists {
		compiled.DisplayName = l.string(name.value, name.path)
	}
	if display, exists := fields["display_name"]; exists {
		compiled.DisplayName = l.string(display.value, display.path)
	}
	if version, exists := fields["kind_version"]; exists {
		compiled.KindVersion = l.string(version.value, version.path)
	}

	executors := executorDeclarations(node, path)
	_, explicitKind := fields["kind"]
	_, explicitConfig := fields["config"]
	if len(executors) > 1 {
		for _, executor := range executors[1:] {
			l.addDiagnostic(CodeInvalidWorkflowShape, executor.key, executor.path,
				"a step may declare only one executor shorthand",
				"Keep one executor field or use explicit kind and config fields.")
		}
	}
	if len(executors) != 0 && (explicitKind || explicitConfig) {
		l.addDiagnostic(CodeInvalidWorkflowShape, executors[0].key, executors[0].path,
			"executor shorthand cannot be combined with explicit kind or config",
			"Use either one executor shorthand or the explicit kind and config fields.")
	}
	if len(executors) != 0 {
		compiled.Kind = executorSourceKinds[executors[0].key.Value]
		compiled.Config = l.executorConfig(executors[0].key.Value, executors[0].value, executors[0].path)
	} else {
		kind, exists := fields["kind"]
		if !exists {
			l.invalidShape(node, path, "step.kind or one executor shorthand is required")
		} else {
			compiled.Kind = l.string(kind.value, kind.path)
		}
		if config, exists := fields["config"]; exists {
			compiled.Config = l.config(config.value, config.path)
		}
	}

	var edges []graph.Edge
	if needs, exists := fields["needs"]; exists {
		compiled.Needs, edges = l.lowerNeeds(needs.value, needs.path, compiled.ID)
	}
	if ready, exists := fields["ready_when"]; exists {
		compiled.ReadyWhen = graph.ReadyRule(l.string(ready.value, ready.path))
	}
	if condition, exists := fields["if"]; exists {
		expression := l.expression(condition.value, condition.path)
		compiled.If = &expression
	}
	if forEach, exists := fields["for_each"]; exists {
		compiled.ForEach = l.lowerForEach(forEach.value, forEach.path)
	}
	if concurrency, exists := fields["concurrency"]; exists {
		l.lowerConcurrency(concurrency.value, concurrency.path, &compiled)
	}
	if bindings, exists := fields["with"]; exists {
		compiled.InputBindings = l.lowerBindings(bindings.value, bindings.path)
	}
	if outputs, exists := fields["outputs"]; exists {
		compiled.Outputs = l.lowerOutputs(outputs.value, outputs.path, nil, false)
	}
	if effects, exists := fields["effects"]; exists {
		compiled.Effects = l.lowerEffects(effects.value, effects.path)
	}
	var retryIdempotency *graph.IdempotencySpec
	if retry, exists := fields["retry"]; exists {
		compiled.Retry, retryIdempotency = l.lowerRetry(retry.value, retry.path)
	}
	if idempotency, exists := fields["idempotency"]; exists {
		if retryIdempotency != nil {
			l.invalidShape(idempotency.value, idempotency.path, "idempotency conflicts with retry.idempotency_key")
		}
		compiled.Idempotency = l.lowerIdempotency(idempotency.value, idempotency.path)
	} else {
		compiled.Idempotency = retryIdempotency
	}
	if timeout, exists := fields["timeout"]; exists {
		compiled.Timeout = l.lowerTimeout(timeout.value, timeout.path)
	}
	if catches, exists := fields["catch"]; exists {
		compiled.Catch = l.lowerCatch(catches.value, catches.path)
	}
	if finally, exists := fields["finally"]; exists {
		compiled.Finally = l.lowerFinally(finally.value, finally.path)
	}
	if switchField, exists := fields["switch"]; exists {
		compiled.Switch = l.lowerSwitch(switchField.value, switchField.path)
	}
	if call, exists := fields["call"]; exists {
		if len(executors) != 0 {
			l.invalidShape(call.value, call.path, "call cannot be combined with an executor shorthand")
		}
		compiled.Call = l.lowerCall(call.value, call.path)
		if compiled.Kind != "" && compiled.Kind != "call" {
			l.invalidShape(call.value, call.path, "call requires kind: call")
		}
	}
	if metadata, exists := fields["metadata"]; exists {
		compiled.Metadata = l.metadata(metadata.value, metadata.path)
	}
	return compiled, edges
}

func executorDeclarations(node *yaml.Node, path []string) []sourceField {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	var fields []sourceField
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if _, ok := executorSourceKinds[key.Value]; ok {
			fields = append(fields, sourceField{key: key, value: value, path: appendPath(path, key.Value)})
		}
	}
	return fields
}

func (l *lowerer) executorConfig(kind string, node *yaml.Node, path []string) graph.Config {
	if node.Kind == yaml.MappingNode {
		return l.config(node, path)
	}
	key := "value"
	switch kind {
	case "cmd":
		key = "command"
	case "sleep":
		key = "duration"
	}
	return graph.Config{key: l.jsonValue(node, path)}
}

func (l *lowerer) config(node *yaml.Node, path []string) graph.Config {
	value := l.jsonValue(node, path)
	object, ok := value.(map[string]any)
	if !ok {
		l.invalidShape(node, path, "config must be a JSON object")
		return nil
	}
	return graph.Config(object)
}

func (l *lowerer) lowerNeeds(node *yaml.Node, path []string, target string) ([]graph.Need, []graph.Edge) {
	items := l.sequence(node, path)
	needs := make([]graph.Need, 0, len(items))
	edges := make([]graph.Edge, 0, len(items))
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		need := graph.Need{Kind: graph.EdgeControl}
		needRef := l.location(item, itemPath)
		need.Source = &needRef
		if item.Kind == yaml.ScalarNode {
			need.Node = l.normalizeID(item, itemPath)
		} else {
			fields := l.mapping(item, itemPath, "node", "kind")
			nodeField, ok := fields["node"]
			if !ok {
				l.invalidShape(item, itemPath, "need.node is required")
			} else {
				need.Node = l.normalizeID(nodeField.value, nodeField.path)
			}
			if kind, exists := fields["kind"]; exists {
				need.Kind = graph.EdgeKind(l.string(kind.value, kind.path))
			}
		}
		needs = append(needs, need)
		edges = append(edges, graph.Edge{From: need.Node, To: target, Kind: need.Kind, Source: &needRef})
	}
	return needs, edges
}

func (l *lowerer) lowerForEach(node *yaml.Node, path []string) *graph.ForEachSpec {
	if node.Kind == yaml.ScalarNode {
		return &graph.ForEachSpec{Items: l.expression(node, path)}
	}
	fields := l.mapping(node, path, "items", "item_name", "index_name", "max_concurrency", "tolerate")
	items, ok := fields["items"]
	if !ok {
		l.invalidShape(node, path, "for_each.items is required")
		return &graph.ForEachSpec{}
	}
	spec := &graph.ForEachSpec{Items: l.expression(items.value, items.path)}
	if field, exists := fields["item_name"]; exists {
		spec.ItemName = l.string(field.value, field.path)
	}
	if field, exists := fields["index_name"]; exists {
		spec.IndexName = l.string(field.value, field.path)
	}
	if field, exists := fields["max_concurrency"]; exists {
		spec.MaxConcurrency = l.integer(field.value, field.path)
	}
	if field, exists := fields["tolerate"]; exists {
		tolerateFields := l.mapping(field.value, field.path, "count", "percentage")
		spec.Tolerate = &graph.ToleratedFailurePolicy{}
		if count, exists := tolerateFields["count"]; exists {
			spec.Tolerate.Count = l.integer(count.value, count.path)
		}
		if percentage, exists := tolerateFields["percentage"]; exists {
			spec.Tolerate.Percentage = l.number(percentage.value, percentage.path)
		}
	}
	return spec
}

func (l *lowerer) lowerConcurrency(node *yaml.Node, path []string, compiled *graph.Node) {
	if node.Kind == yaml.ScalarNode {
		if compiled.ForEach == nil {
			l.invalidShape(node, path, "integer concurrency requires for_each")
			return
		}
		compiled.ForEach.MaxConcurrency = l.integer(node, path)
		return
	}
	items := l.sequence(node, path)
	compiled.Concurrency = make([]graph.ConcurrencyClaim, 0, len(items))
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		fields := l.mapping(item, itemPath, "resource", "amount")
		resource, ok := fields["resource"]
		if !ok {
			l.invalidShape(item, itemPath, "concurrency.resource is required")
			continue
		}
		claim := graph.ConcurrencyClaim{Resource: l.string(resource.value, resource.path)}
		if amount, exists := fields["amount"]; exists {
			claim.Amount = l.integer(amount.value, amount.path)
		}
		compiled.Concurrency = append(compiled.Concurrency, claim)
	}
}

func (l *lowerer) lowerEffects(node *yaml.Node, path []string) graph.EffectSet {
	values := l.strings(node, path)
	effects := make(graph.EffectSet, len(values))
	for i, value := range values {
		effects[i] = graph.Effect(value)
	}
	return effects
}

func (l *lowerer) lowerRetry(node *yaml.Node, path []string) (*graph.RetryPolicy, *graph.IdempotencySpec) {
	fields := l.mapping(node, path, "attempts", "backoff", "on", "idempotency_key")
	retry := &graph.RetryPolicy{}
	if attempts, ok := fields["attempts"]; ok {
		retry.Attempts = l.integer(attempts.value, attempts.path)
	} else {
		l.invalidShape(node, path, "retry.attempts is required")
	}
	if backoff, ok := fields["backoff"]; ok {
		if backoff.value.Kind == yaml.ScalarNode {
			retry.Backoff.Strategy = graph.BackoffStrategy(l.string(backoff.value, backoff.path))
		} else {
			backoffFields := l.mapping(backoff.value, backoff.path, "strategy", "initial_delay", "max_delay", "multiplier")
			if field, exists := backoffFields["strategy"]; exists {
				retry.Backoff.Strategy = graph.BackoffStrategy(l.string(field.value, field.path))
			}
			if field, exists := backoffFields["initial_delay"]; exists {
				retry.Backoff.InitialDelay = graph.Duration(l.string(field.value, field.path))
			}
			if field, exists := backoffFields["max_delay"]; exists {
				retry.Backoff.MaxDelay = graph.Duration(l.string(field.value, field.path))
			}
			if field, exists := backoffFields["multiplier"]; exists {
				retry.Backoff.Multiplier = l.number(field.value, field.path)
			}
		}
	}
	if on, ok := fields["on"]; ok {
		retry.On = l.strings(on.value, on.path)
	}
	var idempotency *graph.IdempotencySpec
	if key, ok := fields["idempotency_key"]; ok {
		expression := l.expression(key.value, key.path)
		idempotency = &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed, Key: &expression}
	}
	return retry, idempotency
}

func (l *lowerer) lowerIdempotency(node *yaml.Node, path []string) *graph.IdempotencySpec {
	fields := l.mapping(node, path, "mode", "key", "scope", "extensions")
	spec := &graph.IdempotencySpec{}
	if mode, ok := fields["mode"]; ok {
		spec.Mode = graph.IdempotencyMode(l.string(mode.value, mode.path))
	}
	if key, ok := fields["key"]; ok {
		expression := l.expression(key.value, key.path)
		spec.Key = &expression
		if spec.Mode == "" {
			spec.Mode = graph.IdempotencyKeyed
		}
	}
	if scope, ok := fields["scope"]; ok {
		spec.Scope = l.string(scope.value, scope.path)
	}
	if extensions, ok := fields["extensions"]; ok {
		spec.Extensions = l.config(extensions.value, extensions.path)
	}
	return spec
}

func (l *lowerer) lowerTimeout(node *yaml.Node, path []string) *graph.TimeoutPolicy {
	fields := l.mapping(node, path, "queue", "execution", "wait", "heartbeat", "schedule_to_close")
	timeout := &graph.TimeoutPolicy{}
	if field, ok := fields["queue"]; ok {
		timeout.Queue = graph.Duration(l.string(field.value, field.path))
	}
	if field, ok := fields["execution"]; ok {
		timeout.Execution = graph.Duration(l.string(field.value, field.path))
	}
	if field, ok := fields["wait"]; ok {
		timeout.Wait = graph.Duration(l.string(field.value, field.path))
	}
	if field, ok := fields["heartbeat"]; ok {
		timeout.Heartbeat = graph.Duration(l.string(field.value, field.path))
	}
	if field, ok := fields["schedule_to_close"]; ok {
		timeout.ScheduleToClose = graph.Duration(l.string(field.value, field.path))
	}
	return timeout
}

func (l *lowerer) lowerCatch(node *yaml.Node, path []string) []graph.CatchRule {
	items := l.sequence(node, path)
	rules := make([]graph.CatchRule, 0, len(items))
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		fields := l.mapping(item, itemPath, "errors", "when", "targets", "bind_as")
		var rule graph.CatchRule
		ruleRef := l.location(item, itemPath)
		rule.Source = &ruleRef
		if errorsField, ok := fields["errors"]; ok {
			rule.Errors = l.strings(errorsField.value, errorsField.path)
		}
		if when, ok := fields["when"]; ok {
			expression := l.expression(when.value, when.path)
			rule.When = &expression
		}
		if targets, ok := fields["targets"]; ok {
			rule.Targets = l.normalizedIDs(targets.value, targets.path)
		} else {
			l.invalidShape(item, itemPath, "catch.targets is required")
		}
		if bindAs, ok := fields["bind_as"]; ok {
			rule.BindAs = l.string(bindAs.value, bindAs.path)
		}
		rules = append(rules, rule)
	}
	return rules
}

func (l *lowerer) lowerFinally(node *yaml.Node, path []string) *graph.FinallySpec {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!bool" {
		if !l.boolean(node, path) {
			return nil
		}
		return &graph.FinallySpec{}
	}
	fields := l.mapping(node, path, "scope")
	spec := &graph.FinallySpec{}
	if scope, ok := fields["scope"]; ok {
		spec.Scope = l.normalizedIDs(scope.value, scope.path)
	}
	return spec
}

func (l *lowerer) lowerSwitch(node *yaml.Node, path []string) *graph.SwitchSpec {
	fields := l.mapping(node, path, "arms", "default")
	spec := &graph.SwitchSpec{}
	if arms, ok := fields["arms"]; ok {
		items := l.sequence(arms.value, arms.path)
		spec.Arms = make([]graph.SwitchArm, 0, len(items))
		for i, item := range items {
			itemPath := appendPath(arms.path, strconv.Itoa(i))
			armFields := l.mapping(item, itemPath, "when", "targets")
			var arm graph.SwitchArm
			armRef := l.location(item, itemPath)
			arm.Source = &armRef
			if when, exists := armFields["when"]; exists {
				arm.When = l.expression(when.value, when.path)
			} else {
				l.invalidShape(item, itemPath, "switch arm.when is required")
			}
			if targets, exists := armFields["targets"]; exists {
				arm.Targets = l.normalizedIDs(targets.value, targets.path)
			} else {
				l.invalidShape(item, itemPath, "switch arm.targets is required")
			}
			spec.Arms = append(spec.Arms, arm)
		}
	} else {
		l.invalidShape(node, path, "switch.arms is required")
	}
	if defaults, ok := fields["default"]; ok {
		spec.Default = l.normalizedIDs(defaults.value, defaults.path)
	}
	return spec
}

func (l *lowerer) lowerCall(node *yaml.Node, path []string) *graph.CallSpec {
	fields := l.mapping(node, path, "definition", "mode", "on_parent_close")
	call := &graph.CallSpec{}
	if definition, ok := fields["definition"]; ok {
		call.Definition = l.lowerDefinition(definition.value, definition.path)
	} else {
		l.invalidShape(node, path, "call.definition is required")
	}
	if mode, ok := fields["mode"]; ok {
		call.Mode = graph.CallMode(l.string(mode.value, mode.path))
	} else {
		l.invalidShape(node, path, "call.mode is required")
	}
	if parentClose, ok := fields["on_parent_close"]; ok {
		call.OnParentClose = graph.ParentClosePolicy(l.string(parentClose.value, parentClose.path))
	}
	return call
}

func (l *lowerer) lowerDefinition(node *yaml.Node, path []string) graph.DefinitionRef {
	fields := l.mapping(node, path, "authority", "kind", "id", "locator", "version", "digest", "provenance")
	var definition graph.DefinitionRef
	if field, ok := fields["authority"]; ok {
		definition.Authority = l.string(field.value, field.path)
	}
	if field, ok := fields["kind"]; ok {
		definition.Kind = l.string(field.value, field.path)
	}
	if field, ok := fields["id"]; ok {
		definition.ID = l.normalizeID(field.value, field.path)
	}
	if field, ok := fields["locator"]; ok {
		definition.Locator = l.string(field.value, field.path)
	}
	if field, ok := fields["version"]; ok {
		definition.Version = l.string(field.value, field.path)
	}
	if field, ok := fields["digest"]; ok {
		definition.Digest = l.string(field.value, field.path)
	}
	if field, ok := fields["provenance"]; ok {
		provenance := l.lowerReferencedProvenance(field.value, field.path)
		definition.Provenance = &provenance
	}
	return definition
}

func (l *lowerer) lowerReferencedProvenance(node *yaml.Node, path []string) graph.Provenance {
	fields := l.mapping(node, path, "authority", "origin", "locator", "revision", "digest", "parents", "metadata")
	var provenance graph.Provenance
	if field, ok := fields["authority"]; ok {
		provenance.Authority = l.string(field.value, field.path)
	}
	if field, ok := fields["origin"]; ok {
		provenance.Origin = l.string(field.value, field.path)
	}
	if field, ok := fields["locator"]; ok {
		provenance.Locator = l.string(field.value, field.path)
	}
	if field, ok := fields["revision"]; ok {
		provenance.Revision = l.string(field.value, field.path)
	}
	if field, ok := fields["digest"]; ok {
		provenance.Digest = l.string(field.value, field.path)
	}
	if field, ok := fields["metadata"]; ok {
		provenance.Metadata = l.metadata(field.value, field.path)
	}
	if field, ok := fields["parents"]; ok {
		provenance = l.lowerProvenanceParents(field.value, field.path, provenance)
	}
	return provenance
}

func (l *lowerer) lowerProvenanceParents(node *yaml.Node, path []string, provenance graph.Provenance) graph.Provenance {
	items := l.sequence(node, path)
	provenance.Parents = make([]graph.ProvenanceRef, 0, len(items))
	for i, item := range items {
		itemPath := appendPath(path, strconv.Itoa(i))
		fields := l.mapping(item, itemPath, "authority", "locator", "digest")
		var parent graph.ProvenanceRef
		if field, ok := fields["authority"]; ok {
			parent.Authority = l.string(field.value, field.path)
		}
		if field, ok := fields["locator"]; ok {
			parent.Locator = l.string(field.value, field.path)
		}
		if field, ok := fields["digest"]; ok {
			parent.Digest = l.string(field.value, field.path)
		}
		provenance.Parents = append(provenance.Parents, parent)
	}
	return provenance
}

func (l *lowerer) normalizedIDs(node *yaml.Node, path []string) []string {
	items := l.sequence(node, path)
	values := make([]string, 0, len(items))
	for i, item := range items {
		values = append(values, l.normalizeID(item, appendPath(path, strconv.Itoa(i))))
	}
	return values
}
