package compile

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

const activationAuthorityProject = "project"

var activationKinds = map[string]struct{}{
	"webhook":  {},
	"schedule": {},
	"message":  {},
	"file":     {},
	"event":    {},
	"one_shot": {},
}

var forbiddenActivationOperationalFields = map[string]struct{}{
	"registration_id": {},
	"enabled":         {},
	"expires_at":      {},
	"fired_count":     {},
	"fire_history":    {},
	"last_fired_at":   {},
	"next_fire_at":    {},
	"callback_token":  {},
	"callback_secret": {},
	"secret":          {},
	"secret_hash":     {},
	"host_owner":      {},
}

func (l *lowerer) lowerActivations(
	node *yaml.Node,
	pathParts []string,
	sourceMap map[string]graph.SourceRef,
	workflowProvenance graph.Provenance,
) []graph.ActivationDeclaration {
	if node == nil || node.Kind != yaml.MappingNode {
		l.invalidActivation(node, pathParts, "on must be a mapping of activation kinds")
		return nil
	}
	if len(node.Content) == 0 {
		l.invalidActivation(node, pathParts, "on must contain at least one activation declaration")
		return nil
	}

	activations := make([]graph.ActivationDeclaration, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		kindPath := appendPath(pathParts, key.Value)
		if _, ok := activationKinds[key.Value]; !ok {
			l.addDiagnostic(CodeUnsupportedSourceField, key, kindPath,
				fmt.Sprintf("source activation kind %q is not supported", key.Value),
				"Use webhook, schedule, message, file, event, or one_shot.")
			continue
		}
		if value.Kind == yaml.SequenceNode {
			if len(value.Content) == 0 {
				l.invalidActivation(value, kindPath, fmt.Sprintf("%s declaration sequence must not be empty", key.Value))
				continue
			}
			for index, item := range value.Content {
				itemPath := appendPath(kindPath, strconv.Itoa(index))
				activation, ok := l.lowerActivation(key.Value, item, itemPath, "", workflowProvenance)
				if ok {
					activations = append(activations, activation)
				}
			}
			continue
		}
		activation, ok := l.lowerActivation(key.Value, value, kindPath, key.Value, workflowProvenance)
		if ok {
			activations = append(activations, activation)
		}
	}

	firstByID := make(map[string]graph.SourceRef, len(activations))
	unique := activations[:0]
	for _, activation := range activations {
		if first, exists := firstByID[activation.ID]; exists {
			l.addActivationDiagnostic(
				CodeDuplicateActivationID,
				activation.Source,
				fmt.Sprintf("activation name normalizes to duplicate ID %q", activation.ID),
				"Give every activation declaration a name that normalizes to a unique ID.",
				diagnostic.RelatedReference{Message: "first declaration of this normalized activation ID", Source: first},
			)
			continue
		}
		firstByID[activation.ID] = *activation.Source
		unique = append(unique, activation)
	}
	activations = unique

	// Activation declaration order has no execution meaning. Canonical identity
	// order keeps semantic graph and plan digests independent of YAML map order.
	sort.Slice(activations, func(i, j int) bool {
		if activations[i].ID == activations[j].ID {
			return activations[i].Kind < activations[j].Kind
		}
		return activations[i].ID < activations[j].ID
	})
	for _, activation := range activations {
		sourceMap[activation.ID] = *activation.Source
	}
	return activations
}

func (l *lowerer) lowerActivation(
	kind string,
	node *yaml.Node,
	pathParts []string,
	defaultName string,
	workflowProvenance graph.Provenance,
) (graph.ActivationDeclaration, bool) {
	if kind == "schedule" && node.Kind == yaml.ScalarNode {
		if defaultName == "" {
			l.invalidActivation(node, pathParts, "schedule declarations in a sequence require a mapping with name and cron")
			return graph.ActivationDeclaration{}, false
		}
		return l.lowerScheduleShorthand(node, pathParts, defaultName, workflowProvenance)
	}
	if node == nil || node.Kind != yaml.MappingNode {
		l.invalidActivation(node, pathParts, fmt.Sprintf("%s declaration must be a mapping", kind))
		return graph.ActivationDeclaration{}, false
	}

	allowed := []string{
		"name", "authority", "extract", "overlap", "starting_deadline", "catchup",
		"deduplication_key", "run_id_reuse", "metadata",
	}
	switch kind {
	case "webhook":
		allowed = append(allowed, "path")
	case "schedule":
		allowed = append(allowed, "cron")
	case "message":
		allowed = append(allowed, "to")
	case "file":
		allowed = append(allowed, "path", "events")
	case "event":
		allowed = append(allowed, "type", "source")
	case "one_shot":
		allowed = append(allowed, "path", "ttl")
	}
	fields := l.mapping(node, pathParts, allowed...)

	name := defaultName
	if field, exists := fields["name"]; exists {
		name = l.activationID(field.value, field.path)
	} else if defaultName == "" {
		l.invalidActivation(node, pathParts, "activation declarations in a sequence require name")
	}
	if defaultName != "" && name == defaultName {
		name = graph.NormalizeID(defaultName)
	}

	if field, exists := fields["authority"]; exists {
		authority := l.string(field.value, field.path)
		if authority != activationAuthorityProject {
			l.addDiagnostic(CodeUnsupportedActivationAuthority, field.value, field.path,
				fmt.Sprintf("workflow source cannot claim activation authority %q", authority),
				"Omit authority or set it to project; host-owned registrations are created through host operations.")
		}
	}

	ref := l.location(node, pathParts)
	activation := graph.ActivationDeclaration{
		ID:   name,
		Kind: kind,
		Provenance: graph.Provenance{
			Authority: activationAuthorityProject,
			Origin:    "workflow-source",
			Locator:   l.source.Locator,
			Revision:  workflowProvenance.Revision,
			Digest:    workflowProvenance.Digest,
		},
		Source: &ref,
	}
	if field, exists := fields["extract"]; exists {
		activation.Inputs = l.lowerBindings(field.value, field.path)
	}
	if field, exists := fields["metadata"]; exists {
		l.rejectOperationalActivationMetadata(field.value, field.path)
		activation.Metadata = l.metadata(field.value, field.path)
	}
	activation.Policy = l.lowerActivationPolicy(fields)
	activation.Config = l.lowerActivationConfig(kind, node, pathParts, fields)
	return activation, name != ""
}

func (l *lowerer) rejectOperationalActivationMetadata(node *yaml.Node, pathParts []string) {
	if node == nil {
		return
	}
	switch node.Kind {
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			key, value := node.Content[index], node.Content[index+1]
			fieldPath := appendPath(pathParts, key.Value)
			if _, forbidden := forbiddenActivationOperationalFields[key.Value]; forbidden {
				l.addDiagnostic(CodeUnsupportedSourceField, key, fieldPath,
					fmt.Sprintf("operational activation field %q is not permitted in source metadata", key.Value),
					"Keep mutable registration state and credentials in the host registration, outside workflow source and ExecutionPlan.")
			}
			l.rejectOperationalActivationMetadata(value, fieldPath)
		}
	case yaml.SequenceNode:
		for index, item := range node.Content {
			l.rejectOperationalActivationMetadata(item, appendPath(pathParts, strconv.Itoa(index)))
		}
	case yaml.DocumentNode, yaml.ScalarNode, yaml.AliasNode:
		return
	}
}

func (l *lowerer) lowerScheduleShorthand(
	node *yaml.Node,
	pathParts []string,
	name string,
	workflowProvenance graph.Provenance,
) (graph.ActivationDeclaration, bool) {
	cron := l.string(node, pathParts)
	if cron == "" {
		l.invalidActivation(node, pathParts, "schedule.cron must not be empty")
		return graph.ActivationDeclaration{}, false
	}
	if !validFiveFieldCron(cron) {
		l.invalidActivation(node, pathParts, "schedule.cron must use supported five-field numeric cron syntax")
	}
	ref := l.location(node, pathParts)
	return graph.ActivationDeclaration{
		ID:     graph.NormalizeID(name),
		Kind:   "schedule",
		Config: graph.Config{"cron": cron},
		Provenance: graph.Provenance{
			Authority: activationAuthorityProject,
			Origin:    "workflow-source",
			Locator:   l.source.Locator,
			Revision:  workflowProvenance.Revision,
			Digest:    workflowProvenance.Digest,
		},
		Source: &ref,
	}, cron != ""
}

func (l *lowerer) lowerActivationPolicy(fields map[string]sourceField) graph.ActivationPolicy {
	var policy graph.ActivationPolicy
	if field, exists := fields["overlap"]; exists {
		switch value := l.string(field.value, field.path); value {
		case "Allow":
			policy.Overlap = graph.OverlapAllow
		case "Forbid":
			policy.Overlap = graph.OverlapForbid
		case "Replace":
			policy.Overlap = graph.OverlapReplace
		default:
			l.invalidActivation(field.value, field.path, "overlap must be Allow, Forbid, or Replace")
		}
	}
	if field, exists := fields["starting_deadline"]; exists {
		value := l.string(field.value, field.path)
		if duration, err := time.ParseDuration(value); err != nil || duration <= 0 {
			l.invalidActivation(field.value, field.path, "starting_deadline must be a positive Go duration")
		} else {
			policy.StartingDeadline = graph.Duration(value)
		}
	}
	if field, exists := fields["catchup"]; exists {
		policy.Catchup = l.boolean(field.value, field.path)
	}
	if field, exists := fields["deduplication_key"]; exists {
		expression := l.expression(field.value, field.path)
		policy.DeduplicationKey = &expression
	}
	if field, exists := fields["run_id_reuse"]; exists {
		switch value := l.string(field.value, field.path); value {
		case "reject":
			policy.RunIDReuse = graph.RunIDReuseReject
		case "allow_duplicate":
			policy.RunIDReuse = graph.RunIDReuseAllowDuplicate
		case "terminate_existing":
			policy.RunIDReuse = graph.RunIDReuseTerminateExisting
		default:
			l.invalidActivation(field.value, field.path, "run_id_reuse must be reject, allow_duplicate, or terminate_existing")
		}
	}
	return policy
}

func (l *lowerer) lowerActivationConfig(
	kind string,
	node *yaml.Node,
	pathParts []string,
	fields map[string]sourceField,
) graph.Config {
	config := make(graph.Config)
	requiredString := func(name string) string {
		field, exists := fields[name]
		if !exists {
			l.invalidActivation(node, pathParts, fmt.Sprintf("%s.%s is required", kind, name))
			return ""
		}
		value := l.string(field.value, field.path)
		if value == "" {
			l.invalidActivation(field.value, field.path, fmt.Sprintf("%s.%s must not be empty", kind, name))
		}
		return value
	}

	switch kind {
	case "webhook":
		value := requiredString("path")
		if value != "" && !validRootRelativeRoute(value) {
			l.invalidActivation(fields["path"].value, fields["path"].path, "webhook.path must be a static root-relative path without query, fragment, or traversal")
		}
		config["path"] = value
	case "schedule":
		value := requiredString("cron")
		if value != "" && !validFiveFieldCron(value) {
			l.invalidActivation(fields["cron"].value, fields["cron"].path, "schedule.cron must use supported five-field numeric cron syntax")
		}
		config["cron"] = value
	case "message":
		value := requiredString("to")
		if value != "" && !validMessageAddress(value) {
			l.invalidActivation(fields["to"].value, fields["to"].path, "message.to must be a static msg:// address with authority and path")
		}
		config["to"] = value
	case "file":
		value := requiredString("path")
		if value != "" && !validFileActivationPath(value) {
			l.invalidActivation(fields["path"].value, fields["path"].path, "file.path must be a static path without traversal, query, or fragment")
		}
		config["path"] = value
		field, exists := fields["events"]
		if !exists {
			l.invalidActivation(node, pathParts, "file.events is required")
			break
		}
		events := l.strings(field.value, field.path)
		seen := make(map[string]struct{}, len(events))
		for index, event := range events {
			switch event {
			case "create", "write", "remove", "rename":
			default:
				l.invalidActivation(field.value.Content[index], appendPath(field.path, strconv.Itoa(index)), "file event must be create, write, remove, or rename")
			}
			if _, duplicate := seen[event]; duplicate {
				l.invalidActivation(field.value.Content[index], appendPath(field.path, strconv.Itoa(index)), fmt.Sprintf("duplicate file event %q", event))
			}
			seen[event] = struct{}{}
		}
		if len(events) == 0 {
			l.invalidActivation(field.value, field.path, "file.events must not be empty")
		}
		config["events"] = events
	case "event":
		value := requiredString("type")
		if value != "" && !validEventType(value) {
			l.invalidActivation(fields["type"].value, fields["type"].path, "event.type must contain only letters, digits, dot, hyphen, underscore, colon, or slash")
		}
		config["type"] = value
		if field, exists := fields["source"]; exists {
			source := l.string(field.value, field.path)
			if source == "" || containsInterpolation(source) {
				l.invalidActivation(field.value, field.path, "event.source must be a non-empty static source identifier")
			}
			config["source"] = source
		}
	case "one_shot":
		value := requiredString("path")
		if value != "" && !validRootRelativeRoute(value) {
			l.invalidActivation(fields["path"].value, fields["path"].path, "one_shot.path must be a static root-relative path without query, fragment, or traversal")
		}
		config["path"] = value
		field, exists := fields["ttl"]
		if !exists {
			l.invalidActivation(node, pathParts, "one_shot.ttl is required")
			break
		}
		ttl := l.string(field.value, field.path)
		if duration, err := time.ParseDuration(ttl); err != nil || duration <= 0 {
			l.invalidActivation(field.value, field.path, "one_shot.ttl must be a positive Go duration")
		}
		config["ttl"] = ttl
	}
	return config
}

func (l *lowerer) activationID(node *yaml.Node, pathParts []string) string {
	raw := l.string(node, pathParts)
	normalized := graph.NormalizeID(raw)
	if err := graph.ValidateID(normalized); err != nil {
		l.invalidActivation(node, pathParts, fmt.Sprintf("activation name %q cannot normalize to a valid ID", raw))
		return ""
	}
	return normalized
}

func (l *lowerer) invalidActivation(node *yaml.Node, pathParts []string, message string) {
	l.addDiagnostic(CodeInvalidActivation, node, pathParts,
		fmt.Sprintf("%s: %s", displayPath(pathParts), message),
		"Use a supported immutable source activation declaration; create operational registrations through the host.")
}

func (l *lowerer) addActivationDiagnostic(
	code diagnostic.Code,
	source *graph.SourceRef,
	message string,
	remediation string,
	related ...diagnostic.RelatedReference,
) {
	l.diagnostics = append(l.diagnostics, diagnostic.Diagnostic{
		Severity: diagnostic.SeverityError,
		Code:     code,
		Message:  message,
		Source:   source,
		Related:  related,
		Remediation: &diagnostic.Remediation{
			Message:       remediation,
			Documentation: sourceFormatDocumentation,
		},
	})
}

func validRootRelativeRoute(value string) bool {
	if !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") || containsInterpolation(value) || containsSpaceOrControl(value) {
		return false
	}
	u, err := url.Parse(value)
	if err != nil || u.IsAbs() || u.Host != "" || u.RawQuery != "" || u.Fragment != "" || u.Path != value {
		return false
	}
	decoded, err := url.PathUnescape(u.EscapedPath())
	if err != nil {
		return false
	}
	return !hasTraversal(decoded)
}

func validFileActivationPath(value string) bool {
	if strings.TrimSpace(value) != value || value == "" || strings.ContainsRune(value, '\x00') ||
		strings.ContainsAny(value, "?#") || containsInterpolation(value) || hasTraversal(value) {
		return false
	}
	if parsed, err := url.Parse(value); err != nil || parsed.Scheme != "" && !isWindowsAbsolutePath(value, parsed.Scheme) {
		return false
	}
	decoded, err := url.PathUnescape(value)
	return err == nil && !hasTraversal(decoded)
}

func isWindowsAbsolutePath(value, scheme string) bool {
	return len(scheme) == 1 && len(value) >= 3 && value[1] == ':' && (value[2] == '\\' || value[2] == '/')
}

func hasTraversal(value string) bool {
	for _, segment := range strings.Split(strings.ReplaceAll(value, "\\", "/"), "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	return false
}

func containsInterpolation(value string) bool {
	return strings.Contains(value, "{{") || strings.Contains(value, "}}")
}

func containsSpaceOrControl(value string) bool {
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func validMessageAddress(value string) bool {
	if containsInterpolation(value) {
		return false
	}
	u, err := url.Parse(value)
	return err == nil && u.Scheme == "msg" && u.User == nil && u.Host != "" && u.Path != "" && u.Path != "/" &&
		u.RawQuery == "" && u.Fragment == ""
}

func validEventType(value string) bool {
	if value == "" || containsInterpolation(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune(".-_:/", r) {
			continue
		}
		return false
	}
	return true
}

func validFiveFieldCron(value string) bool {
	fields := strings.Fields(value)
	if len(fields) != 5 {
		return false
	}
	bounds := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	for index, field := range fields {
		if !validCronField(field, bounds[index][0], bounds[index][1]) {
			return false
		}
	}
	return true
}

func validCronField(value string, minimum, maximum int) bool {
	if value == "" {
		return false
	}
	for _, part := range strings.Split(value, ",") {
		if part == "" {
			return false
		}
		base := part
		if strings.Contains(part, "/") {
			pieces := strings.Split(part, "/")
			if len(pieces) != 2 || pieces[0] == "" || !decimalDigits(pieces[1]) {
				return false
			}
			step, err := strconv.Atoi(pieces[1])
			if err != nil || step <= 0 {
				return false
			}
			base = pieces[0]
		}
		if base == "*" {
			continue
		}
		if strings.Contains(base, "-") {
			pieces := strings.Split(base, "-")
			if len(pieces) != 2 || !decimalDigits(pieces[0]) || !decimalDigits(pieces[1]) {
				return false
			}
			start, startErr := strconv.Atoi(pieces[0])
			end, endErr := strconv.Atoi(pieces[1])
			if startErr != nil || endErr != nil || start < minimum || end > maximum || start > end {
				return false
			}
			continue
		}
		if !decimalDigits(base) {
			return false
		}
		value, err := strconv.Atoi(base)
		if err != nil || value < minimum || value > maximum {
			return false
		}
	}
	return true
}

func decimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
