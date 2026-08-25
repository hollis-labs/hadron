package appworkflow

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const workflowActivationExposurePrefix = "hadron_workflow_activation_v1_"

type workflowActivationExposureWire struct {
	Activation string `json:"activation"`
	Digest     string `json:"digest"`
	Name       string `json:"name"`
	Version    string `json:"version"`
}

// WorkflowActivationExposure binds one durable activation template to the
// exact immutable registry version that authorized it. It is safe routing
// metadata, not a credential or an authorization capability.
type WorkflowActivationExposure struct {
	Definition   graph.DefinitionRef
	ActivationID string
}

// EncodeWorkflowActivationExposureRef produces the sole canonical durable
// encoding accepted by DecodeWorkflowActivationExposureRef: a fixed version
// prefix plus canonical JSON carried as unpadded base64url. The opaque payload
// safely preserves registry names and versions that contain URL delimiters.
func EncodeWorkflowActivationExposureRef(definition graph.DefinitionRef, activationID string) (string, error) {
	if err := validateWorkflowActivationExposure(definition, activationID); err != nil {
		return "", err
	}
	payload, err := json.Marshal(workflowActivationExposureWire{
		Activation: activationID, Digest: definition.Digest, Name: definition.ID, Version: definition.Version,
	})
	if err != nil {
		return "", fmt.Errorf("%w: activation exposure reference cannot be encoded", ErrInvalidActivation)
	}
	encoded := workflowActivationExposurePrefix + base64.RawURLEncoding.EncodeToString(payload)
	if len(encoded) > hoststate.MaximumActivationTextBytes {
		return "", fmt.Errorf("%w: activation exposure reference exceeds its bound", ErrInvalidActivation)
	}
	return encoded, nil
}

// DecodeWorkflowActivationExposureRef strictly decodes and re-canonicalizes a
// durable exposure reference. Unknown, duplicate, missing, or alternatively
// escaped fields are rejected so lifecycle and transport authorization cannot
// interpret the same stored bytes differently.
func DecodeWorkflowActivationExposureRef(encoded string) (WorkflowActivationExposure, error) {
	if len(encoded) > hoststate.MaximumActivationTextBytes || !strings.HasPrefix(encoded, workflowActivationExposurePrefix) {
		return WorkflowActivationExposure{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, workflowActivationExposurePrefix))
	if err != nil || len(raw) == 0 || len(raw) > hoststate.MaximumActivationTextBytes {
		return WorkflowActivationExposure{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
	}
	wire, err := decodeWorkflowActivationExposureWire(raw)
	if err != nil {
		return WorkflowActivationExposure{}, err
	}
	exposure := WorkflowActivationExposure{
		Definition: graph.DefinitionRef{
			Kind: DefinitionKindRegistry, ID: wire.Name, Version: wire.Version, Digest: wire.Digest,
		},
		ActivationID: wire.Activation,
	}
	if validationErr := validateWorkflowActivationExposure(exposure.Definition, exposure.ActivationID); validationErr != nil {
		return WorkflowActivationExposure{}, validationErr
	}
	canonical, err := EncodeWorkflowActivationExposureRef(exposure.Definition, exposure.ActivationID)
	if err != nil || canonical != encoded {
		return WorkflowActivationExposure{}, fmt.Errorf("%w: activation exposure reference is not canonical", ErrInvalidActivation)
	}
	return exposure, nil
}

func decodeWorkflowActivationExposureWire(input []byte) (workflowActivationExposureWire, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
	}
	seen := make(map[string]struct{}, 4)
	result := workflowActivationExposureWire{}
	for decoder.More() {
		token, tokenErr := decoder.Token()
		name, ok := token.(string)
		if tokenErr != nil || !ok {
			return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
		}
		if _, duplicate := seen[name]; duplicate {
			return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
		}
		seen[name] = struct{}{}
		var value string
		if decodeErr := decoder.Decode(&value); decodeErr != nil {
			return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
		}
		switch name {
		case "activation":
			result.Activation = value
		case "digest":
			result.Digest = value
		case "name":
			result.Name = value
		case "version":
			result.Version = value
		default:
			return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || len(seen) != 4 {
		return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return workflowActivationExposureWire{}, fmt.Errorf("%w: activation exposure reference is invalid", ErrInvalidActivation)
	}
	return result, nil
}

func validateWorkflowActivationExposure(definition graph.DefinitionRef, activationID string) error {
	if definition.Kind != DefinitionKindRegistry || definition.Authority != "" || definition.Locator != "" || definition.Provenance != nil ||
		definition.ID != strings.TrimSpace(definition.ID) ||
		hadronregistry.ValidateWorkflowName(definition.ID) != nil || definition.Version != strings.TrimSpace(definition.Version) ||
		hoststate.ValidatePublicText(definition.Version, 256, true) != nil || values.ValidateDigest(definition.Digest) != nil ||
		graph.ValidateID(activationID) != nil {
		return fmt.Errorf("%w: activation exposure identity is invalid", ErrInvalidActivation)
	}
	return nil
}
