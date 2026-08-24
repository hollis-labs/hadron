// Package offlinebuild binds Hadron's file/compiler and native Go build
// surfaces to the extraction-ready workflow/offline contracts.
package offlinebuild

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strconv"
	"strings"

	gateadapter "github.com/hollis-labs/hadron/workflow/adapters/gate"
	llmadapter "github.com/hollis-labs/hadron/workflow/adapters/llm"
	mcpadapter "github.com/hollis-labs/hadron/workflow/adapters/mcp"
	"github.com/hollis-labs/hadron/workflow/adapters/script"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/offline"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type Options struct {
	Registry       stepkind.Registry
	BindingCatalog offline.BindingCatalog
	GoBinary       string
}

type Builder struct {
	registry stepkind.Registry
	bindings offline.BindingCatalog
	goBinary string
}

func New(options Options) (*Builder, error) {
	if options.Registry == nil {
		return nil, fmt.Errorf("offline builder registry is required")
	}
	goBinary := strings.TrimSpace(options.GoBinary)
	if goBinary == "" {
		goBinary = "go"
	}
	return &Builder{registry: options.Registry, bindings: options.BindingCatalog, goBinary: goBinary}, nil
}

// NewDefault returns the deterministic built-in artifact profile. It includes
// only implementations that can be reconstructed in a generated process
// without credentials or host services. Other exact registries remain usable
// through New for library embedding, but cannot be overclaimed by this native
// generator.
func NewDefault() (*Builder, error) {
	registry := stepkind.NewRegistry()
	for _, kind := range []stepkind.StepKind{
		transform.New(), script.New(),
		&validationKind{delegate: &mcpadapter.Kind{}},
		&validationKind{delegate: &llmadapter.Kind{}},
		&validationKind{delegate: &gateadapter.Executor{}},
		&validationKind{delegate: &waitadapter.Sleep{}},
		&validationKind{delegate: &waitadapter.WaitFor{}},
		&validationKind{delegate: &waitadapter.MessageWait{}},
	} {
		if err := registry.Register(kind); err != nil {
			return nil, err
		}
	}
	return New(Options{Registry: registry})
}

type validationKind struct{ delegate stepkind.StepKind }

func (k *validationKind) Spec() stepkind.StepKindSpec { return k.delegate.Spec() }
func (k *validationKind) ValidateConfig(ctx context.Context, config graph.Config) []diagnostic.Diagnostic {
	return k.delegate.ValidateConfig(ctx, config)
}
func (*validationKind) Execute(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	return stepkind.StepResult{}, errors.New("validation-only kind cannot execute")
}

type Request struct {
	SourcePath       string
	Mode             offline.Mode
	ToolName         string
	Bindings         []offline.ExternalBinding
	OutputPath       string
	MaxManifestBytes int
}

type Result struct {
	Manifest     *offline.Manifest
	ManifestJSON []byte
	Diagnostics  []diagnostic.Diagnostic
	OutputPath   string
}

func (b *Builder) Compile(ctx context.Context, request Request) (Result, error) {
	if ctx == nil || b == nil || b.registry == nil {
		return Result{}, fmt.Errorf("offline builder is not initialized")
	}
	loaded, err := compile.LoadFile(request.SourcePath)
	if err != nil {
		return Result{}, err
	}
	if len(loaded.Diagnostics) != 0 {
		return Result{Diagnostics: loaded.Diagnostics}, nil
	}
	compiled := compile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 {
		return Result{Diagnostics: compiled.Diagnostics}, nil
	}
	executionRegistry, err := offline.AdaptExecutionRegistry(*compiled.Plan, b.registry, request.Bindings)
	if err != nil {
		return Result{}, err
	}
	built, err := offline.Build(ctx, compiled.Plan, offline.BuildOptions{
		Registry: executionRegistry, SourceRegistry: b.registry, Bindings: request.Bindings, BindingCatalog: b.bindings,
		Mode: request.Mode, ToolName: request.ToolName, MaxManifestBytes: request.MaxManifestBytes,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{Manifest: built.Manifest, ManifestJSON: bytes.Clone(built.Bytes), Diagnostics: built.Diagnostics}, nil
}

func (b *Builder) BuildExecutable(ctx context.Context, request Request) (Result, error) {
	result, err := b.Compile(ctx, request)
	if err != nil || len(result.Diagnostics) != 0 {
		return result, err
	}
	if strings.TrimSpace(request.OutputPath) == "" {
		return Result{}, fmt.Errorf("offline output path is required")
	}
	if factoryErr := generatedFactoriesAvailable(*result.Manifest); factoryErr != nil {
		return Result{Diagnostics: []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: offline.CodeBindingRequired, Message: factoryErr.Error(), Remediation: &diagnostic.Remediation{Message: "Install a functional generated bridge or use a supported pure embedded kind."}}}}, nil
	}
	absolute, err := filepath.Abs(request.OutputPath)
	if err != nil {
		return Result{}, err
	}
	if mkdirErr := os.MkdirAll(filepath.Dir(absolute), 0o750); mkdirErr != nil {
		return Result{}, mkdirErr
	}
	moduleRoot, err := sourceModuleRoot()
	if err != nil {
		return Result{}, err
	}
	temporary, err := os.MkdirTemp(moduleRoot, ".hadron-offline-build-*")
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	sourcePath := filepath.Join(temporary, "main.go")
	source := generatedSource(result.ManifestJSON)
	if err := os.WriteFile(sourcePath, source, 0o600); err != nil {
		return Result{}, err
	}
	temporaryOutput := filepath.Join(temporary, "artifact")
	// #nosec G204 -- GoBinary is an explicit host-injected compiler seam; all
	// remaining arguments are closed flags or paths created by this function.
	command := exec.CommandContext(ctx, b.goBinary, "build", "-trimpath", "-ldflags=-buildid=", "-o", temporaryOutput, sourcePath)
	command.Env = append([]string(nil), os.Environ()...)
	command.Dir = moduleRoot
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return Result{}, fmt.Errorf("build offline executable: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if err := publishExecutable(temporaryOutput, absolute); err != nil {
		return Result{}, err
	}
	result.OutputPath = absolute
	return result, nil
}

func sourceModuleRoot() (string, error) {
	_, source, _, ok := goruntime.Caller(0)
	if !ok {
		return "", fmt.Errorf("locate Hadron module source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("locate Hadron module root: %w", err)
	}
	return root, nil
}

func publishExecutable(source, destination string) (resultErr error) {
	// #nosec G304 -- source is the private build output path created above.
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := input.Close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	directory := filepath.Dir(destination)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(destination)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() {
		if resultErr != nil {
			_ = temporary.Close()
			_ = os.Remove(temporaryName)
		}
	}()
	if _, copyErr := io.Copy(temporary, input); copyErr != nil {
		return copyErr
	}
	if chmodErr := temporary.Chmod(0o755); chmodErr != nil {
		return chmodErr
	}
	if syncErr := temporary.Sync(); syncErr != nil {
		return syncErr
	}
	if closeErr := temporary.Close(); closeErr != nil {
		return closeErr
	}
	if renameErr := os.Rename(temporaryName, destination); renameErr != nil {
		return renameErr
	}
	// #nosec G304 -- directory is the caller-selected artifact destination.
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	if syncErr := dir.Sync(); syncErr != nil {
		_ = dir.Close()
		return syncErr
	}
	return dir.Close()
}

func ReadBindings(paths []string) ([]offline.ExternalBinding, error) {
	if len(paths) > offline.MaximumBindings {
		return nil, fmt.Errorf("too many binding files")
	}
	bindings := make([]offline.ExternalBinding, 0, len(paths))
	for _, path := range paths {
		// #nosec G304 -- paths are the explicit CLI binding-file inputs.
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read binding %q: %w", path, err)
		}
		if len(data) > offline.DefaultMaxBindingBytes {
			return nil, fmt.Errorf("binding %q exceeds size bound", path)
		}
		var binding offline.ExternalBinding
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&binding); err != nil {
			return nil, fmt.Errorf("decode binding %q: %w", path, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("binding %q contains trailing JSON", path)
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func generatedFactoriesAvailable(manifest offline.Manifest) error {
	bound := make(map[string]bool, len(manifest.Bindings))
	for _, binding := range manifest.Bindings {
		if binding.Binding.Driver == offline.DriverRemoteDaemonHTTP {
			bound[binding.Binding.Kind+"\x00"+binding.Binding.Version] = true
		}
	}
	identities := make([]string, 0, len(manifest.StepKinds))
	for _, spec := range manifest.StepKinds {
		identity := spec.Name + "@" + spec.Version
		if identity != transform.Name+"@"+transform.Version && identity != script.Name+"@"+script.Version && !bound[spec.Name+"\x00"+spec.Version] {
			identities = append(identities, identity)
		}
	}
	if len(identities) != 0 {
		sort.Strings(identities)
		return fmt.Errorf("generated artifact has no reconstructible runtime bridge for %s", strings.Join(identities, ", "))
	}
	return nil
}

// RegisterManifestKinds reconstructs Hadron's closed native kind catalog for
// one generated manifest. The extraction-ready offline package remains free of
// concrete adapter imports.
func RegisterManifestKinds(registry stepkind.Registry, manifest offline.Manifest) error {
	return offline.RegisterManifestKinds(registry, manifest, transform.New(), script.New())
}

// BindRuntimeRegistry attaches runtime expression context to the exact
// transform implementation while preserving every advertised StepKindSpec.
func BindRuntimeRegistry(ctx context.Context, source stepkind.Registry, store offline.ExecutionStore, manifest offline.Manifest) (stepkind.Registry, error) {
	if ctx == nil || source == nil || store == nil {
		return nil, fmt.Errorf("offline runtime registry composition requires context, registry, and store")
	}
	registry := stepkind.NewRegistry()
	for _, spec := range source.List() {
		kind, ok := source.Lookup(spec.Name, spec.Version)
		if !ok {
			return nil, fmt.Errorf("offline runtime registry lookup/list mismatch")
		}
		if spec.Name == transform.Name && spec.Version == transform.Version {
			bound, err := transform.NewWithContextProvider(transform.ContextProviderFunc(func(ctx context.Context, invocation stepkind.Invocation) (values.ExpressionContext, error) {
				expression, err := workflowruntime.BuildExpressionContext(ctx, store, store, manifest.Plan.Graph, workflowruntime.RunID(invocation.Identity.RunID))
				if err != nil {
					return values.ExpressionContext{}, err
				}
				if invocation.Identity.Iteration != "" {
					item, itemOK := invocation.Inputs["item"]
					index, indexOK := invocation.Inputs["index"]
					if !itemOK || !indexOK {
						return values.ExpressionContext{}, fmt.Errorf("fan-out invocation is missing item/index values")
					}
					indexNumber, parseErr := strconv.Atoi(fmt.Sprint(index.Inline))
					if parseErr != nil {
						return values.ExpressionContext{}, fmt.Errorf("fan-out index is invalid")
					}
					expression.Item, expression.Index = &item, &indexNumber
				}
				scoped, _, err := manifest.Visibility.ScopeNodeContext(invocation.Identity.NodeID, expression, values.ExpressionOptions{})
				return scoped, err
			}))
			if err != nil {
				return nil, err
			}
			kind = bound
		}
		if err := registry.Register(kind); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func generatedSource(manifest []byte) []byte {
	quoted := strconv.Quote(string(manifest))
	return []byte(`package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hollis-labs/hadron/internal/offlinebuild"
	"github.com/hollis-labs/hadron/workflow/offline"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const embeddedManifest = ` + quoted + `

func main() {
	manifest, err := offline.ParseManifest([]byte(embeddedManifest))
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	registry := stepkind.NewRegistry()
	if err := offlinebuild.RegisterManifestKinds(registry, manifest); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	options := offline.ExecuteOptions{Registry: registry, RegistryBinder: offline.RuntimeRegistryBinderFunc(offlinebuild.BindRuntimeRegistry)}
	if manifest.Mode == offline.ModeMCPServer {
		err = offline.ServeMCPWithOptions(context.Background(), manifest, options, os.Stdin, os.Stdout)
	} else {
		err = offline.RunCLIWithOptions(context.Background(), manifest, options, os.Args[1:], os.Stdout)
	}
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
`)
}
