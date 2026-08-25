package inmemory_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestStoreIsConcurrentDefensiveAndProcessLifetimeOnly(t *testing.T) {
	store := inmemory.NewStore()
	owner := workflowruntime.ValueOwner{Kind: "example-inputs", RunID: "run-1"}

	const writes = 32
	refs := make(chan values.ValueSetRef, writes)
	errs := make(chan error, writes)
	var workers sync.WaitGroup
	for i := 0; i < writes; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			value, err := values.NewInline(index, values.Metadata{
				Producer:  values.Producer{Kind: "example", Reference: fmt.Sprintf("input-%d", index)},
				MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionRun,
			})
			if err != nil {
				errs <- err
				return
			}
			ref, err := store.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: owner, Values: values.ValueSet{"value": value}})
			if err != nil {
				errs <- err
				return
			}
			refs <- ref
		}(i)
	}
	workers.Wait()
	close(errs)
	close(refs)
	for err := range errs {
		t.Fatal(err)
	}
	unique := make(map[string]struct{}, writes)
	var retained values.ValueSetRef
	for ref := range refs {
		unique[ref.ID] = struct{}{}
		retained = ref
	}
	if len(unique) != writes {
		t.Fatalf("unique value refs = %d, want %d", len(unique), writes)
	}

	loaded, err := store.LoadValues(t.Context(), retained)
	if err != nil {
		t.Fatal(err)
	}
	loaded["value"] = values.Value{}
	reloaded, err := store.LoadValues(t.Context(), retained)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded["value"].Validate(); err != nil {
		t.Fatalf("caller mutation changed stored value: %v", err)
	}

	reopened := inmemory.NewStore()
	if _, err := reopened.LoadValues(t.Context(), retained); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("fresh process-lifetime store LoadValues() error = %v, want ErrNotFound", err)
	}
}
