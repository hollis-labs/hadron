package http_test

import (
	"context"
	"net/netip"
	"testing"

	httpadapter "github.com/hollis-labs/hadron/workflow/adapters/http"
	"github.com/hollis-labs/hadron/workflow/graph"
)

type externalResolver struct{}

func (externalResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("192.0.2.1")}, nil
}

type externalPolicy struct{}

func (externalPolicy) DescribeRequest(context.Context, httpadapter.RequestDeclaration) (httpadapter.PolicyDescription, error) {
	return httpadapter.PolicyDescription{}, nil
}

func (externalPolicy) AuthorizeDestination(context.Context, httpadapter.DestinationRequest) (httpadapter.DestinationAuthorization, error) {
	return httpadapter.DestinationAuthorization{}, nil
}

func TestPublicPolicyAndDescriptionContract(t *testing.T) {
	kind, err := httpadapter.New(httpadapter.Options{Resolver: externalResolver{}, Policy: externalPolicy{}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	description, err := kind.DescribeConfig(t.Context(), graph.Config{"url": "https://example.test/path"})
	if err != nil {
		t.Fatalf("DescribeConfig() error = %v", err)
	}
	if description.Method != "GET" || description.Origin != "https://example.test:443" {
		t.Fatalf("description = %#v", description)
	}
}
