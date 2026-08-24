package http

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/workflow/values"
)

type defaultResolver struct{ resolver *net.Resolver }

func (r defaultResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return r.resolver.LookupNetIP(ctx, network, host)
}

func (t *PinnedTransport) RoundTrip(ctx context.Context, exchange Exchange) (*nethttp.Response, error) {
	if ctx == nil || exchange.Request == nil || exchange.Request.URL == nil || !exchange.Destination.Address.IsValid() {
		return nil, &TransportError{Failure: FailureProtocol, Cause: fmt.Errorf("invalid pinned exchange")}
	}
	if err := ctx.Err(); err != nil {
		return nil, contextTransportError(err)
	}
	if exchange.Request.URL.Scheme != exchange.Destination.Scheme || exchange.Request.URL.Hostname() != exchange.Destination.Host ||
		portOf(exchange.Request.URL) != exchange.Destination.Port || exchange.Request.Host != exchange.Request.URL.Host {
		return nil, &TransportError{Failure: FailureProtocol, Cause: fmt.Errorf("request does not match approved destination")}
	}
	request := exchange.Request.Clone(ctx)
	request.Close = true
	target := net.JoinHostPort(exchange.Destination.Address.String(), strconv.Itoa(int(exchange.Destination.Port)))
	transport := &nethttp.Transport{
		Proxy:                  nil,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		ForceAttemptHTTP2:      false,
		MaxResponseHeaderBytes: t.maxResponseHeaderBytes,
		DialContext: func(dialCtx context.Context, network, _ string) (net.Conn, error) {
			connection, err := t.dialer.DialContext(dialCtx, network, target)
			if err != nil {
				return nil, &TransportError{Failure: FailureConnect, Cause: err}
			}
			return connection, nil
		},
	}
	if exchange.Destination.Scheme == "https" {
		configuration := &tls.Config{MinVersion: tls.VersionTLS12}
		if t.tlsConfig != nil {
			configuration = t.tlsConfig.Clone()
			if configuration.MinVersion == 0 || configuration.MinVersion < tls.VersionTLS12 {
				configuration.MinVersion = tls.VersionTLS12
			}
		}
		configuration.ServerName = exchange.Destination.Host
		transport.TLSClientConfig = configuration
	}
	response, err := transport.RoundTrip(request)
	transport.CloseIdleConnections()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextTransportError(contextErr)
		}
		var transportErr *TransportError
		if errors.As(err, &transportErr) {
			return nil, transportErr
		}
		failure := FailureProtocol
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			failure = FailureTimeout
		} else if errors.Is(err, context.DeadlineExceeded) {
			failure = FailureTimeout
		} else if errors.Is(err, context.Canceled) {
			failure = FailureCanceled
		} else if isTLSFailure(err) {
			failure = FailureTLS
		}
		return nil, &TransportError{Failure: failure, Cause: err}
	}
	return response, nil
}

func contextTransportError(err error) *TransportError {
	failure := FailureCanceled
	if errors.Is(err, context.DeadlineExceeded) {
		failure = FailureTimeout
	}
	return &TransportError{Failure: failure, Cause: err}
}

func isTLSFailure(err error) bool {
	var record tls.RecordHeaderError
	var verification *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	return errors.As(err, &record) || errors.As(err, &verification) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostname) || errors.As(err, &invalid)
}

type resolvedDestination struct {
	Destination DestinationRequest
	RewriteOK   bool
}

func resolveDestination(ctx context.Context, resolver Resolver, policy Policy, parsedURL *url.URL, method string, hop int, redirect *RedirectContext) (resolvedDestination, error) {
	if err := ctx.Err(); err != nil {
		return resolvedDestination{}, contextTransportError(err)
	}
	host := parsedURL.Hostname()
	var addresses []netip.Addr
	if address, err := netip.ParseAddr(host); err == nil {
		addresses = []netip.Addr{address.Unmap()}
	} else {
		resolved, err := resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return resolvedDestination{}, contextTransportError(contextErr)
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return resolvedDestination{}, contextTransportError(err)
			}
			return resolvedDestination{}, &TransportError{Failure: FailureDNS, Cause: err}
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return resolvedDestination{}, contextTransportError(contextErr)
		}
		seen := make(map[netip.Addr]bool)
		for _, address := range resolved {
			address = address.Unmap()
			if address.IsValid() && address.Zone() == "" && !seen[address] {
				seen[address] = true
				addresses = append(addresses, address)
			}
		}
	}
	if len(addresses) == 0 {
		return resolvedDestination{}, &TransportError{Failure: FailureDNS, Cause: fmt.Errorf("resolver returned no addresses")}
	}
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].Compare(addresses[j]) < 0 })
	portValue, _ := strconv.ParseUint(parsedURL.Port(), 10, 16)
	rewriteOK := true
	for _, address := range addresses {
		request := DestinationRequest{
			Scheme: parsedURL.Scheme, Host: host, Port: uint16(portValue), Address: address,
			Path: parsedURL.EscapedPath(), Hop: hop, Method: method, Redirect: cloneRedirectContext(redirect),
		}
		authorization, err := policy.AuthorizeDestination(ctx, request)
		if err != nil {
			return resolvedDestination{}, fmt.Errorf("%w: destination authorization failed: %w", ErrPolicyDenied, err)
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return resolvedDestination{}, contextTransportError(contextErr)
		}
		rewriteOK = rewriteOK && authorization.AllowMethodRewrite
	}
	return resolvedDestination{Destination: DestinationRequest{
		Scheme: parsedURL.Scheme, Host: host, Port: uint16(portValue), Address: addresses[0],
		Path: parsedURL.EscapedPath(), Hop: hop, Method: method, Redirect: cloneRedirectContext(redirect),
	}, RewriteOK: rewriteOK}, nil
}

func cloneRedirectContext(input *RedirectContext) *RedirectContext {
	if input == nil {
		return nil
	}
	cloned := *input
	return &cloned
}

func originOf(parsedURL *url.URL) string {
	return parsedURL.Scheme + "://" + parsedURL.Host
}

func redirectTarget(current *url.URL, rawLocation string, redactor *values.Redactor) (*url.URL, error) {
	if rawLocation == "" {
		return nil, fmt.Errorf("%w: redirect has no Location header", ErrInvalidResult)
	}
	if redactor != nil && redactor.MaskString(rawLocation) != rawLocation {
		return nil, fmt.Errorf("%w: redirect Location contains secret material", ErrInvalidResult)
	}
	reference, err := url.Parse(rawLocation)
	if err != nil {
		return nil, fmt.Errorf("%w: redirect Location is invalid", ErrInvalidResult)
	}
	target := current.ResolveReference(reference)
	normalized, _, err := normalizeURL(target.String())
	if err != nil {
		return nil, fmt.Errorf("%w: redirect target is invalid", ErrInvalidResult)
	}
	if err := validateURLQuery(normalized, false); err != nil {
		return nil, err
	}
	if redactor != nil {
		if redactor.MaskString(normalized.Path) != normalized.Path {
			return nil, fmt.Errorf("%w: redirect path contains secret material", ErrInvalidResult)
		}
		query, queryErr := url.ParseQuery(normalized.RawQuery)
		if queryErr != nil {
			return nil, fmt.Errorf("%w: redirect query is invalid", ErrInvalidResult)
		}
		for key, entries := range query {
			if redactor.MaskString(key) != key {
				return nil, fmt.Errorf("%w: redirect query contains secret material", ErrInvalidResult)
			}
			for _, entry := range entries {
				if redactor.MaskString(entry) != entry {
					return nil, fmt.Errorf("%w: redirect query contains secret material", ErrInvalidResult)
				}
			}
		}
	}
	return normalized, nil
}

func redirectMethod(status int, method string) (string, bool) {
	switch status {
	case nethttp.StatusSeeOther:
		if method != "HEAD" {
			return "GET", true
		}
	case nethttp.StatusMovedPermanently, nethttp.StatusFound:
		if method == "POST" {
			return "GET", true
		}
	}
	return method, false
}

func isRedirectStatus(status int) bool {
	switch status {
	case nethttp.StatusMovedPermanently, nethttp.StatusFound, nethttp.StatusSeeOther,
		nethttp.StatusTemporaryRedirect, nethttp.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func canonicalRedirectKey(parsedURL *url.URL) string {
	cloned := *parsedURL
	cloned.Fragment = ""
	return cloned.String()
}

func stripCrossOriginCredentials(headers nethttp.Header, secretHeaders map[string]struct{}) {
	for name := range headers {
		lower := strings.ToLower(name)
		_, secret := secretHeaders[lower]
		if isSensitiveHeader(lower) || secret {
			headers.Del(name)
		}
	}
}

func sanitizeResponseHeaders(headers nethttp.Header, redactor *values.Redactor) map[string][]string {
	result := make(map[string][]string, len(headers))
	for name, entries := range headers {
		lower := strings.ToLower(name)
		masked := make([]string, len(entries))
		for index, entry := range entries {
			if isSensitiveHeader(lower) || lower == "location" {
				masked[index] = values.RedactedMarker
			} else {
				masked[index] = redactor.MaskString(entry)
			}
		}
		sort.Strings(masked)
		if prior, exists := result[lower]; exists {
			result[lower] = append(prior, masked...)
			sort.Strings(result[lower])
		} else {
			result[lower] = masked
		}
	}
	return result
}
