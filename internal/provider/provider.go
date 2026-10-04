// Package provider defines the upstream provider abstraction (spec section
// 11). This MVP ships two implementations: MockProvider (deterministic,
// no network — used for tests and the demo scenario) and GenericHTTP
// (forwards to a real upstream using an externally supplied cost policy).
package provider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type Usage struct {
	InputUnits  int64
	OutputUnits int64
}

type Response struct {
	StatusCode int
	Body       []byte
	Headers    http.Header
}

// ExecRequest describes one upstream call. URL is used only by
// MockProvider (a no-op label). GenericHTTP deliberately ignores URL and
// instead joins Path onto its own configured, operator-controlled
// BaseURL — the caller can never choose the upstream host or scheme.
// This is the core SSRF control: there is no code path where a
// client-supplied string becomes the request destination.
type ExecRequest struct {
	Method  string
	URL     string // MockProvider only
	Path    string // GenericHTTP: joined onto BaseURL, e.g. "/v1/chat?x=1"
	Headers http.Header
	Body    []byte
}

// hopByHopHeaders per RFC 7230 6.1 — these are connection-specific and
// must never be forwarded across a proxy hop in either direction.
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailers", "Transfer-Encoding", "Upgrade",
}

// FilterHopByHopHeaders returns a copy of h with hop-by-hop headers
// removed. Exported so httpapi can apply the same filtering to the
// client's inbound headers before they ever reach a Provider.
func FilterHopByHopHeaders(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, v := range h {
		out[k] = v
	}
	for _, hh := range hopByHopHeaders {
		out.Del(hh)
	}
	return out
}

const (
	// MaxUpstreamRequestBytes bounds the body we'll send upstream.
	MaxUpstreamRequestBytes = 2 << 20 // 2 MiB
	// MaxUpstreamResponseBytes bounds the body we'll read back from the
	// upstream, so a malicious/misbehaving upstream can't exhaust memory.
	MaxUpstreamResponseBytes = 8 << 20 // 8 MiB
)

// Provider executes an upstream call and reports usage for reconciliation.
// Actual dollar cost is computed by the caller via the pricing registry
// from the reported Usage, per spec section 12 (pricing lives in one place).
type Provider interface {
	Name() string
	Execute(ctx context.Context, req ExecRequest) (Response, Usage, error)
}

// MockProvider simulates an upstream for tests and demos without any
// outbound network call. CostFn lets test/demo code script a traffic
// pattern (normal -> runaway -> recovery) deterministically.
type MockProvider struct {
	NameStr string
	UsageFn func(req ExecRequest) Usage
}

func NewMockProvider(name string, usageFn func(req ExecRequest) Usage) *MockProvider {
	return &MockProvider{NameStr: name, UsageFn: usageFn}
}

func (p *MockProvider) Name() string { return p.NameStr }

func (p *MockProvider) Execute(ctx context.Context, req ExecRequest) (Response, Usage, error) {
	u := Usage{InputUnits: 100, OutputUnits: 100}
	if p.UsageFn != nil {
		u = p.UsageFn(req)
	}
	return Response{StatusCode: 200, Body: []byte(`{"mock":true}`)}, u, nil
}

// GenericHTTP forwards to a single, operator-configured upstream base URL.
// Cost is supplied externally (spec section 13: generic HTTP mode does not
// pretend to know provider-specific economics) — here via a flat
// per-request Usage of 1 "unit", meant to be combined with a flat
// RequestFee rate in the pricing registry (cost_per_request policy).
//
// SSRF posture: the destination is never derived from caller input. Every
// request goes to BaseURL + req.Path, both fixed at construction time by
// the operator (VG_UPSTREAM_URL). AllowedHosts is kept as defense-in-depth
// (e.g. if BaseURL itself were ever misconfigured to something dynamic)
// but the primary control is architectural: there is no parameter through
// which a client picks the host.
type GenericHTTP struct {
	NameStr string
	BaseURL string
	Client  *http.Client

	// AllowedHosts: defense-in-depth allowlist, see doc comment above.
	AllowedHosts map[string]bool
}

func NewGenericHTTP(name, baseURL string, allowedHosts []string) *GenericHTTP {
	m := make(map[string]bool, len(allowedHosts))
	for _, h := range allowedHosts {
		m[h] = true
	}
	return &GenericHTTP{
		NameStr:      name,
		BaseURL:      baseURL,
		AllowedHosts: m,
		Client: &http.Client{
			Timeout: 30 * time.Second,
			// Redirects are a classic SSRF bypass (allowlisted host
			// 302s to an internal address). Refuse to follow any.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (p *GenericHTTP) Name() string { return p.NameStr }

func (p *GenericHTTP) Execute(ctx context.Context, req ExecRequest) (Response, Usage, error) {
	if len(req.Body) > MaxUpstreamRequestBytes {
		return Response{}, Usage{}, NotDispatched(fmt.Errorf("provider: request body of %d bytes exceeds %d byte limit", len(req.Body), MaxUpstreamRequestBytes))
	}

	base := strings.TrimRight(p.BaseURL, "/")
	path := req.Path
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	target := base + path

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, target, bytes.NewReader(req.Body))
	if err != nil {
		return Response{}, Usage{}, NotDispatched(fmt.Errorf("provider: building request: %w", err))
	}

	// Scheme allowlist and no-embedded-credentials, even though target
	// is operator-built — cheap to verify and catches misconfiguration.
	if httpReq.URL.Scheme != "http" && httpReq.URL.Scheme != "https" {
		return Response{}, Usage{}, NotDispatched(fmt.Errorf("provider: upstream scheme %q is not http/https", httpReq.URL.Scheme))
	}
	if httpReq.URL.User != nil {
		return Response{}, Usage{}, NotDispatched(errors.New("provider: upstream URL must not contain embedded credentials"))
	}
	if len(p.AllowedHosts) > 0 && !p.AllowedHosts[httpReq.URL.Host] {
		return Response{}, Usage{}, NotDispatched(fmt.Errorf("provider: host %q is not in the SSRF allowlist for %q", httpReq.URL.Host, p.NameStr))
	}

	httpReq.Header = FilterHopByHopHeaders(req.Headers)

	if cerr := ctx.Err(); cerr != nil {
		return Response{}, Usage{}, NotDispatched(cerr) // cancelled before anything was sent
	}
	resp, err := p.Client.Do(httpReq)
	if err != nil {
		if isDialError(err) {
			return Response{}, Usage{}, NotDispatched(err) // connection never established
		}
		// Timeouts, resets and EOFs after the connection is up are AMBIGUOUS:
		// the upstream may have received, executed and billed the request.
		return Response{}, Usage{}, err
	}
	defer resp.Body.Close()

	// Refused-redirect responses (3xx from CheckRedirect's ErrUseLastResponse)
	// are surfaced as-is to the caller rather than silently followed.
	limited := io.LimitReader(resp.Body, MaxUpstreamResponseBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return Response{}, Usage{}, fmt.Errorf("provider: reading upstream response: %w", err)
	}
	if len(body) > MaxUpstreamResponseBytes {
		return Response{}, Usage{}, fmt.Errorf("provider: upstream response exceeded %d byte limit", MaxUpstreamResponseBytes)
	}

	// Generic HTTP mode: 1 flat "request" unit; actual $ cost comes from a
	// flat RequestFee rate configured in the pricing registry.
	return Response{StatusCode: resp.StatusCode, Body: body, Headers: FilterHopByHopHeaders(resp.Header)},
		Usage{InputUnits: 1, OutputUnits: 0}, nil
}

var ErrProviderNotFound = errors.New("provider: not registered")

// NotDispatchedError marks an Execute failure that PROVES the request never
// reached the upstream (invalid request, blocked by the SSRF allowlist,
// connection never established, context already cancelled). Only these
// failures allow the gateway to return the reserved budget. Every other
// error is ambiguous: the upstream may have executed and billed the call.
type NotDispatchedError struct{ Err error }

func (e *NotDispatchedError) Error() string { return e.Err.Error() }
func (e *NotDispatchedError) Unwrap() error { return e.Err }

// NotDispatched wraps err as a proven pre-dispatch failure.
func NotDispatched(err error) error {
	if err == nil {
		return nil
	}
	return &NotDispatchedError{Err: err}
}

// IsNotDispatched reports whether err is a proven pre-dispatch failure.
func IsNotDispatched(err error) bool {
	var n *NotDispatchedError
	return errors.As(err, &n)
}

// isDialError reports a failure while establishing the connection, i.e.
// before any request bytes could have been sent.
func isDialError(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

type Registry struct {
	providers map[string]Provider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

func (r *Registry) Register(p Provider) {
	r.providers[p.Name()] = p
}

func (r *Registry) Get(name string) (Provider, error) {
	p, ok := r.providers[name]
	if !ok {
		return nil, ErrProviderNotFound
	}
	return p, nil
}
