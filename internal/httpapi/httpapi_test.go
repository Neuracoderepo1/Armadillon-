package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"velocityguard/internal/gateway"
	"velocityguard/internal/ledger"
	"velocityguard/internal/money"
	"velocityguard/internal/pricing"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
	"velocityguard/internal/store"
)

// newTestServer builds a fully wired Server backed by MemoryStore, plus
// two tenants with real API keys, for exercising auth over real HTTP
// (httptest), not just calling Go functions directly.
func newTestServer(t *testing.T) (srv *Server, keyA, keyB, tenantAID, tenantBID string) {
	t.Helper()
	ctx := context.Background()
	st := store.NewMemoryStore()

	tenantA, err := st.CreateTenant(ctx, "Tenant A", "tenant-a-"+t.Name())
	if err != nil {
		t.Fatalf("CreateTenant A: %v", err)
	}
	tenantB, err := st.CreateTenant(ctx, "Tenant B", "tenant-b-"+t.Name())
	if err != nil {
		t.Fatalf("CreateTenant B: %v", err)
	}
	plainA, _, err := st.CreateAPIKey(ctx, tenantA.ID, nil)
	if err != nil {
		t.Fatalf("CreateAPIKey A: %v", err)
	}
	plainB, _, err := st.CreateAPIKey(ctx, tenantB.ID, nil)
	if err != nil {
		t.Fatalf("CreateAPIKey B: %v", err)
	}

	rm := reservation.NewManager()
	rm.SetBudget(tenantA.ID, money.FromFloat(10.00))
	rm.SetBudget(tenantB.ID, money.FromFloat(10.00))
	re := risk.NewEngine(rm, risk.DefaultPolicy())
	pr := pricing.NewRegistry()
	pr.AddRate(pricing.Rate{Provider: "demo-provider", Model: "demo-model",
		InputPerUnit: money.FromFloat(0.0001), OutputPerUnit: money.FromFloat(0.0002), Version: 1})
	mock := provider.NewMockProvider("demo-provider", nil)
	providers := provider.NewRegistry()
	providers.Register(mock)
	l := ledger.New()
	gw := gateway.New(re, rm, pr, providers, l)
	route := gateway.RouteConfig{Route: "/agent/execute", Provider: "demo-provider", Model: "demo-model"}

	s := NewServer(gw, rm, re, l, st, route, "")
	return s, plainA, plainB, tenantA.ID, tenantB.ID
}

func TestProxy_NoAuthHeader_Rejected(t *testing.T) {
	srv, _, _, _, _ := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/proxy/x", "application/json", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", resp.StatusCode)
	}
}

func TestProxy_InvalidKey_Rejected(t *testing.T) {
	srv, _, _, _, _ := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/x", nil)
	req.Header.Set("Authorization", "Bearer vg_live_not-a-real-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", resp.StatusCode)
	}
}

func TestProxy_ValidKey_Allowed(t *testing.T) {
	srv, keyA, _, _, _ := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/x", nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", resp.StatusCode)
	}
}

// TestExposure_TenantIsolation is the HTTP-level version of section 29's
// requirement: a valid key for tenant A must never be able to read
// tenant B's financial exposure or events, no matter what the client
// puts in the URL/body — identity comes only from the authenticated key.
func TestExposure_TenantIsolation(t *testing.T) {
	srv, keyA, _, tenantAID, tenantBID := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/exposure?tenant="+tenantBID, nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["tenant_id"] != tenantAID {
		t.Fatalf("exposure returned tenant_id %v, want %v (a ?tenant= query param must not override the authenticated identity)", body["tenant_id"], tenantAID)
	}
}

type fixedResponseProvider struct {
	response provider.Response
	usage    provider.Usage
}

func (p *fixedResponseProvider) Name() string { return "demo-provider" }

func (p *fixedResponseProvider) Execute(context.Context, provider.ExecRequest) (provider.Response, provider.Usage, error) {
	return p.response, p.usage, nil
}

func TestProxy_ForwardsUpstreamStatusBodyAndSafeHeaders(t *testing.T) {
	srv, keyA, _, _, _ := newTestServer(t)
	upstreamBody := []byte(`{"error":"rate_limited"}`)
	upstreamHeaders := make(http.Header)
	upstreamHeaders.Set("Content-Type", "application/json")
	upstreamHeaders.Set("X-Provider-Request-ID", "provider-request-7")
	upstreamHeaders.Set("Connection", "X-Internal-Only")
	upstreamHeaders.Set("X-Internal-Only", "must-not-cross-proxy")
	upstreamHeaders.Set("X-VelocityGuard-Settlement", "forged")
	srv.GW.Providers.Register(&fixedResponseProvider{
		response: provider.Response{StatusCode: http.StatusTooManyRequests, Body: upstreamBody, Headers: upstreamHeaders},
		usage:    provider.Usage{InputUnits: 100, OutputUnits: 100},
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/x", nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	req.Header.Set("X-Request-ID", "client-request-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("got status %d, want upstream status %d", resp.StatusCode, http.StatusTooManyRequests)
	}
	if string(gotBody) != string(upstreamBody) {
		t.Fatalf("got body %q, want exact upstream body %q", gotBody, upstreamBody)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := resp.Header.Get("X-Provider-Request-ID"); got != "provider-request-7" {
		t.Fatalf("provider request ID = %q, want provider-request-7", got)
	}
	if got := resp.Header.Get("X-VelocityGuard-Request-ID"); got != "client-request-123" {
		t.Fatalf("gateway request ID = %q, want client-request-123", got)
	}
	if got := resp.Header.Get("X-VelocityGuard-Settlement"); got != "settled" {
		t.Fatalf("settlement header = %q, want settled", got)
	}
	if resp.Header.Get("Connection") != "" || resp.Header.Get("X-Internal-Only") != "" {
		t.Fatalf("hop-by-hop headers leaked: Connection=%q X-Internal-Only=%q", resp.Header.Get("Connection"), resp.Header.Get("X-Internal-Only"))
	}
}

func TestProxy_DoesNotForwardUpstreamHeadersWhenBlocked(t *testing.T) {
	srv, keyA, _, _, _ := newTestServer(t)
	srv.Risk.KillAll(true)
	srv.GW.Providers.Register(&fixedResponseProvider{
		response: provider.Response{
			StatusCode: http.StatusOK,
			Body:       []byte(`{"must_not_be_forwarded":true}`),
			Headers:    http.Header{"X-Provider-Request-ID": []string{"must-not-leak"}},
		},
		usage: provider.Usage{InputUnits: 100, OutputUnits: 100},
	})

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/x", nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("got status %d, want 402", resp.StatusCode)
	}
	if resp.Header.Get("X-Provider-Request-ID") != "" {
		t.Fatal("provider response header was forwarded for a blocked request")
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode gateway rejection: %v", err)
	}
	if body["executed"] != false {
		t.Fatalf("blocked request executed unexpectedly: %#v", body)
	}
}
