package httpapi

import (
	"bytes"
	"context"
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

const testOperatorToken = "this-is-a-32-char-operator-token!!"

// newHardeningTestServer is like newTestServer but lets the caller supply
// an operator token and per-key scopes, to exercise requireOperator and
// scope enforcement directly.
func newHardeningTestServer(t *testing.T, operatorToken string, scopesA, scopesB []string) (srv *Server, keyA, keyB string) {
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
	plainA, _, err := st.CreateAPIKey(ctx, tenantA.ID, scopesA)
	if err != nil {
		t.Fatalf("CreateAPIKey A: %v", err)
	}
	plainB, _, err := st.CreateAPIKey(ctx, tenantB.ID, scopesB)
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

	s := NewServer(gw, rm, re, l, st, route, operatorToken)
	return s, plainA, plainB
}

// --- Kill switch operator auth ---

func TestKillSwitch_MissingToken_Unauthorized(t *testing.T) {
	srv, _, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/kill-switch", "application/json", bytes.NewBufferString(`{"scope":"global","on":true}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", resp.StatusCode)
	}
}

func TestKillSwitch_InvalidToken_Unauthorized(t *testing.T) {
	srv, _, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/kill-switch", bytes.NewBufferString(`{"scope":"global","on":true}`))
	req.Header.Set("Authorization", "Bearer wrong-token-wrong-token-wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401", resp.StatusCode)
	}
}

func TestKillSwitch_TenantAPIKey_Rejected(t *testing.T) {
	// A valid tenant API key, even with every scope, must never operate
	// the kill switch — it is operator-only.
	srv, keyA, _ := newHardeningTestServer(t, testOperatorToken, []string{ScopeProxyWrite, ScopeReadExposure, ScopeReadEvents}, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/kill-switch", bytes.NewBufferString(`{"scope":"global","on":true}`))
	req.Header.Set("Authorization", "Bearer "+keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("got status %d, want 401 (tenant key must not authenticate as operator)", resp.StatusCode)
	}
}

func TestKillSwitch_ValidToken_Accepted(t *testing.T) {
	srv, _, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/kill-switch", bytes.NewBufferString(`{"scope":"global","on":true}`))
	req.Header.Set("Authorization", "Bearer "+testOperatorToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", resp.StatusCode)
	}
}

func TestKillSwitch_NoOperatorTokenConfigured_NotFound(t *testing.T) {
	// Empty OperatorToken must disable the endpoint (404), never leave
	// it open.
	srv, _, _ := newHardeningTestServer(t, "", nil, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/kill-switch", bytes.NewBufferString(`{"scope":"global","on":true}`))
	req.Header.Set("Authorization", "Bearer anything")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("got status %d, want 404", resp.StatusCode)
	}
}

// --- Scope enforcement ---

func TestScopes_MissingScope_Forbidden(t *testing.T) {
	// keyA only has read:exposure, not proxy:write.
	srv, keyA, _ := newHardeningTestServer(t, testOperatorToken, []string{ScopeReadExposure}, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/x", nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("got status %d, want 403 (key lacks proxy:write)", resp.StatusCode)
	}
}

func TestScopes_CorrectScope_Allowed(t *testing.T) {
	srv, keyA, _ := newHardeningTestServer(t, testOperatorToken, []string{ScopeProxyWrite}, nil)
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

func TestScopes_EmptyScopeList_LegacyCompatible(t *testing.T) {
	// nil/empty Scopes (as created by pre-scopes callers, and by the
	// original newTestServer helper) must keep working everywhere.
	srv, keyA, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
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
		t.Fatalf("got status %d, want 200 (empty scopes = legacy unscoped)", resp.StatusCode)
	}
}

// --- Secure upstream proxy ---

func TestProxy_ForwardsRealRequestToUpstream(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotAuthHeader string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path + "?" + r.URL.RawQuery
		buf := new(bytes.Buffer)
		buf.ReadFrom(r.Body)
		gotBody = buf.String()
		gotAuthHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("upstream-response"))
	}))
	defer upstream.Close()

	ctx := context.Background()
	st := store.NewMemoryStore()
	tenantA, _ := st.CreateTenant(ctx, "Tenant A", "tenant-a-"+t.Name())
	plainA, _, _ := st.CreateAPIKey(ctx, tenantA.ID, []string{ScopeProxyWrite})

	rm := reservation.NewManager()
	rm.SetBudget(tenantA.ID, money.FromFloat(10.00))
	re := risk.NewEngine(rm, risk.DefaultPolicy())
	pr := pricing.NewRegistry()
	pr.AddRate(pricing.Rate{Provider: "demo-provider", Model: "demo-model",
		InputPerUnit: money.FromFloat(0.0001), OutputPerUnit: money.FromFloat(0.0002), Version: 1})

	generic := provider.NewGenericHTTP("demo-provider", upstream.URL, nil)
	providers := provider.NewRegistry()
	providers.Register(generic)
	l := ledger.New()
	gw := gateway.New(re, rm, pr, providers, l)
	route := gateway.RouteConfig{Route: "/agent/execute", Provider: "demo-provider", Model: "demo-model"}
	srv := NewServer(gw, rm, re, l, st, route, "")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/v1/chat?foo=bar", bytes.NewBufferString(`{"hello":"world"}`))
	req.Header.Set("Authorization", "Bearer "+plainA)
	req.Header.Set("X-Custom-Header", "keep-me")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("gateway response status = %d, want upstream status 201", resp.StatusCode)
	}
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read upstream response: %v", err)
	}
	if string(responseBody) != "upstream-response" {
		t.Fatalf("gateway response body = %q, want exact upstream body", responseBody)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("upstream got method %q, want POST", gotMethod)
	}
	if gotPath != "/v1/chat?foo=bar" {
		t.Errorf("upstream got path %q, want /v1/chat?foo=bar", gotPath)
	}
	if gotBody != `{"hello":"world"}` {
		t.Errorf("upstream got body %q, want the original JSON body", gotBody)
	}
	if gotAuthHeader != "" {
		t.Errorf("upstream received Authorization header %q, want it stripped (tenant key must never be forwarded)", gotAuthHeader)
	}
}

func TestProxy_CallerCannotChooseDestination(t *testing.T) {
	// Even if a client tries to smuggle a destination via the request
	// (there is no field for it, but confirm the path is always relative
	// to the configured upstream, never an absolute attacker URL).
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	ctx := context.Background()
	st := store.NewMemoryStore()
	tenantA, _ := st.CreateTenant(ctx, "Tenant A", "tenant-a-"+t.Name())
	plainA, _, _ := st.CreateAPIKey(ctx, tenantA.ID, []string{ScopeProxyWrite})

	rm := reservation.NewManager()
	rm.SetBudget(tenantA.ID, money.FromFloat(10.00))
	re := risk.NewEngine(rm, risk.DefaultPolicy())
	pr := pricing.NewRegistry()
	pr.AddRate(pricing.Rate{Provider: "demo-provider", Model: "demo-model",
		InputPerUnit: money.FromFloat(0.0001), OutputPerUnit: money.FromFloat(0.0002), Version: 1})

	generic := provider.NewGenericHTTP("demo-provider", upstream.URL, nil)
	providers := provider.NewRegistry()
	providers.Register(generic)
	l := ledger.New()
	gw := gateway.New(re, rm, pr, providers, l)
	route := gateway.RouteConfig{Route: "/agent/execute", Provider: "demo-provider", Model: "demo-model"}
	srv := NewServer(gw, rm, re, l, st, route, "")
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// Path containing a scheme-like prefix must still be treated as a
	// literal sub-path, not re-parsed into a new destination.
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/proxy/http://evil.example.com/steal", nil)
	req.Header.Set("Authorization", "Bearer "+plainA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200 (request still went to the configured upstream)", resp.StatusCode)
	}
}
