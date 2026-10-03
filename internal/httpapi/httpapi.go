// Package httpapi exposes a minimal REST surface over the gateway/risk
// engine so a real HTTP client can drive the system (spec section 24).
// This is intentionally a thin slice of the full API surface described in
// the spec — see README "Implemented vs Not Implemented" for scope.
package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"velocityguard/internal/gateway"
	"velocityguard/internal/ledger"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
	"velocityguard/internal/store"
)

// Scope names enforced on tenant-scoped endpoints. Kept as constants per
// the "no scattered string constants" rule (config.go, section 42).
const (
	ScopeProxyWrite   = "proxy:write"
	ScopeReadExposure = "read:exposure"
	ScopeReadEvents   = "read:events"
)

// tenantCtxKey is an unexported type so context values set by the auth
// middleware can't collide with values set by other packages.
type tenantCtxKey struct{}

type Server struct {
	GW     *gateway.Gateway
	Reserv reservation.Repository
	Risk   *risk.Engine
	Ledger *ledger.Ledger
	Store  store.Store // control-plane auth; see requireAuth
	Route  gateway.RouteConfig
	mux    *http.ServeMux

	// OperatorToken authenticates POST /v1/kill-switch. Tenant API keys
	// (any scope) are never accepted here — see requireOperator. Empty
	// means the kill switch endpoint is disabled (404), not open.
	OperatorToken string

	// ipLimiter and tenantLimiter are opt-in rate limits, configured via
	// SetRateLimits. Both are nil (unlimited) by default so existing
	// callers — including every test that constructs a Server directly —
	// see today's behavior unless they explicitly opt in. cmd/gateway
	// always configures both for real deployments; see main.go.
	ipLimiter     *limiter
	tenantLimiter *limiter
}

// SetRateLimits enables per-IP and per-tenant request rate limiting.
// Pass 0 for any rate/burst pair to leave that limiter disabled. IP
// limiting protects the process itself (connection floods, credential
// stuffing against Authenticate) and applies before auth; tenant
// limiting protects your budget/upstream-provider relationship from a
// single compromised or misbehaving key and applies after auth. Both
// return 429 Too Many Requests when exceeded, never silently drop.
func (s *Server) SetRateLimits(ipRPS, ipBurst, tenantRPS, tenantBurst float64) {
	if ipRPS > 0 && ipBurst > 0 {
		s.ipLimiter = newLimiter(ipRPS, ipBurst)
	}
	if tenantRPS > 0 && tenantBurst > 0 {
		s.tenantLimiter = newLimiter(tenantRPS, tenantBurst)
	}
}

// NewServer wires up the HTTP surface. store.Store is required: every
// tenant-scoped endpoint authenticates via a real API key rather than
// trusting a client-supplied tenant header (see requireAuth). operatorToken
// authenticates the kill switch; pass "" only in local/dev contexts where
// the kill switch should be unreachable rather than silently open.
func NewServer(gw *gateway.Gateway, rm reservation.Repository, re *risk.Engine, l *ledger.Ledger, st store.Store, route gateway.RouteConfig, operatorToken string) *Server {
	s := &Server{GW: gw, Reserv: rm, Risk: re, Ledger: l, Store: st, Route: route, OperatorToken: operatorToken, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Applied before routing/auth so it also protects unauthenticated
	// paths (e.g. repeated bad Authenticate attempts against /proxy/ or
	// /v1/kill-switch), not just successfully-authenticated tenants.
	if !s.ipLimiter.allow(clientIP(r)) {
		http.Error(w, "rate limit exceeded, try again shortly", http.StatusTooManyRequests)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	// Tenant-scoped endpoints require a valid API key AND the matching
	// scope. The kill-switch endpoint is operator-only and authenticated
	// separately (requireOperator) — tenant API keys, of any scope, are
	// never accepted there.
	s.mux.HandleFunc("/v1/exposure", s.requireAuth(ScopeReadExposure, s.handleExposure))
	s.mux.HandleFunc("/v1/events", s.requireAuth(ScopeReadEvents, s.handleEvents))
	s.mux.HandleFunc("/v1/kill-switch", s.requireOperator(s.handleKillSwitch))
	s.mux.HandleFunc("/proxy/", s.requireAuth(ScopeProxyWrite, s.handleProxy))
}

// requireAuth enforces Bearer-token authentication, injects the
// authenticated tenant ID into the request context, and enforces that the
// key carries requiredScope. This replaces trusting a client-supplied
// X-VelocityGuard-Tenant header (section 15/16: never let the caller
// assert its own identity) — the tenant now comes only from a hashed,
// revocable key looked up in Store.
//
// Scope compatibility: a key with a nil/empty Scopes list is treated as
// unscoped/legacy and passes any scope check. This preserves existing
// tests and demo keys created before scopes existed; new keys should
// always be issued with explicit scopes.
func (s *Server) requireAuth(requiredScope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(authHeader, prefix) || len(authHeader) <= len(prefix) {
			http.Error(w, "missing or malformed Authorization: Bearer <key> header", http.StatusUnauthorized)
			return
		}
		plaintext := strings.TrimPrefix(authHeader, prefix)

		tenant, key, err := s.Store.Authenticate(r.Context(), plaintext)
		switch {
		case errors.Is(err, store.ErrNotFound):
			http.Error(w, "invalid API key", http.StatusUnauthorized)
			return
		case errors.Is(err, store.ErrRevoked):
			http.Error(w, "API key has been revoked", http.StatusForbidden)
			return
		case err != nil:
			http.Error(w, "authentication failed", http.StatusInternalServerError)
			return
		}

		if requiredScope != "" && len(key.Scopes) > 0 && !hasScope(key.Scopes, requiredScope) {
			http.Error(w, "API key is missing required scope: "+requiredScope, http.StatusForbidden)
			return
		}
		if requiredScope != "" && len(key.Scopes) == 0 {
			// Audit trail for the legacy/unscoped compatibility path —
			// an unscoped key just passed a scope check it was never
			// explicitly granted. Not blocked (see doc comment above),
			// but never silent either.
			s.Ledger.Append(ledger.Event{
				Type:     ledger.LegacyUnscopedKeyUsed,
				TenantID: tenant.ID,
				Metadata: map[string]string{"scope": requiredScope, "path": r.URL.Path},
			})
		}

		if !s.tenantLimiter.allow(tenant.ID) {
			http.Error(w, "tenant rate limit exceeded, try again shortly", http.StatusTooManyRequests)
			return
		}

		ctx := context.WithValue(r.Context(), tenantCtxKey{}, tenant.ID)
		next(w, r.WithContext(ctx))
	}
}

func hasScope(scopes []string, want string) bool {
	for _, s := range scopes {
		if s == want {
			return true
		}
	}
	return false
}

// requireOperator authenticates the kill switch against the server's
// OperatorToken using a constant-time comparison, independent of tenant
// API key auth entirely. A tenant API key — no matter its scopes — is
// never accepted here. If OperatorToken is unset, the endpoint is
// disabled (404) rather than left open.
func (s *Server) requireOperator(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.OperatorToken == "" {
			http.NotFound(w, r)
			return
		}
		authHeader := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(authHeader, prefix) {
			http.Error(w, "missing Authorization: Bearer <operator token> header", http.StatusUnauthorized)
			return
		}
		supplied := strings.TrimPrefix(authHeader, prefix)
		// ConstantTimeCompare requires equal-length inputs; pad the
		// shorter one so length itself doesn't leak via early return
		// (compare still fails).
		ok := len(supplied) == len(s.OperatorToken) &&
			subtle.ConstantTimeCompare([]byte(supplied), []byte(s.OperatorToken)) == 1
		if !ok {
			http.Error(w, "invalid operator token", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// tenantFromContext retrieves the tenant ID set by requireAuth. It must
// only be called from handlers reachable through requireAuth.
func tenantFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tenantCtxKey{}).(string)
	return id, ok
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleExposure(w http.ResponseWriter, r *http.Request) {
	tenant, _ := tenantFromContext(r.Context()) // set by requireAuth
	exp := s.Reserv.Exposure(tenant)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id": tenant,
		"limit":     exp.Limit.Float(),
		"reserved":  exp.Reserved.Float(),
		"settled":   exp.Settled.Float(),
		"available": exp.Available.Float(),
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	// Scoped strictly to the authenticated tenant — never fall back to
	// "all events" here, or tenant isolation (section 29) breaks.
	tenant, _ := tenantFromContext(r.Context())
	events := s.Ledger.ForTenant(tenant)
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleKillSwitch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Scope string `json:"scope"` // "global" | "tenant" | "provider"
		Value string `json:"value"` // tenant id or provider name, ignored for global
		On    bool   `json:"on"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch body.Scope {
	case "global":
		s.Risk.KillAll(body.On)
	case "tenant":
		s.Risk.KillTenant(body.Value, body.On)
	case "provider":
		s.Risk.KillProvider(body.Value, body.On)
	default:
		http.Error(w, "scope must be global|tenant|provider", http.StatusBadRequest)
		return
	}
	// Audit trail for every kill-switch flip, regardless of scope. Never
	// log the operator token itself here — only the action taken.
	eventType := ledger.KillSwitchOff
	if body.On {
		eventType = ledger.KillSwitchOn
	}
	s.Ledger.Append(ledger.Event{
		Type:     eventType,
		TenantID: body.Value, // meaningful for scope=="tenant"; ignored otherwise
		Metadata: map[string]string{"scope": body.Scope, "value": body.Value},
	})
	writeJSON(w, http.StatusOK, map[string]interface{}{"scope": body.Scope, "value": body.Value, "on": body.On})
}

// handleProxy is the gateway entrypoint: POST /proxy/{tenant} routes the
// request through estimate->reserve->risk->execute->reconcile.
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	tenant, _ := tenantFromContext(r.Context()) // set by requireAuth; never trust a client header for identity
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// Bound the inbound body before we ever touch it — protects both us
	// and whatever upstream we forward to (provider.MaxUpstreamRequestBytes).
	r.Body = http.MaxBytesReader(w, r.Body, provider.MaxUpstreamRequestBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	// The upstream destination is never taken from the client: only the
	// sub-path (and query) survive, to be joined onto the operator's own
	// configured BaseURL inside GenericHTTP — see provider.go doc comment.
	upstreamPath := strings.TrimPrefix(r.URL.Path, "/proxy")
	if upstreamPath == "" {
		upstreamPath = "/"
	}
	if r.URL.RawQuery != "" {
		upstreamPath += "?" + r.URL.RawQuery
	}

	// Never forward the caller's VelocityGuard API key upstream, and
	// strip hop-by-hop headers per RFC 7230 (also re-filtered inside
	// GenericHTTP as defense-in-depth).
	fwdHeaders := provider.FilterHopByHopHeaders(r.Header)
	fwdHeaders.Del("Authorization")
	fwdHeaders.Set("X-Request-ID", requestID)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	result := s.GW.HandleRequest(ctx, requestID, tenant, s.Route,
		provider.ExecRequest{Method: r.Method, Path: upstreamPath, Headers: fwdHeaders, Body: body}, 100, 100, 1)

	status := http.StatusOK
	switch result.Decision.Action {
	case risk.ActionBlock, risk.ActionKillSwitch:
		status = http.StatusPaymentRequired // 402, signaling financial rejection
	case risk.ActionThrottle:
		status = http.StatusTooManyRequests
	}
	if result.Err != nil && status == http.StatusOK {
		status = http.StatusBadGateway
	}

	writeJSON(w, status, map[string]interface{}{
		"request_id":     result.RequestID,
		"action":         result.Decision.Action,
		"risk_score":     result.Decision.RiskScore,
		"risk_level":     result.Decision.RiskLevel,
		"reason":         result.Decision.Reason,
		"circuit_state":  result.Decision.CircuitState,
		"estimated_cost": result.Decision.EstimatedCost.Float(),
		"actual_cost":    result.ActualCost.Float(),
		"executed":       result.Executed,
	})
}
