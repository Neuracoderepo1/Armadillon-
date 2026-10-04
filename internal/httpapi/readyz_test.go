package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"velocityguard/internal/gateway"
)

func TestReadyz(t *testing.T) {
	s := NewServer(nil, nil, nil, nil, nil, gateway.RouteConfig{}, "")
	get := func(path string) int {
		rr := httptest.NewRecorder()
		s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr.Code
	}
	if c := get("/readyz"); c != http.StatusOK {
		t.Fatalf("no readiness check installed (memory mode) must be ready, got %d", c)
	}
	s.SetReadiness(func(context.Context) error { return errors.New("startup recovery has not completed") })
	if c := get("/readyz"); c != http.StatusServiceUnavailable {
		t.Fatalf("impaired ledger must be 503, got %d", c)
	}
	if c := get("/health"); c != http.StatusOK {
		t.Fatalf("/health is liveness only and must stay 200, got %d", c)
	}
	s.SetReadiness(func(context.Context) error { return nil })
	if c := get("/readyz"); c != http.StatusOK {
		t.Fatalf("healthy ledger must be 200, got %d", c)
	}
}
