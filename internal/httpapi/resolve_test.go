package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"velocityguard/internal/ledger"
	"velocityguard/internal/money"
	"velocityguard/internal/reservation"
)

type fakeResolver struct {
	released   []string
	reconciled map[string]money.Micros
	err        error
	rows       []*reservation.Reservation
}

func (f *fakeResolver) ListUnknown(tenant string, limit int) ([]*reservation.Reservation, reservation.UnknownSummary, error) {
	return f.rows, reservation.UnknownSummary{Count: len(f.rows), Held: 5 * money.OneUnit, OldestAge: 90 * time.Second}, f.err
}
func (f *fakeResolver) ResolveUnknownReleased(id string) error {
	if f.err != nil {
		return f.err
	}
	f.released = append(f.released, id)
	return nil
}
func (f *fakeResolver) ResolveUnknownReconciled(id string, a money.Micros) (reservation.ReconcileResult, error) {
	if f.err != nil {
		return reservation.ReconcileResult{}, f.err
	}
	if f.reconciled == nil {
		f.reconciled = map[string]money.Micros{}
	}
	f.reconciled[id] = a
	return reservation.ReconcileResult{}, nil
}

func resolveReq(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestResolve_OperatorOnly_AndDisabledWithoutResolver(t *testing.T) {
	srv, keyA, _ := newHardeningTestServer(t, testOperatorToken, []string{ScopeProxyWrite, ScopeReadExposure, ScopeReadEvents}, nil)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	u := ts.URL + "/v1/reservations/abc/resolve"
	body := `{"outcome":"released","evidence":"provider invoice shows no charge"}`

	// No resolver wired: 404 even for the operator (never "open", never fake).
	if r := resolveReq(t, u, testOperatorToken, body); r.StatusCode != http.StatusNotFound {
		t.Fatalf("no resolver: want 404, got %d", r.StatusCode)
	}
	srv.SetUnknownResolver(&fakeResolver{})
	if r := resolveReq(t, u, "", body); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: want 401, got %d", r.StatusCode)
	}
	// A tenant API key of any scope must never resolve money.
	if r := resolveReq(t, u, keyA, body); r.StatusCode != http.StatusUnauthorized && r.StatusCode != http.StatusForbidden {
		t.Fatalf("tenant key: want 401/403, got %d", r.StatusCode)
	}
	if r := resolveReq(t, u, testOperatorToken, body); r.StatusCode != http.StatusOK {
		t.Fatalf("operator: want 200, got %d", r.StatusCode)
	}
}

func TestResolve_Validation(t *testing.T) {
	srv, _, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
	fr := &fakeResolver{}
	srv.SetUnknownResolver(fr)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	u := ts.URL + "/v1/reservations/abc/resolve"

	bad := []string{
		`{"outcome":"released"}`,                                                // no evidence
		`{"outcome":"released","evidence":"   "}`,                               // blank evidence
		`{"outcome":"released","evidence":"` + strings.Repeat("x", 1001) + `"}`, // too long
		`{"outcome":"deleted","evidence":"x"}`,                                  // bad outcome
		`{"outcome":"reconciled","evidence":"x"}`,                               // missing actual
		`{"outcome":"reconciled","actual_micros":-1,"evidence":"x"}`,            // negative actual
		`{"outcome":"released","actual_micros":5,"evidence":"x"}`,               // actual with release
		`{"outcome":"released","evidence":"x","surprise":1}`,                    // unknown field
		`not json`,
	}
	for _, b := range bad {
		if r := resolveReq(t, u, testOperatorToken, b); r.StatusCode != http.StatusBadRequest {
			t.Errorf("body %.40q: want 400, got %d", b, r.StatusCode)
		}
	}
	if len(fr.released) != 0 || len(fr.reconciled) != 0 {
		t.Fatalf("invalid requests must never reach the ledger: %+v", fr)
	}
}

func TestResolve_Outcomes_AuditedAndErrorsMapped(t *testing.T) {
	srv, _, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
	fr := &fakeResolver{}
	srv.SetUnknownResolver(fr)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	if r := resolveReq(t, ts.URL+"/v1/reservations/r1/resolve", testOperatorToken,
		`{"outcome":"reconciled","actual_micros":30000,"evidence":"invoice #42"}`); r.StatusCode != http.StatusOK {
		t.Fatalf("reconcile: %d", r.StatusCode)
	}
	if fr.reconciled["r1"] != 30000 {
		t.Fatalf("actual not passed through: %+v", fr.reconciled)
	}
	if r := resolveReq(t, ts.URL+"/v1/reservations/r2/resolve", testOperatorToken,
		`{"outcome":"released","evidence":"provider confirms no charge"}`); r.StatusCode != http.StatusOK {
		t.Fatalf("release: %d", r.StatusCode)
	}
	if len(fr.released) != 1 || fr.released[0] != "r2" {
		t.Fatalf("release not passed through: %+v", fr.released)
	}

	var resolved int
	for _, e := range srv.Ledger.Since(0) {
		if e.Type == ledger.ReservationResolved {
			resolved++
			if e.Metadata["evidence"] == "" || e.Metadata["outcome"] == "" || e.ReservationID == "" {
				t.Fatalf("audit event incomplete: %+v", e)
			}
		}
	}
	if resolved != 2 {
		t.Fatalf("want 2 audit events, got %d", resolved)
	}

	// Error mapping; a failed resolution must NOT write an audit event.
	cases := map[error]int{
		reservation.ErrNotFound:            http.StatusNotFound,
		reservation.ErrTerminalState:       http.StatusConflict,
		reservation.ErrPendingActualExists: http.StatusConflict,
		reservation.ErrBackendUnavailable:  http.StatusServiceUnavailable,
	}
	for e, want := range cases {
		fr.err = e
		if r := resolveReq(t, ts.URL+"/v1/reservations/r3/resolve", testOperatorToken,
			`{"outcome":"released","evidence":"x"}`); r.StatusCode != want {
			t.Errorf("%v: want %d, got %d", e, want, r.StatusCode)
		}
	}
	n := 0
	for _, e := range srv.Ledger.Since(0) {
		if e.Type == ledger.ReservationResolved {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("failed resolutions were audited as successes: %d events", n)
	}
}

func TestListUnknown_SummaryAndOperatorOnly(t *testing.T) {
	srv, _, _ := newHardeningTestServer(t, testOperatorToken, nil, nil)
	srv.SetUnknownResolver(&fakeResolver{rows: []*reservation.Reservation{{ID: "r1", TenantID: "t1", Amount: 5 * money.OneUnit}}})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, _ := http.Get(ts.URL + "/v1/reservations/unknown")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: want 401, got %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/reservations/unknown", nil)
	req.Header.Set("Authorization", "Bearer "+testOperatorToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("operator list: %v %v", err, resp)
	}
	var out struct {
		Summary struct {
			Count            int   `json:"count"`
			HeldMicros       int64 `json:"held_micros"`
			OldestAgeSeconds int64 `json:"oldest_age_seconds"`
		} `json:"summary"`
		Reservations []map[string]interface{} `json:"reservations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Summary.Count != 1 || out.Summary.HeldMicros != int64(5*money.OneUnit) || out.Summary.OldestAgeSeconds != 90 || len(out.Reservations) != 1 {
		t.Fatalf("unexpected payload: %+v", out)
	}
}
