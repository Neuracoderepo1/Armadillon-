package provider

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newGeneric(base string, hosts ...string) *GenericHTTP {
	g := NewGenericHTTP("p", base, hosts)
	g.Client.Timeout = 300 * time.Millisecond
	return g
}

// Only failures that prove the request never left may be classified NotDispatched;
// everything after the connection is up is ambiguous (the upstream may have billed).
func TestGenericHTTP_DispatchClassification(t *testing.T) {
	ctx := context.Background()
	req := ExecRequest{Method: "POST", Path: "/x", Body: []byte("{}")}

	t.Run("connection refused is not dispatched", func(t *testing.T) {
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		addr := ln.Addr().String()
		ln.Close() // nothing listens any more
		_, _, err := newGeneric("http://"+addr).Execute(ctx, req)
		if err == nil || !IsNotDispatched(err) {
			t.Fatalf("want NotDispatched, got %v", err)
		}
	})
	t.Run("ssrf allowlist rejection is not dispatched", func(t *testing.T) {
		_, _, err := newGeneric("http://example.invalid", "other.host").Execute(ctx, req)
		if !IsNotDispatched(err) {
			t.Fatalf("want NotDispatched, got %v", err)
		}
	})
	t.Run("oversized body is not dispatched", func(t *testing.T) {
		big := ExecRequest{Method: "POST", Path: "/x", Body: []byte(strings.Repeat("a", MaxUpstreamRequestBytes+1))}
		_, _, err := newGeneric("http://127.0.0.1:1").Execute(ctx, big)
		if !IsNotDispatched(err) {
			t.Fatalf("want NotDispatched, got %v", err)
		}
	})
	t.Run("context cancelled before send is not dispatched", func(t *testing.T) {
		c, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err := newGeneric("http://127.0.0.1:1").Execute(c, req)
		if !IsNotDispatched(err) {
			t.Fatalf("want NotDispatched, got %v", err)
		}
	})
	t.Run("timeout after the request was received is AMBIGUOUS", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Second) // received, "executing", never answers in time
		}))
		defer srv.CloseClientConnections()
		defer srv.Close()
		_, _, err := newGeneric(srv.URL).Execute(ctx, req)
		if err == nil {
			t.Fatal("expected a timeout error")
		}
		if IsNotDispatched(err) {
			t.Fatalf("a post-send timeout must be ambiguous, got NotDispatched: %v", err)
		}
	})
	t.Run("connection dropped mid-response is AMBIGUOUS", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hj, _ := w.(http.Hijacker)
			c, _, _ := hj.Hijack()
			c.Close() // upstream read the request then died
		}))
		defer srv.Close()
		_, _, err := newGeneric(srv.URL).Execute(ctx, req)
		if err == nil || IsNotDispatched(err) {
			t.Fatalf("want ambiguous error, got %v", err)
		}
	})
	t.Run("success is not an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
		defer srv.Close()
		if _, _, err := newGeneric(srv.URL).Execute(ctx, req); err != nil {
			t.Fatal(err)
		}
	})
}
