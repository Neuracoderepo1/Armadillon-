package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const childEnv = "VG_GATEWAY_TEST_CHILD"

// TestMain lets the signal-integration test re-execute this test binary as a
// real gateway process, so SIGTERM/SIGINT are only ever delivered to that
// isolated child and never to the test runner or CI process.
func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// startTestServer starts runServer on a loopback listener with the given
// handler and returns the base URL, the cancel func standing in for the
// signal, and a channel carrying runServer's result.
func startTestServer(t *testing.T, h http.Handler, grace time.Duration) (addr string, cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	res := make(chan error, 1)
	go func() { res <- runServer(ctx, srv, ln, grace) }()
	return ln.Addr().String(), cancel, res
}

func newClient() *http.Client {
	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

func waitErr(t *testing.T, ch <-chan error, d time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		t.Fatalf("timed out after %s waiting for %s", d, what)
		return nil
	}
}

// waitRefused polls until new TCP connections to addr are refused.
func waitRefused(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return
		}
		c.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("listener still accepting new connections after shutdown began")
}

type clientResult struct {
	body string
	err  error
}

func TestGracefulDrain_InFlightRequestCompletes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		io.WriteString(w, "finished")
	})
	addr, cancel, result := startTestServer(t, h, 5*time.Second)

	got := make(chan clientResult, 1)
	go func() {
		resp, err := newClient().Get("http://" + addr + "/")
		if err != nil {
			got <- clientResult{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		got <- clientResult{body: string(b), err: err}
	}()

	waitErr2(t, started, "handler to start")
	cancel() // simulate SIGTERM
	waitRefused(t, addr)

	select {
	case err := <-result:
		t.Fatalf("runServer returned (%v) while a request was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := waitErr(t, result, 3*time.Second, "runServer to return"); err != nil {
		t.Fatalf("graceful shutdown returned error: %v", err)
	}
	r := <-got
	if r.err != nil || r.body != "finished" {
		t.Fatalf("in-flight request not completed cleanly: body=%q err=%v", r.body, r.err)
	}
}

func waitErr2(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestShutdown_StopsAcceptingNewWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var served atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(started)
			<-release
			return
		}
		served.Add(1) // only reachable if new work was (wrongly) accepted
	})
	addr, cancel, result := startTestServer(t, h, 5*time.Second)

	go func() {
		if resp, err := newClient().Get("http://" + addr + "/hold"); err == nil {
			resp.Body.Close()
		}
	}()
	waitErr2(t, started, "holding request to start")
	cancel()
	waitRefused(t, addr)

	if resp, err := newClient().Get("http://" + addr + "/new"); err == nil {
		resp.Body.Close()
		t.Fatalf("new request was served after shutdown began (status %d)", resp.StatusCode)
	}

	close(release)
	if err := waitErr(t, result, 3*time.Second, "runServer to return"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := served.Load(); n != 0 {
		t.Fatalf("handler ran %d times for new work after shutdown began", n)
	}
}

func TestShutdown_TimeoutForcesClose(t *testing.T) {
	started := make(chan struct{})
	connClosed := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done(): // fires when the server force-closes the conn
			close(connClosed)
		case <-time.After(10 * time.Second): // safety net: never hang the test
		}
	})
	addr, cancel, result := startTestServer(t, h, 150*time.Millisecond)

	go func() {
		if resp, err := newClient().Get("http://" + addr + "/"); err == nil {
			resp.Body.Close()
		}
	}()
	waitErr2(t, started, "handler to start")

	begin := time.Now()
	cancel()
	err := waitErr(t, result, 3*time.Second, "bounded shutdown")
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("shutdown took %s, expected ~150ms grace", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline-exceeded error from timed-out shutdown, got %v", err)
	}
	select {
	case <-connClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("connection was not force-closed after the grace period")
	}
}

func TestShutdown_NormalShutdownIsNotAServingFailure(t *testing.T) {
	_, cancel, result := startTestServer(t, http.NotFoundHandler(), time.Second)
	cancel()
	if err := waitErr(t, result, 3*time.Second, "runServer to return"); err != nil {
		t.Fatalf("clean shutdown reported error (http.ErrServerClosed leaked?): %v", err)
	}
}

func TestRunServer_GenuineServeErrorIsReported(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close() // Serve on a closed listener fails immediately
	srv := &http.Server{Handler: http.NotFoundHandler()}
	res := make(chan error, 1)
	go func() { res <- runServer(context.Background(), srv, ln, time.Second) }()
	err = waitErr(t, res, 3*time.Second, "serve failure")
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("expected a genuine serve error, got %v", err)
	}
}

func TestStartWorker_OutlivesSignalAndStopsOnStop(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	running := make(chan struct{})
	exited := make(chan struct{})
	stop := startWorker(parent, func(ctx context.Context) {
		close(running)
		<-ctx.Done()
		close(exited)
	})
	<-running

	cancelParent() // the "signal": must not kill the worker mid-drain
	select {
	case <-exited:
		t.Fatal("worker stopped on signal cancellation; it must run until explicitly stopped")
	case <-time.After(50 * time.Millisecond):
	}

	if !stop(2 * time.Second) {
		t.Fatal("stop did not observe worker exit")
	}
	select {
	case <-exited:
	default:
		t.Fatal("worker context was not cancelled by stop")
	}
}

func TestStartWorker_StopReportsStuckWorker(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	stop := startWorker(context.Background(), func(context.Context) { <-block })
	if stop(50 * time.Millisecond) {
		t.Fatal("stop reported success for a worker that never exited")
	}
}

func TestShutdownTimeoutFromEnv(t *testing.T) {
	cases := []struct {
		env     string
		want    time.Duration
		wantErr bool
	}{
		{"", defaultShutdownTimeout, false},
		{"30s", 30 * time.Second, false},
		{"abc", 0, true},
		{"0s", 0, true},
		{"-1s", 0, true},
	}
	for _, c := range cases {
		t.Setenv("VG_SHUTDOWN_TIMEOUT", c.env)
		got, err := shutdownTimeoutFromEnv()
		if (err != nil) != c.wantErr || got != c.want {
			t.Errorf("env %q: got (%v, %v), want (%v, err=%v)", c.env, got, err, c.want, c.wantErr)
		}
	}
	if defaultShutdownTimeout != 15*time.Second {
		t.Errorf("default shutdown timeout = %s, want 15s", defaultShutdownTimeout)
	}
}

// TestSignalIntegration_Subprocess runs the real gateway (memory mode) in a
// child process and delivers real SIGTERM / SIGINT to that child only.
func TestSignalIntegration_Subprocess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX signals not supported on windows")
	}
	for _, sig := range []os.Signal{syscall.SIGTERM, os.Interrupt} {
		t.Run(sig.String(), func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			ln.Close() // child rebinds this port; small race window is acceptable for a test

			var out bytes.Buffer
			cmd := exec.Command(os.Args[0], "-test.run=^$")
			cmd.Env = append(os.Environ(),
				childEnv+"=1",
				"VG_ADDR="+addr,
				"VG_STORE_MODE=memory",
				"VG_UPSTREAM_URL=",
				"VG_POSTGRES_DSN=",
				"VG_SHUTDOWN_TIMEOUT=5s",
			)
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			t.Cleanup(func() { cmd.Process.Kill() })

			// Wait for the child to serve.
			deadline := time.Now().Add(10 * time.Second)
			for {
				resp, err := newClient().Get("http://" + addr + "/health")
				if err == nil {
					resp.Body.Close()
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("child never became healthy; output:\n%s", out.String())
				}
				time.Sleep(25 * time.Millisecond)
			}

			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-exited:
				if err != nil {
					t.Fatalf("child exited uncleanly after %v: %v\noutput:\n%s", sig, err, out.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("child did not exit after %v\noutput:\n%s", sig, out.String())
			}
			if !strings.Contains(out.String(), "graceful shutdown complete") {
				t.Fatalf("child exited without logging graceful shutdown:\n%s", out.String())
			}
		})
	}
}
