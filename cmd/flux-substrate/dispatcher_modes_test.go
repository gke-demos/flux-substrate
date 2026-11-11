package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gke-demos/flux-substrate/internal/dispatcher"
)

func reserveDispatcherLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.1:0: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestRunModeLocalBoots24TenantFleetOnLoopback(t *testing.T) {
	addr := reserveDispatcherLoopbackAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"--mode=local",
			"--addr=" + addr,
			"--data-dir=" + t.TempDir(),
		})
	}()

	baseURL := "http://" + addr
	httpClient := &http.Client{Timeout: 3 * time.Second}

	pollCtx, pollCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer pollCancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	var ready bool
	for !ready {
		select {
		case runErr := <-errCh:
			t.Fatalf("run(--mode=local) exited early: %v", runErr)
		case <-pollCtx.Done():
			t.Fatalf("timed out waiting for --mode=local to reconcile 24 tenants at %s", baseURL)
		case <-ticker.C:
		}

		resp, err := httpClient.Get(baseURL + "/api/state")
		if err == nil {
			var body struct {
				Mode    string `json:"mode"`
				Summary struct {
					TotalTenants    int `json:"totalTenants"`
					ReadyTenants    int `json:"readyTenants"`
					SuspendedActors int `json:"suspendedActors"`
				} `json:"summary"`
			}
			decErr := json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && decErr == nil &&
				body.Mode == "local" &&
				body.Summary.TotalTenants == 24 &&
				body.Summary.ReadyTenants == 24 &&
				body.Summary.SuspendedActors == 24 {
				ready = true
			}
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run(--mode=local) shutdown error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for --mode=local graceful shutdown")
	}
}

func TestRunModeDispatcherBootsAgainstKubeconfig(t *testing.T) {
	lk, err := dispatcher.NewLocalKubeServer()
	if err != nil {
		t.Fatalf("NewLocalKubeServer: %v", err)
	}
	t.Cleanup(lk.Close)

	kubeconfigPath := filepath.Join(t.TempDir(), "kubeconfig.yaml")
	kubeconfigYAML := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: %s
contexts:
- name: local
  context:
    cluster: local
    user: local
current-context: local
users:
- name: local
  user: {}
`, lk.RestConfig().Host)
	if err := os.WriteFile(kubeconfigPath, []byte(kubeconfigYAML), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	addr := reserveDispatcherLoopbackAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"--mode=dispatcher",
			"--addr=" + addr,
			"--kubeconfig=" + kubeconfigPath,
			"--router-url=http://127.0.0.1:9",
		})
	}()

	pollCtx, pollCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pollCancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	httpClient := &http.Client{Timeout: 2 * time.Second}
	var ready bool
	for !ready {
		select {
		case runErr := <-errCh:
			t.Fatalf("run(--mode=dispatcher) exited early: %v", runErr)
		case <-pollCtx.Done():
			t.Fatal("timed out waiting for --mode=dispatcher /readyz")
		case <-ticker.C:
		}
		resp, reqErr := httpClient.Get("http://" + addr + "/readyz")
		if reqErr == nil {
			var body map[string]string
			_ = json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && body["mode"] == "dispatcher" {
				ready = true
			}
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run(--mode=dispatcher) shutdown error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for --mode=dispatcher shutdown")
	}
}
