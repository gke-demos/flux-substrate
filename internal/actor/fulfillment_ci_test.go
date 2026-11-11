package actor

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestParseAndTestFulfillmentSourceAndOCIPush(t *testing.T) {
	dir := t.TempDir()
	pricingGo := `package main

const PromoBanner = "Agent Substrate v1.0.0 — Custom Gitea Promo"
const EnterpriseDiscountBps = 2200

func CalculateTotal(subtotalCents, discountCents int) int {
	if discountCents <= 0 {
		return subtotalCents
	}
	if discountCents >= subtotalCents {
		return 0
	}
	return subtotalCents - discountCents
}
`
	if err := os.WriteFile(filepath.Join(dir, "pricing.go"), []byte(pricingGo), 0o640); err != nil {
		t.Fatalf("write pricing.go: %v", err)
	}

	meta, err := ParseAndTestFulfillmentSource(context.Background(), dir, "b9cdaca979419cfc09ab4cb360d294dae40b4000")
	if err != nil {
		t.Fatalf("ParseAndTestFulfillmentSource failed: %v", err)
	}
	if meta.ShortSHA != "b9cdaca" {
		t.Fatalf("ShortSHA = %q, want b9cdaca", meta.ShortSHA)
	}
	if meta.PromoBanner != "Agent Substrate v1.0.0 — Custom Gitea Promo" {
		t.Fatalf("PromoBanner = %q", meta.PromoBanner)
	}
	if meta.EnterpriseDiscountBps != 2200 {
		t.Fatalf("EnterpriseDiscountBps = %d, want 2200", meta.EnterpriseDiscountBps)
	}

	// Verify out-of-range EnterpriseDiscountBps fails unit test check.
	badBpsGo := strings.Replace(pricingGo, "2200", "9500", 1)
	if err := os.WriteFile(filepath.Join(dir, "pricing.go"), []byte(badBpsGo), 0o640); err != nil {
		t.Fatalf("write bad pricing.go: %v", err)
	}
	if _, err := ParseAndTestFulfillmentSource(context.Background(), dir, "deadbeef"); err == nil {
		t.Fatalf("expected ParseAndTestFulfillmentSource to fail when EnterpriseDiscountBps > 8000")
	}

	// Verify StampFulfillmentBinary and PushFulfillmentOCI against a mock OCI v2 registry.
	var mu sync.Mutex
	blobs := make(map[string][]byte)
	manifests := make(map[string][]byte)

	ociSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v2/fulfillment-api/blobs/uploads/"):
			dg := r.URL.Query().Get("digest")
			raw, _ := io.ReadAll(r.Body)
			blobs[dg] = raw
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v2/fulfillment-api/manifests/"):
			tag := strings.TrimPrefix(r.URL.Path, "/v2/fulfillment-api/manifests/")
			raw, _ := io.ReadAll(r.Body)
			manifests[tag] = raw
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ociSrv.Close()

	digest, tag, err := PushFulfillmentOCI(
		context.Background(),
		ociSrv.Client(),
		ociSrv.URL,
		"fulfillment-api",
		"test-token",
		meta.ShortSHA,
		meta.PromoBanner,
		meta.EnterpriseDiscountBps,
	)
	if err != nil {
		t.Fatalf("PushFulfillmentOCI failed: %v", err)
	}
	if !strings.HasPrefix(digest, "sha256:") || tag != "flux-latest" {
		t.Fatalf("unexpected digest=%q tag=%q", digest, tag)
	}

	mu.Lock()
	blob := blobs[digest]
	manLatest := manifests["flux-latest"]
	manCommit := manifests["flux-b9cdaca"]
	mu.Unlock()

	if len(blob) == 0 || !bytes.Contains(blob, []byte(`"commit":"b9cdaca"`)) || !bytes.Contains(blob, []byte(`"enterpriseDiscountBps":2200`)) {
		t.Fatalf("stamped ELF binary blob missing expected stamped JSON payload")
	}
	if len(manLatest) == 0 || len(manCommit) == 0 {
		t.Fatalf("expected both flux-latest and flux-b9cdaca manifests to be pushed")
	}
	var parsedMan map[string]any
	if err := json.Unmarshal(manLatest, &parsedMan); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if parsedMan["blobDigest"] != digest {
		t.Fatalf("manifest blobDigest = %v, want %s", parsedMan["blobDigest"], digest)
	}
}
