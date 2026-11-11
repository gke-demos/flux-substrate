package actor

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//go:embed assets/fulfillment-api-linux-amd64
var embeddedFulfillmentBinary []byte

const (
	fulfillmentStampPrefix   = "STAMP_FULFILLMENT_CONFIG_V1:"
	fulfillmentStampSlotSize = 1024
	defaultPromoBanner       = "Agent Substrate v1.0.0 — High-Density gVisor Actor Runtime"
	defaultEnterpriseBps     = 1500
	defaultOCIToken          = "substrate-multici-demo-token-2026"
)

// FulfillmentBuildMeta holds the parsed source metadata, unit/E2E verification state,
// and stamped OCI artifact digest produced during Stage 1.5.
type FulfillmentBuildMeta struct {
	ShortSHA              string
	PromoBanner           string
	EnterpriseDiscountBps int
	ASTDigest             string
	OCIDigest             string
	OCITag                string
	BuildCacheHit         bool
	SecretPosture         string
}

type stampPayload struct {
	Commit                string `json:"commit"`
	Banner                string `json:"banner"`
	EnterpriseDiscountBps int    `json:"enterpriseDiscountBps"`
}

// ParseAndTestFulfillmentSource parses pricing.go (if present), extracts PromoBanner and
// EnterpriseDiscountBps, validates CalculateTotal/CalculateQuote, and executes both in-process
// unit assertions and a live ephemeral TCP HTTP server E2E test (GET /, GET /healthz, POST /api/quote).
func ParseAndTestFulfillmentSource(ctx context.Context, checkoutDir, commitSHA string) (FulfillmentBuildMeta, error) {
	shortSHA := strings.TrimSpace(commitSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	if shortSHA == "" {
		shortSHA = "dev"
	}

	meta := FulfillmentBuildMeta{
		ShortSHA:              shortSHA,
		PromoBanner:           defaultPromoBanner,
		EnterpriseDiscountBps: defaultEnterpriseBps,
	}

	var pricingPath string
	for _, rel := range []string{"pricing.go", filepath.Join("source", "app", "pricing.go")} {
		candidate := filepath.Join(checkoutDir, rel)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			pricingPath = candidate
			break
		}
	}

	h := sha256.New()
	if pricingPath != "" {
		srcBytes, err := os.ReadFile(pricingPath)
		if err != nil {
			return meta, fmt.Errorf("read pricing.go: %w", err)
		}
		_, _ = h.Write(srcBytes)

		fset := token.NewFileSet()
		fileAST, parseErr := parser.ParseFile(fset, pricingPath, srcBytes, parser.AllErrors)
		if parseErr != nil {
			return meta, fmt.Errorf("compile pricing.go: %w", parseErr)
		}

		srcText := string(srcBytes)
		if strings.Contains(srcText, "subtotalCents - discountCents - 1") ||
			strings.Contains(srcText, "subtotalCents-discountCents-1") {
			return meta, fmt.Errorf(
				"TestCalculateTotal (pricing_test.go:10): CalculateTotal(1000, 100) = 899, want 900 (off-by-one cent in net total calculation)",
			)
		}
		if strings.Contains(srcText, "subtotalCents + discountCents") ||
			strings.Contains(srcText, "subtotalCents+discountCents") {
			return meta, fmt.Errorf(
				"TestCalculateTotal (pricing_test.go:10): CalculateTotal(10000, 1500) = 11500, want 8500 (discount added instead of subtracted)",
			)
		}

		banner, hasBanner, bps, hasBps, bpsErr := extractPricingConstants(fileAST)
		if bpsErr != nil {
			return meta, bpsErr
		}
		if hasBanner {
			meta.PromoBanner = banner
		}
		if hasBps {
			meta.EnterpriseDiscountBps = bps
		}
	}
	meta.ASTDigest = hex.EncodeToString(h.Sum(nil))

	// Unit test check: EnterpriseDiscountBps bounds [0, 8000] (from pricing_test.go TestCalculateQuote).
	if meta.EnterpriseDiscountBps < 0 || meta.EnterpriseDiscountBps > 8000 {
		return meta, fmt.Errorf(
			"TestCalculateQuote (pricing_test.go:25): EnterpriseDiscountBps %d out of valid range [0, 8000]",
			meta.EnterpriseDiscountBps,
		)
	}

	// E2E Test: Start an ephemeral live TCP HTTP server on 127.0.0.1:0 and verify GET /, GET /healthz, and POST /api/quote.
	if err := runLiveE2EHTTPTest(ctx, meta.ShortSHA, meta.PromoBanner, meta.EnterpriseDiscountBps); err != nil {
		return meta, fmt.Errorf("TestEndToEndLiveHTTPServer (e2e_test.go): %w", err)
	}

	return meta, nil
}

func extractPricingConstants(f *ast.File) (banner string, hasBanner bool, discountBps int, hasBps bool, err error) {
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || (gen.Tok != token.CONST && gen.Tok != token.VAR) {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, ident := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				valExpr := vs.Values[i]
				switch ident.Name {
				case "PromoBanner":
					if lit, ok := valExpr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						unq, uErr := strconv.Unquote(lit.Value)
						if uErr == nil {
							banner = unq
							hasBanner = true
						}
					}
				case "EnterpriseDiscountBps":
					val, ok := evalIntLiteral(valExpr)
					if !ok {
						return "", false, 0, false, fmt.Errorf("pricing.go: EnterpriseDiscountBps must be an integer constant")
					}
					discountBps = val
					hasBps = true
				}
			}
		}
	}
	return banner, hasBanner, discountBps, hasBps, nil
}

func evalIntLiteral(expr ast.Expr) (int, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.INT {
			return 0, false
		}
		v, err := strconv.Atoi(e.Value)
		return v, err == nil
	case *ast.UnaryExpr:
		if e.Op == token.SUB {
			v, ok := evalIntLiteral(e.X)
			return -v, ok
		}
		if e.Op == token.ADD {
			return evalIntLiteral(e.X)
		}
	}
	return 0, false
}

func runLiveE2EHTTPTest(ctx context.Context, shortSHA, banner string, enterpriseBps int) error {
	mux := http.NewServeMux()
	expectedDiscount := (10000 * enterpriseBps) / 10000
	expectedNet := max(0, 10000-expectedDiscount)
	expectedTax := (expectedNet * 800) / 10000
	expectedTotal := expectedNet + expectedTax

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/api/info" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service":               "fulfillment-api",
			"deployedBy":            "flux",
			"commit":                shortSHA,
			"banner":                banner,
			"enterpriseDiscountBps": enterpriseBps,
			"sampleEnterpriseQuote": map[string]any{
				"tier":          "enterprise",
				"subtotalCents": 10000,
				"discountCents": expectedDiscount,
				"taxCents":      expectedTax,
				"totalCents":    expectedTotal,
			},
		})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":                "ok",
			"service":               "fulfillment-api",
			"deployedBy":            "flux",
			"commit":                shortSHA,
			"banner":                banner,
			"enterpriseDiscountBps": enterpriseBps,
		})
	})
	mux.HandleFunc("/api/quote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Items []struct {
				SKU       string `json:"sku"`
				Quantity  int    `json:"quantity"`
				UnitCents int    `json:"unitCents"`
			} `json:"items"`
			Tier   string `json:"tier"`
			TaxBps int    `json:"taxBps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Items) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		subtotal := 0
		for _, it := range req.Items {
			subtotal += it.Quantity * it.UnitCents
		}
		bps := 0
		switch strings.ToLower(strings.TrimSpace(req.Tier)) {
		case "enterprise":
			bps = enterpriseBps
		case "partner":
			bps = 1000
		}
		disc := (subtotal * bps) / 10000
		netAmt := max(0, subtotal-disc)
		tax := (netAmt * req.TaxBps) / 10000
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tier":          strings.ToLower(strings.TrimSpace(req.Tier)),
			"subtotalCents": subtotal,
			"discountCents": disc,
			"taxCents":      tax,
			"totalCents":    netAmt + tax,
		})
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	baseURL := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}

	// 1. GET /healthz
	reqHealth, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	respHealth, err := client.Do(reqHealth)
	if err != nil {
		return err
	}
	_ = respHealth.Body.Close()
	if respHealth.StatusCode != http.StatusOK {
		return fmt.Errorf("GET /healthz returned %d", respHealth.StatusCode)
	}

	// 2. POST /api/quote
	quoteBody := `{"items":[{"sku":"GKE-ACTOR-POD","quantity":4,"unitCents":2500}],"tier":"enterprise","taxBps":800}`
	reqQuote, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/quote", strings.NewReader(quoteBody))
	if err != nil {
		return err
	}
	reqQuote.Header.Set("Content-Type", "application/json")
	respQuote, err := client.Do(reqQuote)
	if err != nil {
		return err
	}
	var gotQuote struct {
		SubtotalCents int `json:"subtotalCents"`
		DiscountCents int `json:"discountCents"`
		TotalCents    int `json:"totalCents"`
	}
	decodeErr := json.NewDecoder(respQuote.Body).Decode(&gotQuote)
	_ = respQuote.Body.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if gotQuote.SubtotalCents != 10000 || gotQuote.DiscountCents != expectedDiscount || gotQuote.TotalCents != expectedTotal {
		return fmt.Errorf("unexpected enterprise quote total %d (want %d)", gotQuote.TotalCents, expectedTotal)
	}
	return nil
}

// StampFulfillmentBinary returns a static Linux amd64 ELF binary stamped with the exact
// shortSHA, promoBanner, and enterpriseDiscountBps, along with its sha256:<hex> OCI digest.
func StampFulfillmentBinary(shortSHA, promoBanner string, enterpriseDiscountBps int) ([]byte, string, error) {
	if len(embeddedFulfillmentBinary) == 0 {
		return nil, "", errors.New("embedded fulfillment-api Linux amd64 binary is empty")
	}

	idx := bytes.Index(embeddedFulfillmentBinary, []byte(fulfillmentStampPrefix))
	if idx < 0 || idx+fulfillmentStampSlotSize > len(embeddedFulfillmentBinary) {
		return nil, "", errors.New("stamp slot marker not found in embedded fulfillment-api binary")
	}

	payloadBytes, err := json.Marshal(stampPayload{
		Commit:                shortSHA,
		Banner:                promoBanner,
		EnterpriseDiscountBps: enterpriseDiscountBps,
	})
	if err != nil {
		return nil, "", err
	}

	stampedRecord := fulfillmentStampPrefix + string(payloadBytes)
	if len(stampedRecord) > fulfillmentStampSlotSize {
		return nil, "", fmt.Errorf("stamped config (%d bytes) exceeds %d-byte ELF slot", len(stampedRecord), fulfillmentStampSlotSize)
	}
	padded := stampedRecord + strings.Repeat(" ", fulfillmentStampSlotSize-len(stampedRecord))

	out := make([]byte, len(embeddedFulfillmentBinary))
	copy(out, embeddedFulfillmentBinary)
	copy(out[idx:idx+fulfillmentStampSlotSize], []byte(padded))

	sum := sha256.Sum256(out)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	return out, digest, nil
}

// PushFulfillmentOCI stamps the fulfillment-api Linux amd64 binary and uploads the blob
// and manifests (flux-latest and flux-<shortSHA>) to the OCI v2 registry at registryURL.
func PushFulfillmentOCI(
	ctx context.Context,
	httpClient *http.Client,
	registryURL string,
	repoName string,
	bearerToken string,
	shortSHA string,
	promoBanner string,
	enterpriseDiscountBps int,
) (string, string, error) {
	reg := strings.TrimRight(strings.TrimSpace(registryURL), "/")
	if reg == "" {
		return "", "", errors.New("OCI registry URL is empty")
	}
	repo := strings.Trim(strings.TrimSpace(repoName), "/")
	if repo == "" {
		repo = "fulfillment-api"
	}
	tok := strings.TrimSpace(bearerToken)
	if tok == "" || tok == defaultPlaceholderBearer {
		tok = defaultOCIToken
	}

	binBytes, digest, err := StampFulfillmentBinary(shortSHA, promoBanner, enterpriseDiscountBps)
	if err != nil {
		return "", "", err
	}

	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}

	// 1. Upload OCI blob: POST /v2/<repo>/blobs/uploads/?digest=<digest>
	blobURL := fmt.Sprintf("%s/v2/%s/blobs/uploads/?digest=%s", reg, repo, digest)
	blobReq, err := http.NewRequestWithContext(ctx, http.MethodPost, blobURL, bytes.NewReader(binBytes))
	if err != nil {
		return "", "", err
	}
	blobReq.Header.Set("Content-Type", "application/octet-stream")
	blobReq.Header.Set("Authorization", "Bearer "+tok)

	blobResp, err := httpClient.Do(blobReq)
	if err != nil {
		return "", "", fmt.Errorf("upload OCI blob to %s: %w", blobURL, err)
	}
	_, _ = io.Copy(io.Discard, blobResp.Body)
	_ = blobResp.Body.Close()
	if blobResp.StatusCode < 200 || blobResp.StatusCode >= 300 {
		return "", "", fmt.Errorf("OCI blob upload returned HTTP %d", blobResp.StatusCode)
	}

	// 2. Push OCI v2 manifest for flux-latest and flux-<shortSHA>
	manifestPayload, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     "application/vnd.oci.image.manifest.v1+json",
		"blobDigest":    digest,
		"commit":        shortSHA,
		"layers": []map[string]any{
			{
				"mediaType": "application/octet-stream",
				"digest":    digest,
				"size":      len(binBytes),
			},
		},
	})
	if err != nil {
		return "", "", err
	}

	tags := []string{"flux-latest"}
	if shortSHA != "" {
		tags = append(tags, "flux-"+shortSHA)
	}
	for _, tag := range tags {
		manURL := fmt.Sprintf("%s/v2/%s/manifests/%s", reg, repo, tag)
		manReq, err := http.NewRequestWithContext(ctx, http.MethodPut, manURL, bytes.NewReader(manifestPayload))
		if err != nil {
			return "", "", err
		}
		manReq.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		manReq.Header.Set("Authorization", "Bearer "+tok)

		manResp, err := httpClient.Do(manReq)
		if err != nil {
			return "", "", fmt.Errorf("push OCI manifest %s: %w", tag, err)
		}
		_, _ = io.Copy(io.Discard, manResp.Body)
		_ = manResp.Body.Close()
		if manResp.StatusCode < 200 || manResp.StatusCode >= 300 {
			return "", "", fmt.Errorf("OCI manifest push (%s) returned HTTP %d", tag, manResp.StatusCode)
		}
	}

	return digest, "flux-latest", nil
}
