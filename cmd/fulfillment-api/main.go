// Command fulfillment-api is the standalone Linux amd64 HTTP service binary
// packaged and pushed as an OCI v2 artifact by the FluxCD Workflow Actor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	stampPrefix       = "STAMP_FULFILLMENT_CONFIG_V1:"
	maxQuoteBodyBytes = 64 << 10
)

// StampSlot is a fixed 1024-byte region in .rodata that the Workflow Actor
// overwrites in-place with padded JSON before uploading the binary blob to git-oci.
// Exact length: 28 (prefix) + 996 (payload + space padding) = 1024 bytes.
var StampSlot = "STAMP_FULFILLMENT_CONFIG_V1:{\"commit\":\"dev\",\"banner\":\"Agent Substrate v1.0.0 — High-Density gVisor Actor Runtime\",\"enterpriseDiscountBps\":1500}" +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                                    " +
	"                                                                                  "

var (
	BuildCommit           = "dev"
	PromoBanner           = "Agent Substrate v1.0.0 — High-Density gVisor Actor Runtime"
	EnterpriseDiscountBps = 1500
)

type stampedConfig struct {
	Commit                string `json:"commit"`
	Banner                string `json:"banner"`
	EnterpriseDiscountBps int    `json:"enterpriseDiscountBps"`
}

func initStampedConfig() {
	raw := strings.TrimSpace(strings.TrimPrefix(StampSlot, stampPrefix))
	if idx := strings.IndexByte(raw, 0); idx >= 0 {
		raw = strings.TrimSpace(raw[:idx])
	}
	var cfg stampedConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
		if strings.TrimSpace(cfg.Commit) != "" {
			BuildCommit = strings.TrimSpace(cfg.Commit)
		}
		if strings.TrimSpace(cfg.Banner) != "" {
			PromoBanner = cfg.Banner
		}
		if cfg.EnterpriseDiscountBps >= 0 && cfg.EnterpriseDiscountBps <= 8000 {
			EnterpriseDiscountBps = cfg.EnterpriseDiscountBps
		}
	}
}

// LineItem represents a single order item in cents.
type LineItem struct {
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	UnitCents int    `json:"unitCents"`
}

// Quote summarizes the calculated subtotal, tier discount, tax, and net total.
type Quote struct {
	Tier          string `json:"tier"`
	SubtotalCents int    `json:"subtotalCents"`
	DiscountCents int    `json:"discountCents"`
	TaxCents      int    `json:"taxCents"`
	TotalCents    int    `json:"totalCents"`
}

// QuoteRequest is the JSON request payload for POST /api/quote.
type QuoteRequest struct {
	Items  []LineItem `json:"items"`
	Tier   string     `json:"tier"`
	TaxBps int        `json:"taxBps"`
}

// ServiceStatus is returned by GET / and GET /api/info.
type ServiceStatus struct {
	Service               string `json:"service"`
	DeployedBy            string `json:"deployedBy"`
	Commit                string `json:"commit"`
	Banner                string `json:"banner"`
	EnterpriseDiscountBps int    `json:"enterpriseDiscountBps"`
	SampleEnterpriseQuote Quote  `json:"sampleEnterpriseQuote"`
}

func DiscountBpsForTier(tier string) (int, string, error) {
	normalized := strings.ToLower(strings.TrimSpace(tier))
	switch normalized {
	case "", "standard":
		return 0, "standard", nil
	case "partner":
		return 1000, "partner", nil
	case "enterprise":
		return EnterpriseDiscountBps, "enterprise", nil
	default:
		return 0, "", fmt.Errorf("unsupported customer tier %q", tier)
	}
}

func CalculateTotal(subtotalCents, discountCents int) int {
	if discountCents <= 0 {
		return subtotalCents
	}
	if discountCents >= subtotalCents {
		return 0
	}
	return subtotalCents - discountCents
}

func CalculateQuote(items []LineItem, tier string, taxBps int) (Quote, error) {
	if len(items) == 0 {
		return Quote{}, errors.New("order must contain at least one line item")
	}
	if taxBps < 0 || taxBps > 5000 {
		return Quote{}, fmt.Errorf("invalid taxBps %d: must be between 0 and 5000", taxBps)
	}
	discountBps, canonicalTier, err := DiscountBpsForTier(tier)
	if err != nil {
		return Quote{}, err
	}

	subtotal := 0
	for i, item := range items {
		if strings.TrimSpace(item.SKU) == "" {
			return Quote{}, fmt.Errorf("item[%d]: sku is required", i)
		}
		if item.Quantity <= 0 || item.Quantity > 100000 {
			return Quote{}, fmt.Errorf("item[%d]: quantity %d out of range", i, item.Quantity)
		}
		if item.UnitCents < 0 || item.UnitCents > 100000000 {
			return Quote{}, fmt.Errorf("item[%d]: unitCents %d out of range", i, item.UnitCents)
		}
		subtotal += item.Quantity * item.UnitCents
	}

	discount := (subtotal * discountBps) / 10000
	discountedNet := CalculateTotal(subtotal, discount)
	tax := (discountedNet * taxBps) / 10000

	return Quote{
		Tier:          canonicalTier,
		SubtotalCents: subtotal,
		DiscountCents: discount,
		TaxCents:      tax,
		TotalCents:    discountedNet + tax,
	}, nil
}

func currentServiceStatus() ServiceStatus {
	deployedBy := os.Getenv("DEPLOYED_BY")
	if deployedBy == "" {
		deployedBy = "flux"
	}
	commit := BuildCommit
	if envCommit := os.Getenv("BUILD_COMMIT"); envCommit != "" && (commit == "" || commit == "dev") {
		commit = envCommit
	}
	sampleQuote, _ := CalculateQuote([]LineItem{
		{SKU: "GKE-SANDBOX-SLOT", Quantity: 4, UnitCents: 2500},
	}, "enterprise", 800)
	return ServiceStatus{
		Service:               "fulfillment-api",
		DeployedBy:            deployedBy,
		Commit:                commit,
		Banner:                PromoBanner,
		EnterpriseDiscountBps: EnterpriseDiscountBps,
		SampleEnterpriseQuote: sampleQuote,
	}
}

func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/info", handleIndex)
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/api/quote", handleQuote)
	return mux
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	setJSONSecurityHeaders(w)
	if r.URL.Path != "/" && r.URL.Path != "/api/info" {
		writeJSONError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(currentServiceStatus())
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	setJSONSecurityHeaders(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	st := currentServiceStatus()
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":                "ok",
			"service":               st.Service,
			"deployedBy":            st.DeployedBy,
			"commit":                st.Commit,
			"banner":                st.Banner,
			"enterpriseDiscountBps": st.EnterpriseDiscountBps,
		})
	}
}

func handleQuote(w http.ResponseWriter, r *http.Request) {
	setJSONSecurityHeaders(w)
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxQuoteBodyBytes)
	defer r.Body.Close()

	var req QuoteRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON request payload")
		return
	}

	quote, err := CalculateQuote(req.Items, req.Tier, req.TaxBps)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(quote)
}

func setJSONSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	h.Set("Cache-Control", "no-store")
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func main() {
	initStampedConfig()

	var listenAddr string
	flag.StringVar(&listenAddr, "listen", "0.0.0.0:8080", "HTTP listen address")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("fulfillment-api listening", "addr", listenAddr, "commit", BuildCommit, "deployedBy", os.Getenv("DEPLOYED_BY"))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}
}
