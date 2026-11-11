package dispatcher_test

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gke-demos/flux-substrate/internal/dispatcher"
)

func TestMissionControlAndAboutUIRoutes(t *testing.T) {
	backend, err := dispatcher.NewLocalActorBackend(dispatcher.LocalActorBackendConfig{
		BaseDurableDir: t.TempDir(),
		RestConfig:     sharedRestCfg,
	})
	if err != nil {
		t.Fatalf("NewLocalActorBackend: %v", err)
	}
	disp, err := dispatcher.New(dispatcher.Config{
		Mode:       "local",
		KubeClient: sharedKubeClient,
		Backend:    backend,
	})
	if err != nil {
		t.Fatalf("dispatcher.New: %v", err)
	}

	cases := []struct {
		name            string
		path            string
		wantStatus      int
		wantContentType string
		mustContain     []string
	}{
		{
			name:            "MissionControlIndex",
			path:            "/",
			wantStatus:      http.StatusOK,
			wantContentType: "text/html; charset=utf-8",
			mustContain: []string{
				"FluxCD on Agent Substrate",
				`href="/about"`,
				`id="theme-toggle"`,
				`id="theme-dark-btn"`,
				`id="theme-light-btn"`,
				`id="workers-grid"`,
				`id="tenants-grid"`,
				`VIEW-ONLY TELEMETRY DASHBOARD`,
				`id="sso-session-pill"`,
				`id="suite-link-hub"`,
				`id="suite-link-flux"`,
				`id="suite-link-argo"`,
				`id="suite-link-tekton"`,
				`https://cicd.substrate.learngke.dev/`,
				`https://gitea.cicd.substrate.learngke.dev/substrate/fulfillment-api`,
				`1. WARM VS. COLD BUILD SPEED`,
				`2. HIGH-DENSITY MULTIPLEXING WAVE`,
				`3. BAD PUSH &amp; REVERTACTOR`,
				`4. ZERO-SECRET EGRESS INJECTION`,
				`3-Pod / 9-Slot gVisor WorkerPool Multiplexer (W1..W3)`,
				`1. Live Execution Canvas`,
				`2. Actor &amp; Snapshot Turn Inspector`,
			},
		},
		{
			name:            "HowItWorksAbout",
			path:            "/about",
			wantStatus:      http.StatusOK,
			wantContentType: "text/html; charset=utf-8",
			mustContain: []string{
				"How It Works",
				`href="/"`,
				`id="theme-toggle"`,
				`id="topology-svg"`,
				`id="flow-btn-0"`,
				`id="flow-btn-1"`,
				`id="flow-btn-2"`,
				`id="flow-btn-3"`,
				`id="signal-packet"`,
				`Multi-Tenant GitOps Architecture Comparison`,
			},
		},
		{
			name:            "StylesheetBundle",
			path:            "/static/styles.css",
			wantStatus:      http.StatusOK,
			wantContentType: "text/css; charset=utf-8",
			mustContain: []string{
				`:root[data-theme="dark"]`,
				`:root[data-theme="light"]`,
				`.theme-toggle`,
				`.tenants-grid`,
			},
		},
		{
			name:            "MissionControlJS",
			path:            "/static/app.js",
			wantStatus:      http.StatusOK,
			wantContentType: "application/javascript; charset=utf-8",
			mustContain: []string{
				"/api/state",
				"/api/auth/session",
				"/api/tenants/",
				"https://gitea.cicd.substrate.learngke.dev/substrate/fulfillment-api.git",
			},
		},
		{
			name:            "AboutArchitectureJS",
			path:            "/static/about.js",
			wantStatus:      http.StatusOK,
			wantContentType: "application/javascript; charset=utf-8",
			mustContain: []string{
				"SNAPSHOT_FIDELITY_VOLUMES",
				"Impersonate-User",
				"ate-secret://",
				"RevertActor",
			},
		},
		{
			name:       "UnmappedRootRouteReturns404",
			path:       "/nonexistent-page",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StaticHTMLDirectAccessBlocked",
			path:       "/static/index.html",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			disp.Handler().ServeHTTP(rec, req)

			res := rec.Result()
			defer res.Body.Close()
			bodyBytes, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}
			body := string(bodyBytes)

			if res.StatusCode != tc.wantStatus {
				t.Fatalf("GET %s status = %d, want %d (body: %s)", tc.path, res.StatusCode, tc.wantStatus, body)
			}
			if tc.wantContentType != "" {
				if got := res.Header.Get("Content-Type"); got != tc.wantContentType {
					t.Fatalf("GET %s Content-Type = %q, want %q", tc.path, got, tc.wantContentType)
				}
			}
			if csp := res.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
				t.Fatalf("GET %s missing strict Content-Security-Policy header: %q", tc.path, csp)
			}
			for _, snippet := range tc.mustContain {
				if !strings.Contains(body, snippet) {
					t.Errorf("GET %s body missing expected snippet %q", tc.path, snippet)
				}
			}
		})
	}
}

func TestStaticAssetsZeroEmojisAndSafeDOMInvariants(t *testing.T) {
	assets := dispatcher.EmbeddedStaticAssetsForTest()

	forbiddenPatterns := []string{
		"innerHTML",
		"outerHTML",
		"insertAdjacentHTML",
		"document.write",
		"localStorage",
		"sessionStorage",
		"alert(",
		"confirm(",
		"prompt(",
		"style=",
		".style.",
		"Vertex AI",
		"fluid compute",
		"mofilabs",
		"ate-cluster",
	}

	entries, err := fs.ReadDir(assets, "static")
	if err != nil {
		t.Fatalf("read embedded static dir: %v", err)
	}
	if len(entries) < 5 {
		t.Fatalf("expected at least 5 static files, got %d", len(entries))
	}

	for _, entry := range entries {
		name := entry.Name()
		raw, err := fs.ReadFile(assets, "static/"+name)
		if err != nil {
			t.Fatalf("read static/%s: %v", name, err)
		}
		content := string(raw)

		for _, r := range content {
			if isEmojiRune(r) {
				t.Errorf("static/%s contains forbidden emoji rune U+%04X (%q); only SVG icons are permitted", name, r, string(r))
			}
		}

		for _, forbidden := range forbiddenPatterns {
			if strings.Contains(content, forbidden) {
				t.Errorf("static/%s contains forbidden pattern %q", name, forbidden)
			}
		}
	}
}

func isEmojiRune(r rune) bool {
	switch {
	case r >= 0x1F300 && r <= 0x1FAFF: // Misc Symbols/Pictographs, Emoticons, Transport, Supplemental Symbols
		return true
	case r >= 0x2600 && r <= 0x27BF: // Misc Symbols & Dingbats
		return true
	case r >= 0xFE00 && r <= 0xFE0F: // Variation Selectors
		return true
	default:
		return false
	}
}
