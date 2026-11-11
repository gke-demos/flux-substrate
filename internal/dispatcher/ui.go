package dispatcher

import (
	"embed"
	"net/http"
	"path"
	"strings"
)

//go:embed static/*
var staticAssets embed.FS

// EmbeddedStaticAssetsForTest exposes the embedded static assets filesystem for invariant tests.
func EmbeddedStaticAssetsForTest() embed.FS {
	return staticAssets
}

var staticContentTypes = map[string]string{
	"index.html": "text/html; charset=utf-8",
	"about.html": "text/html; charset=utf-8",
	"styles.css": "text/css; charset=utf-8",
	"app.js":     "application/javascript; charset=utf-8",
	"about.js":   "application/javascript; charset=utf-8",
}

func (d *Dispatcher) registerUIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", d.handleIndexPage)
	mux.HandleFunc("GET /about", d.handleAboutPage)
	mux.HandleFunc("GET /static/{file}", d.handleStaticAsset)
}

func (d *Dispatcher) handleIndexPage(w http.ResponseWriter, _ *http.Request) {
	serveEmbeddedFile(w, "index.html")
}

func (d *Dispatcher) handleAboutPage(w http.ResponseWriter, _ *http.Request) {
	serveEmbeddedFile(w, "about.html")
}

func (d *Dispatcher) handleStaticAsset(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.PathValue("file"))
	if name == "" || strings.Contains(name, "/") || strings.Contains(name, "..") {
		http.NotFound(w, r)
		return
	}
	// Only expose CSS and JS bundles under /static/{file}; HTML pages are served at / and /about.
	if !strings.HasSuffix(name, ".css") && !strings.HasSuffix(name, ".js") {
		http.NotFound(w, r)
		return
	}
	serveEmbeddedFile(w, name)
}

func serveEmbeddedFile(w http.ResponseWriter, name string) {
	contentType, ok := staticContentTypes[name]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	data, err := staticAssets.ReadFile(path.Join("static", name))
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
