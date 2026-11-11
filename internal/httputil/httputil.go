package httputil

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const defaultCSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'self'"

var safeNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)

// SecurityHeaders wraps an http.Handler with standard browser hardening headers
// and restricts HTTP methods to GET and POST.
//
// TODO(security): Per-IP rate limiting is intentionally omitted at this inner
// server layer so 24-tenant Commit Wave bursts from the trusted Dispatcher are
// not throttled; rate limiting belongs at the public GKE Gateway/Ingress edge.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		h.Set("Content-Security-Policy", defaultCSP)
		if h.Get("Cache-Control") == "" {
			h.Set("Cache-Control", "no-store")
		}

		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			h.Set("Allow", "GET, POST")
			WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{
				"error": "method not allowed",
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

// WriteJSON encodes payload as application/json with the provided HTTP status code.
func WriteJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

// SecureSubpath validates untrustedSegment as a single safe identifier and returns
// its resolved path strictly inside baseDir, guarding against directory traversal
// and prefix-matching bypasses.
func SecureSubpath(baseDir, untrustedSegment string) (string, error) {
	if untrustedSegment == "" || !safeNamePattern.MatchString(untrustedSegment) || strings.Contains(untrustedSegment, "..") {
		return "", fmt.Errorf("invalid identifier %q", untrustedSegment)
	}
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolve base directory: %w", err)
	}
	cleanBase := filepath.Clean(absBase)
	resolved := filepath.Clean(filepath.Join(cleanBase, untrustedSegment))
	if !strings.HasPrefix(resolved, cleanBase+string(os.PathSeparator)) {
		return "", errors.New("resolved path escapes base directory")
	}
	return resolved, nil
}

// SecureRelativeDir resolves a relative directory path (such as "./" or "overlays/prod")
// strictly inside rootDir, rejecting absolute paths, symlink escapes, or ".." traversals.
func SecureRelativeDir(rootDir, relPath string) (string, error) {
	if filepath.IsAbs(relPath) {
		return "", errors.New("overlay path must be relative")
	}
	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return "", fmt.Errorf("resolve root directory: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", fmt.Errorf("evaluate root directory symlinks: %w", err)
	}
	cleanRoot := filepath.Clean(realRoot)

	if relPath == "" || relPath == "." || relPath == "./" {
		return cleanRoot, nil
	}

	candidate := filepath.Clean(filepath.Join(cleanRoot, relPath))
	if candidate != cleanRoot && !strings.HasPrefix(candidate, cleanRoot+string(os.PathSeparator)) {
		return "", errors.New("overlay path escapes repository worktree")
	}

	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("overlay path %q does not exist in repository", relPath)
		}
		return "", fmt.Errorf("evaluate overlay path: %w", err)
	}
	cleanCandidate := filepath.Clean(realCandidate)
	if cleanCandidate != cleanRoot && !strings.HasPrefix(cleanCandidate, cleanRoot+string(os.PathSeparator)) {
		return "", errors.New("overlay symlink escapes repository worktree")
	}
	return cleanCandidate, nil
}

// RandomToken generates a cryptographically random hex token of byteLen bytes.
func RandomToken(byteLen int) (string, error) {
	if byteLen <= 0 {
		byteLen = 32
	}
	buf := make([]byte, byteLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
