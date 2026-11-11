package dispatcher

import (
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gke-demos/flux-substrate/internal/httputil"
)

const (
	hostSessionCookieName  = "__Host-fs_session"
	localSessionCookieName = "fs_session"
	sessionTTL             = 12 * time.Hour
	loginRateWindow        = 1 * time.Minute
	maxFailedLoginsPerIP   = 10
)

type presenterSession struct {
	id        string
	csrfToken string
	expiresAt time.Time
}

type presenterAuthManager struct {
	token string

	mu             sync.Mutex
	sessions       map[string]presenterSession
	failedAttempts map[string][]time.Time
}

func newPresenterAuthManager(token string) *presenterAuthManager {
	return &presenterAuthManager{
		token:          strings.TrimSpace(token),
		sessions:       make(map[string]presenterSession),
		failedAttempts: make(map[string][]time.Time),
	}
}

func (m *presenterAuthManager) isEnabled() bool {
	return m != nil && m.token != ""
}

func (m *presenterAuthManager) matchesToken(candidate string) bool {
	if !m.isEnabled() {
		return true
	}
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(m.token)) == 1
}

func (m *presenterAuthManager) createSession(now time.Time) (presenterSession, error) {
	sid, err := httputil.RandomToken(32)
	if err != nil {
		return presenterSession{}, err
	}
	csrf, err := httputil.RandomToken(32)
	if err != nil {
		return presenterSession{}, err
	}
	sess := presenterSession{
		id:        sid,
		csrfToken: csrf,
		expiresAt: now.Add(sessionTTL),
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredLocked(now)
	m.sessions[sid] = sess
	return sess, nil
}

func (m *presenterAuthManager) lookupSession(r *http.Request, now time.Time) (presenterSession, bool) {
	sid := extractSessionCookieID(r)
	if sid == "" {
		return presenterSession{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredLocked(now)
	sess, ok := m.sessions[sid]
	if !ok {
		return presenterSession{}, false
	}
	return sess, true
}

func (m *presenterAuthManager) deleteSession(sid string) {
	if sid == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sid)
}

func (m *presenterAuthManager) allowLoginAttempt(ip string, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := now.Add(-loginRateWindow)
	prev := m.failedAttempts[ip]
	kept := prev[:0]
	for _, ts := range prev {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	if len(kept) == 0 {
		delete(m.failedAttempts, ip)
	} else {
		m.failedAttempts[ip] = kept
	}
	return len(kept) < maxFailedLoginsPerIP
}

func (m *presenterAuthManager) recordFailedLogin(ip string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedAttempts[ip] = append(m.failedAttempts[ip], now)
}

func (m *presenterAuthManager) clearFailedLogins(ip string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failedAttempts, ip)
}

func (m *presenterAuthManager) pruneExpiredLocked(now time.Time) {
	for id, sess := range m.sessions {
		if !now.Before(sess.expiresAt) {
			delete(m.sessions, id)
		}
	}
}

func extractSessionCookieID(r *http.Request) string {
	for _, name := range []string{hostSessionCookieName, localSessionCookieName} {
		if c, err := r.Cookie(name); err == nil && strings.TrimSpace(c.Value) != "" {
			return strings.TrimSpace(c.Value)
		}
	}
	return ""
}

func isHTTPSRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https")
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, sessionID string) {
	secure := isHTTPSRequest(r)
	name := localSessionCookieName
	if secure {
		name = hostSessionCookieName
	}
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
}

func clearSessionCookies(w http.ResponseWriter) {
	for _, item := range []struct {
		name   string
		secure bool
	}{
		{name: localSessionCookieName, secure: false},
		{name: hostSessionCookieName, secure: true},
	} {
		http.SetCookie(w, &http.Cookie{
			Name:     item.name,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			Secure:   item.secure,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   -1,
		})
	}
}

func extractHeaderPresenterKey(r *http.Request) (string, bool) {
	if key := strings.TrimSpace(r.Header.Get("X-Presenter-Key")); key != "" {
		return key, true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if scheme, rest, ok := strings.Cut(auth, " "); ok && strings.EqualFold(scheme, "Bearer") {
		if tok := strings.TrimSpace(rest); tok != "" {
			return tok, true
		}
	}
	return "", false
}

func clientIPFromRequest(r *http.Request) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "127.0.0.1"
}

func (d *Dispatcher) requirePresenterAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !d.auth.isEnabled() {
			next(w, r)
			return
		}

		if key, hasHeader := extractHeaderPresenterKey(r); hasHeader {
			if !d.auth.matchesToken(key) {
				httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{
					"error": "invalid presenter key",
				})
				return
			}
			next(w, r)
			return
		}

		sess, ok := d.auth.lookupSession(r, time.Now().UTC())
		if !ok {
			httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{
				"error": "presenter authentication required",
			})
			return
		}

		csrf := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
		if csrf == "" || subtle.ConstantTimeCompare([]byte(csrf), []byte(sess.csrfToken)) != 1 {
			httputil.WriteJSON(w, http.StatusForbidden, map[string]string{
				"error": "missing or invalid X-CSRF-Token",
			})
			return
		}

		next(w, r)
	}
}

func (d *Dispatcher) handleAuthSession(w http.ResponseWriter, r *http.Request) {
	if !d.auth.isEnabled() {
		httputil.WriteJSON(w, http.StatusOK, map[string]any{
			"authRequired":  false,
			"authenticated": true,
			"csrfToken":     "",
		})
		return
	}

	if key, hasHeader := extractHeaderPresenterKey(r); hasHeader && d.auth.matchesToken(key) {
		httputil.WriteJSON(w, http.StatusOK, map[string]any{
			"authRequired":  true,
			"authenticated": true,
			"csrfToken":     "",
		})
		return
	}

	if sess, ok := d.auth.lookupSession(r, time.Now().UTC()); ok {
		httputil.WriteJSON(w, http.StatusOK, map[string]any{
			"authRequired":  true,
			"authenticated": true,
			"csrfToken":     sess.csrfToken,
		})
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"authRequired":  true,
		"authenticated": false,
		"csrfToken":     "",
	})
}

func (d *Dispatcher) handleAuthLogin(w http.ResponseWriter, r *http.Request) {
	if !d.auth.isEnabled() {
		httputil.WriteJSON(w, http.StatusOK, map[string]any{
			"authRequired":  false,
			"authenticated": true,
			"csrfToken":     "",
		})
		return
	}

	now := time.Now().UTC()
	ip := clientIPFromRequest(r)
	if !d.auth.allowLoginAttempt(ip, now) {
		w.Header().Set("Retry-After", "60")
		httputil.WriteJSON(w, http.StatusTooManyRequests, map[string]string{
			"error": "too many failed presenter login attempts; retry in 60 seconds",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var payload struct {
		Token string `json:"token"`
	}
	if err := decodeOptionalJSON(r.Body, &payload); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid login JSON payload",
		})
		return
	}

	presented := strings.TrimSpace(payload.Token)
	if presented == "" {
		if headerKey, ok := extractHeaderPresenterKey(r); ok {
			presented = headerKey
		}
	}

	if !d.auth.matchesToken(presented) {
		d.auth.recordFailedLogin(ip, now)
		httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{
			"error": "invalid presenter token",
		})
		return
	}

	d.auth.clearFailedLogins(ip)
	sess, err := d.auth.createSession(now)
	if err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to initialize presenter session",
		})
		return
	}

	setSessionCookie(w, r, sess.id)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"authRequired":  true,
		"authenticated": true,
		"csrfToken":     sess.csrfToken,
	})
}

func (d *Dispatcher) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	if !d.auth.isEnabled() {
		clearSessionCookies(w)
		httputil.WriteJSON(w, http.StatusOK, map[string]any{
			"authRequired":  false,
			"authenticated": true,
		})
		return
	}

	// Require either valid X-Presenter-Key or valid session cookie + X-CSRF-Token to prevent CSRF logout.
	authorized := false
	if key, hasHeader := extractHeaderPresenterKey(r); hasHeader && d.auth.matchesToken(key) {
		authorized = true
	} else if sess, ok := d.auth.lookupSession(r, time.Now().UTC()); ok {
		csrf := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
		if csrf == "" || subtle.ConstantTimeCompare([]byte(csrf), []byte(sess.csrfToken)) != 1 {
			httputil.WriteJSON(w, http.StatusForbidden, map[string]string{
				"error": "missing or invalid X-CSRF-Token",
			})
			return
		}
		authorized = true
	}

	if !authorized {
		httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{
			"error": "presenter authentication required",
		})
		return
	}

	d.auth.deleteSession(extractSessionCookieID(r))
	clearSessionCookies(w)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"authRequired":  true,
		"authenticated": false,
	})
}

// Ensure json import is referenced.
var _ = json.Marshal
