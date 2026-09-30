package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/localauth"
)

// operatorAuth authenticates the local operator. Loopback is not a
// credential: a request is the operator's only when it carries the operator
// token (Bearer) or a browser session cookie opened with a login code the
// token holder requested. --allow-unauthenticated-loopback restores the old
// behavior for a transition period, loudly.
type operatorAuth struct {
	verifier *localauth.Verifier
	sessions *localauth.Sessions
	logger   *slog.Logger
	now      func() time.Time
	// allowUnauthenticatedLoopback treats a credential-less loopback request
	// as the operator. Default off; every use is logged.
	allowUnauthenticatedLoopback bool
}

var (
	errOperatorCredentialRequired = errors.New("operator credential required: send the token from operator.token as a Bearer token (the hadron CLI does this), or sign in to the browser UI with `hadron ui`")
	errCrossOriginSession         = errors.New("cross-origin request refused: a browser session may only be used from Hadron's own origin")
)

func newOperatorAuth(tokenPath string, logger *slog.Logger) (*operatorAuth, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if _, err := localauth.EnsureToken(tokenPath); err != nil {
		return nil, err
	}
	sessions := localauth.NewSessions(nil)
	return &operatorAuth{
		verifier: localauth.NewVerifier(tokenPath, logger, sessions.EndAll),
		sessions: sessions, logger: logger, now: time.Now,
	}, nil
}

// operatorRequest reports whether r carries the operator's credential. A
// Bearer token that is not the operator token returns (false, nil) so the
// caller can try other credentials (exposure tokens).
func (a *operatorAuth) operatorRequest(r *http.Request) (bool, error) {
	if a == nil || r == nil {
		return false, nil
	}
	if token, ok := bearerToken(r); ok {
		return a.verifier.Verify(token), nil
	}
	if cookie, err := r.Cookie(localauth.SessionCookie); err == nil && cookie.Value != "" {
		if !a.sessions.Valid(cookie.Value) {
			return false, errOperatorCredentialRequired
		}
		if !loopbackRemote(r.RemoteAddr) || !loopbackHost(r.Host) {
			return false, errOperatorCredentialRequired
		}
		if !safeMethod(r.Method) && !sameOriginSource(r) {
			return false, errCrossOriginSession
		}
		return true, nil
	}
	if a.allowUnauthenticatedLoopback && loopbackRemote(r.RemoteAddr) && loopbackHost(r.Host) {
		a.logger.Warn("unauthenticated loopback request treated as the operator (--allow-unauthenticated-loopback); any local process can do this",
			"method", r.Method, "path", r.URL.Path)
		return true, nil
	}
	return false, nil
}

// AuthorizeOperator implements api.OperatorAuthenticator.
func (a *operatorAuth) AuthorizeOperator(r *http.Request) error {
	ok, err := a.operatorRequest(r)
	if err != nil {
		return err
	}
	if !ok {
		return errOperatorCredentialRequired
	}
	return nil
}

func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", false
	}
	if strings.Count(header, " ") != 1 || !strings.HasPrefix(header, "Bearer ") {
		return "", true
	}
	return strings.TrimPrefix(header, "Bearer "), true
}

func safeMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

// sameOriginSource requires a state-changing cookie request to name its
// source: Origin, or Referer when a browser omits Origin, must be this
// daemon's own origin. SameSite=Strict already keeps the cookie off
// cross-site requests; this is the second, independent check.
func sameOriginSource(r *http.Request) bool {
	source := r.Header.Get("Origin")
	if source == "" {
		referer, err := url.Parse(r.Header.Get("Referer"))
		if err != nil || referer.Host == "" {
			return false
		}
		source = referer.Scheme + "://" + referer.Host
	}
	parsed, err := url.Parse(source)
	if err != nil {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return parsed.Scheme == scheme && strings.EqualFold(parsed.Host, r.Host) && parsed.User == nil
}

// ServeHTTP serves the sign-in routes:
//
//	POST /v1/auth/code    operator token required; returns a 60s single-use login code
//	GET  /auth/login      ?code=…; redeems it, sets the session cookie, redirects to /
//	POST /v1/auth/logout  ends the browser session
func (a *operatorAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	switch r.URL.Path {
	case "/v1/auth/code":
		a.serveCode(w, r)
	case "/auth/login":
		a.serveLogin(w, r)
	case "/v1/auth/logout":
		a.serveLogout(w, r)
	default:
		writeAuthJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

func (a *operatorAuth) serveCode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAuthJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	// Only the token holder may mint a login code: a browser session cannot
	// extend itself and the transition flag does not apply here.
	token, ok := bearerToken(r)
	if !ok || !a.verifier.Verify(token) {
		writeAuthJSON(w, http.StatusUnauthorized, map[string]string{"error": "the operator token is required to request a sign-in code"})
		return
	}
	code, expires, err := a.sessions.IssueCode()
	if errors.Is(err, localauth.ErrRateLimited) {
		w.Header().Set("Retry-After", "60")
		writeAuthJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		writeAuthJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not issue a sign-in code"})
		return
	}
	writeAuthJSON(w, http.StatusOK, map[string]string{
		"code": code, "expires_at": expires.UTC().Format(time.RFC3339), "login_path": "/auth/login?code=" + url.QueryEscape(code),
	})
}

func (a *operatorAuth) serveLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !loopbackRemote(r.RemoteAddr) || !loopbackHost(r.Host) {
		http.Error(w, "sign-in is only available on this machine's loopback address", http.StatusForbidden)
		return
	}
	id, err := a.sessions.Redeem(r.URL.Query().Get("code"))
	if errors.Is(err, localauth.ErrRateLimited) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	if err != nil {
		http.Error(w, "This sign-in link is invalid, expired or already used. Run `hadron ui` for a new one.", http.StatusUnauthorized)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: localauth.SessionCookie, Value: id, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteStrictMode, MaxAge: int(localauth.SessionTTL / time.Second),
	})
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (a *operatorAuth) serveLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAuthJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if cookie, err := r.Cookie(localauth.SessionCookie); err == nil {
		if !sameOriginSource(r) {
			writeAuthJSON(w, http.StatusForbidden, map[string]string{"error": errCrossOriginSession.Error()})
			return
		}
		a.sessions.End(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: localauth.SessionCookie, Value: "", Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
	writeAuthJSON(w, http.StatusOK, map[string]string{"status": "signed out"})
}

func writeAuthJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
