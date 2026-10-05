package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestParseSite: a site is an https origin — no plain http, no path.
func TestParseSite(t *testing.T) {
	if u, err := ParseSite(DefaultSite); err != nil || u.Host != "chronicle.impire.dev" {
		t.Fatalf("the hosted site: %v, %v", u, err)
	}
	if _, err := ParseSite(DefaultSite + "/"); err != nil {
		t.Fatalf("a trailing slash: %v", err)
	}
	for _, bad := range []string{"http://chronicle.impire.dev", "chronicle.impire.dev", "https://chronicle.impire.dev/x", "https://", "https://chronicle.impire.dev/?a=b"} {
		if _, err := ParseSite(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// TestDecodeProfile: a profile needs the url, the App and the sentinel;
// the optional fields pass through.
func TestDecodeProfile(t *testing.T) {
	p, err := DecodeProfile([]byte(`{"url":"tls://x:4222","github_client_id":"Iv1.x","sentinel":"S","websocket_url":"wss://x"}`), "test")
	if err != nil || p.URL != "tls://x:4222" || p.GithubClientID != "Iv1.x" || p.WebsocketURL != "wss://x" {
		t.Fatalf("decode: %+v, %v", p, err)
	}
	for _, bad := range []string{`{}`, `{"url":"u","github_client_id":"c"}`, `not json`} {
		if _, err := DecodeProfile([]byte(bad), "test"); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

// TestFetchProfile: the profile is read from the site's well-known path
// over TLS; a redirect off the site's host, an error status and an
// oversized body are refused.
func TestFetchProfile(t *testing.T) {
	profile := `{"url":"tls://x:4222","github_client_id":"Iv1.x","sentinel":"S"}`
	elsewhere := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(profile)) }))
	defer elsewhere.Close()
	mux := http.NewServeMux()
	mux.HandleFunc(WellKnownPath, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(profile)) })
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()
	site, _ := url.Parse(srv.URL)
	ctx := t.Context()

	p, data, err := FetchProfile(ctx, srv.Client(), site)
	if err != nil || p.GithubClientID != "Iv1.x" || string(data) != profile {
		t.Fatalf("fetch: %+v %q %v", p, data, err)
	}

	off := httptest.NewTLSServer(http.RedirectHandler(elsewhere.URL+WellKnownPath, http.StatusFound))
	defer off.Close()
	offSite, _ := url.Parse(off.URL)
	if _, _, err := FetchProfile(ctx, off.Client(), offSite); err == nil || !strings.Contains(err.Error(), "redirected off") {
		t.Fatalf("a redirect to another host: %v", err)
	}

	missing := httptest.NewTLSServer(http.NotFoundHandler())
	defer missing.Close()
	missingSite, _ := url.Parse(missing.URL)
	if _, _, err := FetchProfile(ctx, missing.Client(), missingSite); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("a missing profile: %v", err)
	}

	big := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat(" ", maxProfileBytes+1)))
	}))
	defer big.Close()
	bigSite, _ := url.Parse(big.URL)
	if _, _, err := FetchProfile(ctx, big.Client(), bigSite); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("an oversized profile: %v", err)
	}
}

// TestIdentitySubjects: the identity rides the subject — composed and read
// back — and a subject that is not the identity plane's names none.
func TestIdentitySubjects(t *testing.T) {
	for _, s := range []string{MembershipsSubject(4242, "erin"), AccountCreateSubject(4242, "erin")} {
		id, login, ok := IdentityFromSubject(s)
		if !ok || id != 4242 || login != "erin" {
			t.Fatalf("%s: %d %q %v", s, id, login, ok)
		}
	}
	for _, bad := range []string{"CHRON.CTRL.MEMBER.ADD", MembershipsVerb + ".0.erin", MembershipsVerb + ".x.erin", MembershipsVerb + ".1.Erin"} {
		if _, _, ok := IdentityFromSubject(bad); ok {
			t.Errorf("%s read as an identity", bad)
		}
	}
	if got := ConnectToken(SelectorIdentity, "gho_x"); got != "+:gho_x" {
		t.Fatalf("connect token: %q", got)
	}
	if got := ConnectToken("", "gho_x"); got != ":gho_x" {
		t.Fatalf("connect token, callout's choice: %q", got)
	}
}

// TestLooksLikeNkey: a raw user public key where a principal name should
// be is spotted; a principal name is not.
func TestLooksLikeNkey(t *testing.T) {
	if !looksLikeNkey("UA" + strings.Repeat("A", 54)) {
		t.Fatal("a user key not spotted")
	}
	for _, name := range []string{"erin", "UA" + strings.Repeat("A", 53), "OA" + strings.Repeat("A", 54), "UA" + strings.Repeat("a", 54)} {
		if looksLikeNkey(name) {
			t.Errorf("%q read as a key", name)
		}
	}
}

// fakeGitHub is GitHub's device flow and refresh as one local server: the
// device code, one pending poll then the grant, and a refresh that only
// this client ID's refresh token passes.
func fakeGitHub(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.Form.Get("client_id") != "Iv1.test" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Form.Get("client_secret") != "" {
			t.Errorf("the client sent a client secret to %s", r.URL.Path)
		}
		switch r.URL.Path {
		case "/login/device/code":
			_ = json.NewEncoder(w).Encode(DeviceCode{DeviceCode: "dev1", UserCode: "ABCD-1234", VerificationURI: "https://example/activate", ExpiresIn: 60, Interval: 0})
		case "/login/oauth/access_token":
			switch r.Form.Get("grant_type") {
			case "urn:ietf:params:oauth:grant-type:device_code":
				if polls.Add(1) < 2 {
					_ = json.NewEncoder(w).Encode(Token{Error: "authorization_pending"})
					return
				}
				_ = json.NewEncoder(w).Encode(Token{AccessToken: "tok1", RefreshToken: "ref1", ExpiresIn: 28800})
			case "refresh_token":
				if r.Form.Get("refresh_token") != "ref1" {
					_ = json.NewEncoder(w).Encode(Token{Error: "bad_refresh_token"})
					return
				}
				_ = json.NewEncoder(w).Encode(Token{AccessToken: "tok2", RefreshToken: "ref2", ExpiresIn: 28800})
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &polls
}

// TestDeviceFlowAndRefresh: the device flow polls through pending to the
// grant, and a refresh trades the refresh token for a fresh pair — both
// with the App's client ID alone, never a secret.
func TestDeviceFlowAndRefresh(t *testing.T) {
	srv, polls := fakeGitHub(t)
	gh := GitHubOf(Profile{GithubClientID: "Iv1.test", GithubOAuthBase: srv.URL})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	dc, err := gh.StartDeviceFlow(ctx)
	if err != nil || dc.UserCode != "ABCD-1234" {
		t.Fatalf("device code: %+v, %v", dc, err)
	}
	tok, err := gh.PollDeviceFlow(ctx, dc)
	if err != nil || tok.AccessToken != "tok1" || polls.Load() != 2 {
		t.Fatalf("poll: %+v, %v after %d polls", tok, err, polls.Load())
	}
	fresh, err := gh.Refresh(ctx, tok.RefreshToken)
	if err != nil || fresh.AccessToken != "tok2" {
		t.Fatalf("refresh: %+v, %v", fresh, err)
	}
	if _, err := gh.Refresh(ctx, "stale"); !errors.Is(err, ErrGrantRefused) {
		t.Fatalf("a refused refresh: %v", err)
	}
	wrongApp := &GitHub{ClientID: "Iv1.other", OAuthBase: srv.URL}
	if _, err := wrongApp.StartDeviceFlow(ctx); err == nil {
		t.Fatal("another App's device flow accepted")
	}
}
