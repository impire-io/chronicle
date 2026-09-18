package github_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/impire-io/chronicle/internal/identity/github"
)

func TestValidateToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/user" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		switch r.Header.Get("Authorization") {
		case "Bearer good":
			w.Header().Set("GitHub-Authentication-Token-Expiration", "2030-01-02 15:04:05 UTC")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 12345, "login": "erin"})
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer srv.Close()

	c := &github.Client{ClientID: "cid", APIBase: srv.URL}
	ctx := context.Background()

	id, err := c.ValidateToken(ctx, "good")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if id.ID != 12345 || id.Login != "erin" {
		t.Fatalf("identity = %+v", id)
	}
	if id.TokenExpiry.IsZero() || id.TokenExpiry.Year() != 2030 {
		t.Fatalf("token expiry not read: %v", id.TokenExpiry)
	}

	if _, err := c.ValidateToken(ctx, "bad"); !errors.Is(err, github.ErrInvalidToken) {
		t.Fatalf("want ErrInvalidToken, got %v", err)
	}
}

func TestDeviceFlowAndRefresh(t *testing.T) {
	var polls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/device/code":
			_ = json.NewEncoder(w).Encode(github.DeviceCode{
				DeviceCode: "dev1", UserCode: "ABCD-1234",
				VerificationURI: "https://example/activate", ExpiresIn: 60, Interval: 0,
			})
		case "/login/oauth/access_token":
			_ = r.ParseForm()
			switch r.Form.Get("grant_type") {
			case "urn:ietf:params:oauth:grant-type:device_code":
				// Pending once, then approved: the poll loop must ride
				// through pending without failing.
				if polls.Add(1) == 1 {
					_ = json.NewEncoder(w).Encode(github.Token{Error: "authorization_pending"})
					return
				}
				_ = json.NewEncoder(w).Encode(github.Token{AccessToken: "tok1", RefreshToken: "ref1", ExpiresIn: 28800})
			case "refresh_token":
				if r.Form.Get("refresh_token") != "ref1" {
					_ = json.NewEncoder(w).Encode(github.Token{Error: "bad_refresh_token"})
					return
				}
				_ = json.NewEncoder(w).Encode(github.Token{AccessToken: "tok2", RefreshToken: "ref2", ExpiresIn: 28800})
			default:
				t.Errorf("unexpected grant %q", r.Form.Get("grant_type"))
			}
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := &github.Client{ClientID: "cid", OAuthBase: srv.URL}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dc, err := c.StartDeviceFlow(ctx)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if dc.UserCode != "ABCD-1234" {
		t.Fatalf("device code = %+v", dc)
	}
	// The fake's interval floor is 5s per the contract; shrink the wait by
	// polling with the real method — 0 means the 5s minimum, so this test
	// tolerates one pending round.
	tok, err := c.PollDeviceFlow(ctx, dc)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if tok.AccessToken != "tok1" || tok.RefreshToken != "ref1" {
		t.Fatalf("token = %+v", tok)
	}

	ref, err := c.Refresh(ctx, tok.RefreshToken)
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if ref.AccessToken != "tok2" || ref.RefreshToken != "ref2" {
		t.Fatalf("refreshed = %+v", ref)
	}
	if _, err := c.Refresh(ctx, "stale"); err == nil {
		t.Fatal("stale refresh must fail")
	}
}
