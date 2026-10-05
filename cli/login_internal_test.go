package cli

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/impire-io/chronicle/bridge"
)

// TestChoose: the choice among several accounts reads a number or a name
// from the chooser; without a chooser it teaches --account.
func TestChoose(t *testing.T) {
	ms := []bridge.Membership{
		{Account: "acme", Principal: "erin", Role: "reader"},
		{Account: "erin", Principal: "erin", Role: "admin", Created: true},
	}
	var out bytes.Buffer
	if got, err := choose(&out, ms, strings.NewReader("2\n")); err != nil || got != "erin" {
		t.Fatalf("choose by number: %q, %v", got, err)
	}
	if !strings.Contains(out.String(), "1) acme  as erin (reader)") || !strings.Contains(out.String(), "2) erin  as erin (admin)") {
		t.Fatalf("the choice was not presented:\n%s", out.String())
	}
	if got, err := choose(&out, ms, strings.NewReader("acme\n")); err != nil || got != "acme" {
		t.Fatalf("choose by name: %q, %v", got, err)
	}
	if _, err := choose(&out, ms, strings.NewReader("nowhere\n")); err == nil || !strings.Contains(err.Error(), "--account") {
		t.Fatalf("a wrong choice not taught: %v", err)
	}
	if _, err := choose(&out, ms, nil); err == nil || !strings.Contains(err.Error(), "acme, erin") || !strings.Contains(err.Error(), "--account") {
		t.Fatalf("no chooser not taught: %v", err)
	}
}

// TestCachedProfileNamesItsSite: a cached profile's path gives back the
// site it came from, a port included; a handed-out file names none.
func TestCachedProfileNamesItsSite(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	for _, host := range []string{"chronicle.impire.dev", "127.0.0.1:8443"} {
		site := &url.URL{Scheme: "https", Host: host}
		path, err := cachedProfilePath(site)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := siteOfCachedProfile(path)
		if !ok || got.String() != site.String() {
			t.Fatalf("%s: %v, %v", path, got, ok)
		}
		if _, ok := siteOfCachedProfile(loginStatePath(path)); ok {
			t.Fatalf("the login state %s read as a profile", loginStatePath(path))
		}
	}
	if _, ok := siteOfCachedProfile(filepath.Join(t.TempDir(), "bridge.json")); ok {
		t.Fatal("a handed-out file read as a cached profile")
	}
	if got := loginHint(filepath.Join(t.TempDir(), "bridge.json")); !strings.Contains(got, "--bridge") {
		t.Fatalf("hint for a file: %q", got)
	}
	path, _ := cachedProfilePath(&url.URL{Scheme: "https", Host: "chronicle.impire.dev"})
	if got := loginHint(path); got != "chronicle login" {
		t.Fatalf("hint for the hosted site: %q", got)
	}
}

// TestLoginStateMovesToTheProfilesName: a login kept as login.json beside
// a profile, under the older shared name, is read once and moved to the profile's own
// name; the old file is gone.
func TestLoginStateMovesToTheProfilesName(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "bridge.json")
	legacy := filepath.Join(dir, "login.json")
	if err := os.WriteFile(legacy, []byte(`{"access_token":"tok"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := readLoginState(profile)
	if err != nil || string(data) != `{"access_token":"tok"}` {
		t.Fatalf("legacy login: %q, %v", data, err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("the legacy login stayed: %v", err)
	}
	fi, err := os.Stat(filepath.Join(dir, "bridge.login.json"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the moved login: %v, %v", fi, err)
	}
	if _, err := readLoginState(filepath.Join(t.TempDir(), "other.json")); !os.IsNotExist(err) {
		t.Fatalf("no login at all: %v", err)
	}
}

// TestAccountCreateTeachesLogin: the open account create, with no login
// to speak through, says how to get one.
func TestAccountCreateTeachesLogin(t *testing.T) {
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	var out bytes.Buffer
	if err := accountVerb(t.Context(), []string{"create", "acme"}, &out); err == nil || !strings.Contains(err.Error(), "chronicle login") {
		t.Fatalf("account create without a login: %v", err)
	}
	if err := accountVerb(t.Context(), []string{"destroy", "acme"}, &out); err == nil || !strings.Contains(err.Error(), "create <name>") {
		t.Fatalf("an unknown account sentence: %v", err)
	}
}
