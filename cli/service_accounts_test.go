package cli_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/impire-io/chronicle/bridge"
	"github.com/impire-io/chronicle/cli"
)

// fakePlane is the install as the sign-in form of the service-account
// sentences sees it: it records what was asked and hands a credential
// back.
type fakePlane struct {
	calls  []string
	refuse error
}

func (f *fakePlane) IssueServiceAccount(_ context.Context, account, name, role string) (bridge.ServiceAccountCredential, error) {
	f.calls = append(f.calls, "issue "+account+" "+name+" "+role)
	if f.refuse != nil {
		return bridge.ServiceAccountCredential{}, f.refuse
	}
	return bridge.ServiceAccountCredential{Account: account, Name: name, Role: role, PublicKey: "UABC", Creds: "SECRET-CREDS-FILE"}, nil
}

func (f *fakePlane) RotateServiceAccount(_ context.Context, account, name string) (bridge.ServiceAccountCredential, error) {
	f.calls = append(f.calls, "rotate "+account+" "+name)
	return bridge.ServiceAccountCredential{Account: account, Name: name, Role: "writer", PublicKey: "UDEF", Creds: "FRESH-CREDS-FILE"}, nil
}

func (f *fakePlane) RevokeServiceAccount(_ context.Context, account, name string) (bridge.ServiceAccountRevokeResponse, error) {
	f.calls = append(f.calls, "revoke "+account+" "+name)
	return bridge.ServiceAccountRevokeResponse{Account: account, Name: name, PublicKey: "UDEF"}, nil
}

func (f *fakePlane) Close() { f.calls = append(f.calls, "close") }

// signedIn saves and selects a sign-in context the way `login` does.
func signedIn(t *testing.T) {
	t.Helper()
	t.Setenv("CHRONICLE_CONFIG_HOME", t.TempDir())
	profile := filepath.Join(t.TempDir(), "profile.json")
	if err := cli.SaveContext("acme-erin", cli.Context{URL: "nats://bridge", Bridge: profile, Account: "acme"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.SelectContext("acme-erin"); err != nil {
		t.Fatal(err)
	}
}

// TestServiceAccountSentencesHosted: with a sign-in context the three
// sentences speak to the install about the context's account, and the
// credential is shown exactly once — printed, or written to --out and
// then never printed.
func TestServiceAccountSentencesHosted(t *testing.T) {
	signedIn(t)
	ctx := context.Background()
	plane := &fakePlane{}
	t.Cleanup(cli.SetIdentityDial(func(string) (cli.IdentityPlane, error) { return plane, nil }))

	out := run(ctx, t, "service-account", "create", "billing-svc", "--role", "reader")
	if !strings.Contains(out, "service account billing-svc created (role reader) in account acme") {
		t.Fatalf("create: %s", out)
	}
	if strings.Count(out, "SECRET-CREDS-FILE") != 1 || !strings.Contains(out, "will not be shown again") {
		t.Fatalf("the credential is shown exactly once: %s", out)
	}

	file := filepath.Join(t.TempDir(), "billing.creds")
	out = run(ctx, t, "service-account", "rotate", "billing-svc", "--out", file)
	if strings.Contains(out, "FRESH-CREDS-FILE") || !strings.Contains(out, "written to "+file) || !strings.Contains(out, "the old credential no longer connects") {
		t.Fatalf("rotate --out prints the path, never the credential: %s", out)
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "FRESH-CREDS-FILE" {
		t.Fatalf("the credential file: %q %v", data, err)
	}
	if info, _ := os.Stat(file); info.Mode().Perm() != 0o600 {
		t.Fatalf("the credential file is private: %v", info.Mode())
	}

	out = run(ctx, t, "service-account", "revoke", "billing-svc")
	if !strings.Contains(out, "service account billing-svc revoked from account acme") {
		t.Fatalf("revoke: %s", out)
	}

	want := []string{"issue acme billing-svc reader", "close", "rotate acme billing-svc", "close", "revoke acme billing-svc", "close"}
	if strings.Join(plane.calls, "|") != strings.Join(want, "|") {
		t.Fatalf("the install was asked %v, want %v", plane.calls, want)
	}

	// A refusal reaches the user with the install's own fix in it.
	plane.refuse = errors.New("credential-exists: service account billing-svc already has a credential; rotate it: chronicle service-account rotate billing-svc")
	if _, err := runErr(ctx, "service-account", "create", "billing-svc"); err == nil || !strings.Contains(err.Error(), "rotate it") {
		t.Fatalf("a refusal: %v", err)
	}
}

// TestServiceAccountRotateSelfHosted: without a sign-in the credential is
// the operator's NATS's, so rotate records the new key — the old record
// retired, a new one with the same role — and says what is whose.
func TestServiceAccountRotateSelfHosted(t *testing.T) {
	ctx := context.Background()
	startLocal(ctx, t)
	run(ctx, t, "service-account", "create", "billing-svc", "--role", "reader", "--public-key", "UOLD")
	if _, err := runErr(ctx, "service-account", "rotate", "billing-svc"); err == nil || !strings.Contains(err.Error(), "--public-key") {
		t.Fatalf("rotate self-hosted needs the new key: %v", err)
	}
	out := run(ctx, t, "service-account", "rotate", "billing-svc", "--public-key", "UNEW")
	if !strings.Contains(out, "rotated (role reader): key UNEW recorded") || !strings.Contains(out, "(UOLD) is your NATS's to revoke") {
		t.Fatalf("rotate: %s", out)
	}
	out = run(ctx, t, "service-account", "list")
	if !strings.Contains(out, "UNEW") || strings.Contains(out, "UOLD") {
		t.Fatalf("the registry holds the new key: %s", out)
	}
	if _, err := runErr(ctx, "service-account", "rotate", "nobody", "--public-key", "UX"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("rotate of an unknown name: %v", err)
	}
	run(ctx, t, "member", "add", "jordan", "--role", "reader")
	if _, err := runErr(ctx, "service-account", "rotate", "jordan", "--public-key", "UX"); err == nil || !strings.Contains(err.Error(), "not a service account") {
		t.Fatalf("rotate of a person: %v", err)
	}
}
