package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/internal/natstest"
)

// TestServiceAccountVerbs: the three verbs compose the subject from the
// placed identity and the payload from the sentence, and read the
// install's answer — a credential handed back once, a refusal with its
// code intact. A plain responder stands in for the install; the callout
// that would place the identity is the service's, not this package's.
func TestServiceAccountVerbs(t *testing.T) {
	url := natstest.StartJetStream(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var got struct {
		subject string
		issue   ServiceAccountIssueRequest
		named   ServiceAccountRequest
	}
	svc, err := micro.AddService(nc, micro.Config{Name: "install", Version: "0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = svc.Stop() }()
	add := func(name, verb string, handle func(req micro.Request)) {
		if err := svc.AddEndpoint(name, micro.HandlerFunc(handle), micro.WithEndpointSubject(verb+".*.*")); err != nil {
			t.Fatal(err)
		}
	}
	add("issue", ServiceAccountIssueVerb, func(req micro.Request) {
		got.subject = req.Subject()
		_ = json.Unmarshal(req.Data(), &got.issue)
		if got.issue.Name == "taken" {
			_ = req.Error(CodeCredentialExists, "service account taken already has a credential; rotate it: chronicle service-account rotate taken", nil)
			return
		}
		data, _ := json.Marshal(ServiceAccountCredential{Account: got.issue.Account, Name: got.issue.Name, Role: got.issue.Role, PublicKey: "UABC", Creds: "-----BEGIN NATS USER JWT-----"})
		_ = req.Respond(data)
	})
	add("rotate", ServiceAccountRotateVerb, func(req micro.Request) {
		got.subject = req.Subject()
		_ = json.Unmarshal(req.Data(), &got.named)
		data, _ := json.Marshal(ServiceAccountCredential{Account: got.named.Account, Name: got.named.Name, Role: "writer", PublicKey: "UDEF", Creds: "fresh"})
		_ = req.Respond(data)
	})
	add("revoke", ServiceAccountRevokeVerb, func(req micro.Request) {
		got.subject = req.Subject()
		_ = json.Unmarshal(req.Data(), &got.named)
		data, _ := json.Marshal(ServiceAccountRevokeResponse{Account: got.named.Account, Name: got.named.Name, PublicKey: "UDEF"})
		_ = req.Respond(data)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := &Identity{GithubID: 4242, Login: "erin", nc: nc}

	cred, err := id.IssueServiceAccount(ctx, "acme", "billing-svc", "writer")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if got.subject != ServiceAccountIssueSubject(4242, "erin") || got.issue != (ServiceAccountIssueRequest{Account: "acme", Name: "billing-svc", Role: "writer"}) {
		t.Fatalf("issue spoke %s with %+v", got.subject, got.issue)
	}
	if cred.Creds == "" || cred.PublicKey != "UABC" || cred.Role != "writer" {
		t.Fatalf("issue answered %+v", cred)
	}

	_, err = id.IssueServiceAccount(ctx, "acme", "taken", "writer")
	var se *client.ServiceError
	if !errors.As(err, &se) || se.Code != CodeCredentialExists {
		t.Fatalf("issue on a credentialed name: %v", err)
	}

	cred, err = id.RotateServiceAccount(ctx, "acme", "billing-svc")
	if err != nil || cred.Creds != "fresh" {
		t.Fatalf("rotate: %+v %v", cred, err)
	}
	if got.subject != ServiceAccountRotateSubject(4242, "erin") || got.named != (ServiceAccountRequest{Account: "acme", Name: "billing-svc"}) {
		t.Fatalf("rotate spoke %s with %+v", got.subject, got.named)
	}

	rev, err := id.RevokeServiceAccount(ctx, "acme", "billing-svc")
	if err != nil || rev.PublicKey != "UDEF" || rev.Name != "billing-svc" {
		t.Fatalf("revoke: %+v %v", rev, err)
	}
	if got.subject != ServiceAccountRevokeSubject(4242, "erin") {
		t.Fatalf("revoke spoke %s", got.subject)
	}
}
