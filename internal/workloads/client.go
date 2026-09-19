package workloads

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"github.com/impire-io/chronicle/contract"
)

// Thin request helpers for the dispatch surface, used by the composition
// root and the node's report wiring. They speak to whichever
// chronicle-workloads instance the queue group picks.

// Dispatch asks for a workload to exist. Idempotent: dispatching a born
// workload reports Existed.
func Dispatch(ctx context.Context, nc *nats.Conn, r contract.FleetDispatchRequest) (contract.FleetDispatchResponse, error) {
	return roundTrip[contract.FleetDispatchResponse](ctx, nc, contract.FleetDispatchSubject, r)
}

// StopWorkload retires a workload; its executors destroy and its slots
// release with reason stopped.
func StopWorkload(ctx context.Context, nc *nats.Conn, r contract.FleetStopRequest) (contract.FleetStopResponse, error) {
	return roundTrip[contract.FleetStopResponse](ctx, nc, contract.FleetStopSubject, r)
}

// Register puts an executor on the roster.
func Register(ctx context.Context, nc *nats.Conn, r contract.FleetRegisterRequest) (contract.FleetRegisterResponse, error) {
	return roundTrip[contract.FleetRegisterResponse](ctx, nc, contract.FleetRegisterSubject(r.Executor), r)
}

// Report releases a slot the reporting executor can no longer carry.
func Report(ctx context.Context, nc *nats.Conn, r contract.FleetReportRequest) (contract.FleetReportResponse, error) {
	return roundTrip[contract.FleetReportResponse](ctx, nc, contract.FleetReportSubject(r.Executor), r)
}

func roundTrip[Resp any, Req any](ctx context.Context, nc *nats.Conn, subject string, req Req) (Resp, error) {
	var zero Resp
	data, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("marshal request: %w", err)
	}
	msg, err := nc.RequestWithContext(ctx, subject, data)
	if err != nil {
		return zero, fmt.Errorf("%s: %w", subject, err)
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
		return zero, fmt.Errorf("%s: %s: %s", subject, code, msg.Header.Get(micro.ErrorHeader))
	}
	var resp Resp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return zero, fmt.Errorf("%s: decode response: %w", subject, err)
	}
	return resp, nil
}
