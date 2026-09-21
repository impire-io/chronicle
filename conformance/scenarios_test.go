package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/impire-io/chronicle/client"
	"github.com/impire-io/chronicle/devdir"
	"github.com/impire-io/chronicle/up"
)

// The scenario runner: boots the quick start, connects its local user,
// and speaks each step through the Go client. Derived observations are
// polled — state and indexes are projections that trail the log.

const settle = 15 * time.Second

type step struct {
	Do        string          `json:"do"`
	Log       string          `json:"log"`
	Thing     string          `json:"thing"`
	Type      string          `json:"type"`
	Index     string          `json:"index"`
	Kind      string          `json:"kind"`
	Op        string          `json:"op"`
	Text      string          `json:"text"`
	Prefix    string          `json:"prefix"`
	Def       json.RawMessage `json:"def"`
	Config    json.RawMessage `json:"config"`
	Payload   json.RawMessage `json:"payload"`
	ExpectSeq *uint64         `json:"expectSeq"`
	Limit     int             `json:"limit"`
	Live      bool            `json:"live"`
	After     *uint64         `json:"after"`
	Then      *step           `json:"then"`
	Expect    *expect         `json:"expect"`
}

type expect struct {
	State    json.RawMessage `json:"state"`
	Types    []string        `json:"types"`
	Items    []string        `json:"items"`
	Things   []string        `json:"things"`
	Total    *uint64         `json:"total"`
	Count    *uint64         `json:"count"`
	Error    string          `json:"error"`
	Rolled   *bool           `json:"rolled"`
	Type     string          `json:"type"`
	First    json.RawMessage `json:"first"`
	Contains json.RawMessage `json:"contains"`
	Arrives  string          `json:"arrives"`
}

type scenario struct {
	Name  string `json:"name"`
	Steps []step `json:"steps"`
}

func TestScenarios(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("scenarios", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no scenarios: %v", err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var sc scenario
		if err := json.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		t.Run(sc.Name, func(t *testing.T) { runScenario(t, sc) })
	}
}

func runScenario(t *testing.T, sc scenario) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	dir := t.TempDir()
	l, err := up.Up(ctx, up.Config{Dir: dir, Port: -1})
	if err != nil {
		t.Fatalf("up: %v", err)
	}
	defer l.Stop()
	c, err := client.ConnectNkeyFile(l.URL, devdir.UserNkeyPath(dir), devdir.LocalPrincipal)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()
	r := &runner{t: t, ctx: ctx, c: c}
	for i, s := range sc.Steps {
		r.step(fmt.Sprintf("step %d %s", i+1, s.Do), s)
	}
}

type runner struct {
	t   *testing.T
	ctx context.Context
	c   *client.Client
}

// step runs one sentence and checks its expectation. Errors are checked
// against expect.error; an unexpected error fails the scenario.
func (r *runner) step(label string, s step) {
	r.t.Helper()
	err := r.perform(label, s)
	want := ""
	if s.Expect != nil {
		want = s.Expect.Error
	}
	if want == "" {
		if err != nil {
			r.t.Fatalf("%s: %v", label, err)
		}
		return
	}
	if err == nil {
		r.t.Fatalf("%s: expected error %q, got none", label, want)
	}
	if !matchesError(err, want) {
		r.t.Fatalf("%s: expected error %q, got %v", label, want, err)
	}
}

func matchesError(err error, want string) bool {
	var serr *client.ServiceError
	switch want {
	case "thing-exists":
		return errors.Is(err, client.ErrThingExists)
	case "thing-moved":
		return errors.Is(err, client.ErrThingMoved)
	case "undefined-operation":
		return errors.Is(err, client.ErrUndefinedOperation)
	case "no-responder":
		return errors.Is(err, client.ErrNoResponder)
	case "preflight":
		return !errors.As(err, &serr) && !errors.Is(err, client.ErrThingExists) && !errors.Is(err, client.ErrThingMoved)
	}
	// Otherwise a catalogued code.
	return errors.As(err, &serr) && serr.Code == want
}

func (r *runner) perform(label string, s step) error {
	ctx := r.ctx
	switch s.Do {
	case "log.create":
		_, err := r.c.CreateLog(ctx, s.Log, "")
		return err
	case "type.define":
		var def client.TypeDefinition
		if err := json.Unmarshal(s.Def, &def); err != nil {
			return fmt.Errorf("def: %w", err)
		}
		_, err := r.c.DefineType(ctx, s.Log, s.Type, def)
		return err
	case "index.declare":
		_, err := r.c.DeclareIndex(ctx, s.Log, s.Index, s.Kind, s.Config)
		return err
	case "index.delete":
		_, err := r.c.DeleteIndex(ctx, s.Log, s.Index)
		return err
	case "create":
		_, err := r.c.CreateThing(ctx, s.Log, s.Thing, s.Payload)
		return err
	case "create.op":
		op := s.Op
		if op == "" {
			op = "create"
		}
		_, err := r.c.CreateWith(ctx, s.Log, s.Thing, op, s.Payload)
		return err
	case "do":
		var opts []client.AppendOpt
		if s.ExpectSeq != nil {
			opts = append(opts, client.WithExpectedSeq(*s.ExpectSeq))
		}
		_, err := r.c.Append(ctx, s.Log, s.Thing, s.Op, s.Payload, opts...)
		return err
	case "do.guarded":
		// The exactness recipe's guard: the state's seq is the last op
		// that moved it, and nothing has landed since when the guard holds.
		sv, err := r.c.State(ctx, s.Log, s.Thing)
		if err != nil {
			return err
		}
		_, err = r.c.Append(ctx, s.Log, s.Thing, s.Op, s.Payload, client.WithExpectedSeq(sv.Seq))
		return err
	case "rollup":
		resp, err := r.c.RollupThing(ctx, s.Log, s.Thing)
		if err != nil {
			return err
		}
		if s.Expect != nil && s.Expect.Rolled != nil && resp.Rolled != *s.Expect.Rolled {
			return fmt.Errorf("rolled=%v (%s), want %v", resp.Rolled, resp.Reason, *s.Expect.Rolled)
		}
		return nil
	case "get":
		return r.until(label, s, func() error {
			sv, err := r.c.State(ctx, s.Log, s.Thing)
			if err != nil {
				return err
			}
			return subset(sv.State, s.Expect.State)
		})
	case "history":
		return r.until(label, s, func() error {
			var types []string
			for op, err := range r.c.Replay(ctx, s.Log, s.Thing) {
				if err != nil {
					return err
				}
				types = append(types, op.Type)
			}
			if !reflect.DeepEqual(types, s.Expect.Types) {
				return fmt.Errorf("history types %v, want %v", types, s.Expect.Types)
			}
			return nil
		})
	case "query":
		return r.until(label, s, func() error {
			st := r.c.QueryIndex(ctx, s.Log, s.Index, s.Text, s.Limit)
			var things []string
			for hit, err := range st.Items() {
				if err != nil {
					return err
				}
				things = append(things, hit.Thing)
			}
			tr, ok := st.Trailer()
			if !ok {
				return errors.New("no trailer")
			}
			if s.Expect.Things != nil && !sameSet(things, s.Expect.Things) {
				return fmt.Errorf("hits %v, want %v", things, s.Expect.Things)
			}
			if s.Expect.Total != nil && tr.Total != *s.Expect.Total {
				return fmt.Errorf("total %d, want %d", tr.Total, *s.Expect.Total)
			}
			if s.Expect.Count != nil && st.Count() != *s.Expect.Count {
				return fmt.Errorf("count %d, want %d", st.Count(), *s.Expect.Count)
			}
			return nil
		})
	case "list.logs", "list.types", "list.indexes", "list.members", "list.things":
		return r.until(label, s, func() error {
			var items []string
			var err error
			switch s.Do {
			case "list.logs":
				items, err = collect(r.c.ListLogs(ctx))
			case "list.types":
				items, err = collect(r.c.ListTypes(ctx, s.Log))
			case "list.things":
				items, err = collect(r.c.ListThings(ctx, s.Log, s.Prefix))
			case "list.indexes":
				for info, ierr := range r.c.ListIndexes(ctx, s.Log) {
					if ierr != nil {
						return ierr
					}
					items = append(items, info.Name)
				}
			case "list.members":
				for m, merr := range r.c.ListMembers(ctx) {
					if merr != nil {
						return merr
					}
					items = append(items, m.Name)
				}
			}
			if err != nil {
				return err
			}
			if !sameSet(items, s.Expect.Items) {
				return fmt.Errorf("items %v, want %v", items, s.Expect.Items)
			}
			return nil
		})
	case "watch":
		return r.live(label, s, func(ctx context.Context, got chan<- json.RawMessage) error {
			for sv, err := range r.c.Watch(ctx, s.Log, s.Thing) {
				if err != nil {
					return err
				}
				got <- sv.State
			}
			return nil
		})
	case "tail":
		var opts []client.TailOpt
		if s.Live {
			opts = append(opts, client.Live())
		}
		if s.After != nil {
			opts = append(opts, client.After(*s.After))
		}
		return r.live(label, s, func(ctx context.Context, got chan<- json.RawMessage) error {
			for op, err := range r.c.Tail(ctx, s.Log, s.Thing, opts...) {
				if err != nil {
					return err
				}
				got <- json.RawMessage(fmt.Sprintf("%q", op.Type))
			}
			return nil
		})
	case "watch.declarations":
		return r.live(label, s, func(ctx context.Context, got chan<- json.RawMessage) error {
			for d, err := range r.c.WatchDeclarations(ctx, s.Log) {
				if err != nil {
					return err
				}
				got <- json.RawMessage(fmt.Sprintf("%q", d.Kind+":"+d.Name))
			}
			return nil
		})
	}
	return fmt.Errorf("unknown sentence %q", s.Do)
}

// live runs a subscribe step: open the iterator, take what is at rest
// (expect.first), perform the write in `then`, and expect what lands
// (expect.contains, expect.arrives, expect.type, or expect.types).
func (r *runner) live(label string, s step, open func(ctx context.Context, got chan<- json.RawMessage) error) error {
	ctx, cancel := context.WithTimeout(r.ctx, settle)
	defer cancel()
	got := make(chan json.RawMessage, 64)
	errc := make(chan error, 1)
	go func() { errc <- open(ctx, got) }()
	next := func() (json.RawMessage, error) {
		select {
		case v := <-got:
			return v, nil
		case err := <-errc:
			if err == nil {
				err = errors.New("the iterator ended")
			}
			return nil, err
		case <-ctx.Done():
			return nil, fmt.Errorf("nothing arrived within %s", settle)
		}
	}
	e := s.Expect
	// What is at rest first.
	if len(e.First) > 0 {
		var firstSet []string
		if json.Unmarshal(e.First, &firstSet) == nil && len(firstSet) > 0 {
			var seen []string
			for len(seen) < len(firstSet) {
				v, err := next()
				if err != nil {
					return fmt.Errorf("%s: first: %w", label, err)
				}
				var item string
				_ = json.Unmarshal(v, &item)
				seen = append(seen, item)
			}
			if !sameSet(seen, firstSet) {
				return fmt.Errorf("%s: first %v, want %v", label, seen, firstSet)
			}
		} else {
			v, err := next()
			if err != nil {
				return fmt.Errorf("%s: first: %w", label, err)
			}
			if err := subset(v, e.First); err != nil {
				return fmt.Errorf("%s: first: %w", label, err)
			}
		}
	}
	// The write, once the iterator is open — a live tail must be
	// subscribed before the op lands, so give the consumer a moment.
	if s.Then != nil {
		time.Sleep(500 * time.Millisecond)
		if err := r.perform(label+" then", *s.Then); err != nil {
			return fmt.Errorf("%s: then: %w", label, err)
		}
	}
	// What lands.
	switch {
	case len(e.Contains) > 0:
		for {
			v, err := next()
			if err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			if subset(v, e.Contains) == nil {
				return nil
			}
		}
	case e.Arrives != "" || e.Type != "":
		want := e.Arrives
		if want == "" {
			want = e.Type
		}
		for {
			v, err := next()
			if err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			var item string
			_ = json.Unmarshal(v, &item)
			if item == want {
				return nil
			}
		}
	case len(e.Types) > 0:
		var types []string
		for len(types) < len(e.Types) {
			v, err := next()
			if err != nil {
				return fmt.Errorf("%s: %w", label, err)
			}
			var item string
			_ = json.Unmarshal(v, &item)
			types = append(types, item)
		}
		if !reflect.DeepEqual(types, e.Types) {
			return fmt.Errorf("%s: types %v, want %v", label, types, e.Types)
		}
	}
	return nil
}

// until polls a derived observation until it holds or the settle window
// passes; the last failure is the error.
func (r *runner) until(label string, s step, check func() error) error {
	if s.Expect != nil && s.Expect.Error != "" {
		// An expected refusal is observed once, not waited for.
		return check()
	}
	deadline := time.Now().Add(settle)
	for {
		err := check()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: %w", label, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func collect[T any](items func(yield func(T, error) bool)) ([]T, error) {
	var out []T
	for item, err := range items {
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func sameSet(got, want []string) bool {
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if len(g) == 0 && len(w) == 0 {
		return true
	}
	return reflect.DeepEqual(g, w)
}

// subset checks every key named in want equals in got (JSON values).
func subset(got, want json.RawMessage) error {
	var g, w map[string]any
	if err := json.Unmarshal(got, &g); err != nil {
		return fmt.Errorf("state %s is not an object", got)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		return fmt.Errorf("expectation %s is not an object", want)
	}
	for k, v := range w {
		if !reflect.DeepEqual(g[k], v) {
			return fmt.Errorf("state %s: %s = %v, want %v", got, k, g[k], v)
		}
	}
	return nil
}
