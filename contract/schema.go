package contract

import (
	"bytes"
	"encoding/json"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// CompileSchema compiles one JSON Schema document — the one validator the
// SDK pre-flight, the fold's read-side marking, and the control verbs'
// write-side checks share (03-meta-and-state.md § type records). It lives
// in the contract because the fold rules do (design 12): an SDK performing
// the exactness recipe judges with exactly this.
func CompileSchema(raw json.RawMessage) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", doc); err != nil {
		return nil, err
	}
	return compiler.Compile("schema.json")
}
