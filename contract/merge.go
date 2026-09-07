package contract

import (
	"encoding/json"
	"fmt"
)

// MergePatch applies an RFC 7386 JSON Merge Patch to a target document:
// object members merge recursively, a null member deletes the target's
// member, and any non-object patch replaces the target wholesale. It is the
// semantics of EffectMerge (decision 0011), implemented once here for the
// fold and any reader that wants the same answer.
func MergePatch(target, patch json.RawMessage) (json.RawMessage, error) {
	var p any
	if err := json.Unmarshal(patch, &p); err != nil {
		return nil, fmt.Errorf("merge patch: %w", err)
	}
	patchObj, ok := p.(map[string]any)
	if !ok {
		// A non-object patch replaces the target (RFC 7386 § 2).
		return append(json.RawMessage(nil), patch...), nil
	}
	var t any
	if len(target) > 0 {
		if err := json.Unmarshal(target, &t); err != nil {
			return nil, fmt.Errorf("merge target: %w", err)
		}
	}
	targetObj, ok := t.(map[string]any)
	if !ok {
		// A non-object target is discarded; the patch applies to {}.
		targetObj = map[string]any{}
	}
	merged, err := json.Marshal(mergeObjects(targetObj, patchObj))
	if err != nil {
		return nil, fmt.Errorf("merge result: %w", err)
	}
	return merged, nil
}

func mergeObjects(target, patch map[string]any) map[string]any {
	for k, v := range patch {
		if v == nil {
			delete(target, k)
			continue
		}
		if pv, ok := v.(map[string]any); ok {
			if tv, ok := target[k].(map[string]any); ok {
				target[k] = mergeObjects(tv, pv)
				continue
			}
			target[k] = mergeObjects(map[string]any{}, pv)
			continue
		}
		target[k] = v
	}
	return target
}
