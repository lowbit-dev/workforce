package contract

import "encoding/json"

// JsonOrBytes holds a value as raw bytes.
//
// When marshaled to JSON it is inlined verbatim if the content is valid JSON,
// or base64-encoded otherwise. This means JSON-producing task binaries return
// readable objects in API responses without any extra decoding step.
//
// Use Decode to unmarshal into a typed value, or AsAny for untyped access.
type JsonOrBytes []byte

// MarshalJSON inlines valid JSON verbatim. Non-JSON bytes fall back to a
// base64-encoded JSON string, matching the default []byte behaviour.
func (r JsonOrBytes) MarshalJSON() ([]byte, error) {
	if len(r) > 0 && json.Valid(r) {
		return r, nil
	}
	return json.Marshal([]byte(r))
}

// UnmarshalJSON handles both inline JSON values and base64-encoded strings,
// ensuring correct round-trips when reading results back from a JSON store.
func (r *JsonOrBytes) UnmarshalJSON(data []byte) error {
	if len(data) > 1 && data[0] == '"' {
		// Quoted string — base64-encoded raw bytes.
		var b []byte
		if err := json.Unmarshal(data, &b); err != nil {
			return err
		}
		*r = b
		return nil
	}
	// Inline JSON value — store verbatim.
	*r = append((*r)[:0], data...)
	return nil
}

// Decode unmarshals the result into v.
func (r JsonOrBytes) Decode(v any) error {
	return json.Unmarshal(r, v)
}

// AsAny returns the result as an untyped Go value.
func (r JsonOrBytes) AsAny() (any, error) {
	var v any
	return v, json.Unmarshal(r, &v)
}
