package contract

import (
	"encoding/json"
	"testing"
)

// ---- MarshalJSON ----

func TestJsonOrBytesMarshalJSON_ValidJSONInlined(t *testing.T) {
	r := JsonOrBytes(`{"status":"ok","count":3}`)
	got, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"status":"ok","count":3}` {
		t.Fatalf("expected verbatim JSON, got %s", got)
	}
}

func TestJsonOrBytesMarshalJSON_ValidJSONArray(t *testing.T) {
	r := JsonOrBytes(`[1,2,3]`)
	got, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `[1,2,3]` {
		t.Fatalf("expected verbatim array, got %s", got)
	}
}

func TestJsonOrBytesMarshalJSON_ValidJSONScalar(t *testing.T) {
	r := JsonOrBytes(`42`)
	got, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `42` {
		t.Fatalf("expected verbatim scalar, got %s", got)
	}
}

func TestJsonOrBytesMarshalJSON_NonJSONFallsBackToBase64(t *testing.T) {
	r := JsonOrBytes("not json at all")
	got, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	// Must be a quoted base64 string, not the raw bytes verbatim.
	if len(got) < 2 || got[0] != '"' {
		t.Fatalf("expected base64 JSON string, got %s", got)
	}
	// Round-trip: unmarshal back should give original bytes.
	var rt JsonOrBytes
	if err := json.Unmarshal(got, &rt); err != nil {
		t.Fatal(err)
	}
	if string(rt) != "not json at all" {
		t.Fatalf("round-trip failed: got %q", rt)
	}
}

func TestJsonOrBytesMarshalJSON_Empty(t *testing.T) {
	r := JsonOrBytes(nil)
	got, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	// nil []byte marshals to the base64 of nil → "null"
	if string(got) != "null" {
		t.Fatalf("expected null, got %s", got)
	}
}

// ---- UnmarshalJSON ----

func TestJsonOrBytesUnmarshalJSON_InlineObject(t *testing.T) {
	raw := []byte(`{"x":1}`)
	var r JsonOrBytes
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if string(r) != `{"x":1}` {
		t.Fatalf("expected verbatim bytes, got %s", r)
	}
}

func TestJsonOrBytesUnmarshalJSON_InlineArray(t *testing.T) {
	raw := []byte(`["a","b"]`)
	var r JsonOrBytes
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if string(r) != `["a","b"]` {
		t.Fatalf("expected verbatim bytes, got %s", r)
	}
}

func TestJsonOrBytesUnmarshalJSON_Base64StringRoundTrip(t *testing.T) {
	original := JsonOrBytes("binary\x00data")
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	// encoded should be a quoted base64 string
	if len(encoded) < 2 || encoded[0] != '"' {
		t.Fatalf("expected base64 string, got %s", encoded)
	}
	var decoded JsonOrBytes
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded) != string(original) {
		t.Fatalf("round-trip mismatch: got %q, want %q", decoded, original)
	}
}

// ---- Decode ----

func TestJsonOrBytesDecode(t *testing.T) {
	type payload struct {
		Name  string `json:"name"`
		Score int    `json:"score"`
	}
	r := JsonOrBytes(`{"name":"alice","score":99}`)
	var p payload
	if err := r.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Name != "alice" || p.Score != 99 {
		t.Fatalf("unexpected: %+v", p)
	}
}

func TestJsonOrBytesDecode_Empty(t *testing.T) {
	r := JsonOrBytes(nil)
	var v any
	err := r.Decode(&v)
	if err == nil {
		t.Fatal("expected error decoding empty JsonOrBytes")
	}
}

// ---- AsAny ----

func TestJsonOrBytesAsAny_Object(t *testing.T) {
	r := JsonOrBytes(`{"key":"value","n":7}`)
	v, err := r.AsAny()
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", v)
	}
	if m["key"] != "value" {
		t.Fatalf("unexpected key: %v", m["key"])
	}
	if m["n"] != float64(7) {
		t.Fatalf("unexpected n: %v", m["n"])
	}
}

func TestJsonOrBytesAsAny_Array(t *testing.T) {
	r := JsonOrBytes(`[1,2,3]`)
	v, err := r.AsAny()
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.([]any)
	if !ok {
		t.Fatalf("expected slice, got %T", v)
	}
	if len(arr) != 3 {
		t.Fatalf("expected length 3, got %d", len(arr))
	}
}

// ---- Full JSON round-trip embedded in a struct ----

func TestJsonOrBytesRoundTripInStruct(t *testing.T) {
	type envelope struct {
		ID     string      `json:"id"`
		Result JsonOrBytes `json:"result"`
	}
	original := envelope{
		ID:     "test-1",
		Result: JsonOrBytes(`{"merged":true,"count":5}`),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	// result must be inlined, not base64.
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	resultField, ok := raw["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected result to be inline JSON object, got %T: %v", raw["result"], raw["result"])
	}
	if resultField["merged"] != true {
		t.Fatalf("unexpected merged value: %v", resultField["merged"])
	}

	// Unmarshal back and verify Decode works.
	var decoded envelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	type result struct {
		Merged bool `json:"merged"`
		Count  int  `json:"count"`
	}
	var res result
	if err := decoded.Result.Decode(&res); err != nil {
		t.Fatal(err)
	}
	if !res.Merged || res.Count != 5 {
		t.Fatalf("unexpected result after round-trip: %+v", res)
	}
}
