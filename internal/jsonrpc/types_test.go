package jsonrpc

import (
	"encoding/json"
	"testing"
)

func TestParseSingleRequest(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`)
	reqs, batch, err := ParseRequests(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch {
		t.Fatal("expected single, got batch")
	}
	if len(reqs) != 1 || reqs[0].Method != "eth_blockNumber" {
		t.Fatalf("bad parse: %+v", reqs)
	}
	if reqs[0].ID.String() != "7" {
		t.Fatalf("id round-trip failed: %q", reqs[0].ID.String())
	}
}

func TestParseBatchAndNotification(t *testing.T) {
	body := []byte(`[
		{"jsonrpc":"2.0","id":"abc","method":"eth_call","params":[{"to":"0x1"}]},
		{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"]}
	]`)
	reqs, batch, err := ParseRequests(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !batch || len(reqs) != 2 {
		t.Fatalf("expected batch of 2, got batch=%v len=%d", batch, len(reqs))
	}
	if reqs[0].ID.String() != "abc" {
		t.Fatalf("string id mismatch: %q", reqs[0].ID.String())
	}
	if !reqs[1].IsNotification() {
		t.Fatal("second request should be a notification")
	}
}

func TestParseErrors(t *testing.T) {
	cases := [][]byte{
		nil,
		[]byte(`not json`),
		[]byte(`[]`),
		[]byte(`{"jsonrpc":"1.0","id":1,"method":"x"}`),
		[]byte(`{"jsonrpc":"2.0","id":1}`),
	}
	for i, b := range cases {
		if _, _, err := ParseRequests(b); err == nil {
			t.Fatalf("case %d: expected parse error", i)
		}
	}
}

func TestResponseIDPreserved(t *testing.T) {
	ids := []string{`1`, `"str-id"`, `null`, `42.5`}
	for _, id := range ids {
		resp := MakeResult(RawID(id), json.RawMessage(`"x"`))
		out, err := json.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}
		var back map[string]json.RawMessage
		if err := json.Unmarshal(out, &back); err != nil {
			t.Fatal(err)
		}
		if string(back["id"]) != id {
			t.Fatalf("id not preserved: want %s got %s", id, back["id"])
		}
	}
}
