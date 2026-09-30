package gateway

import (
	"context"
	"encoding/json"
	"testing"
)

func newSimpleGateway(t *testing.T, fakes ...*fakeProvider) *Gateway {
	t.Helper()
	return buildTestGateway(t, NewQuota(0, 0), 0, readOnlyPolicies, fakes...)
}

// 1) 方法不支持：明确返回 -32601，且不调用任何 provider。
func TestProtocol_MethodNotFound(t *testing.T) {
	p1 := newFakeProvider("p1")
	gw := newSimpleGateway(t, p1)
	out, status := gw.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":55,"method":"eth_unknown","params":[]}`))
	if status != httpStatusOK {
		t.Fatalf("json-rpc method error still http 200, got %d", status)
	}
	var resp Response
	if err := json.Unmarshal(out, &resp); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if resp.Error == nil || Code(resp.Error.Code) != CodeMethodNotFound {
		t.Fatalf("want -32601, got %+v", resp.Error)
	}
	if p1.callCount() != 0 {
		t.Fatalf("unsupported method must not reach providers, calls=%d", p1.callCount())
	}
}

// 2) JSON 语法错误：-32700 + HTTP 400，id 为 null。
func TestProtocol_ParseError(t *testing.T) {
	gw := newSimpleGateway(t)
	out, status := gw.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","id":`))
	if status != httpStatusBadJSON {
		t.Fatalf("want http 400, got %d", status)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	if resp.Error == nil || Code(resp.Error.Code) != CodeParseError {
		t.Fatalf("want -32700, got %+v", resp.Error)
	}
	if string(resp.ID) != "null" {
		t.Fatalf("parse error id must be null, got %s", resp.ID)
	}
}

// 3) 非 2.0 或缺少 method：-32600。
func TestProtocol_InvalidRequest(t *testing.T) {
	gw := newSimpleGateway(t)
	cases := [][]byte{
		[]byte(`{"jsonrpc":"1.0","id":1,"method":"eth_blockNumber"}`),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":""}`),
		[]byte(`[1,2,3]`), // 批量中的非对象元素
	}
	for i, raw := range cases {
		out, status := gw.Handle(context.Background(), raw)
		_ = status
		var check = func(r *Response) {
			if r.Error == nil || Code(r.Error.Code) != CodeInvalidRequest {
				t.Fatalf("case %d: want -32600, got %+v", i, r.Error)
			}
		}
		var single Response
		if err := json.Unmarshal(out, &single); err == nil && single.Error != nil {
			check(&single)
			continue
		}
		var batch []Response
		if err := json.Unmarshal(out, &batch); err == nil {
			for _, r := range batch {
				check(&r)
			}
			continue
		}
		t.Fatalf("case %d: unparsable output %s", i, out)
	}
}

// 4) 空批量请求：-32600。
func TestProtocol_EmptyBatch(t *testing.T) {
	gw := newSimpleGateway(t)
	out, status := gw.Handle(context.Background(), []byte(`[]`))
	if status != httpStatusBadJSON {
		t.Fatalf("want 400 for empty batch, got %d", status)
	}
	var resp Response
	_ = json.Unmarshal(out, &resp)
	if resp.Error == nil || Code(resp.Error.Code) != CodeInvalidRequest {
		t.Fatalf("want -32600 for empty batch, got %+v", resp.Error)
	}
}

// 5) 通知（无 id）：HTTP 204，无响应体；不产生 JSON-RPC 响应。
func TestProtocol_NotificationNoResponse(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{resp: okResp(1, `{}`)})
	gw := newSimpleGateway(t, p1)
	out, status := gw.Handle(context.Background(), []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber"}`))
	if status != httpStatusNoContent {
		t.Fatalf("notification must be 204, got %d", status)
	}
	if len(out) != 0 {
		t.Fatalf("notification must have empty body, got %s", out)
	}
}

//  6. 批量混合：成功、失败、不支持、通知交错，响应数量与有 id 请求一致，
//     且每个响应的 id 与请求严格对应、顺序不要求但绝不能串。
func TestProtocol_BatchIDsCorrespond(t *testing.T) {
	p1 := newFakeProvider("p1",
		fakeBehavior{resp: okResp(1, `"r-for-1"`)},
		fakeBehavior{fail: &UpstreamFailure{Kind: FailureTransport, Cause: errSimple("boom")}},
		fakeBehavior{resp: okResp(3, `"r-for-3"`)},
	)
	gw := newSimpleGateway(t, p1)
	raw := []byte(`[
		{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},
		{"jsonrpc":"2.0","method":"eth_blockNumber"},
		{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"},
		{"jsonrpc":"2.0","id":3,"method":"eth_blockNumber"},
		{"jsonrpc":"2.0","id":4,"method":"eth_unknown"}
	]`)
	out, status := gw.Handle(context.Background(), raw)
	if status != httpStatusOK {
		t.Fatalf("batch http status want 200, got %d", status)
	}
	var batch []Response
	if err := json.Unmarshal(out, &batch); err != nil {
		t.Fatalf("batch output not json array: %v: %s", err, out)
	}
	// 4 个带 id 的请求 -> 4 条响应（通知不计）。
	if len(batch) != 4 {
		t.Fatalf("want 4 responses (notification excluded), got %d: %s", len(batch), out)
	}
	byID := map[int64]*Response{}
	for i := range batch {
		var id int64
		if err := json.Unmarshal(batch[i].ID, &id); err != nil {
			t.Fatalf("bad id %s: %v", batch[i].ID, err)
		}
		if _, dup := byID[id]; dup {
			t.Fatalf("duplicate response for id %d", id)
		}
		byID[id] = &batch[i]
	}
	if string(byID[1].Result) != `"r-for-1"` {
		t.Fatalf("id1 result wrong: %s", byID[1].Result)
	}
	if byID[2].Error == nil || Code(byID[2].Error.Code) != CodeNoProvider {
		t.Fatalf("id2 should be -32001 (only provider failed), got %+v", byID[2].Error)
	}
	if string(byID[3].Result) != `"r-for-3"` {
		t.Fatalf("id3 result wrong: %s", byID[3].Result)
	}
	if byID[4].Error == nil || Code(byID[4].Error.Code) != CodeMethodNotFound {
		t.Fatalf("id4 should be -32601, got %+v", byID[4].Error)
	}
}

// 7) 单个请求但顶层是数组里只有通知：204。
func TestProtocol_BatchAllNotifications(t *testing.T) {
	p1 := newFakeProvider("p1")
	gw := newSimpleGateway(t, p1)
	_, status := gw.Handle(context.Background(),
		[]byte(`[{"jsonrpc":"2.0","method":"eth_blockNumber"}]`))
	if status != httpStatusNoContent {
		t.Fatalf("all-notification batch must be 204, got %d", status)
	}
}
