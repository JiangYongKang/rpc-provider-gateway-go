package gateway

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 1) QPS=2：前两个请求放行，第三个在令牌补充前被明确拒绝为 -32004。
func TestQuota_QPSReject(t *testing.T) {
	q := NewQuota(0, 2)
	rel1, err := q.Acquire(context.Background())
	if err != nil || rel1 == nil {
		t.Fatalf("first acquire should pass: %v", err)
	}
	rel2, err := q.Acquire(context.Background())
	if err != nil || rel2 == nil {
		t.Fatalf("second acquire should pass: %v", err)
	}
	_, err = q.Acquire(context.Background())
	if err == nil {
		t.Fatal("third acquire must be rejected")
	}
	ge := AsError(err)
	if ge.Code != CodeRateLimited {
		t.Fatalf("want -32004, got %d", ge.Code)
	}
	if !contains(string(mustJSON(ge.Details)), "qps_exceeded") {
		t.Fatalf("reject reason must be distinguishable, details=%v", ge.Details)
	}
	rel1()
	rel2()
}

// 2) 等待令牌补充后再次放行（令牌桶按秒补充）。
func TestQuota_QPSReplenishes(t *testing.T) {
	q := NewQuota(0, 5)
	for i := 0; i < 5; i++ {
		rel, err := q.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		defer rel()
	}
	if _, err := q.Acquire(context.Background()); err == nil {
		t.Fatal("bucket should be empty now")
	}
	time.Sleep(1100 * time.Millisecond)
	rel, err := q.Acquire(context.Background())
	if err != nil {
		t.Fatalf("after 1s token should replenish: %v", err)
	}
	rel()
}

// 3) max_concurrent=2：第三个并发请求立即拒绝，前两个在途结束后名额释放。
func TestQuota_MaxConcurrentReject(t *testing.T) {
	q := NewQuota(2, 0)
	rel1, _ := q.Acquire(context.Background())
	rel2, _ := q.Acquire(context.Background())
	_, err := q.Acquire(context.Background())
	if err == nil {
		t.Fatal("must reject when all slots occupied")
	}
	if AsError(err).Code != CodeRateLimited {
		t.Fatalf("want -32004, got %v", err)
	}
	rel1()
	rel3, err := q.Acquire(context.Background())
	if err != nil {
		t.Fatalf("freed slot must be reusable: %v", err)
	}
	rel3()
	rel2()
}

//  4. 被限流的请求绝不触碰任何 provider（在 Serve 层面验证），
//     且错误码与“方法不支持 (-32601)”“全部不可用 (-32001)”互不相同。
func TestServe_RateLimitedDoesNotCallProviders(t *testing.T) {
	p1 := newFakeProvider("p1", fakeBehavior{resp: okResp(1, `"x"`)})
	gw := buildTestGateway(t, NewQuota(1, 0), 0, readOnlyPolicies, p1)

	// 占住唯一的并发名额。
	holder, _ := gw.quota.Acquire(context.Background())
	resp := gw.Serve(context.Background(), mustReq(`200`, "eth_blockNumber", ""))
	if resp.Error == nil || Code(resp.Error.Code) != CodeRateLimited {
		t.Fatalf("want -32004, got %+v", resp.Error)
	}
	if p1.callCount() != 0 {
		t.Fatalf("rate-limited request must not reach providers, calls=%d", p1.callCount())
	}
	if CodeRateLimited == CodeMethodNotFound || CodeRateLimited == CodeNoProvider {
		t.Fatal("error codes must be distinct")
	}
	holder()
}

//  5. 并发压力：N 个不同 id 的请求并发打到带两个 provider 的网关，
//     每个响应的 id 必须与请求严格对应，结果不得串包。
func TestServe_ConcurrentIDsDoNotCross(t *testing.T) {
	const n = 80
	// 每个 provider 都根据请求 id 返回对应结果。
	idProvider := &idEchoProvider{name: "echo", delay: 5 * time.Millisecond}
	gw := buildTestGatewayWithPolicies(t, NewQuota(n, 0), 3*time.Second, readOnlyPolicies, idProvider)

	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			id := fmt.Sprintf(`"id-%d"`, i)
			params := fmt.Sprintf(`[%d]`, i)
			resp := gw.Serve(context.Background(), mustReq(id, "eth_getBalance", params))
			if resp.Error != nil {
				errs <- fmt.Errorf("id=%s unexpected error: %v", id, resp.Error)
				return
			}
			if string(resp.ID) != id {
				errs <- fmt.Errorf("id cross: want %s got %s", id, resp.ID)
				return
			}
			// 结果里必须带回同样的序号，证明请求/响应没有错配。
			if !contains(string(resp.Result), fmt.Sprintf(`"echo":%d`, i)) {
				errs <- fmt.Errorf("id=%s wrong result body %s", id, resp.Result)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if idProvider.calls != n {
		t.Fatalf("want exactly %d provider calls, got %d", n, idProvider.calls)
	}
	if idProvider.maxFlight < 2 {
		t.Fatalf("expected real concurrency, maxFlight=%d", idProvider.maxFlight)
	}
	t.Logf("concurrent requests=%d, provider peak in-flight=%d", n, idProvider.maxFlight)
}

// 6) 并发撞上 QPS 限额：被拒请求得到 -32004，其余成功；两者 id 仍各自对应。
func TestServe_ConcurrentQPSLimit(t *testing.T) {
	const qps = 10
	idProvider := &idEchoProvider{name: "echo", delay: 20 * time.Millisecond}
	gw := buildTestGatewayWithPolicies(t, NewQuota(0, qps), 3*time.Second, readOnlyPolicies, idProvider)

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, limited := 0, 0
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			id := fmt.Sprintf(`%d`, 300+i)
			resp := gw.Serve(context.Background(), mustReq(id, "eth_blockNumber", ""))
			mu.Lock()
			defer mu.Unlock()
			if resp.Error != nil {
				if Code(resp.Error.Code) != CodeRateLimited {
					t.Errorf("id=%s unexpected non-limit error: %v", id, resp.Error)
					return
				}
				limited++
			} else {
				if string(resp.ID) != id {
					t.Errorf("id cross: want %s got %s", id, resp.ID)
					return
				}
				ok++
			}
		}()
	}
	wg.Wait()
	t.Logf("qps=%d total=%d ok=%d rate_limited=%d", qps, n, ok, limited)
	if limited == 0 {
		t.Fatal("some requests should have been rate limited")
	}
	if ok+limited != n {
		t.Fatalf("no request may be silently dropped: ok=%d limited=%d total=%d", ok, limited, n)
	}
}
