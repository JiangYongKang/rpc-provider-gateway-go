package gateway

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Quota 同时限制网关的并发在途请求数与每秒请求数（QPS）。
// 超出任一配额的请求立即被拒绝（不排队等待），返回的错误可被 errors.Is 识别为
// ErrRateLimited，从而与“provider 不可用”“方法不支持”明确区分。
type Quota struct {
	maxConcurrent int
	qps           int

	sem chan struct{}

	mu     sync.Mutex
	tokens float64 // 当前令牌数
	last   time.Time

	now func() time.Time
}

// NewQuota 创建配额控制器；maxConcurrent 或 qps <= 0 表示不限制该维度。
// 令牌桶容量取 qps（允许与 QPS 等量的瞬时突发），初始为满。
func NewQuota(maxConcurrent, qps int) *Quota {
	q := &Quota{
		maxConcurrent: maxConcurrent,
		qps:           qps,
		now:           time.Now,
	}
	if maxConcurrent > 0 {
		q.sem = make(chan struct{}, maxConcurrent)
	}
	if qps > 0 {
		q.tokens = float64(qps)
		q.last = q.now()
	}
	return q
}

// ErrRateLimited 在配额不足时返回，可被 errors.Is 识别。
var ErrRateLimited = errors.New("rate limited: request quota exceeded")

// Acquire 尝试占用一个调用名额，成功时返回 release 函数（必须且只能调用一次）。
// 超额时返回包装了 ErrRateLimited 的 *Error。
func (q *Quota) Acquire(ctx context.Context) (func(), error) {
	_ = ctx // 配额不足立即拒绝，不在此处排队
	if q.qps > 0 && !q.consumeToken() {
		return nil, NewError(CodeRateLimited, "", ErrRateLimited, map[string]any{
			"reason": "qps_exceeded",
			"qps":    q.qps,
		})
	}
	if q.maxConcurrent > 0 {
		select {
		case q.sem <- struct{}{}:
		default:
			return nil, NewError(CodeRateLimited, "", ErrRateLimited, map[string]any{
				"reason":         "max_concurrent_exceeded",
				"max_concurrent": q.maxConcurrent,
			})
		}
	}
	var released bool
	var mu sync.Mutex
	return func() {
		mu.Lock()
		defer mu.Unlock()
		if released {
			return
		}
		released = true
		if q.maxConcurrent > 0 {
			<-q.sem
		}
	}, nil
}

// consumeToken 尝试取一个令牌；不足返回 false（立即拒绝，不等待补充）。
func (q *Quota) consumeToken() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	if !q.last.IsZero() && now.After(q.last) {
		elapsed := now.Sub(q.last).Seconds()
		q.tokens += elapsed * float64(q.qps)
		if cap := float64(q.qps); q.tokens > cap {
			q.tokens = cap
		}
		q.last = now
	}
	if q.tokens < 1 {
		return false
	}
	q.tokens--
	return true
}
