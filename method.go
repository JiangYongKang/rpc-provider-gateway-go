package gateway

import "time"

// MethodPolicy 描述一个 JSON-RPC 方法的重试与缓存策略。
type MethodPolicy struct {
	// ReadOnly 为 true 表示只读方法：允许跨 provider 故障转移，允许缓存。
	ReadOnly bool
	// Cacheable 为 true 表示成功结果可按参数缓存（要求 ReadOnly）。
	Cacheable bool
	// TTL 为缓存有效期；Cacheable 为 true 时必须 > 0。
	TTL time.Duration
}

// Retryable 返回该方法是否允许在 provider 之间故障转移。
// 当前规则：只有显式声明为只读的方法允许转移；
// 发交易等非幂等方法绝不因切换而重复执行。
func (p MethodPolicy) Retryable() bool { return p.ReadOnly }

// MethodRouter 维护方法名到策略的映射。
type MethodRouter struct {
	policies map[string]MethodPolicy
}

// NewMethodRouter 创建路由器。
// 未显式配置的方法一律视为：不支持、非只读、不可缓存、不允许故障转移。
// 若某个方法声明 Cacheable 但 TTL<=0 或 ReadOnly=false，构造时会被规整为不可缓存。
func NewMethodRouter(policies map[string]MethodPolicy) *MethodRouter {
	normalized := make(map[string]MethodPolicy, len(policies))
	for name, p := range policies {
		if !p.ReadOnly {
			// 非只读（非幂等）方法永远不可缓存。
			p.Cacheable = false
			p.TTL = 0
		}
		if p.Cacheable && p.TTL <= 0 {
			p.Cacheable = false
		}
		normalized[name] = p
	}
	return &MethodRouter{policies: normalized}
}

// Policy 返回方法策略；未配置的方法返回零值策略（非只读、不可缓存）。
func (r *MethodRouter) Policy(method string) MethodPolicy {
	if p, ok := r.policies[method]; ok {
		return p
	}
	return MethodPolicy{}
}

// IsSupported 判断方法是否在路由表中。
// 网关只对显式登记的方法进行路由，其它方法返回 -32601 Method not found，
// 从而把“方法不支持”与“provider 不可用/限流”明确区分开。
func (r *MethodRouter) IsSupported(method string) bool {
	_, ok := r.policies[method]
	return ok
}
