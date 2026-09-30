package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
)

// maxBodyBytes 限制请求体大小，防止异常大报文耗尽内存。
const maxBodyBytes = 16 << 20

// HTTPServer 把 Gateway 挂到指定路径上提供 HTTP JSON-RPC 服务。
type HTTPServer struct {
	gw       *Gateway
	path     string
	addr     string
	listener net.Listener
	srv      *http.Server
}

// NewHTTPServer 创建 HTTP 服务。
func NewHTTPServer(gw *Gateway, addr, path string) *HTTPServer {
	if path == "" {
		path = "/"
	}
	s := &HTTPServer{gw: gw, path: path, addr: addr}
	mux := http.NewServeMux()
	mux.HandleFunc(path, s.ServeHTTP)
	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Addr 返回实际监听地址（Start 之后可用，":0" 时能拿到随机端口）。
func (s *HTTPServer) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Start 立即建立监听并在后台提供服务；端口不可用时同步返回错误。
func (s *HTTPServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return NewError(CodeGatewayError, "listen failed", err, map[string]any{"addr": s.addr})
	}
	s.listener = ln
	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.gw.log.Printf("http server stopped: %v", err)
		}
	}()
	return nil
}

// Shutdown 优雅关闭。
func (s *HTTPServer) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// ServeHTTP 实现 http.Handler。
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeRPCError(w, http.StatusMethodNotAllowed, nil,
			NewError(CodeInvalidRequest, "Only POST is supported", nil, nil))
		return
	}
	if r.Header.Get("Content-Type") != "" &&
		!containsContentType(r.Header.Get("Content-Type"), "application/json") {
		writeRPCError(w, http.StatusUnsupportedMediaType, nil,
			NewError(CodeInvalidRequest, "Content-Type must be application/json", nil, nil))
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		writeRPCError(w, http.StatusBadRequest, nil,
			NewError(CodeParseError, "", err, nil))
		return
	}
	out, status := s.gw.Handle(r.Context(), body)
	if status == httpStatusNoContent || len(out) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}

func containsContentType(header, target string) bool {
	// Content-Type 可能带 charset，如 application/json; charset=utf-8。
	for i := 0; i+len(target) <= len(header); i++ {
		if header[i:i+len(target)] == target {
			return true
		}
	}
	return false
}

func writeRPCError(w http.ResponseWriter, httpStatus int, id []byte, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	_, _ = w.Write(errorEnvelope(id, err))
}
