package praefect

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// NewServerFactory returns factory object for initialization of praefect gRPC servers.
func NewServerFactory(
	deps *Dependencies,
	opts ...ServerOption,
) *ServerFactory {
	return &ServerFactory{
		deps: deps,
		opts: opts,
	}
}

// ServerFactory is a factory of praefect grpc servers
type ServerFactory struct {
	mtx              sync.Mutex
	deps             *Dependencies
	secure, insecure []*grpc.Server
	opts             []ServerOption
}

// Serve starts serving on the provided listener with newly created grpc.Server
func (s *ServerFactory) Serve(l net.Listener, secure bool) error {
	srv, err := s.Create(secure)
	if err != nil {
		return err
	}

	return srv.Serve(l)
}

// Stop stops all servers created by the factory.
func (s *ServerFactory) Stop() {
	for _, srv := range s.all() {
		srv.Stop()
	}
}

// GracefulStop stops both the secure and insecure servers gracefully.
func (s *ServerFactory) GracefulStop() {
	wg := sync.WaitGroup{}

	for _, srv := range s.all() {
		wg.Add(1)

		go func(s *grpc.Server) {
			s.GracefulStop()
			wg.Done()
		}(srv)
	}

	wg.Wait()
}

// Create returns newly instantiated and initialized with interceptors instance of the gRPC server.
func (s *ServerFactory) Create(secure bool) (*grpc.Server, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if !secure {
		s.insecure = append(s.insecure, s.createGRPC(nil))
		return s.insecure[len(s.insecure)-1], nil
	}

	cert, err := s.deps.Config.TLS.Certificate()
	if err != nil {
		return nil, fmt.Errorf("load certificate key pair: %w", err)
	}

	// The Go language maintains a list of cipher suites that do not have known security issues.
	// This list of cipher suites should be used instead of the default list.
	var secureCiphers []uint16
	for _, cipher := range tls.CipherSuites() {
		secureCiphers = append(secureCiphers, cipher.ID)
	}

	s.secure = append(s.secure, s.createGRPC(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   s.deps.Config.TLS.MinVersion.ProtocolVersion(),
		CipherSuites: secureCiphers,
	})))

	return s.secure[len(s.secure)-1], nil
}

func (s *ServerFactory) createGRPC(creds credentials.TransportCredentials) *grpc.Server {
	return NewGRPCServer(s.deps, creds, s.opts...)
}

func (s *ServerFactory) all() []*grpc.Server {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	servers := make([]*grpc.Server, 0, len(s.secure)+len(s.insecure))
	servers = append(servers, s.secure...)
	servers = append(servers, s.insecure...)
	return servers
}
