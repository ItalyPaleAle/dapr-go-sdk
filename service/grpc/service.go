/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package grpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/dapr/go-sdk/client"
	pb "github.com/dapr/go-sdk/dapr/proto/runtime/v1"

	"github.com/dapr/go-sdk/actor"
	"github.com/dapr/go-sdk/actor/config"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/go-sdk/service/internal"
)

// NewService creates new Service.
func NewService(address string) (s common.Service, err error) {
	if address == "" {
		return nil, errors.New("nil address")
	}
	lis, err := net.Listen("tcp", address)
	if err != nil {
		err = errors.Wrapf(err, "failed to TCP listen on: %s", address)
		return
	}
	s = newService(lis)
	return
}

// NewServiceFromClient creates a new Service without a listener, creating an outbound connection.
// Note: the client object should not be used to make calls after this.
func NewServiceFromClient(c *client.GRPCClient) (common.Service, error) {
	protoClient := c.GrpcClient()
	_, err := protoClient.ConnectAppCallback(context.TODO(), &pb.ConnectAppCallbackRequest{})
	if err != nil {
		return nil, fmt.Errorf("error from ConnectAppCallback: %w", err)
	}

	// Switch the connection to a listener
	l := listenerFromConn{
		conn: c.RawConn(),
	}
	return newService(l), nil
}

// listenerFromConn implements net.Listener from an existing connection
type listenerFromConn struct {
	conn net.Conn
}

func (l listenerFromConn) Accept() (net.Conn, error) {
	fmt.Println("Accepted remote", l.conn.RemoteAddr(), "local", l.conn.LocalAddr())
	return l.conn, nil
}

func (l listenerFromConn) Close() error {
	return l.conn.Close()
}

func (l listenerFromConn) Addr() net.Addr {
	return l.conn.RemoteAddr()
}

// NewServiceWithListener creates new Service with specific listener.
func NewServiceWithListener(lis net.Listener) common.Service {
	return newService(lis)
}

func newService(lis net.Listener) *Server {
	s := &Server{
		listener:        lis,
		invokeHandlers:  make(map[string]common.ServiceInvocationHandler),
		topicRegistrar:  make(internal.TopicRegistrar),
		bindingHandlers: make(map[string]common.BindingInvocationHandler),
		authToken:       os.Getenv(common.AppAPITokenEnvVar),
	}

	gs := grpc.NewServer()
	pb.RegisterAppCallbackServer(gs, s)
	pb.RegisterAppCallbackHealthCheckServer(gs, s)
	s.grpcServer = gs

	return s
}

// Server is the gRPC service implementation for Dapr.
type Server struct {
	pb.UnimplementedAppCallbackServer
	pb.UnimplementedAppCallbackHealthCheckServer
	listener           net.Listener
	invokeHandlers     map[string]common.ServiceInvocationHandler
	topicRegistrar     internal.TopicRegistrar
	bindingHandlers    map[string]common.BindingInvocationHandler
	healthCheckHandler common.HealthCheckHandler
	authToken          string
	grpcServer         *grpc.Server
	started            uint32
}

func (s *Server) RegisterActorImplFactory(f actor.Factory, opts ...config.Option) {
	panic("Actor is not supported by gRPC API")
}

// Start registers the server and starts it.
func (s *Server) Start() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return errors.New("a gRPC server can only be started once")
	}
	return s.grpcServer.Serve(s.listener)
}

// Stop stops the previously-started service.
func (s *Server) Stop() error {
	if atomic.LoadUint32(&s.started) == 0 {
		return nil
	}
	s.grpcServer.Stop()
	s.grpcServer = nil
	return nil
}

// GrecefulStop stops the previously-started service gracefully.
func (s *Server) GracefulStop() error {
	if atomic.LoadUint32(&s.started) == 0 {
		return nil
	}
	s.grpcServer.GracefulStop()
	s.grpcServer = nil
	return nil
}

// GrpcServer returns the grpc.Server object managed by the server.
func (s *Server) GrpcServer() *grpc.Server {
	return s.grpcServer
}
