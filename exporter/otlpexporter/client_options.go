// Copyright 2019 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlpexporter

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	DefaultAgentPort uint16 = 55678
	DefaultAgentHost string = "localhost"
)

type ClientOption interface {
	withClient(e *Client)
}

//type resourceDetector resource.Detector
//
//var _ ClientOption = (*resourceDetector)(nil)
//
//func (rd resourceDetector) withClient(e *Client) {
//	e.resourceDetector = resource.Detector(rd)
//}
//
// WithResourceDetector allows one to register a resource detector. Resource Detector is used
// to detect resources associated with the application. Detected resource is exported
// along with the metrics. If the detector fails then it panics.
// If a resource detector is not provided then by default it detects from the environment.
//func WithResourceDetector(rd resource.Detector) ClientOption {
//	return resourceDetector(rd)
//}

type insecureGrpcConnection int

var _ ClientOption = (*insecureGrpcConnection)(nil)

func (igc *insecureGrpcConnection) withClient(e *Client) {
	e.canDialInsecure = true
}

// WithInsecure disables client transport security for the exporter's gRPC connection
// just like grpc.WithInsecure() https://godoc.org/google.golang.org/grpc#WithInsecure
// does. Note, by default, client security is required unless WithInsecure is used.
func WithInsecure() ClientOption { return new(insecureGrpcConnection) }

type addressSetter string

func (as addressSetter) withClient(e *Client) {
	e.agentAddress = string(as)
}

var _ ClientOption = (*addressSetter)(nil)

// WithAddress allows one to set the address that the exporter will
// connect to the agent on. If unset, it will instead try to use
// connect to DefaultAgentHost:DefaultAgentPort
func WithAddress(addr string) ClientOption {
	return addressSetter(addr)
}

type serviceNameSetter string

func (sns serviceNameSetter) withClient(e *Client) {
	e.serviceName = string(sns)
}

var _ ClientOption = (*serviceNameSetter)(nil)

// WithServiceName allows one to set/override the service name
// that the exporter will report to the agent.
func WithServiceName(serviceName string) ClientOption {
	return serviceNameSetter(serviceName)
}

type reconnectionPeriod time.Duration

func (rp reconnectionPeriod) withClient(e *Client) {
	e.reconnectionPeriod = time.Duration(rp)
}

func WithReconnectionPeriod(rp time.Duration) ClientOption {
	return reconnectionPeriod(rp)
}

type compressorSetter string

func (c compressorSetter) withClient(e *Client) {
	e.compressor = string(c)
}

// UseCompressor will set the compressor for the gRPC client to use when sending requests.
// It is the responsibility of the caller to ensure that the compressor set has been registered
// with google.golang.org/grpc/encoding. This can be done by encoding.RegisterCompressor. Some
// compressors auto-register on import, such as gzip, which can be registered by calling
// `import _ "google.golang.org/grpc/encoding/gzip"`
func UseCompressor(compressorName string) ClientOption {
	return compressorSetter(compressorName)
}

type headerSetter map[string]string

func (h headerSetter) withClient(e *Client) {
	e.headers = map[string]string(h)
}

// WithHeaders will send the provided headers when the gRPC stream connection
// is instantiated
func WithHeaders(headers map[string]string) ClientOption {
	return headerSetter(headers)
}

type clientCredentials struct {
	credentials.TransportCredentials
}

var _ ClientOption = (*clientCredentials)(nil)

// WithTLSCredentials allows the connection to use TLS credentials
// when talking to the server. It takes in grpc.TransportCredentials instead
// of say a Certificate file or a tls.Certificate, because the retrieving
// these credentials can be done in many ways e.g. plain file, in code tls.Config
// or by certificate rotation, so it is up to the caller to decide what to use.
func WithTLSCredentials(creds credentials.TransportCredentials) ClientOption {
	return &clientCredentials{TransportCredentials: creds}
}

func (cc *clientCredentials) withClient(e *Client) {
	e.clientTransportCredentials = cc.TransportCredentials
}

type grpcDialOptions []grpc.DialOption

var _ ClientOption = (*grpcDialOptions)(nil)

// WithGRPCDialOption opens support to any grpc.DialOption to be used. If it conflicts
// with some other configuration the GRPC specified via the agent the ones here will
// take preference since they are set last.
func WithGRPCDialOption(opts ...grpc.DialOption) ClientOption {
	return grpcDialOptions(opts)
}

func (opts grpcDialOptions) withClient(e *Client) {
	e.grpcDialOptions = opts
}
