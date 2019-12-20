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
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	otlpagentmetric "github.com/open-telemetry/opentelemetry-proto/gen/go/agent/metrics/v1"
	otlpagenttrace "github.com/open-telemetry/opentelemetry-proto/gen/go/agent/traces/v1"
	otlptrace "github.com/open-telemetry/opentelemetry-proto/gen/go/agent/traces/v1"
	otlpresource "github.com/open-telemetry/opentelemetry-proto/gen/go/resource/v1"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

var startupMu sync.Mutex
var startTime time.Time

func init() {
	startupMu.Lock()
	startTime = time.Now()
	startupMu.Unlock()
}

//var _ trace.Client = (*Client)(nil)
//var _ view.Client = (*Client)(nil)

type Client struct {
	// mu protects the non-atomic and non-channel variables
	mu sync.RWMutex
	// senderMu protects the concurrent unsafe send on traceClient client
	senderMu sync.Mutex
	// recvMu protects the concurrent unsafe recv on traceClient client
	recvMu             sync.Mutex
	started            bool
	stopped            bool
	agentAddress       string
	serviceName        string
	canDialInsecure    bool
	traceClient        otlpagenttrace.TraceServiceClient
	metricsClient      otlpagentmetric.MetricsServiceClient
	grpcClientConn     *grpc.ClientConn
	reconnectionPeriod time.Duration
	//resourceDetector   resource.Detector
	resource          *otlpresource.Resource
	compressor        string
	headers           map[string]string
	lastConnectErrPtr unsafe.Pointer

	startOnce      sync.Once
	stopCh         chan bool
	disconnectedCh chan bool

	backgroundConnectionDoneCh chan bool

	traceBundler *bundler.Bundler

	// viewDataBundler is the bundler to enable conversion
	// from OpenCensus-Go view.Data to metricspb.Metric.
	// Please do not confuse it with metricsBundler!
	//viewDataBundler *bundler.Bundler

	clientTransportCredentials credentials.TransportCredentials

	grpcDialOptions []grpc.DialOption
}

func NewClient(opts ...ClientOption) (*Client, error) {
	exp, err := NewUnstartedClient(opts...)
	if err != nil {
		return nil, err
	}
	if err := exp.Start(); err != nil {
		return nil, err
	}
	return exp, nil
}

const spanDataBufferSize = 300

func NewUnstartedClient(opts ...ClientOption) (*Client, error) {
	e := new(Client)
	for _, opt := range opts {
		opt.withClient(e)
	}
	traceBundler := bundler.NewBundler(([]*otlptrace.ResourceSpans)(nil), func(bundle interface{}) {
		e.uploadTraces(bundle.([][]*otlptrace.ResourceSpans))
	})
	traceBundler.DelayThreshold = 2 * time.Second
	traceBundler.BundleCountThreshold = spanDataBufferSize
	e.traceBundler = traceBundler

	//viewDataBundler := bundler.NewBundler((*view.Data)(nil), func(bundle interface{}) {
	//	e.uploadViewData(bundle.([]*view.Data))
	//})
	//viewDataBundler.DelayThreshold = 2 * time.Second
	//viewDataBundler.BundleCountThreshold = 500 // TODO: (@odeke-em) make this configurable.
	//e.viewDataBundler = viewDataBundler
	//e.nodeInfo = NodeWithStartTime(e.serviceName)
	//if e.resourceDetector != nil {
	//	//res, err := e.resourceDetector(context.Background())
	//	//if err != nil {
	//	//	panic(fmt.Sprintf("Error detecting resource. err:%v\n", err))
	//	//}
	//	//if res != nil {
	//	//	e.resource = resourceToResourcePb(res)
	//	//}
	//} else {
	//	//e.resource = resourceProtoFromEnv()
	//}

	return e, nil
}

const (
	maxInitialConfigRetries = 10
	maxInitialTracesRetries = 10
)

var (
	errAlreadyStarted = errors.New("already started")
	errNotStarted     = errors.New("not started")
	errStopped        = errors.New("stopped")
)

// Start dials to the agent, establishing a connection to it. It also
// initiates the Config and Trace services by sending over the initial
// messages that consist of the node identifier. Start invokes a background
// connector that will reattempt connections to the agent periodically
// if the connection dies.
func (ae *Client) Start() error {
	var err = errAlreadyStarted
	ae.startOnce.Do(func() {
		ae.mu.Lock()
		ae.started = true
		ae.disconnectedCh = make(chan bool, 1)
		ae.stopCh = make(chan bool)
		ae.backgroundConnectionDoneCh = make(chan bool)
		ae.mu.Unlock()

		// An optimistic first connection attempt to ensure that
		// applications under heavy load can immediately process
		// data. See https://github.com/census-ecosystem/opencensus-go-exporter-ocagent/pull/63
		if err := ae.connect(); err == nil {
			ae.setStateConnected()
		} else {
			ae.setStateDisconnected(err)
		}
		go ae.indefiniteBackgroundConnection()

		err = nil
	})

	return err
}

func (ae *Client) prepareAgentAddress() string {
	if ae.agentAddress != "" {
		return ae.agentAddress
	}
	return fmt.Sprintf("%s:%d", DefaultAgentHost, DefaultAgentPort)
}

func (ae *Client) enableConnectionStreams(cc *grpc.ClientConn) error {
	ae.mu.RLock()
	started := ae.started
	//nodeInfo := ae.nodeInfo
	ae.mu.RUnlock()

	if !started {
		return errNotStarted
	}

	ae.mu.Lock()
	// If the previous clientConn was non-nil, close it
	if ae.grpcClientConn != nil {
		_ = ae.grpcClientConn.Close()
	}
	ae.grpcClientConn = cc
	ae.mu.Unlock()

	if err := ae.createTraceServiceConnection(ae.grpcClientConn); err != nil {
		return err
	}

	return ae.createMetricsServiceConnection(ae.grpcClientConn)
}

func (ae *Client) createTraceServiceConnection(cc *grpc.ClientConn) error {
	// Initiate the trace service by sending over node identifier info.
	traceSvcClient := otlpagenttrace.NewTraceServiceClient(cc)
	//ctx := context.Background()
	//if len(ae.headers) > 0 {
	//	ctx = metadata.NewOutgoingContext(ctx, metadata.New(ae.headers))
	//}
	//traceClient, err := traceSvcClient.Export(ctx)
	//if err != nil {
	//	return fmt.Errorf("Client.Start:: TraceServiceClient: %v", err)
	//}
	//
	//firstTraceMessage := &otlpagenttrace.ExportTraceServiceRequest{
	//	ResourceSpans: []*otlpagenttrace.ResourceSpans{
	//		{Resource: ae.resource},
	//	},
	//}
	//if err := traceClient.Send(firstTraceMessage); err != nil {
	//	return fmt.Errorf("Client.Start:: Failed to initiate the Config service: %v", err)
	//}

	ae.mu.Lock()
	ae.traceClient = traceSvcClient
	ae.mu.Unlock()

	// Initiate the config service by sending over node identifier info.
	//configStream, err := traceSvcClient.Config(context.Background())
	//if err != nil {
	//	return fmt.Errorf("Client.Start:: ConfigStream: %v", err)
	//}
	//firstCfgMessage := &agenttracepb.CurrentLibraryConfig{Node: node}
	//if err := configStream.Send(firstCfgMessage); err != nil {
	//	return fmt.Errorf("Client.Start:: Failed to initiate the Config service: %v", err)
	//}

	// In the background, handle trace configurations that are beamed down
	// by the agent, but also reply to it with the applied configuration.
	//go ae.handleConfigStreaming(configStream)

	return nil
}

func (ae *Client) createMetricsServiceConnection(cc *grpc.ClientConn) error {
	metricsSvcClient := otlpagentmetric.NewMetricsServiceClient(cc)
	//metricsClient, err := metricsSvcClient.Export(context.Background())
	//if err != nil {
	//	return fmt.Errorf("MetricsClient: failed to start the service client: %v", err)
	//}
	//// Initiate the metrics service by sending over the first message just containing the Node and Resource.
	//firstMetricsMessage := &agentmetricspb.ExportMetricsServiceRequest{
	//	Node:     node,
	//	Resource: ae.resource,
	//}
	//if err := metricsClient.Send(firstMetricsMessage); err != nil {
	//	return fmt.Errorf("MetricsClient:: failed to send the first message: %v", err)
	//}

	ae.mu.Lock()
	ae.metricsClient = metricsSvcClient
	ae.mu.Unlock()

	// With that we are good to go and can start sending metrics
	return nil
}

func (ae *Client) dialToAgent() (*grpc.ClientConn, error) {
	addr := ae.prepareAgentAddress()
	var dialOpts []grpc.DialOption
	if ae.clientTransportCredentials != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(ae.clientTransportCredentials))
	} else if ae.canDialInsecure {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}
	if ae.compressor != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(ae.compressor)))
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(&ocgrpc.ClientHandler{}))
	if len(ae.grpcDialOptions) != 0 {
		dialOpts = append(dialOpts, ae.grpcDialOptions...)
	}

	ctx := context.Background()
	if len(ae.headers) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(ae.headers))
	}
	return grpc.DialContext(ctx, addr, dialOpts...)
}

// Stop shuts down all the connections and resources
// related to the exporter.
func (ae *Client) Stop() error {
	ae.mu.RLock()
	cc := ae.grpcClientConn
	started := ae.started
	stopped := ae.stopped
	ae.mu.RUnlock()

	if !started {
		return errNotStarted
	}
	if stopped {
		// TODO: tell the user that we've already stopped, so perhaps a sentinel error?
		return nil
	}

	ae.Flush()

	// Now close the underlying gRPC connection.
	var err error
	if cc != nil {
		err = cc.Close()
	}

	// At this point we can change the state variables: started and stopped
	ae.mu.Lock()
	ae.started = false
	ae.stopped = true
	ae.mu.Unlock()
	close(ae.stopCh)

	// Ensure that the backgroundConnector returns
	<-ae.backgroundConnectionDoneCh

	return err
}

func (ae *Client) ExportSpan(sd []*otlptrace.ResourceSpans) {
	if sd == nil {
		return
	}
	_ = ae.traceBundler.Add(sd, 1)
}

func (ae *Client) ExportTraceServiceRequest(batch *otlpagenttrace.ExportTraceServiceRequest) error {
	if batch == nil || len(batch.ResourceSpans) == 0 {
		return nil
	}

	select {
	case <-ae.stopCh:
		return errStopped

	default:
		if lastConnectErr := ae.lastConnectError(); lastConnectErr != nil {
			return fmt.Errorf("ExportTraceServiceRequest: no active connection, last connection error: %v", lastConnectErr)
		}

		ae.senderMu.Lock()
		_, err := ae.traceClient.Export(context.Background(), batch)
		ae.senderMu.Unlock()
		if err != nil {
			ae.setStateDisconnected(err)
			if err != io.EOF {
				return err
			}
		}
		return nil
	}
}

//func (ae *Client) ExportView(vd *view.Data) {
//	if vd == nil {
//		return
//	}
//	_ = ae.viewDataBundler.Add(vd, 1)
//}
//
// ExportMetricsServiceRequest sends proto metrics with the metrics service client.
func (ae *Client) ExportMetricsServiceRequest(batch *otlpagentmetric.ExportMetricsServiceRequest) error {
	if batch == nil || len(batch.ResourceMetrics) == 0 {
		return nil
	}

	select {
	case <-ae.stopCh:
		return errStopped

	default:
		if lastConnectErr := ae.lastConnectError(); lastConnectErr != nil {
			return fmt.Errorf("ExportMetricsServiceRequest: no active connection, last connection error: %v", lastConnectErr)
		}

		ae.senderMu.Lock()
		_, err := ae.metricsClient.Export(context.Background(), batch)
		ae.senderMu.Unlock()
		if err != nil {
			ae.setStateDisconnected(err)
			if err != io.EOF {
				return err
			}
		}
		return nil
	}
}

func ocSpanDataToPbSpans(sdl [][]*otlptrace.ResourceSpans) []*otlptrace.ResourceSpans {
	if len(sdl) == 0 {
		return nil
	}
	protoSpans := make([]*otlptrace.ResourceSpans, 0, len(sdl))
	for _, sd := range sdl {
		if sd != nil {
			protoSpans = append(protoSpans, sd...)
		}
	}
	return protoSpans
}

func (ae *Client) uploadTraces(sdl [][]*otlptrace.ResourceSpans) {
	select {
	case <-ae.stopCh:
		return

	default:
		if !ae.connected() {
			return
		}

		protoSpans := ocSpanDataToPbSpans(sdl)
		if len(protoSpans) == 0 {
			return
		}
		ae.senderMu.Lock()
		_, err := ae.traceClient.Export(context.Background(), &otlpagenttrace.ExportTraceServiceRequest{
			ResourceSpans: protoSpans,
		})
		ae.senderMu.Unlock()
		if err != nil {
			ae.setStateDisconnected(err)
		}
	}
}

//func ocViewDataToPbMetrics(vdl []*view.Data) []*metricspb.Metric {
//	if len(vdl) == 0 {
//		return nil
//	}
//	metrics := make([]*metricspb.Metric, 0, len(vdl))
//	for _, vd := range vdl {
//		if vd != nil {
//			vmetric, err := viewDataToMetric(vd)
//			// TODO: (@odeke-em) somehow report this error, if it is non-nil.
//			if err == nil && vmetric != nil {
//				metrics = append(metrics, vmetric)
//			}
//		}
//	}
//	return metrics
//}
//
//func (ae *Client) uploadViewData(vdl []*view.Data) {
//	protoMetrics := ocViewDataToPbMetrics(vdl)
//	if len(protoMetrics) == 0 {
//		return
//	}
//	req := &agentmetricspb.ExportMetricsServiceRequest{
//		Metrics:  protoMetrics,
//		Resource: resourceProtoFromEnv(),
//		// TODO:(@odeke-em)
//		// a) Figure out how to derive a Node from the environment
//		// or better letting users of the exporter configure it.
//	}
//	ae.ExportMetricsServiceRequest(req)
//}
//
func (ae *Client) Flush() {
	ae.traceBundler.Flush()
	//ae.viewDataBundler.Flush()
}

//func resourceToResourcePb(rs *resource.Resource) *otlpresource.Resource {
//	rprs := &resourcepb.Resource{
//		Type: rs.Type,
//	}
//	if rs.Labels != nil {
//		rprs.Labels = make(map[string]string)
//		for k, v := range rs.Labels {
//			rprs.Labels[k] = v
//		}
//	}
//	return rprs
//}
//
func (ae *Client) lastConnectError() error {
	errPtr := (*error)(atomic.LoadPointer(&ae.lastConnectErrPtr))
	if errPtr == nil {
		return nil
	}
	return *errPtr
}

func (ae *Client) saveLastConnectError(err error) {
	var errPtr *error
	if err != nil {
		errPtr = &err
	}
	atomic.StorePointer(&ae.lastConnectErrPtr, unsafe.Pointer(errPtr))
}

func (ae *Client) setStateDisconnected(err error) {
	ae.saveLastConnectError(err)
	select {
	case ae.disconnectedCh <- true:
	default:
	}
}

func (ae *Client) setStateConnected() {
	ae.saveLastConnectError(nil)
}

func (ae *Client) connected() bool {
	return ae.lastConnectError() == nil
}

const defaultConnReattemptPeriod = 10 * time.Second

func (ae *Client) indefiniteBackgroundConnection() error {
	defer func() {
		ae.backgroundConnectionDoneCh <- true
	}()

	connReattemptPeriod := ae.reconnectionPeriod
	if connReattemptPeriod <= 0 {
		connReattemptPeriod = defaultConnReattemptPeriod
	}

	// No strong seeding required, nano time can
	// already help with pseudo uniqueness.
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63n(1024)))

	// maxJitter: 1 + (70% of the connectionReattemptPeriod)
	maxJitter := int64(1 + 0.7*float64(connReattemptPeriod))

	for {
		// Otherwise these will be the normal scenarios to enable
		// reconnections if we trip out.
		// 1. If we've stopped, return entirely
		// 2. Otherwise block until we are disconnected, and
		//    then retry connecting
		select {
		case <-ae.stopCh:
			return errStopped

		case <-ae.disconnectedCh:
			// Normal scenario that we'll wait for
		}

		if err := ae.connect(); err == nil {
			ae.setStateConnected()
		} else {
			ae.setStateDisconnected(err)
		}

		// Apply some jitter to avoid lockstep retrials of other
		// agent-clients. Lockstep retrials could result in an
		// innocent DDOS, by clogging the machine's resources and network.
		jitter := time.Duration(rng.Int63n(maxJitter))
		select {
		case <-ae.stopCh:
			return errStopped
		case <-time.After(connReattemptPeriod + jitter):
		}
	}
}

func (ae *Client) connect() error {
	cc, err := ae.dialToAgent()
	if err != nil {
		return err
	}
	return ae.enableConnectionStreams(cc)
}

// NodeWithStartTime creates a node using nodeName and derives:
//  Hostname from the environment
//  Pid from the current process
//  StartTimestamp from the start time of this process
//  Language and library information.
//func NodeWithStartTime(nodeName string) *commonpb.Node {
//	return &commonpb.Node{
//		Identifier: &commonpb.ProcessIdentifier{
//			HostName:       os.Getenv("HOSTNAME"),
//			Pid:            uint32(os.Getpid()),
//			StartTimestamp: internal.TimeToTimestamp(startTime),
//		},
//		LibraryInfo: &commonpb.LibraryInfo{
//			Language:           commonpb.LibraryInfo_GO_LANG,
//			ExporterVersion:    "",
//			CoreLibraryVersion: opencensus.Version(),
//		},
//		ServiceInfo: &commonpb.ServiceInfo{
//			Name: nodeName,
//		},
//		Attributes: make(map[string]string),
//	}
//}
