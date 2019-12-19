// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jaegerreceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	apacheThrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/gorilla/mux"
	"github.com/jaegertracing/jaeger/cmd/agent/app/configmanager"
	jSamplingConfig "github.com/jaegertracing/jaeger/cmd/agent/app/configmanager/grpc"
	"github.com/jaegertracing/jaeger/cmd/agent/app/httpserver"
	"github.com/jaegertracing/jaeger/cmd/agent/app/processors"
	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers/thriftudp"
	"github.com/jaegertracing/jaeger/cmd/collector/app"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2"
	"github.com/jaegertracing/jaeger/thrift-gen/baggage"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	jaegerThrift "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/sampling"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector/component"
	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/observability"
	"github.com/open-telemetry/opentelemetry-collector/oterr"
	"github.com/open-telemetry/opentelemetry-collector/receiver"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector/translator/trace/jaeger"
)

// Receiver type is used to receive spans that were originally intended to be sent to Jaeger.
// This receiver is basically a Jaeger collector.
type otlpReceiver struct {
	// mu protects the fields of this type
	mu sync.Mutex

	nextConsumer consumer.OTLPTraceConsumer

	startOnce sync.Once
	stopOnce  sync.Once

	config *Configuration

	grpc            *grpc.Server
	tchanServer     *otlpTchannelReceiver
	collectorServer *http.Server

	agentSamplingManager *jSamplingConfig.SamplingManager
	agentProcessors      []processors.Processor
	agentServer          *http.Server

	defaultAgentCtx context.Context
	logger          *zap.Logger
}

type otlpTchannelReceiver struct {
	nextConsumer consumer.OTLPTraceConsumer

	tchannel *tchannel.Channel
}

// New creates a TraceReceiver that receives traffic as a collector with both Thrift and HTTP transports.
func NewOTLP(ctx context.Context, config *Configuration, nextConsumer consumer.OTLPTraceConsumer, logger *zap.Logger) (receiver.TraceReceiver, error) {
	logger.Info("Creating OTLP Jaeger receiver")
	return &otlpReceiver{
		config:          config,
		defaultAgentCtx: observability.ContextWithReceiverName(context.Background(), "jaeger-agent"),
		nextConsumer:    nextConsumer,
		tchanServer: &otlpTchannelReceiver{
			nextConsumer: nextConsumer,
		},
		logger: logger,
	}, nil
}

var _ receiver.TraceReceiver = (*otlpReceiver)(nil)

func (jr *otlpReceiver) collectorAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.CollectorHTTPPort
	}
	if port <= 0 {
		port = defaultCollectorHTTPPort
	}
	return fmt.Sprintf(":%d", port)
}

// TODO https://github.com/open-telemetry/opentelemetry-collector/issues/267
//	Remove ThriftTChannel support.
func (jr *otlpReceiver) tchannelAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.CollectorThriftPort
	}
	if port <= 0 {
		port = defaultTChannelPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *otlpReceiver) grpcAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.CollectorGRPCPort
	}
	if port <= 0 {
		port = defaultGRPCPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *otlpReceiver) agentCompactThriftAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.AgentCompactThriftPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *otlpReceiver) agentCompactThriftEnabled() bool {
	return jr.config != nil && jr.config.AgentCompactThriftPort > 0
}

func (jr *otlpReceiver) agentBinaryThriftAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.AgentBinaryThriftPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *otlpReceiver) agentBinaryThriftEnabled() bool {
	return jr.config != nil && jr.config.AgentBinaryThriftPort > 0
}

func (jr *otlpReceiver) agentHTTPPortAddr() string {
	var port int
	if jr.config != nil {
		port = jr.config.AgentHTTPPort
	}
	return fmt.Sprintf(":%d", port)
}

func (jr *otlpReceiver) agentHTTPEnabled() bool {
	return jr.config != nil && jr.config.AgentHTTPPort > 0
}

func (jr *otlpReceiver) TraceSource() string {
	return traceSource
}

func (jr *otlpReceiver) Start(host component.Host) error {
	jr.mu.Lock()
	defer jr.mu.Unlock()

	var err = oterr.ErrAlreadyStarted
	jr.startOnce.Do(func() {
		if err = jr.startAgent(host); err != nil && err != oterr.ErrAlreadyStarted {
			jr.stopTraceReceptionLocked()
			return
		}

		if err = jr.startCollector(host); err != nil && err != oterr.ErrAlreadyStarted {
			jr.stopTraceReceptionLocked()
			return
		}

		err = nil
	})
	return err
}

func (jr *otlpReceiver) Shutdown() error {
	jr.mu.Lock()
	defer jr.mu.Unlock()

	return jr.stopTraceReceptionLocked()
}

func (jr *otlpReceiver) stopTraceReceptionLocked() error {
	var err = oterr.ErrAlreadyStopped
	jr.stopOnce.Do(func() {
		var errs []error

		if jr.agentServer != nil {
			if aerr := jr.agentServer.Close(); aerr != nil {
				errs = append(errs, aerr)
			}
			jr.agentServer = nil
		}
		for _, processor := range jr.agentProcessors {
			processor.Stop()
		}

		if jr.collectorServer != nil {
			if cerr := jr.collectorServer.Close(); cerr != nil {
				errs = append(errs, cerr)
			}
			jr.collectorServer = nil
		}
		if jr.tchanServer.tchannel != nil {
			jr.tchanServer.tchannel.Close()
			jr.tchanServer.tchannel = nil
		}
		if jr.grpc != nil {
			jr.grpc.Stop()
			jr.grpc = nil
		}
		if len(errs) == 0 {
			err = nil
			return
		}
		// Otherwise combine all these errors
		buf := new(bytes.Buffer)
		for _, err := range errs {
			fmt.Fprintf(buf, "%s\n", err.Error())
		}
		err = errors.New(buf.String())
	})

	return err
}

func consumeOTLPTrace(ctx context.Context, batches []*jaeger.Batch, consumer consumer.OTLPTraceConsumer) ([]*jaeger.BatchSubmitResponse, error) {
	// log.Printf("consumeOTLPTrace %d batched", len(batches))

	jbsr := make([]*jaeger.BatchSubmitResponse, 0, len(batches))

	for _, batch := range batches {
		td, err := jaegertranslator.ThriftBatchToOTLP(batch)
		// TODO: (@odeke-em) add this error for Jaeger observability
		ok := false

		if err == nil {
			ok = true
			consumer.ConsumeOTLPTrace(ctx, td)
			// We MUST unconditionally record metrics from this reception.
			// TODO: calc total!!!
			observability.RecordMetricsForTraceReceiver(ctx, len(batch.Spans), len(batch.Spans)-len(td.ResourceSpanList))
		}

		jbsr = append(jbsr, &jaeger.BatchSubmitResponse{
			Ok: ok,
		})
	}

	return jbsr, nil
}

func (jr *otlpReceiver) SubmitBatches(batches []*jaeger.Batch, options app.SubmitBatchOptions) ([]*jaeger.BatchSubmitResponse, error) {
	ctx := context.Background()
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, collectorReceiverTagValue)

	return consumeOTLPTrace(ctxWithReceiverName, batches, jr.nextConsumer)
}

func (jtr *otlpTchannelReceiver) SubmitBatches(ctx thrift.Context, batches []*jaeger.Batch) ([]*jaeger.BatchSubmitResponse, error) {
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, tchannelCollectorReceiverTagValue)

	return consumeOTLPTrace(ctxWithReceiverName, batches, jtr.nextConsumer)
}

var _ reporter.Reporter = (*otlpReceiver)(nil)
var _ api_v2.CollectorServiceServer = (*otlpReceiver)(nil)
var _ configmanager.ClientConfigManager = (*otlpReceiver)(nil)

// EmitZipkinBatch implements cmd/agent/reporter.Reporter and it forwards
// Zipkin spans received by the Jaeger agent processor.
func (jr *otlpReceiver) EmitZipkinBatch(spans []*zipkincore.Span) error {
	return nil
}

// EmitBatch implements cmd/agent/reporter.Reporter and it forwards
// Jaeger spans received by the Jaeger agent processor.
func (jr *otlpReceiver) EmitBatch(batch *jaeger.Batch) error {
	td, err := jaegertranslator.ThriftBatchToOTLP(batch)
	if err != nil {
		observability.RecordMetricsForTraceReceiver(jr.defaultAgentCtx, len(batch.Spans), len(batch.Spans))
		return err
	}

	err = jr.nextConsumer.ConsumeOTLPTrace(jr.defaultAgentCtx, td)
	// TODO: calc total!!!
	observability.RecordMetricsForTraceReceiver(jr.defaultAgentCtx, len(batch.Spans), len(batch.Spans)-len(td.ResourceSpanList))

	return err
}

func (jr *otlpReceiver) GetSamplingStrategy(serviceName string) (*sampling.SamplingStrategyResponse, error) {
	return jr.agentSamplingManager.GetSamplingStrategy(serviceName)
}

func (jr *otlpReceiver) GetBaggageRestrictions(serviceName string) ([]*baggage.BaggageRestriction, error) {
	br, err := jr.agentSamplingManager.GetBaggageRestrictions(serviceName)
	if err != nil {
		// Baggage restrictions are not yet implemented - refer to - https://github.com/jaegertracing/jaeger/issues/373
		// As of today, GetBaggageRestrictions() always returns an error.
		// However, we `return nil, nil` here in order to serve a valid `200 OK` response.
		return nil, nil
	}
	return br, nil
}

func (jr *otlpReceiver) PostSpans(ctx context.Context, r *api_v2.PostSpansRequest) (*api_v2.PostSpansResponse, error) {
	// log.Printf("Received Jaeger gRPC %d spans", len(r.Batch.Spans))

	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, collectorReceiverTagValue)

	td, err := jaegertranslator.ProtoBatchToOTLP(r.Batch)
	if err != nil {
		observability.RecordMetricsForTraceReceiver(ctxWithReceiverName, len(r.Batch.Spans), len(r.Batch.Spans))
		return nil, err
	}

	err = jr.nextConsumer.ConsumeOTLPTrace(ctx, td)
	// TODO: calc total!!!
	observability.RecordMetricsForTraceReceiver(ctxWithReceiverName, len(r.Batch.Spans), len(r.Batch.Spans)-len(td.ResourceSpanList))
	if err != nil {
		return nil, err
	}

	return &api_v2.PostSpansResponse{}, err
}

func (jr *otlpReceiver) startAgent(_ component.Host) error {
	if !jr.agentBinaryThriftEnabled() && !jr.agentCompactThriftEnabled() && !jr.agentHTTPEnabled() {
		return nil
	}

	if jr.agentBinaryThriftEnabled() {
		processor, err := jr.buildProcessor(jr.agentBinaryThriftAddr(), apacheThrift.NewTBinaryProtocolFactoryDefault())
		if err != nil {
			return err
		}
		jr.agentProcessors = append(jr.agentProcessors, processor)
	}

	if jr.agentCompactThriftEnabled() {
		processor, err := jr.buildProcessor(jr.agentCompactThriftAddr(), apacheThrift.NewTCompactProtocolFactory())
		if err != nil {
			return err
		}
		jr.agentProcessors = append(jr.agentProcessors, processor)
	}

	for _, processor := range jr.agentProcessors {
		go processor.Serve()
	}

	// Start upstream grpc client before serving sampling endpoints over HTTP
	if jr.config.RemoteSamplingEndpoint != "" {
		conn, err := grpc.Dial(jr.config.RemoteSamplingEndpoint, grpc.WithInsecure())
		if err != nil {
			jr.logger.Error("Error creating grpc connection to jaeger remote sampling endpoint", zap.String("endpoint", jr.config.RemoteSamplingEndpoint))
			return err
		}

		jr.agentSamplingManager = jSamplingConfig.NewConfigManager(conn)
	}

	if jr.agentHTTPEnabled() {
		jr.agentServer = httpserver.NewHTTPServer(jr.agentHTTPPortAddr(), jr, metrics.NullFactory)

		go func() {
			if err := jr.agentServer.ListenAndServe(); err != nil {
				jr.logger.Error("http server failure", zap.Error(err))
			}
		}()
	}

	return nil
}

func (jr *otlpReceiver) buildProcessor(address string, factory apacheThrift.TProtocolFactory) (processors.Processor, error) {
	handler := jaegerThrift.NewAgentProcessor(jr)
	transport, err := thriftudp.NewTUDPServerTransport(address)
	if err != nil {
		return nil, err
	}
	server, err := servers.NewTBufferedServer(transport, defaultAgentQueueSize, defaultAgentMaxPacketSize, metrics.NullFactory)
	if err != nil {
		return nil, err
	}
	processor, err := processors.NewThriftProcessor(server, defaultAgentServerWorkers, metrics.NullFactory, factory, handler, jr.logger)
	if err != nil {
		return nil, err
	}
	return processor, nil
}

func (jr *otlpReceiver) startCollector(host component.Host) error {
	tch, terr := tchannel.NewChannel("jaeger-collector", new(tchannel.ChannelOptions))
	if terr != nil {
		return fmt.Errorf("failed to create NewTChannel: %v", terr)
	}

	server := thrift.NewServer(tch)
	server.Register(jaeger.NewTChanCollectorServer(jr.tchanServer))

	taddr := jr.tchannelAddr()
	tln, terr := net.Listen("tcp", taddr)
	if terr != nil {
		return fmt.Errorf("failed to bind to TChannel address %q: %v", taddr, terr)
	}
	tch.Serve(tln)
	jr.tchanServer.tchannel = tch

	// Now the collector that runs over HTTP
	caddr := jr.collectorAddr()
	cln, cerr := net.Listen("tcp", caddr)
	if cerr != nil {
		// Abort and close tch
		tch.Close()
		return fmt.Errorf("failed to bind to Collector address %q: %v", caddr, cerr)
	}

	nr := mux.NewRouter()
	apiHandler := app.NewAPIHandler(jr)
	apiHandler.RegisterRoutes(nr)
	jr.collectorServer = &http.Server{Handler: nr}
	go func() {
		_ = jr.collectorServer.Serve(cln)
	}()

	jr.grpc = grpc.NewServer(jr.config.CollectorGRPCOptions...)
	gaddr := jr.grpcAddr()
	gln, gerr := net.Listen("tcp", gaddr)
	if gerr != nil {
		// Abort and close tch, cln
		tch.Close()
		cln.Close()
		return fmt.Errorf("failed to bind to gRPC address %q: %v", gaddr, gerr)
	}

	api_v2.RegisterCollectorServiceServer(jr.grpc, jr)

	go func() {
		if err := jr.grpc.Serve(gln); err != nil {
			host.ReportFatalError(err)
		}
	}()

	return nil
}
