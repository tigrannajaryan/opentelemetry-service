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

package otlptrace

import (
	"context"
	"errors"

	otlpagenttrace "github.com/open-telemetry/opentelemetry-proto/gen/go/agent/traces/v1"
	"go.opencensus.io/trace"

	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/observability"
	"github.com/open-telemetry/opentelemetry-collector/oterr"
)

const (
	defaultNumWorkers = 4

	messageChannelSize = 64
)

// Receiver is the type used to handle spans from OpenCensus exporters.
type Receiver struct {
	nextConsumer consumer.OTLPTraceConsumer
	numWorkers   int
	workers      []*receiverWorker
	messageChan  chan *traceDataWithCtx
}

type traceDataWithCtx struct {
	data *consumerdata.OTLPTrace
	ctx  context.Context
}

// New creates a new opencensus.Receiver reference.
func New(nextConsumer consumer.OTLPTraceConsumer, opts ...Option) (*Receiver, error) {
	if nextConsumer == nil {
		return nil, oterr.ErrNilNextConsumer
	}

	messageChan := make(chan *traceDataWithCtx, messageChannelSize)
	ocr := &Receiver{
		nextConsumer: nextConsumer,
		numWorkers:   defaultNumWorkers,
		messageChan:  messageChan,
	}
	for _, opt := range opts {
		opt(ocr)
	}

	// Setup and startup worker pool
	workers := make([]*receiverWorker, 0, ocr.numWorkers)
	for index := 0; index < ocr.numWorkers; index++ {
		worker := newReceiverWorker(ocr)
		go worker.listenOn(messageChan)
		workers = append(workers, worker)
	}
	ocr.workers = workers

	return ocr, nil
}

var _ otlpagenttrace.TraceServiceServer = (*Receiver)(nil)

var errUnimplemented = errors.New("unimplemented")

var errTraceExportProtocolViolation = errors.New("protocol violation: Export's first message must have a Node")

const receiverTagValue = "oc_trace"

// Export is the gRPC method that receives streamed traces from
// OpenCensus-traceproto compatible libraries/applications.
func (ocr *Receiver) Export(
	ctx context.Context,
	recv *otlpagenttrace.ExportTraceServiceRequest,
) (*otlpagenttrace.ExportTraceServiceResponse, error) {
	// We need to ensure that it propagates the receiver name as a tag
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, receiverTagValue)
	td := &consumerdata.OTLPTrace{
		ResourceSpanList: recv.ResourceSpans,
	}
	ocr.messageChan <- &traceDataWithCtx{data: td, ctx: ctxWithReceiverName}
	observability.RecordMetricsForTraceReceiver(ctxWithReceiverName, len(td.ResourceSpanList), 0)
	return &otlpagenttrace.ExportTraceServiceResponse{}, nil
}

// Stop the receiver and its workers
func (ocr *Receiver) Stop() {
	for _, worker := range ocr.workers {
		worker.stopListening()
	}
}

type receiverWorker struct {
	receiver *Receiver
	cancel   chan struct{}
}

func newReceiverWorker(receiver *Receiver) *receiverWorker {
	return &receiverWorker{
		receiver: receiver,
		cancel:   make(chan struct{}),
	}
}

func (rw *receiverWorker) listenOn(cn <-chan *traceDataWithCtx) {
	for {
		select {
		case tdWithCtx := <-cn:
			rw.export(tdWithCtx.ctx, tdWithCtx.data)
		case <-rw.cancel:
			return
		}
	}
}

func (rw *receiverWorker) stopListening() {
	close(rw.cancel)
}

func (rw *receiverWorker) export(longLivedCtx context.Context, tracedata *consumerdata.OTLPTrace) {
	if tracedata == nil {
		return
	}

	if len(tracedata.ResourceSpanList) == 0 {
		return
	}

	// Trace this method
	ctx, span := trace.StartSpan(context.Background(), "OpenCensusTraceReceiver.Export")
	defer span.End()

	// TODO: (@odeke-em) investigate if it is necessary
	// to group nodes with their respective spans during
	// spansAndNode list unfurling then send spans grouped per node

	// If the starting RPC has a parent span, then add it as a parent link.
	observability.SetParentLink(longLivedCtx, span)

	rw.receiver.nextConsumer.ConsumeOTLPTrace(ctx, *tracedata)

	span.Annotate([]trace.Attribute{
		trace.Int64Attribute("num_spans", int64(len(tracedata.ResourceSpanList))),
	}, "")
}
