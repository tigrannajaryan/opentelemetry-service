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

package otlpmetrics

import (
	"context"
	"errors"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	otlpagentmetric "github.com/open-telemetry/opentelemetry-proto/gen/go/agent/metrics/v1"
	"go.opencensus.io/trace"
	"google.golang.org/api/support/bundler"

	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/observability"
	"github.com/open-telemetry/opentelemetry-collector/oterr"
)

// Receiver is the type used to handle metrics from OpenCensus exporters.
type Receiver struct {
	nextConsumer       consumer.MetricsConsumer
	metricBufferPeriod time.Duration
	metricBufferCount  int
}

// New creates a new ocmetrics.Receiver reference.
func New(nextConsumer consumer.MetricsConsumer, opts ...Option) (*Receiver, error) {
	if nextConsumer == nil {
		return nil, oterr.ErrNilNextConsumer
	}
	ocr := &Receiver{nextConsumer: nextConsumer}
	for _, opt := range opts {
		opt.WithReceiver(ocr)
	}
	return ocr, nil
}

var _ otlpagentmetric.MetricsServiceServer = (*Receiver)(nil)

var errMetricsExportProtocolViolation = errors.New("protocol violation: Export's first message must have a Node")

const receiverTagValue = "oc_metrics"

// Export is the gRPC method that receives streamed metrics from
// OpenCensus-metricproto compatible libraries/applications.
func (ocr *Receiver) Export(
	ctx context.Context,
	recv *otlpagentmetric.ExportMetricsServiceRequest,
) (*otlpagentmetric.ExportMetricsServiceResponse, error) {
	return &otlpagentmetric.ExportMetricsServiceResponse{}, nil
}

func processReceivedMetrics(ni *commonpb.Node, resource *resourcepb.Resource, metrics []*metricspb.Metric, bundler *bundler.Bundler) {
	// Firstly, we'll add them to the bundler.
	if len(metrics) > 0 {
		bundlerPayload := &consumerdata.MetricsData{Node: ni, Metrics: metrics, Resource: resource}
		bundler.Add(bundlerPayload, len(bundlerPayload.Metrics))
	}
}

func (ocr *Receiver) batchMetricExporting(longLivedRPCCtx context.Context, payload interface{}) {
	mds := payload.([]*consumerdata.MetricsData)
	if len(mds) == 0 {
		return
	}

	// Trace this method
	ctx, span := trace.StartSpan(context.Background(), "OpenCensusMetricsReceiver.Export")
	defer span.End()

	// TODO: (@odeke-em) investigate if it is necessary
	// to group nodes with their respective metrics during
	// bundledMetrics list unfurling then send metrics grouped per node

	// If the starting RPC has a parent span, then add it as a parent link.
	observability.SetParentLink(longLivedRPCCtx, span)

	nMetrics := int64(0)
	for _, md := range mds {
		ocr.nextConsumer.ConsumeMetricsData(ctx, *md)
		nMetrics += int64(len(md.Metrics))
	}

	span.Annotate([]trace.Attribute{
		trace.Int64Attribute("num_metrics", nMetrics),
	}, "")
}
