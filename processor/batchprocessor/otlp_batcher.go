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

package batchprocessor

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/api/support/bundler"

	"github.com/open-telemetry/opentelemetry-collector/component"
	"github.com/open-telemetry/opentelemetry-collector/consumer"
	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/processor"
)

// otlpBatcher is a component that accepts spans, and places them into batches grouped by node and resource.
//
// otlpBatcher implements consumer.TraceConsumer
//
// otlpBatcher is a composition of four main pieces. First is its buckets map which maps nodes to buckets.
// Second is the nodebatcher which keeps a batch associated with a single node, and sends it downstream.
// Third is a bucketTicker that ticks every so often and closes any open and not recently sent batches.
//
// When we no longer have to batch by node, the following changes should be made:
//   1) otlpBatcher should be removed and nodebatcher should be promoted to otlpBatcher
//   2) bucketTicker should be simplified significantly and replaced with a single ticker, since
//      tracking by node is no longer needed.
type otlpBatcher struct {
	sender consumer.OTLPTraceConsumer
	name   string
	logger *zap.Logger

	removeAfterCycles uint32
	sendBatchSize     uint32
	numTickers        int
	tickTime          time.Duration
	timeout           time.Duration

	traceBundler *bundler.Bundler
}

var _ consumer.OTLPTraceConsumer = (*otlpBatcher)(nil)

// NewBatcher creates a new otlpBatcher that batches spans by node and resource
func NewOTLPBatcher(name string, logger *zap.Logger, sender consumer.OTLPTraceConsumer) processor.OTLPTraceProcessor {
	// Init with defaults
	b := &otlpBatcher{
		name:   name,
		sender: sender,
		logger: logger,

		removeAfterCycles: defaultRemoveAfterCycles,
		sendBatchSize:     defaultSendBatchSize,
		numTickers:        defaultNumTickers,
		tickTime:          defaultTickTime,
		timeout:           defaultTimeout,
	}

	b.traceBundler = bundler.NewBundler((*consumerdata.OTLPTrace)(nil), func(bundle interface{}) {
		b.sendTraces(bundle.([]*consumerdata.OTLPTrace))
	})
	b.traceBundler.BundleCountThreshold = 1000
	b.traceBundler.HandlerLimit = 4

	// start tickers after options loaded in
	return b
}

// ConsumeTraceData implements otlpBatcher as a SpanProcessor and takes the provided spans and adds them to
// batches
func (b *otlpBatcher) ConsumeTraceData(ctx context.Context, td consumerdata.TraceData) error {
	panic("otlpBatcher.ConsumeTraceData Not implemented")
	//return nil
}

// ConsumeTraceData implements otlpBatcher as a SpanProcessor and takes the provided spans and adds them to
// batches
func (b *otlpBatcher) ConsumeOTLPTrace(ctx context.Context, td consumerdata.OTLPTrace) error {
	b.traceBundler.Add(&td, 1)

	return nil
}

func (b *otlpBatcher) sendTraces(tds []*consumerdata.OTLPTrace) {
	if len(tds) == 0 {
		return
	}
	list := tds[0].ResourceSpanList
	for i := 1; i < len(tds); i++ {
		list = append(list, tds[i].ResourceSpanList...)
	}
	b.sender.ConsumeOTLPTrace(context.Background(), consumerdata.OTLPTrace{ResourceSpanList: list})
}

func (b *otlpBatcher) GetCapabilities() processor.Capabilities {
	return processor.Capabilities{MutatesConsumedData: false}
}

// Start is invoked during service startup.
func (b *otlpBatcher) Start(host component.Host) error {
	return nil
}

// Shutdown is invoked during service shutdown.
func (b *otlpBatcher) Shutdown() error {
	// TODO: flush accumulated data.
	b.traceBundler.Flush()
	return nil
}
