// Copyright The OpenTelemetry Authors
//
// Copyright 2019 Splunk, Inc.
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

package httpclient

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.opencensus.io/stats/view"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	v12 "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/logs/v1"
	v1 "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/metrics/v1"
	otlptrace "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/trace/v1"
)

const (
	idleConnTimeout     = 30 * time.Second
	tlsHandshakeTimeout = 10 * time.Second
	dialerTimeout       = 30 * time.Second
	dialerKeepAlive     = 30 * time.Second

	// default values
	defaultNumWorkers  uint = 8
	defaultMaxIdleCons      = 100
	defaultHTTPTimeout      = 10 * time.Second
)

type sendRequest struct {
	message []byte
	items   int64
	batches int64
}

type TargetUrls struct {
	Traces  string
	Metrics string
	Logs    string
}

// Client implements an HTTP sender for the SAPM protocol
type Client struct {
	numWorkers         uint
	maxIdleCons        uint
	targetUrls         TargetUrls
	httpClient         *http.Client
	disableCompression bool
	closeCh            chan struct{}

	workers chan *worker
}

// New creates a new SAPM Client
func New(targetUrls TargetUrls, opts ...Option) (*Client, error) {
	views := metricViews()
	if err := view.Register(views...); err != nil {
		return nil, err
	}

	c := &Client{
		targetUrls:  targetUrls,
		numWorkers:  defaultNumWorkers,
		maxIdleCons: defaultMaxIdleCons,
	}

	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}

	if c.httpClient == nil {
		c.httpClient = &http.Client{
			Timeout: defaultHTTPTimeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   dialerTimeout,
					KeepAlive: dialerKeepAlive,
				}).DialContext,
				MaxIdleConns:        int(c.maxIdleCons),
				MaxIdleConnsPerHost: int(c.maxIdleCons),
				IdleConnTimeout:     idleConnTimeout,
				TLSHandshakeTimeout: tlsHandshakeTimeout,
			},
		}
	}

	c.closeCh = make(chan struct{})
	c.workers = make(chan *worker, c.numWorkers)
	for i := uint(0); i < c.numWorkers; i++ {
		w := newWorker(c.httpClient, c.disableCompression)
		c.workers <- w
	}

	return c, nil
}

func (sa *Client) ExportTraces(ctx context.Context, request *otlptrace.ExportTraceServiceRequest) error {
	items := 0
	for _, rs := range request.ResourceSpans {
		for _, ils := range rs.InstrumentationLibrarySpans {
			items += len(ils.Spans)
		}
	}
	return sa.Export(ctx, request, len(request.ResourceSpans), items, sa.targetUrls.Traces)
}

func (sa *Client) ExportMetrics(ctx context.Context, request *v1.ExportMetricsServiceRequest) error {
	items := 0
	for _, rs := range request.ResourceMetrics {
		for _, ils := range rs.InstrumentationLibraryMetrics {
			items += len(ils.Metrics)
		}
	}
	return sa.Export(ctx, request, len(request.ResourceMetrics), items, sa.targetUrls.Metrics)
}

func (sa *Client) ExportLogs(ctx context.Context, request *v12.ExportLogsServiceRequest) error {
	items := 0
	for _, rs := range request.ResourceLogs {
		for _, ils := range rs.InstrumentationLibraryLogs {
			items += len(ils.Logs)
		}
	}
	return sa.Export(ctx, request, len(request.ResourceLogs), items, sa.targetUrls.Logs)
}

// Export takes a request and uses one of the available workers to export it synchronously.
// It returns an error in case a request cannot be processed. It's up to the caller to retry.
func (sa *Client) Export(ctx context.Context, request proto.Message, batches, spansCount int, url string) error {
	w := <-sa.workers
	sendErr := w.export(ctx, request, batches, spansCount, url)
	sa.workers <- w
	if sendErr != nil {
		if sendErr.RetryDelaySeconds > 0 {
			return exporterhelper.NewThrottleRetry(sendErr.Err,
				time.Duration(sendErr.RetryDelaySeconds)*time.Second)
		}
		if sendErr.Permanent {
			return consumererror.Permanent(sendErr)
		}
		return sendErr
	}
	return nil
}

// Stop waits for all inflight requests to finish and then drains the worker pool so no more work can be done.
// It returns once all workers are drained from the pool. Note that the client can accept new requests while
// Stop() waits for other requests to finish.
func (sa *Client) Stop() {
	wg := sync.WaitGroup{}
	wg.Add(int(sa.numWorkers))
	close(sa.closeCh)
	for i := uint(0); i < sa.numWorkers; i++ {
		go func() {
			<-sa.workers
			wg.Done()
		}()
	}
	wg.Wait()
}
