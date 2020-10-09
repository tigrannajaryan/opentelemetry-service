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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/consumer/consumererror"
	otlptracecol "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/trace/v1"
	otlpcommon "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/common/v1"
	otlpresource "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/resource/v1"
	otlptrace "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/trace/v1"
)

var defaultTargetUrls = TargetUrls{
	Traces:  "http://localhost/v1/traces",
	Metrics: "http://localhost/v1/metrics",
	Logs:    "http://localhost/v1/logs",
}

var errBadContentType = errors.New("bad content type")

func parseRequest(req *http.Request, request proto.Message) error {
	// content type MUST be application/x-protobuf
	if req.Header.Get(headerContentType) != headerValueXProtobuf {
		return errBadContentType
	}

	var reader io.Reader

	gr, _ := gzip.NewReader(bytes.NewReader([]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 255, 255, 1, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0}))

	// content encoding SHOULD be gzip
	if req.Header.Get(headerContentEncoding) == headerValueGZIP {
		// get the gzip reader
		// reset the reader with the request body
		if err := gr.Reset(req.Body); err != nil {
			return err
		}
		reader = gr
	} else {
		reader = req.Body
	}

	jeff := &bytes.Buffer{}
	tmp := make([]byte, 32*1024)

	if _, err := io.CopyBuffer(jeff, reader, tmp); err != nil {
		return err
	}

	// unmarshal request body
	return proto.Unmarshal(jeff.Bytes(), request)
}

func assertRequestEqualBatches(t *testing.T, r *http.Request, wanted proto.Message) {
	in := reflect.ValueOf(wanted)
	assert.False(t, in.IsNil())
	out := reflect.New(in.Type().Elem())
	dst := out.Interface().(proto.Message)

	err := parseRequest(r, dst)
	assert.NoError(t, err)
	assert.EqualValues(t, wanted, dst)
}

func TestDefaults(t *testing.T) {
	c, err := New(defaultTargetUrls)
	require.NoError(t, err)

	hc := c.httpClient

	assert.Equal(t, defaultHTTPTimeout, hc.Timeout)
	assert.Equal(t, defaultNumWorkers, uint(len(c.workers)))
}

func TestClient(t *testing.T) {
	transport := &mockTransport{}
	c, err := New(defaultTargetUrls, WithHTTPClient(newMockHTTPClient(transport)))
	require.NoError(t, err)

	sendRequests := []*otlptracecol.ExportTraceServiceRequest{}

	for i := 0; i < 10; i++ {
		request := &otlptracecol.ExportTraceServiceRequest{
			ResourceSpans: []*otlptrace.ResourceSpans{
				{
					Resource: &otlpresource.Resource{
						Attributes: []*otlpcommon.KeyValue{
							{
								Key: "service.name",
								Value: &otlpcommon.AnyValue{
									Value: &otlpcommon.AnyValue_StringValue{
										StringValue: "test_service_" + strconv.Itoa(i),
									},
								},
							},
						},
					},
					InstrumentationLibrarySpans: nil,
				},
			},
		}
		sendRequests = append(sendRequests, request)
	}

	for _, request := range sendRequests {
		err := c.ExportTraces(context.Background(), request)
		require.Nil(t, err)
	}

	rcvRequests := transport.requests()
	assert.Len(t, rcvRequests, len(sendRequests))

	for i, want := range sendRequests {
		assertRequestEqualBatches(t, rcvRequests[i].r, want)
	}

}

func TestFailure(t *testing.T) {
	transport := &mockTransport{statusCode: 500}
	c, err := New(
		defaultTargetUrls,
		WithHTTPClient(newMockHTTPClient(transport)),
	)
	require.NoError(t, err)

	request := &otlptracecol.ExportTraceServiceRequest{}

	err = c.ExportTraces(context.Background(), request)
	require.NotNil(t, err)
	assert.Equal(t, "error exporting items. server responded with status 500", err.Error())

	requests := transport.requests()
	require.Len(t, requests, 1)
	assertRequestEqualBatches(t, requests[0].r, request)

	transport.reset(200)
	transport.err = errors.New("transport error")

	err = c.ExportTraces(context.Background(), request)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "transport error")

	requests = transport.requests()
	require.Len(t, requests, 1)
	assertRequestEqualBatches(t, requests[0].r, request)
}

func TestRetries(t *testing.T) {
	transport := &mockTransport{statusCode: 500}
	c, err := New(
		defaultTargetUrls,
		WithHTTPClient(newMockHTTPClient(transport)),
	)
	require.NoError(t, err)

	batches := &otlptracecol.ExportTraceServiceRequest{}

	err = c.ExportTraces(context.Background(), batches)
	require.NotNil(t, err)
	assert.Equal(t, err.Error(), "error exporting items. server responded with status 500")
	serr := err.(*ErrSend)
	assert.False(t, serr.Permanent)

	requests := transport.requests()
	require.Len(t, requests, 1)
	assertRequestEqualBatches(t, requests[0].r, batches)
}

func TestBadRequest(t *testing.T) {
	transport := &mockTransport{}

	c, err := New(
		defaultTargetUrls,
		WithHTTPClient(newMockHTTPClient(transport)),
	)
	require.NoError(t, err)

	batches := &otlptracecol.ExportTraceServiceRequest{}

	for _, code := range []int{400, 401} {
		transport.reset(code)
		err = c.ExportTraces(context.Background(), batches)
		require.NotNil(t, err)
		require.True(t, consumererror.IsPermanent(err))
		err = consumererror.UnwrapPermanent(err)
		require.IsType(t, &ErrSend{}, err)
		serr := err.(*ErrSend)
		assert.True(t, serr.Permanent)
		assert.Equal(t, err.Error(), "dropping request: server responded with: "+strconv.Itoa(code))

		requests := transport.requests()
		require.Len(t, requests, 1)
		assertRequestEqualBatches(t, requests[0].r, batches)
	}
}

func TestWorkers(t *testing.T) {
	workerDelay := time.Millisecond * 200
	transport := &mockTransport{delay: workerDelay}

	// tell client to use a single worker
	// add delay to transport
	c, err := New(
		defaultTargetUrls,
		WithHTTPClient(newMockHTTPClient(transport)),
		WithWorkers(1),
	)
	require.NoError(t, err)

	numRequests := 4
	wg := sync.WaitGroup{}
	wg.Add(numRequests)

	requestsBatches := make([]*otlptracecol.ExportTraceServiceRequest, numRequests)
	for i := 0; i < numRequests; i++ {
		requestsBatches[i] = &otlptracecol.ExportTraceServiceRequest{}
	}

	then := time.Now()
	for _, batches := range requestsBatches {
		go func(b *otlptracecol.ExportTraceServiceRequest) {
			assert.NoError(t, c.ExportTraces(context.Background(), b))
			wg.Done()
		}(batches)
	}
	wg.Wait()

	requests := transport.requests()
	require.Len(t, requests, 4)

	// ensure each batch took at least (workerDelay * batch's queue position) to complete
	for i, b := range requestsBatches {
		r := requests[i]
		delay := r.receivedAt.Sub(then)
		assert.GreaterOrEqual(t, int(delay), int(workerDelay*time.Duration(i)))
		assertRequestEqualBatches(t, r.r, b)
	}

	// reset transport to remove delay and empty recorded requests
	transport.reset(200)
	c, err = New(
		defaultTargetUrls,
		WithHTTPClient(newMockHTTPClient(transport)),
		WithWorkers(4),
	)
	require.NoError(t, err)

	wg = sync.WaitGroup{}
	wg.Add(numRequests)

	then = time.Now()
	for _, batches := range requestsBatches {
		go func(b *otlptracecol.ExportTraceServiceRequest) {
			err := c.ExportTraces(context.Background(), b)
			require.Nil(t, err)
			wg.Done()
		}(batches)
	}
	wg.Wait()

	requests = transport.requests()
	require.Len(t, requests, 4)

	// ensure all four requests completed within 100ms
	timeout := time.Millisecond * time.Duration(100)
	for i, b := range requestsBatches {
		r := requests[i]
		delay := r.receivedAt.Sub(then)
		assert.LessOrEqual(t, int(delay), int(timeout))
		assertRequestEqualBatches(t, r.r, b)
	}
}

func TestClientStop(t *testing.T) {
	transport := &mockTransport{
		statusCode: 429,
		headers: map[string]string{
			"Retry-After": "100",
		},
	}
	c, err := New(
		defaultTargetUrls,
		WithHTTPClient(newMockHTTPClient(transport)),
	)
	require.NoError(t, err)

	// should take more than 1 second
	batches := &otlptracecol.ExportTraceServiceRequest{}
	err = c.ExportTraces(context.Background(), batches)
	time.Sleep(10 * time.Millisecond)
	assert.NotNil(t, err)

	// if client is stopped, it should ignore pausing and return immediately
	then := time.Now()
	go func() {
		err = c.ExportTraces(context.Background(), batches)
		assert.NotNil(t, err)
	}()
	c.Stop()
	assert.True(t, time.Since(then) < time.Duration(101)*time.Millisecond)
}
