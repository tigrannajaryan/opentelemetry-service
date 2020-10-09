// Copyright The OpenTelemetry Authors
//
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
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	otlptracecol "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/collector/trace/v1"
	otlpcommon "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/common/v1"
	otlpresource "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/resource/v1"
	otlptrace "go.opentelemetry.io/collector/internal/data/opentelemetry-proto-gen/trace/v1"
)

var (
	testRequest = &otlptracecol.ExportTraceServiceRequest{
		ResourceSpans: []*otlptrace.ResourceSpans{
			{
				Resource: &otlpresource.Resource{
					Attributes: []*otlpcommon.KeyValue{
						{
							Key: "service.name",
							Value: &otlpcommon.AnyValue{
								Value: &otlpcommon.AnyValue_StringValue{
									StringValue: "Service A",
								},
							},
						},
					},
				},
				InstrumentationLibrarySpans: []*otlptrace.InstrumentationLibrarySpans{
					{
						Spans: []*otlptrace.Span{
							{
								TraceId: otlpcommon.NewTraceID([]byte{1, 1}),
								SpanId:  otlpcommon.NewSpanID([]byte{1}),
								Name:    "op1",
							},
							{
								TraceId: otlpcommon.NewTraceID([]byte{2, 2}),
								SpanId:  otlpcommon.NewSpanID([]byte{2}),
								Name:    "op2",
							},
						},
					},
				},
			},
			{
				Resource: &otlpresource.Resource{
					Attributes: []*otlpcommon.KeyValue{
						{
							Key: "service.name",
							Value: &otlpcommon.AnyValue{
								Value: &otlpcommon.AnyValue_StringValue{
									StringValue: "Service B",
								},
							},
						},
					},
				},
				InstrumentationLibrarySpans: []*otlptrace.InstrumentationLibrarySpans{
					{
						Spans: []*otlptrace.Span{
							{
								TraceId: otlpcommon.NewTraceID([]byte{3, 3}),
								SpanId:  otlpcommon.NewSpanID([]byte{3}),
								Name:    "op3",
							},
							{
								TraceId: otlpcommon.NewTraceID([]byte{4, 4}),
								SpanId:  otlpcommon.NewSpanID([]byte{4}),
								Name:    "op4",
							},
						},
					},
				},
			},
		},
	}
	testBatchesCount = 2
	testSpansCount   = 4
)

func newTestWorker(c *http.Client) *worker {
	return newWorker(c, false)
}

func TestPrepare(t *testing.T) {
	w := newTestWorker(newMockHTTPClient(&mockTransport{}))
	sr, err := w.prepare(context.Background(), testRequest, testBatchesCount, testSpansCount)
	assert.NoError(t, err)

	assert.Equal(t, testBatchesCount, int(sr.batches))
	assert.Equal(t, int64(testSpansCount), sr.items)

	// cannot unmarshal compressed message
	err = proto.Unmarshal(sr.message, &otlptracecol.ExportTraceServiceRequest{})
	require.Error(t, err)

	gz, err := gzip.NewReader(bytes.NewReader(sr.message))
	require.NoError(t, err)
	defer gz.Close()

	contents, err := ioutil.ReadAll(gz)
	require.NoError(t, err)

	psr := &otlptracecol.ExportTraceServiceRequest{}
	err = proto.Unmarshal(contents, psr)
	require.NoError(t, err)

	require.Len(t, psr.ResourceSpans, testBatchesCount)

	require.EqualValues(t, testRequest, psr)
}

func TestPrepareNoCompression(t *testing.T) {
	w := newWorker(newMockHTTPClient(&mockTransport{}), true)
	sr, err := w.prepare(context.Background(), testRequest, testBatchesCount, testSpansCount)
	assert.NoError(t, err)

	assert.Equal(t, testBatchesCount, int(sr.batches))
	assert.Equal(t, int64(testSpansCount), sr.items)

	psr := &otlptracecol.ExportTraceServiceRequest{}
	err = proto.Unmarshal(sr.message, psr)
	require.NoError(t, err)

	require.Len(t, psr.ResourceSpans, testBatchesCount)

	require.EqualValues(t, testRequest, psr)
}

func TestWorkerSend(t *testing.T) {
	transport := &mockTransport{}
	w := newTestWorker(newMockHTTPClient(transport))

	ctx := context.Background()
	sr, err := w.prepare(ctx, testRequest, testBatchesCount, testSpansCount)
	require.NoError(t, err)

	err = w.send(ctx, sr, "")
	require.Nil(t, err)

	received := transport.requests()
	require.Len(t, received, 1)

	r := received[0].r
	assert.Equal(t, r.Method, "POST")
	assert.Equal(t, r.Header.Get(headerContentEncoding), headerValueGZIP)
	assert.Equal(t, r.Header.Get(headerContentType), headerValueXProtobuf)
}

func TestWorkerSendNoCompression(t *testing.T) {
	transport := &mockTransport{}
	w := newWorker(newMockHTTPClient(transport), true)

	ctx := context.Background()
	sr, err := w.prepare(ctx, testRequest, testBatchesCount, testSpansCount)
	require.NoError(t, err)

	err = w.send(ctx, sr, "")
	require.Nil(t, err)

	received := transport.requests()
	require.Len(t, received, 1)

	r := received[0].r
	assert.Equal(t, r.Method, "POST")
	assert.Equal(t, r.Header.Get(headerContentEncoding), "")
	assert.Equal(t, r.Header.Get(headerContentType), headerValueXProtobuf)
}

func TestWorkerSendErrors(t *testing.T) {
	transport := &mockTransport{statusCode: 400}
	w := newTestWorker(newMockHTTPClient(transport))

	ctx := context.Background()
	sr, err := w.prepare(ctx, testRequest, testBatchesCount, testSpansCount)
	require.NoError(t, err)

	sendErr := w.send(ctx, sr, "")
	require.NotNil(t, sendErr)
	assert.Equal(t, 400, sendErr.StatusCode)
	assert.True(t, sendErr.Permanent)
	assert.Equal(t, 0, sendErr.RetryDelaySeconds)

	transport.reset(500)
	sendErr = w.send(ctx, sr, "")
	require.NotNil(t, sendErr)
	assert.Equal(t, 500, sendErr.StatusCode)
	assert.False(t, sendErr.Permanent)
	assert.Equal(t, 0, sendErr.RetryDelaySeconds)

	transport.reset(429)
	sendErr = w.send(ctx, sr, "")
	require.NotNil(t, sendErr)
	assert.Equal(t, 429, sendErr.StatusCode)
	assert.False(t, sendErr.Permanent)
	assert.Equal(t, defaultRateLimitingBackoffSeconds, sendErr.RetryDelaySeconds)

	transport.reset(429)
	transport.headers = map[string]string{headerRetryAfter: "100"}
	sendErr = w.send(ctx, sr, "")
	require.NotNil(t, sendErr)
	assert.Equal(t, 429, sendErr.StatusCode)
	assert.False(t, sendErr.Permanent)
	assert.Equal(t, 100, sendErr.RetryDelaySeconds)

	transport.reset(200)
	transport.err = errors.New("test error")
	sendErr = w.send(ctx, sr, "")
	require.NotNil(t, sendErr)
	assert.Contains(t, sendErr.Error(), "test error")
	assert.Equal(t, 0, sendErr.StatusCode)
	assert.False(t, sendErr.Permanent)
	assert.Equal(t, 0, sendErr.RetryDelaySeconds)
}
