// Copyright 2020, OpenTelemetry Authors
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

package jaeger

import (
	"encoding/base64"
	"fmt"
	"reflect"

	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"

	"github.com/open-telemetry/opentelemetry-collector/consumer/pdata"
	"github.com/open-telemetry/opentelemetry-collector/translator/conventions"
	tracetranslator "github.com/open-telemetry/opentelemetry-collector/translator/trace"
)

func ThriftBatchToInternalTraces(batch *jaeger.Batch) pdata.Traces {
	traceData := pdata.NewTraces()
	jProcess := batch.GetProcess()
	jSpans := batch.GetSpans()

	if jProcess == nil && len(jSpans) == 0 {
		return traceData
	}

	rss := traceData.ResourceSpans()
	rss.Resize(1)
	rs := rss.At(0)
	jThriftProcessToInternalResource(jProcess, rs.Resource())

	if len(jSpans) == 0 {
		return traceData
	}

	ilss := rs.InstrumentationLibrarySpans()
	ilss.Resize(1)
	jThriftSpansToInternal(jSpans, ilss.At(0).Spans())

	return traceData
}

func jThriftProcessToInternalResource(process *jaeger.Process, dest pdata.Resource) {
	if process == nil {
		return
	}

	dest.InitEmpty()

	serviceName := process.GetServiceName()
	tags := process.GetTags()
	if serviceName == "" && tags == nil {
		return
	}

	attrs := dest.Attributes()

	if serviceName != "" {
		attrs.UpsertString(conventions.AttributeServiceName, serviceName)
	}

	for _, tag := range tags {
		switch tag.GetVType() {
		case jaeger.TagType_STRING:
			attrs.UpsertString(tag.Key, tag.GetVStr())
		case jaeger.TagType_BOOL:
			attrs.UpsertBool(tag.Key, tag.GetVBool())
		case jaeger.TagType_LONG:
			attrs.UpsertInt(tag.Key, tag.GetVLong())
		case jaeger.TagType_DOUBLE:
			attrs.UpsertDouble(tag.Key, tag.GetVDouble())
		case jaeger.TagType_BINARY:
			attrs.UpsertString(tag.Key, base64.StdEncoding.EncodeToString(tag.GetVBinary()))
		default:
			attrs.UpsertString(tag.Key, fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType()))
		}
	}

	// Handle special keys translations.
	translateHostnameAttr(attrs)
	translateJaegerVersionAttr(attrs)
}

func jThriftSpansToInternal(spans []*jaeger.Span, dest pdata.SpanSlice) {
	if len(spans) == 0 {
		return
	}

	dest.Resize(len(spans))
	i := 0
	for _, span := range spans {
		if span == nil || reflect.DeepEqual(span, blankJaegerProtoSpan) {
			continue
		}
		jThriftSpanToInternal(span, dest.At(i))
		i++
	}

	if i < len(spans) {
		dest.Resize(i)
	}
}

func jThriftSpanToInternal(span *jaeger.Span, dest pdata.Span) {
	dest.SetTraceID(pdata.TraceID(tracetranslator.Int64ToByteTraceID(span.TraceIdHigh, span.TraceIdLow)))
	dest.SetSpanID(pdata.SpanID(tracetranslator.Int64ToByteSpanID(span.SpanId)))
	dest.SetName(span.OperationName)
	dest.SetStartTime(microsecondsToUnixNano(span.StartTime))
	dest.SetEndTime(microsecondsToUnixNano(span.StartTime + span.Duration))

	parentSpanID := span.ParentSpanId
	if parentSpanID != 0 {
		dest.SetParentSpanID(pdata.SpanID(tracetranslator.Int64ToByteSpanID(parentSpanID)))
	}

	attrs := jThriftTagsToInternalAttributes(span.Tags)
	setInternalSpanStatus(attrs, dest.Status())
	if spanKindAttr, ok := attrs[tracetranslator.TagSpanKind]; ok {
		dest.SetKind(jSpanKindToInternal(spanKindAttr.StringVal()))
	}
	dest.Attributes().InitFromMap(attrs)

	jThriftLogsToSpanEvents(span.Logs, dest.Events())
	jThriftReferencesToSpanLinks(span.References, parentSpanID, dest.Links())
}

// jThriftTagsToInternalAttributes sets internal span links based on jaeger span references skipping excludeParentID
func jThriftTagsToInternalAttributes(tags []*jaeger.Tag) map[string]pdata.AttributeValue {
	attrs := make(map[string]pdata.AttributeValue)

	for _, tag := range tags {
		switch tag.GetVType() {
		case jaeger.TagType_STRING:
			attrs[tag.Key] = pdata.NewAttributeValueString(tag.GetVStr())
		case jaeger.TagType_BOOL:
			attrs[tag.Key] = pdata.NewAttributeValueBool(tag.GetVBool())
		case jaeger.TagType_LONG:
			attrs[tag.Key] = pdata.NewAttributeValueInt(tag.GetVLong())
		case jaeger.TagType_DOUBLE:
			attrs[tag.Key] = pdata.NewAttributeValueDouble(tag.GetVDouble())
		case jaeger.TagType_BINARY:
			attrs[tag.Key] = pdata.NewAttributeValueString(base64.StdEncoding.EncodeToString(tag.GetVBinary()))
		default:
			attrs[tag.Key] = pdata.NewAttributeValueString(fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType()))
		}
	}

	return attrs
}

func jThriftLogsToSpanEvents(logs []*jaeger.Log, dest pdata.SpanEventSlice) {
	if len(logs) == 0 {
		return
	}

	dest.Resize(len(logs))

	for i, log := range logs {
		event := dest.At(i)

		event.SetTimestamp(microsecondsToUnixNano(log.Timestamp))
		attrs := jThriftTagsToInternalAttributes(log.Fields)
		if name, ok := attrs["message"]; ok {
			event.SetName(name.StringVal())
		}
		if len(attrs) > 0 {
			event.Attributes().InitFromMap(attrs)
		}
	}
}

func jThriftReferencesToSpanLinks(refs []*jaeger.SpanRef, excludeParentID int64, dest pdata.SpanLinkSlice) {
	if len(refs) == 0 || len(refs) == 1 && refs[0].SpanId == excludeParentID && refs[0].RefType == jaeger.SpanRefType_CHILD_OF {
		return
	}

	dest.Resize(len(refs))
	i := 0
	for _, ref := range refs {
		link := dest.At(i)
		if ref.SpanId == excludeParentID && ref.RefType == jaeger.SpanRefType_CHILD_OF {
			continue
		}

		link.SetTraceID(pdata.NewTraceID(tracetranslator.Int64ToByteTraceID(ref.TraceIdHigh, ref.TraceIdLow)))
		link.SetSpanID(pdata.NewSpanID(tracetranslator.Int64ToByteSpanID(ref.SpanId)))
		i++
	}

	// Reduce slice size in case if excludeParentID was skipped
	if i < len(refs) {
		dest.Resize(i)
	}
}

// microsecondsToUnixNano converts epoch microseconds to pdata.TimestampUnixNano
func microsecondsToUnixNano(ms int64) pdata.TimestampUnixNano {
	return pdata.TimestampUnixNano(uint64(ms) * 1000)
}
