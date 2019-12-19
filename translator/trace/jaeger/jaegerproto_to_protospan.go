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

package jaeger

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	otlpagenttrace "github.com/open-telemetry/opentelemetry-proto/gen/go/agent/traces/v1"
	otlpcommon "github.com/open-telemetry/opentelemetry-proto/gen/go/common/v1"
	otlpresource "github.com/open-telemetry/opentelemetry-proto/gen/go/resource/v1"
	otlptrace "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"

	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/internal"
	"github.com/open-telemetry/opentelemetry-collector/translator/conventions"
	tracetranslator "github.com/open-telemetry/opentelemetry-collector/translator/trace"
)

// ProtoBatchToOCProto converts a single Jaeger Proto batch of spans to a OC proto batch.
func ProtoBatchToOCProto(batch model.Batch) (consumerdata.TraceData, error) {
	ocbatch := consumerdata.TraceData{
		Node:  jProtoProcessToOCProtoNode(batch.GetProcess()),
		Spans: jProtoSpansToOCProtoSpans(batch.GetSpans()),
	}

	return ocbatch, nil
}

// ProtoBatchToOTLP converts a single Jaeger Proto batch of spans to a OC proto batch.
func ProtoBatchToOTLP(batch model.Batch) (consumerdata.OTLPTrace, error) {
	ocbatch := consumerdata.OTLPTrace{
		ResourceSpanList: []*otlpagenttrace.ResourceSpans{
			{
				Resource: jProtoProcessToOTLPResource(batch.GetProcess()),
				Spans:    jProtoSpansToOTLPSpans(batch.GetSpans()),
			},
		},
	}

	return ocbatch, nil
}

func jProtoProcessToOCProtoNode(p *model.Process) *commonpb.Node {
	if p == nil {
		return nil
	}

	node := &commonpb.Node{
		Identifier:  &commonpb.ProcessIdentifier{},
		LibraryInfo: &commonpb.LibraryInfo{},
		ServiceInfo: &commonpb.ServiceInfo{Name: p.GetServiceName()},
	}

	pTags := p.GetTags()
	attribs := make(map[string]string)

	for _, tag := range pTags {
		// Special treatment for special keys in the tags.
		switch tag.GetKey() {
		case "hostname":
			node.Identifier.HostName = tag.GetVStr()
			continue
		case "jaeger.version":
			node.LibraryInfo.ExporterVersion = "Jaeger-" + tag.GetVStr()
			continue
		}

		switch tag.GetVType() {
		case model.ValueType_STRING:
			attribs[tag.Key] = tag.GetVStr()
		case model.ValueType_BOOL:
			attribs[tag.Key] = strconv.FormatBool(tag.GetVBool())
		case model.ValueType_INT64:
			attribs[tag.Key] = strconv.FormatInt(tag.GetVInt64(), 10)
		case model.ValueType_FLOAT64:
			attribs[tag.Key] = strconv.FormatFloat(tag.GetVFloat64(), 'f', -1, 64)
		case model.ValueType_BINARY:
			attribs[tag.Key] = base64.StdEncoding.EncodeToString(tag.GetVBinary())
		default:
			attribs[tag.Key] = fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType())
		}
	}

	if len(attribs) > 0 {
		node.Attributes = attribs
	}
	return node
}

var blankJaegerProtoSpan = new(jaeger.Span)

func jProtoSpansToOCProtoSpans(jspans []*model.Span) []*tracepb.Span {
	spans := make([]*tracepb.Span, 0, len(jspans))
	for _, jspan := range jspans {
		if jspan == nil || reflect.DeepEqual(jspan, blankJaegerProtoSpan) {
			continue
		}

		_, sKind, sStatus, sAttributes := jProtoTagsToAttributes(jspan.Tags)
		span := &tracepb.Span{
			TraceId: tracetranslator.UInt64ToByteTraceID(jspan.TraceID.High, jspan.TraceID.Low),
			SpanId:  tracetranslator.UInt64ToByteSpanID(uint64(jspan.SpanID)),
			// TODO: Tracestate: Check RFC status and if is applicable,
			ParentSpanId: tracetranslator.UInt64ToByteSpanID(uint64(jspan.ParentSpanID())),
			Name:         strToTruncatableString(jspan.OperationName),
			Kind:         sKind,
			StartTime:    internal.TimeToTimestamp(jspan.StartTime),
			EndTime:      internal.TimeToTimestamp(jspan.StartTime.Add(jspan.Duration)),
			Attributes:   sAttributes,
			// TODO: StackTrace: OpenTracing defines a semantic key for "stack", should we attempt to its content to StackTrace?
			TimeEvents: jProtoLogsToOCProtoTimeEvents(jspan.Logs),
			Links:      jProtoReferencesToOCProtoLinks(jspan.References),
			Status:     sStatus,
		}

		spans = append(spans, span)
	}
	return spans
}

func jProtoLogsToOCProtoTimeEvents(logs []model.Log) *tracepb.Span_TimeEvents {
	if logs == nil {
		return nil
	}

	timeEvents := make([]*tracepb.Span_TimeEvent, 0, len(logs))

	for _, log := range logs {
		description, _, _, attribs := jProtoTagsToAttributes(log.Fields)
		var annotation *tracepb.Span_TimeEvent_Annotation
		if attribs != nil {
			annotation = &tracepb.Span_TimeEvent_Annotation{
				Description: strToTruncatableString(description),
				Attributes:  attribs,
			}
		}
		timeEvent := &tracepb.Span_TimeEvent{
			Time:  internal.TimeToTimestamp(log.Timestamp),
			Value: &tracepb.Span_TimeEvent_Annotation_{Annotation: annotation},
		}

		timeEvents = append(timeEvents, timeEvent)
	}

	return &tracepb.Span_TimeEvents{TimeEvent: timeEvents}
}

func jProtoReferencesToOCProtoLinks(jrefs []model.SpanRef) *tracepb.Span_Links {
	if jrefs == nil {
		return nil
	}

	links := make([]*tracepb.Span_Link, 0, len(jrefs))

	for _, jref := range jrefs {
		var linkType tracepb.Span_Link_Type
		if jref.RefType == model.SpanRefType_CHILD_OF {
			// Wording on OC for Span_Link_PARENT_LINKED_SPAN: The linked span is a parent of the current span.
			linkType = tracepb.Span_Link_PARENT_LINKED_SPAN
		} else {
			// TODO: SpanRefType_FOLLOWS_FROM doesn't map well to OC, so treat all other cases as unknown
			linkType = tracepb.Span_Link_TYPE_UNSPECIFIED
		}

		link := &tracepb.Span_Link{
			TraceId: tracetranslator.UInt64ToByteTraceID(jref.TraceID.High, jref.TraceID.Low),
			SpanId:  tracetranslator.UInt64ToByteSpanID(uint64(jref.SpanID)),
			Type:    linkType,
		}
		links = append(links, link)
	}

	return &tracepb.Span_Links{Link: links}
}

func jProtoTagsToAttributes(tags []model.KeyValue) (string, tracepb.Span_SpanKind, *tracepb.Status, *tracepb.Span_Attributes) {
	if tags == nil {
		return "", tracepb.Span_SPAN_KIND_UNSPECIFIED, nil, nil
	}

	var sKind tracepb.Span_SpanKind

	var statusCodePtr *int32
	var statusMessage string
	var httpStatusCodePtr *int32
	var httpStatusMessage string
	var message string

	sAttribs := make(map[string]*tracepb.AttributeValue)

	for _, tag := range tags {
		// Take the opportunity to get the "span.kind" per OpenTracing spec, however, keep it also on the attributes.
		// TODO: (@pjanotti): Replace any OpenTracing literals by importing github.com/opentracing/opentracing-go/ext?
		switch tag.Key {
		case "span.kind":
			switch tag.GetVStr() {
			case "client":
				sKind = tracepb.Span_CLIENT
			case "server":
				sKind = tracepb.Span_SERVER
			}

		case tracetranslator.TagStatusCode:
			statusCodePtr = statusCodeFromProtoTag(tag)
			continue

		case tracetranslator.TagStatusMsg:
			statusMessage = tag.GetVStr()
			continue

		case tracetranslator.TagHTTPStatusCode:
			httpStatusCodePtr = statusCodeFromProtoTag(tag)

		case tracetranslator.TagHTTPStatusMsg:
			httpStatusMessage = tag.GetVStr()

		case "message":
			message = tag.GetVStr()
		}

		attrib := &tracepb.AttributeValue{}
		switch tag.GetVType() {
		case model.ValueType_STRING:
			attrib.Value = &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: tag.GetVStr()},
			}
		case model.ValueType_BOOL:
			attrib.Value = &tracepb.AttributeValue_BoolValue{
				BoolValue: tag.GetVBool(),
			}
		case model.ValueType_INT64:
			attrib.Value = &tracepb.AttributeValue_IntValue{
				IntValue: tag.GetVInt64(),
			}
		case model.ValueType_FLOAT64:
			attrib.Value = &tracepb.AttributeValue_DoubleValue{
				DoubleValue: tag.GetVFloat64(),
			}
		case model.ValueType_BINARY:
			attrib.Value = &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: base64.StdEncoding.EncodeToString(tag.GetVBinary())},
			}
		default:
			attrib.Value = &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{Value: fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType())},
			}
		}

		sAttribs[tag.Key] = attrib
	}

	if statusCodePtr == nil {
		statusCodePtr = httpStatusCodePtr
		statusMessage = httpStatusMessage
	}

	var sStatus *tracepb.Status
	if statusCodePtr != nil || statusMessage != "" {
		statusCode := int32(0)
		if statusCodePtr != nil {
			statusCode = *statusCodePtr
		}
		sStatus = &tracepb.Status{Message: statusMessage, Code: statusCode}
	}

	var sAttributes *tracepb.Span_Attributes
	if len(sAttribs) > 0 {
		sAttributes = &tracepb.Span_Attributes{AttributeMap: sAttribs}
	}
	return message, sKind, sStatus, sAttributes
}

func jProtoProcessToOTLPResource(p *model.Process) *otlpresource.Resource {
	if p == nil {
		return nil
	}

	pTags := p.GetTags()
	attribs := make([]*otlpcommon.AttributeKeyValue, 0, len(pTags))

	if p.GetServiceName() != "" {
		attrib := &otlpcommon.AttributeKeyValue{
			Key:         conventions.AttributeServiceName,
			StringValue: p.GetServiceName(),
			Type:        otlpcommon.AttributeKeyValue_STRING,
		}
		attribs = append(attribs, attrib)
	}

	for _, tag := range pTags {
		var attrib *otlpcommon.AttributeKeyValue
		// Special treatment for special keys in the tags.
		switch tag.GetKey() {
		case "hostname":
			attrib = &otlpcommon.AttributeKeyValue{
				Key:         conventions.AttributeHostHostname,
				StringValue: tag.GetVStr(),
				Type:        otlpcommon.AttributeKeyValue_STRING,
			}
			continue
		case "jaeger.version":
			attrib = &otlpcommon.AttributeKeyValue{
				Key:         conventions.AttributeLibraryVersion,
				StringValue: "Jaeger-" + tag.GetVStr(),
				Type:        otlpcommon.AttributeKeyValue_STRING,
			}
			continue
		default:
			t := tag
			attrib = jPrototagsToOTLPAttribute(&t)
		}

		attribs = append(attribs, attrib)
	}

	return &otlpresource.Resource{
		Attributes: attribs,
	}
}

func jProtoSpansToOTLPSpans(jspans []*model.Span) []*otlptrace.Span {
	spans := make([]*otlptrace.Span, 0, len(jspans))
	for _, jspan := range jspans {
		if jspan == nil || reflect.DeepEqual(jspan, blankJaegerSpan) {
			continue
		}

		startTime := jspan.StartTime
		_, sKind, sStatus, sAttributes := jPrototagsToOTLPAttributes(jspan.Tags)
		span := &otlptrace.Span{
			TraceId: tracetranslator.UInt64ToByteTraceID(jspan.TraceID.High, jspan.TraceID.Low),
			SpanId:  tracetranslator.UInt64ToByteSpanID(uint64(jspan.SpanID)),
			// TODO: Tracestate: Check RFC status and if is applicable,
			ParentSpanId:      tracetranslator.UInt64ToByteSpanID(uint64(jspan.ParentSpanID())),
			Name:              jspan.OperationName,
			Kind:              sKind,
			StartTimeUnixnano: internal.TimeToUnixNano(startTime),
			EndTimeUnixnano:   internal.TimeToUnixNano(startTime.Add(time.Duration(jspan.Duration) * time.Microsecond)),
			Attributes:        sAttributes,
			// TODO: StackTrace: OpenTracing defines a semantic key for "stack", should we attempt to its content to StackTrace?
			Events: jProtoLogsToOTLPTimeEvents(jspan.Logs),
			Links:  jProtoReferencesToOTLPLinks(jspan.References),
			Status: sStatus,
		}

		spans = append(spans, span)
	}
	return spans
}

func jProtoLogsToOTLPTimeEvents(logs []model.Log) []*otlptrace.Span_Event {
	if logs == nil {
		return nil
	}

	timeEvents := make([]*otlptrace.Span_Event, 0, len(logs))

	for _, log := range logs {
		description, _, _, attribs := jPrototagsToOTLPAttributes(log.Fields)
		timeEvent := &otlptrace.Span_Event{
			TimeUnixnano: internal.TimeToUnixNano(log.Timestamp),
			Description:  description,
			Attributes:   attribs,
		}

		timeEvents = append(timeEvents, timeEvent)
	}

	return timeEvents
}

func jProtoReferencesToOTLPLinks(jrefs []model.SpanRef) []*otlptrace.Span_Link {
	if jrefs == nil {
		return nil
	}

	links := make([]*otlptrace.Span_Link, 0, len(jrefs))

	for _, jref := range jrefs {
		//var linkType otlptrace.Span_Link_Type
		//if jref.RefType == jaeger.SpanRefType_CHILD_OF {
		//	// Wording on OC for Span_Link_PARENT_LINKED_SPAN: The linked span is a parent of the current span.
		//	linkType = otlptrace.Span_Link_PARENT_LINKED_SPAN
		//} else {
		//	// TODO: SpanRefType_FOLLOWS_FROM doesn't map well to OC, so treat all other cases as unknown
		//	linkType = otlptrace.Span_Link_TYPE_UNSPECIFIED
		//}

		link := &otlptrace.Span_Link{
			TraceId: tracetranslator.UInt64ToByteTraceID(jref.TraceID.High, jref.TraceID.Low),
			SpanId:  tracetranslator.UInt64ToByteSpanID(uint64(jref.SpanID)),
			//Type:    linkType,
		}
		links = append(links, link)
	}

	return links
}

func jPrototagsToOTLPAttribute(tag *model.KeyValue) *otlpcommon.AttributeKeyValue {
	attrib := &otlpcommon.AttributeKeyValue{
		Key: tag.Key,
	}
	switch tag.GetVType() {
	case model.ValueType_STRING:
		attrib.StringValue = tag.GetVStr()
		attrib.Type = otlpcommon.AttributeKeyValue_STRING
	case model.ValueType_FLOAT64:
		attrib.DoubleValue = tag.GetVFloat64()
		attrib.Type = otlpcommon.AttributeKeyValue_DOUBLE
	case model.ValueType_BOOL:
		attrib.BoolValue = tag.GetVBool()
		attrib.Type = otlpcommon.AttributeKeyValue_BOOL
	case model.ValueType_INT64:
		attrib.IntValue = tag.GetVInt64()
		attrib.Type = otlpcommon.AttributeKeyValue_INT
	case model.ValueType_BINARY:
		attrib.StringValue = base64.StdEncoding.EncodeToString(tag.GetVBinary())
		attrib.Type = otlpcommon.AttributeKeyValue_STRING
	default:
		attrib.StringValue = fmt.Sprintf("<Unknown Jaeger TagType %q>", tag.GetVType())
		attrib.Type = otlpcommon.AttributeKeyValue_STRING
	}
	return attrib
}

func jPrototagsToOTLPAttributes(tags []model.KeyValue) (string, otlptrace.Span_SpanKind, *otlptrace.Status, []*otlpcommon.AttributeKeyValue) {
	if tags == nil {
		return "", otlptrace.Span_SPAN_KIND_UNSPECIFIED, nil, nil
	}

	var sKind otlptrace.Span_SpanKind

	var statusCodePtr *int32
	var statusMessage string
	var httpStatusCodePtr *int32
	var httpStatusMessage string
	var message string

	sAttribs := make([]*otlpcommon.AttributeKeyValue, 0, len(tags))

	for _, tag := range tags {
		// Take the opportunity to get the "span.kind" per OpenTracing spec, however, keep it also on the attributes.
		switch tag.Key {
		case tracetranslator.TagSpanKind:
			switch tag.GetVStr() {
			case "client":
				sKind = otlptrace.Span_CLIENT
			case "server":
				sKind = otlptrace.Span_SERVER
			}

		case tracetranslator.TagStatusCode:
			statusCodePtr = statusCodeFromProtoTag(tag)
			continue

		case tracetranslator.TagStatusMsg:
			statusMessage = tag.GetVStr()
			continue

		case tracetranslator.TagHTTPStatusCode:
			httpStatusCodePtr = statusCodeFromProtoTag(tag)

		case tracetranslator.TagHTTPStatusMsg:
			httpStatusMessage = tag.GetVStr()

		case "message":
			message = tag.GetVStr()
		}

		t := tag
		attrib := jPrototagsToOTLPAttribute(&t)
		sAttribs = append(sAttribs, attrib)
	}

	if statusCodePtr == nil {
		statusCodePtr = httpStatusCodePtr
		statusMessage = httpStatusMessage
	}

	var sStatus *otlptrace.Status
	if statusCodePtr != nil || statusMessage != "" {
		statusCode := int32(0)
		if statusCodePtr != nil {
			statusCode = *statusCodePtr
		}
		sStatus = &otlptrace.Status{Message: statusMessage, Code: otlptrace.Status_StatusCode(statusCode)}
	}

	return message, sKind, sStatus, sAttribs
}
