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

package component

// Component is either a receiver, exporter, processor or extension.
type Component interface {
	// Shutdown is invoked during service shutdown.
	Shutdown() error
}

type Pipeline interface {
	// Get components of specified kind and type from a pipeline. Only enabled components
	// are returned. Typically used by component to find another components with which
	// they want to interact. Used together with Host.FindPipelines allows components,
	// which are designed to work tightly together to discover each other.
	GetComponents(pipeline Pipeline, kind Kind, typeStr string) []Component
}

type Kind int

const (
	_ Kind = iota // skip 0
	ReceiverKind
	ProcessorKind
	ExporterKind
	ExtensionKind
)

type Host interface {
	// ReportFatalError is used to report to the host that the extension
	// encountered a fatal error (i.e.: an error that the instance can't recover
	// from) after its start function had already returned.
	ReportFatalError(err error)

	// Find a Pipeline to which the specified component belongs. The component must
	// be part of the pipeline and must be enabled. Typically used for components
	// to find out the pipelines to which they are attached to.
	// FindPipelines(component Component) []Pipeline
}
