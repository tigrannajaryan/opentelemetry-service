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

package otlpexporter

import (
	"crypto/x509"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/open-telemetry/opentelemetry-collector/compression"
	compressiongrpc "github.com/open-telemetry/opentelemetry-collector/compression/grpc"
	"github.com/open-telemetry/opentelemetry-collector/config/configgrpc"
	"github.com/open-telemetry/opentelemetry-collector/config/configmodels"
	"github.com/open-telemetry/opentelemetry-collector/exporter"
)

const (
	// The value of "type" key in configuration.
	typeStr = "otlp"
)

// Factory is the factory for OpenCensus exporter.
type Factory struct {
}

// Type gets the type of the Exporter config created by this factory.
func (f *Factory) Type() string {
	return typeStr
}

// CreateDefaultConfig creates the default configuration for exporter.
func (f *Factory) CreateDefaultConfig() configmodels.Exporter {
	return &Config{
		ExporterSettings: configmodels.ExporterSettings{
			TypeVal: typeStr,
			NameVal: typeStr,
		},
		GRPCSettings: configgrpc.GRPCSettings{
			Headers: map[string]string{},
		},
	}
}

// CreateTraceExporter creates a trace exporter based on this config.
func (f *Factory) CreateTraceExporter(logger *zap.Logger, config configmodels.Exporter) (exporter.TraceExporter, error) {
	ocac := config.(*Config)
	opts, err := f.OCAgentOptions(logger, ocac)
	if err != nil {
		return nil, err
	}
	return NewTraceExporter(logger, config, opts...)
}

// OCAgentOptions takes the oc exporter Config and generates ocagent Options
func (f *Factory) OCAgentOptions(logger *zap.Logger, ocac *Config) ([]ClientOption, error) {
	if ocac.Endpoint == "" {
		return nil, &ocExporterError{
			code: errEndpointRequired,
			msg:  "OpenCensus exporter config requires an Endpoint",
		}
	}
	opts := []ClientOption{WithAddress(ocac.Endpoint)}
	if ocac.Compression != "" {
		if compressionKey := compressiongrpc.GetGRPCCompressionKey(ocac.Compression); compressionKey != compression.Unsupported {
			opts = append(opts, UseCompressor(compressionKey))
		} else {
			return nil, &ocExporterError{
				code: errUnsupportedCompressionType,
				msg:  fmt.Sprintf("OpenCensus exporter unsupported compression type %q", ocac.Compression),
			}
		}
	}
	if ocac.CertPemFile != "" {
		creds, err := credentials.NewClientTLSFromFile(ocac.CertPemFile, "")
		if err != nil {
			return nil, &ocExporterError{
				code: errUnableToGetTLSCreds,
				msg:  fmt.Sprintf("OpenCensus exporter unable to read TLS credentials from pem file %q: %v", ocac.CertPemFile, err),
			}
		}
		opts = append(opts, WithTLSCredentials(creds))
	} else if ocac.UseSecure {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, &ocExporterError{
				code: errUnableToGetTLSCreds,
				msg: fmt.Sprintf(
					"OpenCensus exporter unable to read certificates from system pool: %v", err),
			}
		}
		creds := credentials.NewClientTLSFromCert(certPool, "")
		opts = append(opts, WithTLSCredentials(creds))
	} else {
		opts = append(opts, WithInsecure())
	}
	if len(ocac.Headers) > 0 {
		opts = append(opts, WithHeaders(ocac.Headers))
	}
	if ocac.ReconnectionDelay > 0 {
		opts = append(opts, WithReconnectionPeriod(ocac.ReconnectionDelay))
	}
	if ocac.KeepaliveParameters != nil {
		opts = append(opts, WithGRPCDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                ocac.KeepaliveParameters.Time,
			Timeout:             ocac.KeepaliveParameters.Timeout,
			PermitWithoutStream: ocac.KeepaliveParameters.PermitWithoutStream,
		})))
	}
	return opts, nil
}

// CreateMetricsExporter creates a metrics exporter based on this config.
func (f *Factory) CreateMetricsExporter(logger *zap.Logger, config configmodels.Exporter) (exporter.MetricsExporter, error) {
	oCfg := config.(*Config)
	opts, err := f.OCAgentOptions(logger, oCfg)
	if err != nil {
		return nil, err
	}
	return NewMetricsExporter(logger, config, opts...)
}
