// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlphttpexporter

import (
	"errors"
	"net/url"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter/httpclient"
)

// Config defines configuration for OTLP/HTTP exporter.
type Config struct {
	configmodels.ExporterSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	confighttp.HTTPClientSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings  `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings  `mapstructure:"retry_on_failure"`

	// NumWorkers is the number of workers that should be used to export traces.
	// Exporter can make as many requests in parallel as the number of workers. Defaults to 8.
	NumWorkers uint `mapstructure:"num_workers"`

	// MaxConnections is used to set a limit to the maximum idle HTTP connection the exporter can keep open.
	MaxConnections uint `mapstructure:"max_connections"`

	// Disable GZip compression.
	DisableCompression bool `mapstructure:"disable_compression"`
}

const (
	defaultEndpointScheme = "https"
	defaultNumWorkers     = 8
)

func (c *Config) validate() error {
	if c.Endpoint == "" {
		return errors.New("`endpoint` not specified")
	}

	e, err := url.Parse(c.Endpoint)
	if err != nil {
		return err
	}

	if e.Scheme == "" {
		e.Scheme = defaultEndpointScheme
	}
	c.Endpoint = e.String()
	return nil
}

func (c *Config) clientOptions() []httpclient.Option {
	opts := []httpclient.Option{}
	if c.NumWorkers > 0 {
		opts = append(opts, httpclient.WithWorkers(c.NumWorkers))
	}

	if c.MaxConnections > 0 {
		opts = append(opts, httpclient.WithMaxConnections(c.MaxConnections))
	}

	if c.DisableCompression {
		opts = append(opts, httpclient.WithDisabledCompression())
	}

	return opts
}
