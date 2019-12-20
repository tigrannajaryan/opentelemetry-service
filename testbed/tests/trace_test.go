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

// Package tests contains test cases. To run the tests go to tests directory and run:
// TESTBED_CONFIG=local.yaml go test -v

package tests

// This file contains Test functions which initiate the tests. The tests can be either
// coded in this file or use scenarios from perf_scenarios.go.

import (
	"path"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector/testbed/testbed"
)

// TestMain is used to initiate setup, execution and tear down of testbed.
func TestMain(m *testing.M) {
	testbed.DoTestMain(m)
}

func TestTrace10kSPS(t *testing.T) {
	tests := []struct {
		name         string
		sender       testbed.DataSender
		receiver     testbed.DataReceiver
		resourceSpec testbed.ResourceSpec
	}{
		{
			"JaegerGrpc",
			testbed.NewJaegerGrpcDataSender(testbed.GetAvailablePort(t)),
			testbed.NewJaegerGrpcDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 152,
				ExpectedMaxRAM: 1189,
			},
		},
		{
			"JaegerThrift",
			testbed.NewJaegerThriftDataSender(testbed.GetAvailablePort(t)),
			testbed.NewJaegerDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 52,
				ExpectedMaxRAM: 1189,
			},
		},
		{
			"OpenCensus",
			testbed.NewOCTraceDataSender(testbed.GetAvailablePort(t)),
			testbed.NewOCDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 42,
				ExpectedMaxRAM: 1184,
			},
		},
		{
			"OTLP",
			testbed.NewOTLPTraceDataSender(testbed.GetAvailablePort(t)),
			testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 42,
				ExpectedMaxRAM: 1184,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			Scenario10kItemsPerSecond(
				t,
				test.sender,
				test.receiver,
				testbed.LoadOptions{ItemsPerBatch: 100},
				test.resourceSpec,
			)
		})
	}
}

func TestTraceNoBackend10kSPSJaeger(t *testing.T) {
	tc := testbed.NewTestCase(
		t,
		testbed.NewJaegerThriftDataSender(testbed.DefaultJaegerPort),
		testbed.NewOCDataReceiver(testbed.DefaultOCPort),
	)
	defer tc.Stop()

	tc.SetResourceLimits(testbed.ResourceSpec{
		ExpectedMaxCPU: 60,
		ExpectedMaxRAM: 198,
	})

	tc.StartAgent()
	tc.StartLoad(testbed.LoadOptions{DataItemsPerSecond: 10000})

	tc.Sleep(tc.Duration)
}

func TestTrace1kSPSWithAttrs(t *testing.T) {
	Scenario1kSPSWithAttrs(t, []string{}, []TestCase{
		// No attributes.
		{
			attrCount:      0,
			attrSizeByte:   0,
			expectedMaxCPU: 30,
			expectedMaxRAM: 100,
		},

		// We generate 10 attributes each with average key length of 100 bytes and
		// average value length of 50 bytes so total size of attributes values is
		// 15000 bytes.
		{
			attrCount:      100,
			attrSizeByte:   50,
			expectedMaxCPU: 120,
			expectedMaxRAM: 100,
		},

		// Approx 10 KiB attributes.
		{
			attrCount:      10,
			attrSizeByte:   1000,
			expectedMaxCPU: 100,
			expectedMaxRAM: 100,
		},

		// Approx 100 KiB attributes.
		{
			attrCount:      20,
			attrSizeByte:   5000,
			expectedMaxCPU: 250,
			expectedMaxRAM: 100,
		},
	})
}

func TestTraceBallast1kSPSWithAttrs(t *testing.T) {
	args := []string{"--mem-ballast-size-mib", "1000"}
	Scenario1kSPSWithAttrs(t, args, []TestCase{
		// No attributes.
		{
			attrCount:      0,
			attrSizeByte:   0,
			expectedMaxCPU: 30,
			expectedMaxRAM: 2000,
		},
		{
			attrCount:      100,
			attrSizeByte:   50,
			expectedMaxCPU: 80,
			expectedMaxRAM: 2000,
		},
		{
			attrCount:      10,
			attrSizeByte:   1000,
			expectedMaxCPU: 80,
			expectedMaxRAM: 2000,
		},
		{
			attrCount:      20,
			attrSizeByte:   5000,
			expectedMaxCPU: 120,
			expectedMaxRAM: 2000,
		},
	})
}

func TestTraceBallast1kSPSAddAttrs(t *testing.T) {
	args := []string{"--mem-ballast-size-mib", "1000"}
	Scenario1kSPSWithAttrs(
		t,
		args,
		[]TestCase{
			{
				attrCount:      0,
				attrSizeByte:   0,
				expectedMaxCPU: 30,
				expectedMaxRAM: 2000,
			},
			{
				attrCount:      100,
				attrSizeByte:   50,
				expectedMaxCPU: 80,
				expectedMaxRAM: 2000,
			},
			{
				attrCount:      10,
				attrSizeByte:   1000,
				expectedMaxCPU: 80,
				expectedMaxRAM: 2000,
			},
			{
				attrCount:      20,
				attrSizeByte:   5000,
				expectedMaxCPU: 120,
				expectedMaxRAM: 2000,
			},
		},
		testbed.WithConfigFile(path.Join("testdata", "add-attributes-config.yaml")),
	)
}
