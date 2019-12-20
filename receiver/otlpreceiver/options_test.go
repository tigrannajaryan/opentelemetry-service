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

package otlpreceiver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNoopOption(t *testing.T) {
	plainReceiver := new(Receiver)

	subjectReceiver := new(Receiver)
	opts := []Option{noopOption(0), noopOption(0)}
	for _, opt := range opts {
		opt.withReceiver(subjectReceiver)
	}

	require.Equal(t, plainReceiver, subjectReceiver, "noopOption has side effects")
}
