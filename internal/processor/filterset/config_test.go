// Copyright 2020 OpenTelemetry Authors
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

package filterset

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/open-telemetry/opentelemetry-collector/internal/processor/filterset/regexp"
	"github.com/open-telemetry/opentelemetry-collector/testutils/configtestutils"
)

func readTestdataConfigYamls(t *testing.T, filename string) map[string]Config {
	testFile := path.Join(".", "testdata", filename)
	v, err := configtestutils.CreateViperYamlUnmarshaler(testFile)
	if err != nil {
		t.Errorf("Error configuring viper: %v", err)
	}

	cfgs := map[string]Config{}
	if err = v.UnmarshalExact(&cfgs); err != nil {
		t.Errorf("Error unmarshaling yaml from test file %v: %v", testFile, err)
	}

	return cfgs
}

func TestConfig(t *testing.T) {
	actualConfigs := readTestdataConfigYamls(t, "config.yaml")
	expectedConfigs := map[string]Config{
		"regexp/default": {
			MatchType: Regexp,
		},
		"regexp/emptyoptions": {
			MatchType: Regexp,
		},
		"regexp/withoptions": {
			MatchType: Regexp,
			RegexpConfig: &regexp.Config{
				CacheEnabled:       false,
				CacheMaxNumEntries: 10,
			},
		},
		"strict/default": {
			MatchType: Strict,
		},
	}

	for testName, actualCfg := range actualConfigs {
		t.Run(testName, func(t *testing.T) {
			expCfg, ok := expectedConfigs[testName]
			assert.True(t, ok)
			assert.Equal(t, expCfg, actualCfg)

			fs, err := CreateFilterSet([]string{}, &actualCfg)
			assert.Nil(t, err)
			assert.NotNil(t, fs)
		})
	}
}

func TestConfigInvalid(t *testing.T) {
	actualConfigs := readTestdataConfigYamls(t, "config_invalid.yaml")
	expectedConfigs := map[string]Config{
		"invalid/matchtype": {
			MatchType: "invalid",
		},
	}

	for testName, actualCfg := range actualConfigs {
		t.Run(testName, func(t *testing.T) {
			expCfg, ok := expectedConfigs[testName]
			assert.True(t, ok)
			assert.Equal(t, expCfg, actualCfg)

			fs, err := CreateFilterSet([]string{}, &actualCfg)
			assert.NotNil(t, err)
			assert.Nil(t, fs)
		})
	}
}
