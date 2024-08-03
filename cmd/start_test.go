// Copyright (C) 2024 The Daguflow/Dagu Authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package cmd

import (
	"testing"

	"github.com/daguflow/dagu/internal/test"
)

func TestStartCommand(t *testing.T) {
	setup := test.SetupTest(t)
	defer setup.Cleanup()

	tests := []cmdTest{
		{
			args:        []string{"start", testDAGFile("success.yaml")},
			expectedOut: []string{"1 finished"},
		},
		{
			args:        []string{"start", testDAGFile("params.yaml")},
			expectedOut: []string{"params is p1 and p2"},
		},
		{
			args: []string{
				"start",
				`--params="p3 p4"`,
				testDAGFile("params.yaml"),
			},
			expectedOut: []string{"params is p3 and p4"},
		},
	}

	for _, tc := range tests {
		testRunCommand(t, startCmd(), tc)
	}
}
