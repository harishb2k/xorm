// Copyright 2021 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package convert

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestString2Time(t *testing.T) {
	expectedLoc, err := time.LoadLocation("Asia/Shanghai")
	assert.NoError(t, err)

	var kases = map[string]time.Time{
		"2021-06-06T22:58:20+08:00": time.Date(2021, 6, 6, 22, 58, 20, 0, expectedLoc),
		"2021-07-11 10:44:00":       time.Date(2021, 7, 11, 18, 44, 0, 0, expectedLoc),
		"2021-08-10T10:33:04Z":      time.Date(2021, 8, 10, 18, 33, 04, 0, expectedLoc),
	}
	for layout, tm := range kases {
		t.Run(layout, func(t *testing.T) {
			target, err := String2Time(layout, time.UTC, expectedLoc)
			assert.NoError(t, err)
			assert.EqualValues(t, tm, *target)
		})
	}
}
