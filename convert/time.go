// Copyright 2021 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package convert

import (
	"fmt"
	"time"
)

// String2Time converts a string to time with original location
func String2Time(s string, originalLocation *time.Location, convertedLocation *time.Location) (*time.Time, error) {
	if len(s) == 19 {
		dt, err := time.ParseInLocation("2006-01-02 15:04:05", s, originalLocation)
		if err != nil {
			return nil, err
		}
		dt = dt.In(convertedLocation)
		return &dt, nil
	} else if len(s) == 20 && s[10] == 'T' && s[19] == 'Z' {
		dt, err := time.ParseInLocation("2006-01-02T15:04:05Z", s, originalLocation)
		if err != nil {
			return nil, err
		}
		dt = dt.In(convertedLocation)
		return &dt, nil
	}
	return nil, fmt.Errorf("unsupported convertion from %s to time", s)
}
