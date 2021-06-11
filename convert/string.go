// Copyright 2021 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package convert

import (
	"database/sql"
	"fmt"
	"strconv"
)

func ConvertAssignString(v interface{}) (string, error) {
	switch vv := v.(type) {
	case *sql.NullString:
		if vv.Valid {
			return vv.String, nil
		}
		return "", nil
	case *int64:
		if vv != nil {
			return strconv.FormatInt(*vv, 10), nil
		}
		return "", nil
	case *int8:
		if vv != nil {
			return strconv.FormatInt(int64(*vv), 10), nil
		}
		return "", nil
	case *sql.RawBytes:
		if vv != nil && len([]byte(*vv)) > 0 {
			return string(*vv), nil
		}
		return "", nil
	default:
		return "", fmt.Errorf("unsupported type: %#v", vv)
	}
}
