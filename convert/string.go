// Copyright 2021 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package convert

import (
	"database/sql"
	"fmt"
	"strconv"
)

// ConvertAssignString converts an interface to string
func ConvertAssignString(v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}
	switch vv := v.(type) {
	case *int64:
		return strconv.FormatInt(*vv, 10), nil
	case *int8:
		return strconv.FormatInt(int64(*vv), 10), nil
	case *sql.NullString:
		if vv.Valid {
			return vv.String, nil
		}
		return "", nil
	case *sql.RawBytes:
		if len([]byte(*vv)) > 0 {
			return string(*vv), nil
		}
		return "", nil
	case *sql.NullInt32:
		if vv.Valid {
			return fmt.Sprintf("%d", vv.Int32), nil
		}
		return "", nil
	case *sql.NullInt64:
		if vv.Valid {
			return fmt.Sprintf("%d", vv.Int64), nil
		}
		return "", nil
	case *sql.NullFloat64:
		if vv.Valid {
			return fmt.Sprintf("%g", vv.Float64), nil
		}
		return "", nil
	case *sql.NullBool:
		if vv.Valid {
			if vv.Bool {
				return "true", nil
			}
			return "false", nil
		}
		return "", nil
	case *sql.NullTime:
		if vv.Valid {
			return vv.Time.Format("2006-01-02 15:04:05"), nil
		}
		return "", nil
	default:
		return "", fmt.Errorf("convert assign string unsupported type: %#v", vv)
	}
}
