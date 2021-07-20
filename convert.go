// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"xorm.io/xorm/convert"
)

var errNilPtr = errors.New("destination pointer is nil") // embedded in descriptive error

func strconvErr(err error) error {
	if ne, ok := err.(*strconv.NumError); ok {
		return ne.Err
	}
	return err
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func asString(src interface{}) string {
	switch v := src.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case *sql.NullString:
		return v.String
	case *sql.NullInt32:
		return fmt.Sprintf("%d", v.Int32)
	case *sql.NullInt64:
		return fmt.Sprintf("%d", v.Int64)
	}
	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return fmt.Sprintf("%v", src)
}

func asInt64(src interface{}) (int64, error) {
	switch v := src.(type) {
	case int:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case string:
		return strconv.ParseInt(v, 10, 64)
	case *sql.NullString:
		return strconv.ParseInt(v.String, 10, 64)
	case *sql.NullInt32:
		return int64(v.Int32), nil
	case *sql.NullInt64:
		return int64(v.Int64), nil
	}

	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), nil
	case reflect.Float64, reflect.Float32:
		return int64(rv.Float()), nil
	case reflect.String:
		return strconv.ParseInt(rv.String(), 10, 64)
	}
	return 0, fmt.Errorf("unsupported value %T as int64", src)
}

func asFloat64(src interface{}) (float64, error) {
	switch v := src.(type) {
	case int:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	case *sql.NullString:
		return strconv.ParseFloat(v.String, 64)
	case *sql.NullInt32:
		return float64(v.Int32), nil
	case *sql.NullInt64:
		return float64(v.Int64), nil
	case *sql.NullFloat64:
		return v.Float64, nil
	}

	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	case reflect.Float64, reflect.Float32:
		return float64(rv.Float()), nil
	case reflect.String:
		return strconv.ParseFloat(rv.String(), 64)
	}
	return 0, fmt.Errorf("unsupported value %T as int64", src)
}

func asBigFloat(src interface{}) (*big.Float, error) {
	res := big.NewFloat(0)
	switch v := src.(type) {
	case int:
		res.SetInt64(int64(v))
		return res, nil
	case int16:
		res.SetInt64(int64(v))
		return res, nil
	case int32:
		res.SetInt64(int64(v))
		return res, nil
	case int8:
		res.SetInt64(int64(v))
		return res, nil
	case int64:
		res.SetInt64(int64(v))
		return res, nil
	case uint:
		res.SetUint64(uint64(v))
		return res, nil
	case uint8:
		res.SetUint64(uint64(v))
		return res, nil
	case uint16:
		res.SetUint64(uint64(v))
		return res, nil
	case uint32:
		res.SetUint64(uint64(v))
		return res, nil
	case uint64:
		res.SetUint64(uint64(v))
		return res, nil
	case []byte:
		res.SetString(string(v))
		return res, nil
	case string:
		res.SetString(v)
		return res, nil
	case *sql.NullString:
		if v.Valid {
			res.SetString(v.String)
			return res, nil
		}
		return nil, nil
	case *sql.NullInt32:
		if v.Valid {
			res.SetInt64(int64(v.Int32))
			return res, nil
		}
		return nil, nil
	case *sql.NullInt64:
		if v.Valid {
			res.SetInt64(int64(v.Int64))
			return res, nil
		}
		return nil, nil
	}

	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		res.SetInt64(rv.Int())
		return res, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		res.SetUint64(rv.Uint())
		return res, nil
	case reflect.Float64, reflect.Float32:
		res.SetFloat64(rv.Float())
		return res, nil
	case reflect.String:
		res.SetString(rv.String())
		return res, nil
	}
	return nil, fmt.Errorf("unsupported value %T as big.Float", src)
}

func asBytes(src interface{}) ([]byte, bool) {
	switch t := src.(type) {
	case []byte:
		return t, true
	case *sql.NullString:
		if !t.Valid {
			return nil, true
		}
		return []byte(t.String), true
	case *sql.RawBytes:
		return *t, true
	}

	rv := reflect.ValueOf(src)

	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(nil, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(nil, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(nil, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(nil, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(nil, rv.Bool()), true
	case reflect.String:
		return []byte(rv.String()), true
	}
	return nil, false
}

func asTime(src interface{}, dbLoc *time.Location, uiLoc *time.Location) (*time.Time, error) {
	switch t := src.(type) {
	case string:
		return convert.String2Time(t, dbLoc, uiLoc)
	case *sql.NullString:
		if !t.Valid {
			return nil, nil
		}
		return convert.String2Time(t.String, dbLoc, uiLoc)
	case []uint8:
		if t == nil {
			return nil, nil
		}
		return convert.String2Time(string(t), dbLoc, uiLoc)
	case *sql.NullTime:
		if !t.Valid {
			return nil, nil
		}
		z, _ := t.Time.Zone()
		if len(z) == 0 || t.Time.Year() == 0 || t.Time.Location().String() != dbLoc.String() {
			tm := time.Date(t.Time.Year(), t.Time.Month(), t.Time.Day(), t.Time.Hour(),
				t.Time.Minute(), t.Time.Second(), t.Time.Nanosecond(), dbLoc).In(uiLoc)
			return &tm, nil
		}
		tm := t.Time.In(uiLoc)
		return &tm, nil
	case *time.Time:
		z, _ := t.Zone()
		if len(z) == 0 || t.Year() == 0 || t.Location().String() != dbLoc.String() {
			tm := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(),
				t.Minute(), t.Second(), t.Nanosecond(), dbLoc).In(uiLoc)
			return &tm, nil
		}
		tm := t.In(uiLoc)
		return &tm, nil
	case time.Time:
		z, _ := t.Zone()
		if len(z) == 0 || t.Year() == 0 || t.Location().String() != dbLoc.String() {
			tm := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(),
				t.Minute(), t.Second(), t.Nanosecond(), dbLoc).In(uiLoc)
			return &tm, nil
		}
		tm := t.In(uiLoc)
		return &tm, nil
	case int:
		tm := time.Unix(int64(t), 0).In(uiLoc)
		return &tm, nil
	case int64:
		tm := time.Unix(t, 0).In(uiLoc)
		return &tm, nil
	case *sql.NullInt64:
		tm := time.Unix(t.Int64, 0).In(uiLoc)
		return &tm, nil
	}
	return nil, fmt.Errorf("unsupported value %#v as time", src)
}

// convertAssign copies to dest the value in src, converting it if possible.
// An error is returned if the copy would result in loss of information.
// dest should be a pointer type.
func convertAssign(dest, src interface{}, originalLocation *time.Location, convertedLocation *time.Location) error {
	// Common cases, without reflect.
	switch s := src.(type) {
	case *interface{}:
		return convertAssign(dest, *s, originalLocation, convertedLocation)
	case string:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = s
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s)
			return nil
		}
	case []byte:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = string(s)
			return nil
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(s)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = cloneBytes(s)
			return nil
		}
	case time.Time:
		switch d := dest.(type) {
		case *string:
			*d = s.Format(time.RFC3339Nano)
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s.Format(time.RFC3339Nano))
			return nil
		}
	case nil:
		switch d := dest.(type) {
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = nil
			return nil
		}
	case *sql.NullString:
		switch d := dest.(type) {
		case *int:
			if s.Valid {
				*d, _ = strconv.Atoi(s.String)
			}
			return nil
		case *int64:
			if s.Valid {
				*d, _ = strconv.ParseInt(s.String, 10, 64)
			}
			return nil
		case *string:
			if s.Valid {
				*d = s.String
			}
			return nil
		case *time.Time:
			if s.Valid {
				var err error
				dt, err := convert.String2Time(s.String, originalLocation, convertedLocation)
				if err != nil {
					return err
				}
				*d = *dt
			}
			return nil
		case *sql.NullTime:
			if s.Valid {
				var err error
				dt, err := convert.String2Time(s.String, originalLocation, convertedLocation)
				if err != nil {
					return err
				}
				d.Valid = true
				d.Time = *dt
			}
			return nil
		case *big.Float:
			if s.Valid {
				if d == nil {
					d = big.NewFloat(0)
				}
				d.SetString(s.String)
			}
			return nil
		}
	case *sql.NullInt32:
		switch d := dest.(type) {
		case *int:
			if s.Valid {
				*d = int(s.Int32)
			}
			return nil
		case *int8:
			if s.Valid {
				*d = int8(s.Int32)
			}
			return nil
		case *int16:
			if s.Valid {
				*d = int16(s.Int32)
			}
			return nil
		case *int32:
			if s.Valid {
				*d = s.Int32
			}
			return nil
		case *int64:
			if s.Valid {
				*d = int64(s.Int32)
			}
			return nil
		}
	case *sql.NullInt64:
		switch d := dest.(type) {
		case *int:
			if s.Valid {
				*d = int(s.Int64)
			}
			return nil
		case *int8:
			if s.Valid {
				*d = int8(s.Int64)
			}
			return nil
		case *int16:
			if s.Valid {
				*d = int16(s.Int64)
			}
			return nil
		case *int32:
			if s.Valid {
				*d = int32(s.Int64)
			}
			return nil
		case *int64:
			if s.Valid {
				*d = s.Int64
			}
			return nil
		}
	case *sql.NullFloat64:
		switch d := dest.(type) {
		case *int:
			if s.Valid {
				*d = int(s.Float64)
			}
			return nil
		case *float64:
			if s.Valid {
				*d = s.Float64
			}
			return nil
		}
	case *sql.NullBool:
		switch d := dest.(type) {
		case *bool:
			if s.Valid {
				*d = s.Bool
			}
			return nil
		}
	case *sql.NullTime:
		switch d := dest.(type) {
		case *time.Time:
			if s.Valid {
				*d = s.Time
			}
			return nil
		case *string:
			if s.Valid {
				*d = s.Time.In(convertedLocation).Format("2006-01-02 15:04:05")
			}
			return nil
		}
	case *convert.NullUint32:
		switch d := dest.(type) {
		case *uint8:
			if s.Valid {
				*d = uint8(s.Uint32)
			}
			return nil
		case *uint16:
			if s.Valid {
				*d = uint16(s.Uint32)
			}
			return nil
		case *uint:
			if s.Valid {
				*d = uint(s.Uint32)
			}
			return nil
		}
	case *convert.NullUint64:
		switch d := dest.(type) {
		case *uint64:
			if s.Valid {
				*d = s.Uint64
			}
			return nil
		}
	case *sql.RawBytes:
		switch d := dest.(type) {
		case convert.Conversion:
			return d.FromDB(*s)
		}
	}

	var sv reflect.Value

	switch d := dest.(type) {
	case *string:
		sv = reflect.ValueOf(src)
		switch sv.Kind() {
		case reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			*d = asString(src)
			return nil
		}
	case *[]byte:
		if b, ok := asBytes(src); ok {
			*d = b
			return nil
		}
	case *bool:
		bv, err := driver.Bool.ConvertValue(src)
		if err == nil {
			*d = bv.(bool)
		}
		return err
	case *interface{}:
		*d = src
		return nil
	}

	return convertAssignV(reflect.ValueOf(dest), src)
}

func convertAssignV(dv reflect.Value, src interface{}) error {
	if src == nil {
		return nil
	}

	if dv.Type().Implements(scannerType) {
		return dv.Interface().(sql.Scanner).Scan(src)
	}

	switch dv.Kind() {
	case reflect.Ptr:
		if dv.IsNil() {
			dv.Set(reflect.New(dv.Type().Elem()))
		}
		return convertAssignV(dv.Elem(), src)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64, err := asInt64(src)
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T to a %s: %v", src, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64, err := convert.AsUint64(src)
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T to a %s: %v", src, dv.Kind(), err)
		}
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		f64, err := asFloat64(src)
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T to a %s: %v", src, dv.Kind(), err)
		}
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		dv.SetString(asString(src))
		return nil
	case reflect.Bool:
		b, err := asBool(src)
		if err != nil {
			return err
		}
		dv.SetBool(b)
		return nil
	case reflect.Slice, reflect.Map, reflect.Struct, reflect.Array:
		data, ok := asBytes(src)
		if !ok {
			return fmt.Errorf("onvertAssignV: src cannot be as bytes %#v", src)
		}
		if data == nil {
			return nil
		}
		if dv.Kind() != reflect.Ptr {
			dv = dv.Addr()
		}
		return json.Unmarshal(data, dv.Interface())
	default:
		return fmt.Errorf("convertAssignV: unsupported Scan, storing driver.Value type %T into type %T", src, dv.Interface())
	}
}

func asKind(vv reflect.Value, tp reflect.Type) (interface{}, error) {
	switch tp.Kind() {
	case reflect.Ptr:
		return asKind(vv.Elem(), tp.Elem())
	case reflect.Int64:
		return vv.Int(), nil
	case reflect.Int:
		return int(vv.Int()), nil
	case reflect.Int32:
		return int32(vv.Int()), nil
	case reflect.Int16:
		return int16(vv.Int()), nil
	case reflect.Int8:
		return int8(vv.Int()), nil
	case reflect.Uint64:
		return vv.Uint(), nil
	case reflect.Uint:
		return uint(vv.Uint()), nil
	case reflect.Uint32:
		return uint32(vv.Uint()), nil
	case reflect.Uint16:
		return uint16(vv.Uint()), nil
	case reflect.Uint8:
		return uint8(vv.Uint()), nil
	case reflect.String:
		return vv.String(), nil
	case reflect.Slice:
		if tp.Elem().Kind() == reflect.Uint8 {
			v, err := strconv.ParseInt(string(vv.Interface().([]byte)), 10, 64)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}
	return nil, fmt.Errorf("unsupported primary key type: %v, %v", tp, vv)
}

func asBool(src interface{}) (bool, error) {
	switch v := src.(type) {
	case bool:
		return v, nil
	case *bool:
		return *v, nil
	case *sql.NullBool:
		return v.Bool, nil
	case int64:
		return v > 0, nil
	case int:
		return v > 0, nil
	case int8:
		return v > 0, nil
	case int16:
		return v > 0, nil
	case int32:
		return v > 0, nil
	case []byte:
		if len(v) == 0 {
			return false, nil
		}
		if v[0] == 0x00 {
			return false, nil
		} else if v[0] == 0x01 {
			return true, nil
		}
		return strconv.ParseBool(string(v))
	case string:
		return strconv.ParseBool(v)
	case *sql.NullInt64:
		return v.Int64 > 0, nil
	case *sql.NullInt32:
		return v.Int32 > 0, nil
	default:
		return false, fmt.Errorf("unknow type %T as bool", src)
	}
}

// str2PK convert string value to primary key value according to tp
func str2PKValue(s string, tp reflect.Type) (reflect.Value, error) {
	var err error
	var result interface{}
	var defReturn = reflect.Zero(tp)

	switch tp.Kind() {
	case reflect.Int:
		result, err = strconv.Atoi(s)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as int: %s", s, err.Error())
		}
	case reflect.Int8:
		x, err := strconv.Atoi(s)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as int8: %s", s, err.Error())
		}
		result = int8(x)
	case reflect.Int16:
		x, err := strconv.Atoi(s)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as int16: %s", s, err.Error())
		}
		result = int16(x)
	case reflect.Int32:
		x, err := strconv.Atoi(s)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as int32: %s", s, err.Error())
		}
		result = int32(x)
	case reflect.Int64:
		result, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as int64: %s", s, err.Error())
		}
	case reflect.Uint:
		x, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as uint: %s", s, err.Error())
		}
		result = uint(x)
	case reflect.Uint8:
		x, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as uint8: %s", s, err.Error())
		}
		result = uint8(x)
	case reflect.Uint16:
		x, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as uint16: %s", s, err.Error())
		}
		result = uint16(x)
	case reflect.Uint32:
		x, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as uint32: %s", s, err.Error())
		}
		result = uint32(x)
	case reflect.Uint64:
		result, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return defReturn, fmt.Errorf("convert %s as uint64: %s", s, err.Error())
		}
	case reflect.String:
		result = s
	default:
		return defReturn, errors.New("unsupported convert type")
	}
	return reflect.ValueOf(result).Convert(tp), nil
}

func str2PK(s string, tp reflect.Type) (interface{}, error) {
	v, err := str2PKValue(s, tp)
	if err != nil {
		return nil, err
	}
	return v.Interface(), nil
}
