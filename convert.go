// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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
	case *sql.RawBytes:
		return string(*v)
	case *sql.NullBool:
		if v.Valid {
			return strconv.FormatBool(v.Bool)
		}
		return ""
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
	case reflect.Float64:
		return int64(rv.Float()), nil
	case reflect.Float32:
		return int64(rv.Float()), nil
	case reflect.String:
		return strconv.ParseInt(rv.String(), 10, 64)
	}
	return 0, fmt.Errorf("unsupported value %T as int64", src)
}

func asUint64(src interface{}) (uint64, error) {
	switch v := src.(type) {
	case int:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return v, nil
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case string:
		return strconv.ParseUint(v, 10, 64)
	case *sql.NullString:
		return strconv.ParseUint(v.String, 10, 64)
	case *sql.NullInt32:
		return uint64(v.Int32), nil
	case *sql.NullInt64:
		return uint64(v.Int64), nil
	}

	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64(rv.Uint()), nil
	case reflect.Float64:
		return uint64(rv.Float()), nil
	case reflect.Float32:
		return uint64(rv.Float()), nil
	case reflect.String:
		return strconv.ParseUint(rv.String(), 10, 64)
	}
	return 0, fmt.Errorf("unsupported value %T as uint64", src)
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
	}

	rv := reflect.ValueOf(src)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	case reflect.Float64:
		return float64(rv.Float()), nil
	case reflect.Float32:
		return float64(rv.Float()), nil
	case reflect.String:
		return strconv.ParseFloat(rv.String(), 64)
	}
	return 0, fmt.Errorf("unsupported value %T as int64", src)
}

func asBytes(buf []byte, rv reflect.Value) (b []byte, ok bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.AppendUint(buf, rv.Uint(), 10), true
	case reflect.Float32:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 32), true
	case reflect.Float64:
		return strconv.AppendFloat(buf, rv.Float(), 'g', -1, 64), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.String:
		s := rv.String()
		return append(buf, s...), true
	}
	return
}

// convertAssign copies to dest the value in src, converting it if possible.
// An error is returned if the copy would result in loss of information.
// dest should be a pointer type.
func convertAssign(dest, src interface{}, originalLocation *time.Location, convertedLocation *time.Location) error {
	fmt.Printf("======= %#v ------> %#v \n", src, dest)
	// Common cases, without reflect.
	switch s := src.(type) {
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
	case int64:
		switch d := dest.(type) {
		case *uint64:
			*d = uint64(s)
			return nil
		}
		fmt.Println("======", src, dest)
	case int, int32, int16, int8:
		fmt.Println("22222222")
	case uint, uint32, uint64, uint8, uint16:
		fmt.Println("3333333")
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
		case *int64:
			if s.Valid {
				*d, _ = strconv.ParseInt(s.String, 10, 64)
			}
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
	case *NullUint32:
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
	case *NullUint64:
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
		sv = reflect.ValueOf(src)
		if b, ok := asBytes(nil, sv); ok {
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

	return convertAssignV(reflect.ValueOf(dest), src, originalLocation, convertedLocation)
}

func convertAssignV(dpv reflect.Value, src interface{}, originalLocation, convertedLocation *time.Location) error {
	if dpv.Kind() != reflect.Ptr {
		return fmt.Errorf("destination %s not a pointer", dpv.Kind())
	}
	if dpv.IsNil() {
		return errNilPtr
	}

	var sv = reflect.ValueOf(src)

	dv := reflect.Indirect(dpv)
	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		switch b := src.(type) {
		case []byte:
			dv.Set(reflect.ValueOf(cloneBytes(b)))
		default:
			dv.Set(sv)
		}
		return nil
	}

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	switch dv.Kind() {
	case reflect.Ptr:
		if src == nil {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		}

		dv.Set(reflect.New(dv.Type().Elem()))
		fmt.Println("333333")
		return convertAssign(dv.Interface(), src, originalLocation, convertedLocation)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i64, err := asInt64(src)
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T to a %s: %v", src, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u64, err := asUint64(src)
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
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, dpv.Interface())
}

func asKind(vv reflect.Value, tp reflect.Type) (interface{}, error) {
	switch tp.Kind() {
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
	default:
		return vv.Interface(), nil
	}
	return nil, fmt.Errorf("asKind unsupported  type: %v, %v", tp, vv)
}

func asBool(bs []byte) (bool, error) {
	if len(bs) == 0 {
		return false, nil
	}
	if bs[0] == 0x00 {
		return false, nil
	} else if bs[0] == 0x01 {
		return true, nil
	}
	return strconv.ParseBool(string(bs))
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

var (
	_ sql.Scanner = &NullUint64{}
)

// NullUint64 represents an uint64 that may be null.
// NullUint64 implements the Scanner interface so
// it can be used as a scan destination, similar to NullString.
type NullUint64 struct {
	Uint64 uint64
	Valid  bool
}

// Scan implements the Scanner interface.
func (n *NullUint64) Scan(value interface{}) error {
	if value == nil {
		n.Uint64, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	var err error
	n.Uint64, err = asUint64(value)
	return err
}

// Value implements the driver Valuer interface.
func (n NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Uint64, nil
}

var (
	_ sql.Scanner = &NullUint32{}
)

// NullUint32 represents an uint32 that may be null.
// NullUint32 implements the Scanner interface so
// it can be used as a scan destination, similar to NullString.
type NullUint32 struct {
	Uint32 uint32
	Valid  bool // Valid is true if Uint32 is not NULL
}

// Scan implements the Scanner interface.
func (n *NullUint32) Scan(value interface{}) error {
	if value == nil {
		n.Uint32, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	i64, err := asUint64(value)
	if err != nil {
		return err
	}
	n.Uint32 = uint32(i64)
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUint32) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return int64(n.Uint32), nil
}

var (
	_ sql.Scanner = &EmptyScanner{}
)

type EmptyScanner struct{}

func (EmptyScanner) Scan(value interface{}) error {
	return nil
}
