// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
	"database/sql"
	"errors"
>>>>>>> 6e19325 (refactor driver)
	"fmt"
	"reflect"
	"strconv"
	"time"

>>>>>>> 634f82a (refactor driver)
	"xorm.io/xorm/core"
)

// Query runs a raw sql and return records as []map[string][]byte
func (session *Session) Query(sqlOrArgs ...interface{}) ([]map[string][]byte, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	return session.queryBytes(sqlStr, args...)
}

<<<<<<< HEAD
=======
func value2String(rawValue *reflect.Value) (str string, err error) {
	aa := reflect.TypeOf((*rawValue).Interface())
	vv := reflect.ValueOf((*rawValue).Interface())
	switch aa.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(vv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(vv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		str = strconv.FormatFloat(vv.Float(), 'f', -1, 64)
	case reflect.String:
		str = vv.String()
	case reflect.Array, reflect.Slice:
		switch aa.Elem().Kind() {
		case reflect.Uint8:
			data := rawValue.Interface().([]byte)
			str = string(data)
			if str == "\x00" {
				str = "0"
			}
		default:
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	// time type
	case reflect.Struct:
		if aa.ConvertibleTo(schemas.TimeType) {
			str = vv.Convert(schemas.TimeType).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	case reflect.Bool:
		str = strconv.FormatBool(vv.Bool())
	case reflect.Complex128, reflect.Complex64:
		str = fmt.Sprintf("%v", vv.Complex())
	/* TODO: unsupported types below
	   case reflect.Map:
	   case reflect.Ptr:
	   case reflect.Uintptr:
	   case reflect.UnsafePointer:
	   case reflect.Chan, reflect.Func, reflect.Interface:
	*/
	default:
		err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
	}
	return
}

// genRowsScanResults according
func (session *Session) genRowsScanResults(rows *core.Rows, types []*sql.ColumnType) ([]interface{}, error) {
	var scanResults = make([]interface{}, len(types))
	var err error
	for i, t := range types {
		scanResults[i], err = session.engine.driver.GenScanResult(t.DatabaseTypeName())
		if err != nil {
			return nil, err
		}
	}
	return scanResults, nil
}

>>>>>>> 634f82a (refactor driver)
func (session *Session) rows2Strings(rows *core.Rows) (resultsSlice []map[string]string, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		result, err := row2mapStr(rows, types, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func (session *Session) rows2SliceString(rows *core.Rows) (resultsSlice [][]string, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		record, err := session.engine.row2sliceStr(rows, types, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, record)
	}

	return resultsSlice, nil
}

// QueryString runs a raw sql and return records as []map[string]string
func (session *Session) QueryString(sqlOrArgs ...interface{}) ([]map[string]string, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.rows2Strings(rows)
}

// QuerySliceString runs a raw sql and return records as [][]string
func (session *Session) QuerySliceString(sqlOrArgs ...interface{}) ([][]string, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.rows2SliceString(rows)
}

func (session *Session) rows2Interfaces(rows *core.Rows) (resultsSlice []map[string]interface{}, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := session.engine.row2mapInterface(rows, types, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

// QueryInterface runs a raw sql and return records as []map[string]interface{}
func (session *Session) QueryInterface(sqlOrArgs ...interface{}) ([]map[string]interface{}, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	sqlStr, args, err := session.statement.GenQuerySQL(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return session.rows2Interfaces(rows)
}
