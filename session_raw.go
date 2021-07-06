// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"xorm.io/xorm/core"
	"xorm.io/xorm/schemas"
)

func (session *Session) queryPreprocess(sqlStr *string, paramStr ...interface{}) {
	for _, filter := range session.engine.dialect.Filters() {
		*sqlStr = filter.Do(*sqlStr)
	}

	session.lastSQL = *sqlStr
	session.lastSQLArgs = paramStr
}

func (session *Session) queryRows(sqlStr string, args ...interface{}) (*core.Rows, error) {
	defer session.resetStatement()
	if session.statement.LastError != nil {
		return nil, session.statement.LastError
	}

	session.queryPreprocess(&sqlStr, args...)

	session.lastSQL = sqlStr
	session.lastSQLArgs = args

	if session.isAutoCommit {
		var db *core.DB
		if session.sessionType == groupSession {
			db = session.engine.engineGroup.Slave().DB()
		} else {
			db = session.DB()
		}

		if session.prepareStmt {
			// don't clear stmt since session will cache them
			stmt, err := session.doPrepare(db, sqlStr)
			if err != nil {
				return nil, err
			}

			rows, err := stmt.QueryContext(session.ctx, args...)
			if err != nil {
				return nil, err
			}
			return rows, nil
		}

		rows, err := db.QueryContext(session.ctx, sqlStr, args...)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}

	rows, err := session.tx.QueryContext(session.ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (session *Session) queryRow(sqlStr string, args ...interface{}) *core.Row {
	return core.NewRow(session.queryRows(sqlStr, args...))
}

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

func value2Bytes(rawValue *reflect.Value) ([]byte, error) {
	str, err := value2String(rawValue)
	if err != nil {
		return nil, err
	}
	return []byte(str), nil
}

func (session *Session) queryBytes(sqlStr string, args ...interface{}) ([]map[string][]byte, error) {
	rows, err := session.queryRows(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows2maps(rows)
}

func (session *Session) exec(sqlStr string, args ...interface{}) (sql.Result, error) {
	defer session.resetStatement()

	session.queryPreprocess(&sqlStr, args...)

	session.lastSQL = sqlStr
	session.lastSQLArgs = args

	if !session.isAutoCommit {
		return session.tx.ExecContext(session.ctx, sqlStr, args...)
	}

	if session.prepareStmt {
		stmt, err := session.doPrepare(session.DB(), sqlStr)
		if err != nil {
			return nil, err
		}

		res, err := stmt.ExecContext(session.ctx, args...)
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	return session.DB().ExecContext(session.ctx, sqlStr, args...)
}

// Exec raw sql
func (session *Session) Exec(sqlOrArgs ...interface{}) (sql.Result, error) {
	if session.isAutoClose {
		defer session.Close()
	}

	if len(sqlOrArgs) == 0 {
		return nil, ErrUnSupportedType
	}

	sqlStr, args, err := session.statement.ConvertSQLOrArgs(sqlOrArgs...)
	if err != nil {
		return nil, err
	}

	return session.exec(sqlStr, args...)
}
