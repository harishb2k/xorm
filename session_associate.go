// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"errors"
	"fmt"
	"reflect"

	"xorm.io/xorm/internal/utils"
	"xorm.io/xorm/schemas"
)

// Load loads associated fields from database
func (session *Session) Load(beanOrSlices interface{}, cols ...string) error {
	v := reflect.ValueOf(beanOrSlices)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice {
		return session.loadFindSlice(v, cols...)
	} else if v.Kind() == reflect.Map {
		return session.loadFindMap(v, cols...)
	} else if v.Kind() == reflect.Struct {
		return session.loadGet(v, cols...)
	}
	return errors.New("unsupported load type, must struct, slice or map")
}

func isStringInSlice(s string, slice []string) bool {
	for _, e := range slice {
		if s == e {
			return true
		}
	}
	return false
}

// loadFind load 's belongs to tag field immedicatlly
func (session *Session) loadFindSlice(v reflect.Value, cols ...string) error {
	if v.Kind() != reflect.Slice {
		return errors.New("only slice is supported")
	}

	if v.Len() <= 0 {
		return nil
	}

	tableValue := v.Index(0)
	if tableValue.Kind() == reflect.Ptr {
		tableValue = tableValue.Elem()
	}
	tb, err := session.engine.tagParser.ParseWithCache(tableValue)
	if err != nil {
		return err
	}

	type Va struct {
		v   []reflect.Value
		pk  []interface{}
		col *schemas.Column
	}

	var pks = make(map[*schemas.Column]*Va)
	for _, col := range tb.Columns() {
		if col.AssociateTable == nil || col.AssociateType != schemas.AssociateBelongsTo {
			continue
		}

		if len(cols) > 0 && !isStringInSlice(col.Name, cols) {
			continue
		}

		pkCols := col.AssociateTable.PKColumns()
		if len(pkCols) != 1 {
			return fmt.Errorf("unsupported primary key number")
		}

		pks[col] = &Va{
			col: pkCols[0],
		}
	}

	for i := 0; i < v.Len(); i++ {
		value := v.Index(i)
		for col, va := range pks {
			colV, err := col.ValueOfV(&value)
			if err != nil {
				return err
			}

			pkCols := col.AssociateTable.PKColumns()
			pkV, err := pkCols[0].ValueOfV(colV)
			if err != nil {
				return err
			}
			vv := pkV.Interface()
			if !utils.IsZero(vv) { // TODO: duplicate primary key
				va.v = append(va.v, *colV)
				va.pk = append(va.pk, vv)
			}
		}
	}

	for col, va := range pks {
		pkCols := col.AssociateTable.PKColumns()
		mp := reflect.MakeMap(reflect.MapOf(pkCols[0].FieldType, col.FieldType))
		x := reflect.New(mp.Type())
		x.Elem().Set(mp)

		err = session.In(va.col.Name, va.pk...).find(x.Interface())
		if err != nil {
			return err
		}

		for _, v := range va.v {
			pkCols := col.AssociateTable.PKColumns()
			pkV, err := pkCols[0].ValueOfV(&v)
			if err != nil {
				return err
			}

			v.Set(mp.MapIndex(*pkV))
		}
	}
	return nil
}

// loadFindMap load 's belongs to tag field immedicatlly
func (session *Session) loadFindMap(v reflect.Value, cols ...string) error {
	if v.Kind() != reflect.Map {
		return errors.New("only map is supported")
	}

	if v.Len() <= 0 {
		return nil
	}

	vv := v.Index(0)
	if vv.Kind() == reflect.Ptr {
		vv = vv.Elem()
	}
	tb, err := session.engine.tagParser.ParseWithCache(vv)
	if err != nil {
		return err
	}

	var pks = make(map[*schemas.Column][]interface{})
	for i := 0; i < v.Len(); i++ {
		ev := v.Index(i)

		for _, col := range tb.Columns() {
			if len(cols) > 0 && !isStringInSlice(col.Name, cols) {
				continue
			}

			if col.AssociateTable != nil {
				if col.AssociateType == schemas.AssociateBelongsTo {
					colV, err := col.ValueOfV(&ev)
					if err != nil {
						return err
					}

					vv := colV.Interface()
					/*var colPtr reflect.Value
					if colV.Kind() == reflect.Ptr {
						colPtr = *colV
					} else {
						colPtr = colV.Addr()
					}*/

					if !utils.IsZero(vv) {
						pks[col] = append(pks[col], vv)
					}
				}
			}
		}
	}

	for col, pk := range pks {
		slice := reflect.MakeSlice(col.FieldType, 0, len(pk))
		err = session.In(col.Name, pk...).find(slice.Addr().Interface())
		if err != nil {
			return err
		}
	}
	return nil
}

// loadGet load bean's belongs to tag field immedicatlly
func (session *Session) loadGet(v reflect.Value, cols ...string) error {
	if session.isAutoClose {
		defer session.Close()
	}

	tb, err := session.engine.tagParser.ParseWithCache(v)
	if err != nil {
		return err
	}

	for _, col := range tb.Columns() {
		if len(cols) > 0 && !isStringInSlice(col.Name, cols) {
			continue
		}

		if col.AssociateTable == nil || col.AssociateType != schemas.AssociateBelongsTo {
			continue
		}

		colV, err := col.ValueOfV(&v)
		if err != nil {
			return err
		}

		var colPtr reflect.Value
		if colV.Kind() == reflect.Ptr {
			colPtr = *colV
		} else {
			colPtr = colV.Addr()
		}

		pks := col.AssociateTable.PKColumns()
		pkV, err := pks[0].ValueOfV(colV)
		if err != nil {
			return err
		}
		vv := pkV.Interface()

		if !utils.IsZero(vv) && session.cascadeLevel > 0 {
			has, err := session.ID(vv).NoAutoCondition().get(colPtr.Interface())
			if err != nil {
				return err
			}
			if !has {
				return errors.New("load bean does not exist")
			}
			session.cascadeLevel--
		}
	}
	return nil
}
