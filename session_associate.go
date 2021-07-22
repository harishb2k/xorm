// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"errors"
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
		return session.loadFind(beanOrSlices, cols...)
	} else if v.Kind() == reflect.Struct {
		return session.loadGet(beanOrSlices, cols...)
	}
	return errors.New("unsupported load type, must struct or slice")
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
func (session *Session) loadFind(slices interface{}, cols ...string) error {
	v := reflect.ValueOf(slices)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return errors.New("only slice is supported")
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
func (session *Session) loadGet(bean interface{}, cols ...string) error {
	if session.isAutoClose {
		defer session.Close()
	}

	v := reflect.Indirect(reflect.ValueOf(bean))
	tb, err := session.engine.tagParser.ParseWithCache(v)
	if err != nil {
		return err
	}

	for _, col := range tb.Columns() {
		if len(cols) > 0 && !isStringInSlice(col.Name, cols) {
			continue
		}

		if col.AssociateTable != nil {
			if col.AssociateType == schemas.AssociateBelongsTo {
				colV, err := col.ValueOfV(&v)
				if err != nil {
					return err
				}

				vv := colV.Interface()
				var colPtr reflect.Value
				if colV.Kind() == reflect.Ptr {
					colPtr = *colV
				} else {
					colPtr = colV.Addr()
				}

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
		}
	}
	return nil
}
