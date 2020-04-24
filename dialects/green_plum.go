// Copyright 2020 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dialects

import (
	"fmt"
	"strings"

	"xorm.io/xorm/schemas"
)

type greenplum struct {
	postgres
}


func (db *greenplum) CreateTableSQL(table *schemas.Table, tableName string) ([]string, bool) {
	var sql string
	sql = "CREATE TABLE IF NOT EXISTS "
	if tableName == "" {
		tableName = table.Name
	}

	quoter := db.Quoter()
	sql += quoter.Quote(tableName)
	sql += " ("

	if len(table.ColumnsSeq()) > 0 {
		var uniqueCols []string
		pkList := table.PrimaryKeys

		for _, colName := range table.ColumnsSeq() {
			col := table.GetColumn(colName)
			s, _ := ColumnString(db, col, col.IsPrimaryKey && len(pkList) == 1)
			sql += s
			sql = strings.TrimSpace(sql)
			sql += ", "

			for _, v := range col.Indexes {
				if v == schemas.UniqueType {
					uniqueCols = append(uniqueCols, colName)
					break
				}
			}
		}

		if len(pkList) > 1 {
			sql += "PRIMARY KEY ( "
			sql += quoter.Join(pkList, ",")
			sql += " ), "
		}

		sql = sql[:len(sql)-2]

		if len(uniqueCols) > 0 {
			sql += fmt.Sprintf(" distributed by (%s)", strings.Join(uniqueCols, ","))
		}
	}
	sql += ")"

	return []string{sql}, true
}