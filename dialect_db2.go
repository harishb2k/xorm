// Copyright 2015 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"xorm.io/xorm/core"
)

type db2 struct {
	core.Base
}

func (db *db2) Init(d *core.DB, uri *core.Uri, drivername, dataSourceName string) error {
	err := db.Base.Init(d, db, uri, drivername, dataSourceName)
	if err != nil {
		return err
	}
	return nil
}

func (db *db2) SqlType(c *core.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case core.TinyInt:
		res = core.SmallInt
		return res
	case core.Bit:
		res = core.Boolean
		return res
	case core.Binary, core.VarBinary:
		return core.Bytea
	case core.DateTime:
		res = core.TimeStamp
	case core.TimeStampz:
		return "timestamp with time zone"
	case core.TinyText, core.MediumText, core.LongText:
		res = core.Text
	case core.NVarchar:
		res = core.Varchar
	case core.Uuid:
		return core.Uuid
	case core.Blob, core.TinyBlob, core.MediumBlob, core.LongBlob:
		return core.Bytea
	default:
		res = t
	}

	if strings.EqualFold(res, "bool") {
		// for bool, we don't need length information
		return res
	}
	hasLen1 := (c.Length > 0)
	hasLen2 := (c.Length2 > 0)

	if hasLen2 {
		res += "(" + strconv.Itoa(c.Length) + "," + strconv.Itoa(c.Length2) + ")"
	} else if hasLen1 {
		res += "(" + strconv.Itoa(c.Length) + ")"
	}
	return res
}

func (db *db2) SupportInsertMany() bool {
	return true
}

func (db *db2) IsReserved(name string) bool {
	_, ok := postgresReservedWords[name]
	return ok
}

func (db *db2) Quote(name string) string {
	name = strings.Replace(name, ".", `"."`, -1)
	return "\"" + name + "\""
}

func (db *db2) AutoIncrStr() string {
	return ""
}

func (db *db2) SupportEngine() bool {
	return false
}

func (db *db2) SupportCharset() bool {
	return false
}

func (db *db2) IndexOnTable() bool {
	return false
}

func (db *db2) CreateTableSql(table *core.Table, tableName, storeEngine, charset string) string {
	var sql string
	sql = "CREATE TABLE "
	if tableName == "" {
		tableName = table.Name
	}

	sql += db.Quote(tableName) + " ("

	pkList := table.PrimaryKeys

	for _, colName := range table.ColumnsSeq() {
		col := table.GetColumn(colName)
		sql += col.StringNoPk(db)
		if col.IsAutoIncrement {
			sql += " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1 )"
		}
		sql = strings.TrimSpace(sql)
		sql += ", "
	}

	if len(pkList) > 0 {
		sql += "PRIMARY KEY ( "
		sql += db.Quote(strings.Join(pkList, db.Quote(",")))
		sql += " ), "
	}

	sql = sql[:len(sql)-2] + ")"
	return sql
}

func (db *db2) IndexCheckSql(tableName, idxName string) (string, []interface{}) {
	if len(db.Schema) == 0 {
		args := []interface{}{tableName, idxName}
		return `SELECT indexname FROM pg_indexes WHERE tablename = ? AND indexname = ?`, args
	}

	args := []interface{}{db.Schema, tableName, idxName}
	return `SELECT indexname FROM pg_indexes ` +
		`WHERE schemaname = ? AND tablename = ? AND indexname = ?`, args
}

func (db *db2) TableCheckSql(tableName string) (string, []interface{}) {
	if len(db.Schema) == 0 {
		args := []interface{}{tableName}
		return `SELECT tablename FROM pg_tables WHERE tablename = ?`, args
	}

	args := []interface{}{db.Schema, tableName}
	return `SELECT tablename FROM pg_tables WHERE schemaname = ? AND tablename = ?`, args
}

func (db *db2) ModifyColumnSql(tableName string, col *core.Column) string {
	if len(db.Schema) == 0 {
		return fmt.Sprintf("alter table %s ALTER COLUMN %s TYPE %s",
			tableName, col.Name, db.SqlType(col))
	}
	return fmt.Sprintf("alter table %s.%s ALTER COLUMN %s TYPE %s",
		db.Schema, tableName, col.Name, db.SqlType(col))
}

func (db *db2) DropIndexSql(tableName string, index *core.Index) string {
	quote := db.Quote
	idxName := index.Name

	tableName = strings.Replace(tableName, `"`, "", -1)
	tableName = strings.Replace(tableName, `.`, "_", -1)

	if !strings.HasPrefix(idxName, "UQE_") &&
		!strings.HasPrefix(idxName, "IDX_") {
		if index.Type == core.UniqueType {
			idxName = fmt.Sprintf("UQE_%v_%v", tableName, index.Name)
		} else {
			idxName = fmt.Sprintf("IDX_%v_%v", tableName, index.Name)
		}
	}
	if db.Uri.Schema != "" {
		idxName = db.Uri.Schema + "." + idxName
	}
	return fmt.Sprintf("DROP INDEX %v", quote(idxName))
}

func (db *db2) IsColumnExist(tableName, colName string) (bool, error) {
	args := []interface{}{db.Schema, tableName, colName}
	query := "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = $1 AND table_name = $2" +
		" AND column_name = $3"
	if len(db.Schema) == 0 {
		args = []interface{}{tableName, colName}
		query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $1" +
			" AND column_name = $2"
	}
	db.LogSQL(query, args)

	rows, err := db.DB().Query(query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func (db *db2) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	args := []interface{}{tableName}
	s := `Select c.colname as column_name,
	c.colno as position,
	c.typename as data_type,
	c.length,
	c.scale,
	c.remarks as description,   
	case when  c.nulls = 'Y' then 1 else 0 end as nullable,
	default as default_value,
	case when c.identity ='Y' then 1 else 0 end as is_identity,
	case when c.generated ='' then 0 else 1 end as  is_computed,
	c.text as computed_formula
from syscat.columns c
inner join syscat.tables t on 
   t.tabschema = c.tabschema and t.tabname = c.tabname
where t.type = 'T' AND c.tabname = ?`

	var f string
	if len(db.Schema) != 0 {
		args = append(args, db.Schema)
		f = " AND c.tabschema = ?"
	}
	s = s + f

	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := make(map[string]*core.Column)
	colSeq := make([]string, 0)

	for rows.Next() {
		col := new(core.Column)
		col.Indexes = make(map[string]int)

		var colName, position, dataType, numericScale string
		var description, colDefault, computedFormula, maxLenStr *string
		var isComputed bool
		err = rows.Scan(&colName, &position, &dataType, &maxLenStr, &numericScale, &description, &col.Nullable, &colDefault, &col.IsPrimaryKey, &isComputed, &computedFormula)
		if err != nil {
			return nil, nil, err
		}

		//fmt.Println(colName, position, dataType, maxLenStr, numericScale, description, col.Nullable, colDefault, col.IsPrimaryKey, isComputed, computedFormula)
		var maxLen int
		if maxLenStr != nil {
			maxLen, err = strconv.Atoi(*maxLenStr)
			if err != nil {
				return nil, nil, err
			}
		}

		col.Name = strings.Trim(colName, `" `)
		if colDefault != nil {
			col.DefaultIsEmpty = false
			col.Default = *colDefault
		}

		if colDefault != nil && strings.HasPrefix(*colDefault, "nextval(") {
			col.IsAutoIncrement = true
		}

		switch dataType {
		case "character", "CHARACTER":
			col.SQLType = core.SQLType{Name: core.Char, DefaultLength: 0, DefaultLength2: 0}
		case "timestamp without time zone":
			col.SQLType = core.SQLType{Name: core.DateTime, DefaultLength: 0, DefaultLength2: 0}
		case "timestamp with time zone":
			col.SQLType = core.SQLType{Name: core.TimeStampz, DefaultLength: 0, DefaultLength2: 0}
		case "double precision":
			col.SQLType = core.SQLType{Name: core.Double, DefaultLength: 0, DefaultLength2: 0}
		case "boolean":
			col.SQLType = core.SQLType{Name: core.Bool, DefaultLength: 0, DefaultLength2: 0}
		case "time without time zone":
			col.SQLType = core.SQLType{Name: core.Time, DefaultLength: 0, DefaultLength2: 0}
		case "oid":
			col.SQLType = core.SQLType{Name: core.BigInt, DefaultLength: 0, DefaultLength2: 0}
		default:
			col.SQLType = core.SQLType{Name: strings.ToUpper(dataType), DefaultLength: 0, DefaultLength2: 0}
		}
		if _, ok := core.SqlTypes[col.SQLType.Name]; !ok {
			return nil, nil, fmt.Errorf("Unknown colType: %v", dataType)
		}

		col.Length = maxLen

		if col.SQLType.IsText() || col.SQLType.IsTime() {
			if col.Default != "" {
				col.Default = "'" + col.Default + "'"
			} else {
				if col.DefaultIsEmpty {
					col.Default = "''"
				}
			}
		}
		cols[col.Name] = col
		colSeq = append(colSeq, col.Name)
	}

	return colSeq, cols, nil
}

func (db *db2) GetTables() ([]*core.Table, error) {
	args := []interface{}{}
	s := "SELECT TABNAME FROM SYSCAT.TABLES WHERE type = 'T' AND OWNERTYPE = 'U'"
	if len(db.Schema) != 0 {
		args = append(args, db.Schema)
		s = s + " AND TABSCHEMA = ?"
	}

	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*core.Table, 0)
	for rows.Next() {
		table := core.NewEmptyTable()
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		table.Name = name
		tables = append(tables, table)
	}
	return tables, nil
}

func (db *db2) GetIndexes(tableName string) (map[string]*core.Index, error) {
	args := []interface{}{tableName}
	s := fmt.Sprintf(`select uniquerule,
    indname as index_name,
    replace(substring(colnames,2,length(colnames)),'+',',') as columns  
from syscat.indexes WHERE tabname = ?`)
	if len(db.Schema) != 0 {
		args = append(args, db.Schema)
		s = s + " AND tabschema=?"
	}
	db.LogSQL(s, args)

	rows, err := db.DB().Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*core.Index, 0)
	for rows.Next() {
		var indexTypeName, indexName, columns string
		/*when 'P' then 'Primary key'
		        when 'U' then 'Unique'
				when 'D' then 'Nonunique'*/
		err = rows.Scan(&indexTypeName, &indexName, &columns)
		if err != nil {
			return nil, err
		}
		indexName = strings.Trim(indexName, `" `)
		if strings.HasSuffix(indexName, "_pkey") {
			continue
		}
		var indexType int
		if strings.EqualFold(indexTypeName, "U") {
			indexType = core.UniqueType
		} else if strings.EqualFold(indexTypeName, "D") {
			indexType = core.IndexType
		}
		var isRegular bool
		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			newIdxName := indexName[5+len(tableName):]
			isRegular = true
			if newIdxName != "" {
				indexName = newIdxName
			}
		}

		index := &core.Index{Name: indexName, Type: indexType, Cols: make([]string, 0)}
		colNames := strings.Split(columns, ",")
		for _, colName := range colNames {
			index.Cols = append(index.Cols, strings.Trim(colName, `" `))
		}
		index.IsRegular = isRegular
		indexes[index.Name] = index
	}
	return indexes, nil
}

func (db *db2) Filters() []core.Filter {
	return []core.Filter{&core.QuoteFilter{}}
}

type db2Driver struct{}

func (p *db2Driver) Parse(driverName, dataSourceName string) (*core.Uri, error) {
	var dbName string
	var defaultSchema string

	kv := strings.Split(dataSourceName, ";")
	for _, c := range kv {
		vv := strings.SplitN(strings.TrimSpace(c), "=", 2)
		if len(vv) == 2 {
			switch strings.ToLower(vv[0]) {
			case "database":
				dbName = vv[1]
			case "uid":
				defaultSchema = vv[1]
			}
		}
	}

	if dbName == "" {
		return nil, errors.New("no db name provided")
	}
	return &core.Uri{
		DbName: dbName,
		DbType: "db2",
		Schema: defaultSchema,
	}, nil
}
