// Copyright 2020 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dialects

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"xorm.io/xorm/core"
	"xorm.io/xorm/schemas"
)

var (
	db2ReservedWords = map[string]bool{}
	db2Quoter        = schemas.Quoter{
		Prefix:     '"',
		Suffix:     '"',
		IsReserved: schemas.AlwaysReserve,
	}
)

type db2 struct {
	Base
}

func (db *db2) Init(uri *URI) error {
	db.quoter = db2Quoter
	return db.Base.Init(db, uri)
}

func (db *db2) SQLType(c *schemas.Column) string {
	var res string
	switch t := c.SQLType.Name; t {
	case schemas.TinyInt:
		res = schemas.SmallInt
		return res
	case schemas.Bit:
		res = schemas.Boolean
		return res
	case schemas.Binary, schemas.VarBinary:
		return schemas.Bytea
	case schemas.DateTime:
		res = schemas.TimeStamp
	case schemas.TimeStampz:
		return "timestamp with time zone"
	case schemas.TinyText, schemas.MediumText, schemas.LongText:
		res = schemas.Text
	case schemas.NVarchar:
		res = schemas.Varchar
	case schemas.Uuid:
		return schemas.Uuid
	case schemas.Blob, schemas.TinyBlob, schemas.MediumBlob, schemas.LongBlob:
		return schemas.Bytea
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
	_, ok := db2ReservedWords[name]
	return ok
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

func (db *db2) CreateTableSQL(table *schemas.Table, tableName string) ([]string, bool) {
	var sql string
	sql = "CREATE TABLE "
	if tableName == "" {
		tableName = table.Name
	}

	sql += db.Quoter().Quote(tableName) + " ("

	pkList := table.PrimaryKeys

	for _, colName := range table.ColumnsSeq() {
		col := table.GetColumn(colName)
		s, _ := ColumnString(db, col, false)
		sql += s
		if col.IsAutoIncrement {
			sql += " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1 )"
		}
		sql = strings.TrimSpace(sql)
		sql += ", "
	}

	if len(pkList) > 0 {
		sql += "PRIMARY KEY ( "
		sql += db.Quoter().Join(pkList, ",")
		sql += " ), "
	}

	sql = sql[:len(sql)-2] + ")"
	return []string{sql}, false
}

func (db *db2) IndexCheckSQL(tableName, idxName string) (string, []interface{}) {
	if len(db.uri.Schema) == 0 {
		args := []interface{}{tableName, idxName}
		return `SELECT indexname FROM pg_indexes WHERE tablename = ? AND indexname = ?`, args
	}

	args := []interface{}{db.uri.Schema, tableName, idxName}
	return `SELECT indexname FROM pg_indexes ` +
		`WHERE schemaname = ? AND tablename = ? AND indexname = ?`, args
}

func (db *db2) SetQuotePolicy(quotePolicy QuotePolicy) {
	switch quotePolicy {
	case QuotePolicyNone:
		var q = oracleQuoter
		q.IsReserved = schemas.AlwaysNoReserve
		db.quoter = q
	case QuotePolicyReserved:
		var q = oracleQuoter
		q.IsReserved = db.IsReserved
		db.quoter = q
	case QuotePolicyAlways:
		fallthrough
	default:
		db.quoter = oracleQuoter
	}
}

func (db *db2) IsTableExist(queryer core.Queryer, ctx context.Context, tableName string) (bool, error) {
	if len(db.uri.Schema) == 0 {
		return db.HasRecords(queryer, ctx, `SELECT tablename FROM pg_tables WHERE tablename = ?`, tableName)
	}
	return db.HasRecords(queryer, ctx, `SELECT tablename FROM pg_tables WHERE schemaname = ? AND tablename = ?`,
		db.uri.Schema, tableName,
	)
}

func (db *db2) ModifyColumnSQL(tableName string, col *schemas.Column) string {
	if len(db.uri.Schema) == 0 {
		return fmt.Sprintf("alter table %s ALTER COLUMN %s TYPE %s",
			tableName, col.Name, db.SQLType(col))
	}
	return fmt.Sprintf("alter table %s.%s ALTER COLUMN %s TYPE %s",
		db.uri.Schema, tableName, col.Name, db.SQLType(col))
}

func (db *db2) DropIndexSQL(tableName string, index *schemas.Index) string {
	quote := db.Quoter().Quote
	idxName := index.Name

	tableName = strings.Replace(tableName, `"`, "", -1)
	tableName = strings.Replace(tableName, `.`, "_", -1)

	if !strings.HasPrefix(idxName, "UQE_") &&
		!strings.HasPrefix(idxName, "IDX_") {
		if index.Type == schemas.UniqueType {
			idxName = fmt.Sprintf("UQE_%v_%v", tableName, index.Name)
		} else {
			idxName = fmt.Sprintf("IDX_%v_%v", tableName, index.Name)
		}
	}
	if db.uri.Schema != "" {
		idxName = db.uri.Schema + "." + idxName
	}
	return fmt.Sprintf("DROP INDEX %v", quote(idxName))
}

func (db *db2) IsColumnExist(queryer core.Queryer, ctx context.Context, tableName, colName string) (bool, error) {
	args := []interface{}{db.uri.Schema, tableName, colName}
	query := "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = $1 AND table_name = $2" +
		" AND column_name = $3"
	if len(db.uri.Schema) == 0 {
		args = []interface{}{tableName, colName}
		query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $1" +
			" AND column_name = $2"
	}

	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func (db *db2) GetColumns(queryer core.Queryer, ctx context.Context, tableName string) ([]string, map[string]*schemas.Column, error) {
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
	if len(db.uri.Schema) != 0 {
		args = append(args, db.uri.Schema)
		f = " AND c.tabschema = ?"
	}
	s = s + f

	rows, err := queryer.QueryContext(ctx, s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := make(map[string]*schemas.Column)
	colSeq := make([]string, 0)

	for rows.Next() {
		col := new(schemas.Column)
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
			col.SQLType = schemas.SQLType{Name: schemas.Char, DefaultLength: 0, DefaultLength2: 0}
		case "timestamp without time zone":
			col.SQLType = schemas.SQLType{Name: schemas.DateTime, DefaultLength: 0, DefaultLength2: 0}
		case "timestamp with time zone":
			col.SQLType = schemas.SQLType{Name: schemas.TimeStampz, DefaultLength: 0, DefaultLength2: 0}
		case "double precision":
			col.SQLType = schemas.SQLType{Name: schemas.Double, DefaultLength: 0, DefaultLength2: 0}
		case "boolean":
			col.SQLType = schemas.SQLType{Name: schemas.Bool, DefaultLength: 0, DefaultLength2: 0}
		case "time without time zone":
			col.SQLType = schemas.SQLType{Name: schemas.Time, DefaultLength: 0, DefaultLength2: 0}
		case "oid":
			col.SQLType = schemas.SQLType{Name: schemas.BigInt, DefaultLength: 0, DefaultLength2: 0}
		default:
			col.SQLType = schemas.SQLType{Name: strings.ToUpper(dataType), DefaultLength: 0, DefaultLength2: 0}
		}
		if _, ok := schemas.SqlTypes[col.SQLType.Name]; !ok {
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

func (db *db2) GetTables(queryer core.Queryer, ctx context.Context) ([]*schemas.Table, error) {
	args := []interface{}{}
	s := "SELECT TABNAME FROM SYSCAT.TABLES WHERE type = 'T' AND OWNERTYPE = 'U'"
	if len(db.uri.Schema) != 0 {
		args = append(args, db.uri.Schema)
		s = s + " AND TABSCHEMA = ?"
	}

	rows, err := queryer.QueryContext(ctx, s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*schemas.Table, 0)
	for rows.Next() {
		table := schemas.NewEmptyTable()
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

func (db *db2) GetIndexes(queryer core.Queryer, ctx context.Context, tableName string) (map[string]*schemas.Index, error) {
	args := []interface{}{tableName}
	s := fmt.Sprintf(`select uniquerule,
    indname as index_name,
    replace(substring(colnames,2,length(colnames)),'+',',') as columns  
from syscat.indexes WHERE tabname = ?`)
	if len(db.uri.Schema) != 0 {
		args = append(args, db.uri.Schema)
		s = s + " AND tabschema=?"
	}

	rows, err := queryer.QueryContext(ctx, s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*schemas.Index, 0)
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
			indexType = schemas.UniqueType
		} else if strings.EqualFold(indexTypeName, "D") {
			indexType = schemas.IndexType
		}
		var isRegular bool
		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			newIdxName := indexName[5+len(tableName):]
			isRegular = true
			if newIdxName != "" {
				indexName = newIdxName
			}
		}

		index := &schemas.Index{Name: indexName, Type: indexType, Cols: make([]string, 0)}
		colNames := strings.Split(columns, ",")
		for _, colName := range colNames {
			index.Cols = append(index.Cols, strings.Trim(colName, `" `))
		}
		index.IsRegular = isRegular
		indexes[index.Name] = index
	}
	return indexes, nil
}

func (db *db2) Filters() []Filter {
	return []Filter{}
}

type db2Driver struct{}

func (p *db2Driver) Parse(driverName, dataSourceName string) (*URI, error) {
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
	return &URI{
		DBName: dbName,
		DBType: "db2",
		Schema: defaultSchema,
	}, nil
}
