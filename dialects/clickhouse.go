// Copyright 2020 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dialects

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"net/url"
	"strings"

	"xorm.io/xorm/convert"
	"xorm.io/xorm/core"
	"xorm.io/xorm/schemas"
)

type clickhouse struct {
	Base
	baseDriver
}

func (p *clickhouse) Parse(driverName, dataSourceName string) (*URI, error) {
	return ParseClickHouse(dataSourceName)
}

func (db *clickhouse) Init(uri *URI) error {
	return db.Base.Init(db, uri)
}

func (db *clickhouse) Version(ctx context.Context, queryer core.Queryer) (*schemas.Version, error) {
	rows, err := queryer.QueryContext(ctx,
		"SELECT SERVERPROPERTY('productversion'), SERVERPROPERTY ('productlevel') AS ProductLevel, SERVERPROPERTY ('edition') AS ProductEdition")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var version, level, edition string
	if !rows.Next() {
		if rows.Err() != nil {
			return nil, rows.Err()
		}
		return nil, errors.New("unknow version")
	}

	if err := rows.Scan(&version, &level, &edition); err != nil {
		return nil, err
	}

	// MSSQL: Microsoft SQL Server 2017 (RTM-CU13) (KB4466404) - 14.0.3048.4 (X64) Nov 30 2018 12:57:58 Copyright (C) 2017 Microsoft Corporation Developer Edition (64-bit) on Linux (Ubuntu 16.04.5 LTS)
	return &schemas.Version{
		Number:  version,
		Level:   level,
		Edition: edition,
	}, nil
}

func (db *clickhouse) ColumnTypeKind(t string) int {
	switch t {
	case "Date", "DateTime", "DateTime64":
		return schemas.TIME_TYPE
	case "String", "FixedString", "UUID", "Enum":
		return schemas.TEXT_TYPE
	case "Int8", "Int16", "Int32", "Int64", "Int128", "Int256",
		"UInt8", "UInt16", "UInt32", "UInt64", "UInt256",
		"Float32", "Float64",
		"Decimal32", "Decimal64", "Decimal128", "Decimal256":
		return schemas.NUMERIC_TYPE
	default:
		if strings.HasPrefix(t, "Array(") {
			return schemas.ARRAY_TYPE
		}
		return schemas.UNKNOW_TYPE
	}
}

func (db *clickhouse) IsReserved(name string) bool {
	return false
}

func (db *clickhouse) SQLType(c *schemas.Column) string {
	return ""
}

func (db *clickhouse) SetQuotePolicy(quotePolicy QuotePolicy) {
}

func (*clickhouse) AutoIncrStr() string {
	return ""
}

func (*clickhouse) CreateTableSQL(t *schemas.Table, tableName string) ([]string, bool) {
	return nil, false
}

func (*clickhouse) IsTableExist(queryer core.Queryer, ctx context.Context, tableName string) (bool, error) {
	return false, nil
}

func (*clickhouse) Filters() []Filter {
	return []Filter{}
}

func (*clickhouse) GetColumns(core.Queryer, context.Context, string) ([]string, map[string]*schemas.Column, error) {
	return nil, nil, nil
}

func (db *clickhouse) GetIndexes(queryer core.Queryer, ctx context.Context, tableName string) (map[string]*schemas.Index, error) {
	return nil, nil
}

func (db *clickhouse) IndexCheckSQL(tableName, idxName string) (string, []interface{}) {
	return "", nil
}

func (db *clickhouse) GetTables(queryer core.Queryer, ctx context.Context) ([]*schemas.Table, error) {
	return nil, nil
}

func (db *clickhouse) GenScanResult(colType string) (interface{}, error) {
	switch colType {
	case "Date", "DateTime", "DateTime64":
		return &sql.NullString{}, nil
	case "String", "FixedString", "UUID", "Enum":
		return &sql.NullString{}, nil
	case "Int8", "Int16", "Int32":
		return &sql.NullInt32{}, nil
	case "Int64":
		return &sql.NullInt64{}, nil
	case "Int128", "Int256":
		return &big.Int{}, nil
	case "UInt8", "UInt16", "UInt32":
		return &convert.NullUint32{}, nil
	case "UInt64":
		return &convert.NullUint64{}, nil
	case "UInt256":
		return &big.Int{}, nil
	case "Float32", "Float64":
		return &sql.NullFloat64{}, nil
	case "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		return &sql.NullString{}, nil
	default:
		return &sql.RawBytes{}, nil
	}
}

// ParseClickHouse parsed clickhouse connection string
// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
func ParseClickHouse(connStr string) (*URI, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	}
	forms := u.Query()
	return &URI{
		DBType: schemas.CLICKHOUSE,
		Proto:  u.Scheme,
		Host:   u.Hostname(),
		Port:   u.Port(),
		DBName: forms.Get("database"),
		User:   forms.Get("username"),
		Passwd: forms.Get("password"),
	}, nil
}
