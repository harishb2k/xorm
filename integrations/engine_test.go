// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package integrations

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"xorm.io/xorm"
	"xorm.io/xorm/schemas"

	_ "gitee.com/travelliu/dm"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	_ "github.com/ziutek/mymysql/godrv"
	_ "modernc.org/sqlite"
)

func TestPing(t *testing.T) {
	if err := testEngine.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestPingContext(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	ctx, canceled := context.WithTimeout(context.Background(), time.Nanosecond)
	defer canceled()

	time.Sleep(time.Nanosecond)

	err := testEngine.(*xorm.Engine).PingContext(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

func TestAutoTransaction(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	type TestTx struct {
		Id      int64     `xorm:"autoincr pk"`
		Msg     string    `xorm:"varchar(255)"`
		Created time.Time `xorm:"created"`
	}

	assert.NoError(t, testEngine.Sync(new(TestTx)))

	engine := testEngine.(*xorm.Engine)

	// will success
	_, err := engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err := session.Insert(TestTx{Msg: "hi"})
		assert.NoError(t, err)

		return nil, nil
	})
	assert.NoError(t, err)

	has, err := engine.Exist(&TestTx{Msg: "hi"})
	assert.NoError(t, err)
	assert.EqualValues(t, true, has)

	// will rollback
	_, err = engine.Transaction(func(session *xorm.Session) (interface{}, error) {
		_, err := session.Insert(TestTx{Msg: "hello"})
		assert.NoError(t, err)

		return nil, fmt.Errorf("rollback")
	})
	assert.Error(t, err)

	has, err = engine.Exist(&TestTx{Msg: "hello"})
	assert.NoError(t, err)
	assert.EqualValues(t, false, has)
}

func assertSync(t *testing.T, beans ...interface{}) {
	for _, bean := range beans {
		t.Run(testEngine.TableName(bean, true), func(t *testing.T) {
			assert.NoError(t, testEngine.DropTables(bean))
			assert.NoError(t, testEngine.Sync(bean))
		})
	}
}

func TestDump(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	type TestDumpStruct struct {
		Id      int64
		Name    string
		IsMan   bool
		Created time.Time `xorm:"created"`
	}

	assertSync(t, new(TestDumpStruct))

	cnt, err := testEngine.Insert([]TestDumpStruct{
		{Name: "1", IsMan: true},
		{Name: "2\n"},
		{Name: "3;"},
		{Name: "4\n;\n''"},
		{Name: "5'\n"},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 5, cnt)

	fp := fmt.Sprintf("%v.sql", testEngine.Dialect().URI().DBType)
	os.Remove(fp)
	assert.NoError(t, testEngine.DumpAllToFile(fp))

	assert.NoError(t, PrepareEngine())

	sess := testEngine.NewSession()
	defer sess.Close()
	assert.NoError(t, sess.Begin())
	_, err = sess.ImportFile(fp)
	assert.NoError(t, err)
	assert.NoError(t, sess.Commit())

	for _, tp := range []schemas.DBType{schemas.SQLITE, schemas.MYSQL, schemas.POSTGRES, schemas.MSSQL} {
		name := fmt.Sprintf("dump_%v.sql", tp)
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, testEngine.DumpAllToFile(name, tp))
		})
	}
}

var dbtypes = []schemas.DBType{schemas.SQLITE, schemas.MYSQL, schemas.POSTGRES, schemas.MSSQL}

func TestDumpTables(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	type TestDumpTableStruct struct {
		Id      int64
		Name    string
		IsMan   bool
		Created time.Time `xorm:"created"`
	}

	assertSync(t, new(TestDumpTableStruct))

	_, err := testEngine.Insert([]TestDumpTableStruct{
		{Name: "1", IsMan: true},
		{Name: "2\n"},
		{Name: "3;"},
		{Name: "4\n;\n''"},
		{Name: "5'\n"},
	})
	assert.NoError(t, err)

	fp := fmt.Sprintf("%v-table.sql", testEngine.Dialect().URI().DBType)
	os.Remove(fp)
	tb, err := testEngine.TableInfo(new(TestDumpTableStruct))
	assert.NoError(t, err)
	assert.NoError(t, testEngine.(*xorm.Engine).DumpTablesToFile([]*schemas.Table{tb}, fp))

	assert.NoError(t, PrepareEngine())

	sess := testEngine.NewSession()
	defer sess.Close()
	assert.NoError(t, sess.Begin())
	_, err = sess.ImportFile(fp)
	assert.NoError(t, err)
	assert.NoError(t, sess.Commit())

	for _, tp := range dbtypes {
		name := fmt.Sprintf("dump_%v-table.sql", tp)
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, testEngine.(*xorm.Engine).DumpTablesToFile([]*schemas.Table{tb}, name, tp))
		})
	}

	assert.NoError(t, testEngine.DropTables(new(TestDumpTableStruct)))

	importPath := fmt.Sprintf("dump_%v-table.sql", testEngine.Dialect().URI().DBType)
	t.Run("import_"+importPath, func(t *testing.T) {
		sess := testEngine.NewSession()
		defer sess.Close()
		assert.NoError(t, sess.Begin())
		_, err = sess.ImportFile(importPath)
		assert.NoError(t, err)
		assert.NoError(t, sess.Commit())
	})
}

func TestDumpTables2(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	type TestDumpTableStruct2 struct {
		Id      int64
		Created time.Time `xorm:"Default CURRENT_TIMESTAMP"`
	}

	assertSync(t, new(TestDumpTableStruct2))

	fp := fmt.Sprintf("./dump2-%v-table.sql", testEngine.Dialect().URI().DBType)
	os.Remove(fp)
	tb, err := testEngine.TableInfo(new(TestDumpTableStruct2))
	assert.NoError(t, err)
	assert.NoError(t, testEngine.(*xorm.Engine).DumpTablesToFile([]*schemas.Table{tb}, fp))
}

func TestSetSchema(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	if testEngine.Dialect().URI().DBType == schemas.POSTGRES {
		oldSchema := testEngine.Dialect().URI().Schema
		testEngine.SetSchema("my_schema")
		assert.EqualValues(t, "my_schema", testEngine.Dialect().URI().Schema)
		testEngine.SetSchema(oldSchema)
		assert.EqualValues(t, oldSchema, testEngine.Dialect().URI().Schema)
	}
}

func TestImport(t *testing.T) {
	if testEngine.Dialect().URI().DBType != schemas.MYSQL {
		t.Skip()
		return
	}
	sess := testEngine.NewSession()
	defer sess.Close()
	assert.NoError(t, sess.Begin())
	_, err := sess.ImportFile("./testdata/import1.sql")
	assert.NoError(t, err)
	assert.NoError(t, sess.Commit())

	assert.NoError(t, sess.Begin())
	_, err = sess.ImportFile("./testdata/import2.sql")
	assert.NoError(t, err)
	assert.NoError(t, sess.Commit())
}

func TestDBVersion(t *testing.T) {
	assert.NoError(t, PrepareEngine())

	version, err := testEngine.DBVersion()
	assert.NoError(t, err)

	fmt.Println(testEngine.Dialect().URI().DBType, "version is", version)
}

func TestGetColumns(t *testing.T) {
	if testEngine.Dialect().URI().DBType != schemas.POSTGRES {
		t.Skip()
		return
	}
	type TestCommentStruct struct {
		HasComment int
		NoComment  int
	}

	assertSync(t, new(TestCommentStruct))

	comment := "this is a comment"
	sql := fmt.Sprintf("comment on column %s.%s is '%s'", testEngine.TableName(new(TestCommentStruct), true), "has_comment", comment)
	_, err := testEngine.Exec(sql)
	assert.NoError(t, err)

	tables, err := testEngine.DBMetas()
	assert.NoError(t, err)
	tableName := testEngine.GetColumnMapper().Obj2Table("TestCommentStruct")
	var hasComment, noComment string
	for _, table := range tables {
		if table.Name == tableName {
			col := table.GetColumn("has_comment")
			assert.NotNil(t, col)
			hasComment = col.Comment
			col2 := table.GetColumn("no_comment")
			assert.NotNil(t, col2)
			noComment = col2.Comment
			break
		}
	}
	assert.Equal(t, comment, hasComment)
	assert.Zero(t, noComment)
}
