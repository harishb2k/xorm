// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package integrations

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtendsTag(t *testing.T) {
	assert.NoError(t, prepareEngine())

	table := testEngine.TableInfo(new(Userdetail))
	assert.NotNil(t, table)
	assert.EqualValues(t, 3, len(table.ColumnsSeq()))
	assert.EqualValues(t, "id", table.ColumnsSeq()[0])
	assert.EqualValues(t, "intro", table.ColumnsSeq()[1])
	assert.EqualValues(t, "profile", table.ColumnsSeq()[2])

	table = testEngine.TableInfo(new(Userinfo))
	assert.NotNil(t, table)
	assert.EqualValues(t, 8, len(table.ColumnsSeq()))
	assert.EqualValues(t, "id", table.ColumnsSeq()[0])
	assert.EqualValues(t, "username", table.ColumnsSeq()[1])
	assert.EqualValues(t, "departname", table.ColumnsSeq()[2])
	assert.EqualValues(t, "created", table.ColumnsSeq()[3])
	assert.EqualValues(t, "detail_id", table.ColumnsSeq()[4])
	assert.EqualValues(t, "height", table.ColumnsSeq()[5])
	assert.EqualValues(t, "avatar", table.ColumnsSeq()[6])
	assert.EqualValues(t, "is_man", table.ColumnsSeq()[7])

	table = testEngine.TableInfo(new(UserAndDetail))
	assert.NotNil(t, table)
	assert.EqualValues(t, 11, len(table.ColumnsSeq()))
	assert.EqualValues(t, "id", table.ColumnsSeq()[0])
	assert.EqualValues(t, "username", table.ColumnsSeq()[1])
	assert.EqualValues(t, "departname", table.ColumnsSeq()[2])
	assert.EqualValues(t, "created", table.ColumnsSeq()[3])
	assert.EqualValues(t, "detail_id", table.ColumnsSeq()[4])
	assert.EqualValues(t, "height", table.ColumnsSeq()[5])
	assert.EqualValues(t, "avatar", table.ColumnsSeq()[6])
	assert.EqualValues(t, "is_man", table.ColumnsSeq()[7])
	assert.EqualValues(t, "id", table.ColumnsSeq()[8])
	assert.EqualValues(t, "intro", table.ColumnsSeq()[9])
	assert.EqualValues(t, "profile", table.ColumnsSeq()[10])

	cols := table.Columns()
	assert.EqualValues(t, 11, len(cols))
	assert.EqualValues(t, "Userinfo.Uid", cols[0].FieldName)
	assert.EqualValues(t, "Userinfo.Username", cols[1].FieldName)
	assert.EqualValues(t, "Userinfo.Departname", cols[2].FieldName)
	assert.EqualValues(t, "Userinfo.Created", cols[3].FieldName)
	assert.EqualValues(t, "Userinfo.Detail", cols[4].FieldName)
	assert.EqualValues(t, "Userinfo.Height", cols[5].FieldName)
	assert.EqualValues(t, "Userinfo.Avatar", cols[6].FieldName)
	assert.EqualValues(t, "Userinfo.IsMan", cols[7].FieldName)
	assert.EqualValues(t, "Userdetail.Id", cols[8].FieldName)
	assert.EqualValues(t, "Userdetail.Intro", cols[9].FieldName)
	assert.EqualValues(t, "Userdetail.Profile", cols[10].FieldName)
}
