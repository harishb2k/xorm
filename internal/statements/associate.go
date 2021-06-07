// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package statements

type cascadeMode int

const (
	cascadeCompitable cascadeMode = iota // load field beans with another SQL with no
	cascadeEager                         // load field beans with another SQL
	cascadeJoin                          // load field beans with join
	cascadeLazy                          // don't load anything
)
