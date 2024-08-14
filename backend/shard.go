// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "strings"

type shardTpl struct {
	tpl   string
	parts []string
	dbCnt int
	mmCnt int
}

func newShardTpl(tpl string) *shardTpl {
	st := &shardTpl{tpl: tpl}
	for i := 0; i < len(tpl); {
		for j := i; j < len(tpl); {
			if j <= len(tpl)-3 && (tpl[j:j+3] == ShardKeyVarDb || tpl[j:j+3] == ShardKeyVarMm) {
				if j > i {
					st.parts = append(st.parts, tpl[i:j])
				}
				st.parts = append(st.parts, tpl[j:j+3])
				if tpl[j:j+3] == ShardKeyVarDb {
					st.dbCnt++
				} else if tpl[j:j+3] == ShardKeyVarMm {
					st.mmCnt++
				}
				i, j = j+3, j+3
				continue
			}
			j++
			if j == len(tpl) {
				st.parts = append(st.parts, tpl[i:j])
				i = j
				break
			}
		}
	}
	return st
}

func (st *shardTpl) GetKey(db, mm string) string {
	var b strings.Builder
	b.Grow(len(st.tpl) + (len(db)-len(ShardKeyVarDb))*st.dbCnt + (len(mm)-len(ShardKeyVarMm))*st.mmCnt)
	for _, part := range st.parts {
		if part == ShardKeyVarDb {
			b.WriteString(db)
		} else if part == ShardKeyVarMm {
			b.WriteString(mm)
		} else {
			b.WriteString(part)
		}
	}
	return b.String()
}
