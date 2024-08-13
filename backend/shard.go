// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "strings"

type shardTpl struct {
	tpl   string
	items []string
	dbCnt int
	mmCnt int
}

func newShardTpl(tpl string) *shardTpl {
	sTpl := &shardTpl{tpl: tpl}
	for i := 0; i < len(tpl); {
		for j := i; j < len(tpl); {
			if j <= len(tpl)-3 && (tpl[j:j+3] == ShardKeyVarDb || tpl[j:j+3] == ShardKeyVarMm) {
				if j > i {
					sTpl.items = append(sTpl.items, tpl[i:j])
				}
				sTpl.items = append(sTpl.items, tpl[j:j+3])
				if tpl[j:j+3] == ShardKeyVarDb {
					sTpl.dbCnt++
				} else if tpl[j:j+3] == ShardKeyVarMm {
					sTpl.mmCnt++
				}
				i, j = j+3, j+3
				continue
			}
			j++
			if j == len(tpl) {
				sTpl.items = append(sTpl.items, tpl[i:j])
				i = j
				break
			}
		}
	}
	return sTpl
}

func (sTpl *shardTpl) GetKey(db, mm string) string {
	var b strings.Builder
	b.Grow(len(sTpl.tpl) + (len(db)-len(ShardKeyVarDb))*sTpl.dbCnt + (len(mm)-len(ShardKeyVarMm))*sTpl.mmCnt)
	for _, item := range sTpl.items {
		if item == ShardKeyVarDb {
			b.WriteString(db)
		} else if item == ShardKeyVarMm {
			b.WriteString(mm)
		} else {
			b.WriteString(item)
		}
	}
	return b.String()
}
