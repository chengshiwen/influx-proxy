// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "strings"

type shardTpl struct {
	tpl    string
	parts  []string
	orgCnt int
	bkCnt  int
	mmCnt  int
}

var ShardKeyVar = []string{ShardKeyVarOrg, ShardKeyVarBk, ShardKeyVarMm}

func newShardTpl(tpl string) *shardTpl {
	st := &shardTpl{tpl: tpl}
	for i := 0; i < len(tpl); {
		for j := i; j < len(tpl); {
			found := false
			for _, v := range ShardKeyVar {
				n := len(v)
				if j <= len(tpl)-n && tpl[j:j+n] == v {
					if j > i {
						st.parts = append(st.parts, tpl[i:j])
					}
					st.parts = append(st.parts, tpl[j:j+n])
					if tpl[j:j+n] == ShardKeyVarOrg {
						st.orgCnt++
					} else if tpl[j:j+n] == ShardKeyVarBk {
						st.bkCnt++
					} else if tpl[j:j+n] == ShardKeyVarMm {
						st.mmCnt++
					}
					i, j = j+n, j+n
					found = true
					break
				}
			}
			if found {
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

func (st *shardTpl) GetKey(org, bk, mm string) string {
	var b strings.Builder
	b.Grow(len(st.tpl) + (len(org)-len(ShardKeyVarOrg))*st.orgCnt + (len(bk)-len(ShardKeyVarBk))*st.bkCnt + (len(mm)-len(ShardKeyVarMm))*st.mmCnt)
	for _, part := range st.parts {
		if part == ShardKeyVarOrg {
			b.WriteString(org)
		} else if part == ShardKeyVarBk {
			b.WriteString(bk)
		} else if part == ShardKeyVarMm {
			b.WriteString(mm)
		} else {
			b.WriteString(part)
		}
	}
	return b.String()
}
