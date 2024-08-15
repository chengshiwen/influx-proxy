// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestShardTpl(t *testing.T) {
	tests := []struct {
		name   string
		tpl    string
		org    string
		bk     string
		mm     string
		parts  []string
		orgCnt int
		bkCnt  int
		mmCnt  int
		render string
	}{
		{
			name:   "test1",
			tpl:    "%org,%bk,%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", ",", "%bk", ",", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "org,bucket,measurement",
		},
		{
			name:   "test2",
			tpl:    "shard-%org-%bk-%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard-", "%org", "-", "%bk", "-", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shard-org-bucket-measurement",
		},
		{
			name:   "test3",
			tpl:    "%org-%bk-%mm-key",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", "-", "%bk", "-", "%mm", "-key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "org-bucket-measurement-key",
		},
		{
			name:   "test4",
			tpl:    "shard-%org-%bk-%mm-key",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard-", "%org", "-", "%bk", "-", "%mm", "-key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shard-org-bucket-measurement-key",
		},
		{
			name:   "test5",
			tpl:    "shard-%mm-%bk-%org-%mm-%bk-%org-key",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard-", "%mm", "-", "%bk", "-", "%org", "-", "%mm", "-", "%bk", "-", "%org", "-key"},
			orgCnt: 2,
			bkCnt:  2,
			mmCnt:  2,
			render: "shard-measurement-bucket-org-measurement-bucket-org-key",
		},
		{
			name:   "test6",
			tpl:    "%org%bk%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", "%bk", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "orgbucketmeasurement",
		},
		{
			name:   "test7",
			tpl:    "shard%org%bk%mm",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard", "%org", "%bk", "%mm"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shardorgbucketmeasurement",
		},
		{
			name:   "test8",
			tpl:    "%org%bk%mmkey",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"%org", "%bk", "%mm", "key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "orgbucketmeasurementkey",
		},
		{
			name:   "test9",
			tpl:    "shard%org%bk%mmkey",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard", "%org", "%bk", "%mm", "key"},
			orgCnt: 1,
			bkCnt:  1,
			mmCnt:  1,
			render: "shardorgbucketmeasurementkey",
		},
		{
			name:   "test10",
			tpl:    "shard%mm%bk%org%mm%bk%orgkey",
			org:    "org",
			bk:     "bucket",
			mm:     "measurement",
			parts:  []string{"shard", "%mm", "%bk", "%org", "%mm", "%bk", "%org", "key"},
			orgCnt: 2,
			bkCnt:  2,
			mmCnt:  2,
			render: "shardmeasurementbucketorgmeasurementbucketorgkey",
		},
	}
	for _, tt := range tests {
		st := newShardTpl(tt.tpl)
		if !slices.Equal(st.parts, tt.parts) || st.orgCnt != tt.orgCnt || st.bkCnt != tt.bkCnt || st.mmCnt != tt.mmCnt {
			t.Errorf("%v: got %+v, %d, %d, %d, want %+v, %d, %d, %d", tt.name, st.parts, st.orgCnt, st.bkCnt, st.mmCnt, tt.parts, tt.orgCnt, tt.bkCnt, tt.mmCnt)
		}
		if render := st.GetKey(tt.org, tt.bk, tt.mm); render != tt.render {
			t.Errorf("%v: got %s, want %s", tt.name, render, tt.render)
		}
	}
}

func BenchmarkGetKeyByPlus(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByPlus("org", "bk", "measurement")
	}
}

func getKeyByPlus(org, bk, mm string) string {
	return org + "," + bk + "," + mm
}

func BenchmarkGetKeyByBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByBuilder("org", "bk", "measurement")
	}
}

func getKeyByBuilder(org, bk, mm string) string {
	var b strings.Builder
	b.Grow(len(org) + len(bk) + len(mm) + 1)
	b.WriteString(org)
	b.WriteString(",")
	b.WriteString(bk)
	b.WriteString(",")
	b.WriteString(mm)
	return b.String()
}

func BenchmarkGetKeyByShardTpl(b *testing.B) {
	st := newShardTpl("%org,%bk,%mm")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.GetKey("org", "bk", "measurement")
	}
}

func BenchmarkGetKeyBySprintf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyBySprintf("org", "bk", "measurement")
	}
}

func getKeyBySprintf(org, bk, mm string) string {
	return fmt.Sprintf("%s,%s,%s", org, bk, mm)
}

func BenchmarkGetKeyByReplaceAll(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByReplaceAll("org", "bk", "measurement")
	}
}

func getKeyByReplaceAll(org, bk, mm string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll("%org,%bk,%mm", "%org", org), "%bk", bk), "%mm", mm)
}

func BenchmarkGetKeyByReplacer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByReplacer("org", "bk", "measurement")
	}
}

func getKeyByReplacer(org, bk, mm string) string {
	return strings.NewReplacer("%org", org, "%bk", bk, "%mm", mm).Replace("%org,%bk,%mm")
}
