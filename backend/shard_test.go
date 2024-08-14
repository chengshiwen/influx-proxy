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
		db     string
		mm     string
		parts  []string
		dbCnt  int
		mmCnt  int
		render string
	}{
		{
			name:   "test1",
			tpl:    "%db,%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", ",", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "database,measurement",
		},
		{
			name:   "test2",
			tpl:    "shard-%db-%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard-", "%db", "-", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "shard-database-measurement",
		},
		{
			name:   "test3",
			tpl:    "%db-%mm-key",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", "-", "%mm", "-key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "database-measurement-key",
		},
		{
			name:   "test4",
			tpl:    "shard-%db-%mm-key",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard-", "%db", "-", "%mm", "-key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "shard-database-measurement-key",
		},
		{
			name:   "test5",
			tpl:    "shard-%mm-%db-%mm-%db-key",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard-", "%mm", "-", "%db", "-", "%mm", "-", "%db", "-key"},
			dbCnt:  2,
			mmCnt:  2,
			render: "shard-measurement-database-measurement-database-key",
		},
		{
			name:   "test6",
			tpl:    "%db%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "databasemeasurement",
		},
		{
			name:   "test7",
			tpl:    "shard%db%mm",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard", "%db", "%mm"},
			dbCnt:  1,
			mmCnt:  1,
			render: "sharddatabasemeasurement",
		},
		{
			name:   "test8",
			tpl:    "%db%mmkey",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"%db", "%mm", "key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "databasemeasurementkey",
		},
		{
			name:   "test9",
			tpl:    "shard%db%mmkey",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard", "%db", "%mm", "key"},
			dbCnt:  1,
			mmCnt:  1,
			render: "sharddatabasemeasurementkey",
		},
		{
			name:   "test10",
			tpl:    "shard%mm%db%mm%dbkey",
			db:     "database",
			mm:     "measurement",
			parts:  []string{"shard", "%mm", "%db", "%mm", "%db", "key"},
			dbCnt:  2,
			mmCnt:  2,
			render: "shardmeasurementdatabasemeasurementdatabasekey",
		},
	}
	for _, tt := range tests {
		st := newShardTpl(tt.tpl)
		if !slices.Equal(st.parts, tt.parts) || st.dbCnt != tt.dbCnt || st.mmCnt != tt.mmCnt {
			t.Errorf("%v: got %+v, %d, %d, want %+v, %d, %d", tt.name, st.parts, st.dbCnt, st.mmCnt, tt.parts, tt.dbCnt, tt.mmCnt)
		}
		if render := st.GetKey(tt.db, tt.mm); render != tt.render {
			t.Errorf("%v: got %s, want %s", tt.name, render, tt.render)
		}
	}
}

func BenchmarkGetKeyByPlus(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByPlus("database", "measurement")
	}
}

func getKeyByPlus(db, mm string) string {
	return db + "," + mm
}

func BenchmarkGetKeyByBuilder(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByBuilder("database", "measurement")
	}
}

func getKeyByBuilder(db, mm string) string {
	var b strings.Builder
	b.Grow(len(db) + len(mm) + 1)
	b.WriteString(db)
	b.WriteString(",")
	b.WriteString(mm)
	return b.String()
}

func BenchmarkGetKeyByShardTpl(b *testing.B) {
	st := newShardTpl("%db,%mm")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.GetKey("database", "measurement")
	}
}

func BenchmarkGetKeyBySprintf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyBySprintf("database", "measurement")
	}
}

func getKeyBySprintf(db, mm string) string {
	return fmt.Sprintf("%s,%s", db, mm)
}

func BenchmarkGetKeyByReplaceAll(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByReplaceAll("database", "measurement")
	}
}

func getKeyByReplaceAll(db, mm string) string {
	return strings.ReplaceAll(strings.ReplaceAll("%db,%mm", "%db", db), "%mm", mm)
}

func BenchmarkGetKeyByReplacer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getKeyByReplacer("database", "measurement")
	}
}

func getKeyByReplacer(db, mm string) string {
	return strings.NewReplacer("%db", db, "%mm", mm).Replace("%db,%mm")
}
