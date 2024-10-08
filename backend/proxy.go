// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chengshiwen/influx-proxy/util"
	"github.com/influxdata/influxdb1-client/models"
)

type Proxy struct {
	Circles []*Circle
	dbSet   util.Set
	sTpl    *shardTpl
}

func NewProxy(cfg *ProxyConfig) (ip *Proxy) {
	err := util.MakeDir(cfg.DataDir)
	if err != nil {
		log.Fatalf("create data dir error: %s", err)
		return
	}
	ip = &Proxy{
		Circles: make([]*Circle, len(cfg.Circles)),
		dbSet:   util.NewSet(),
		sTpl:    newShardTpl(cfg.ShardKey),
	}
	for idx, circfg := range cfg.Circles {
		ip.Circles[idx] = NewCircle(circfg, cfg, idx)
		ip.Circles[idx].getKeyFn = ip.GetKey
	}
	for _, db := range cfg.DBList {
		ip.dbSet.Add(db)
	}
	rand.New(rand.NewSource(time.Now().UnixNano()))
	return
}

func (ip *Proxy) GetKey(db, mm string) string {
	return ip.sTpl.GetKey(db, mm)
}

func (ip *Proxy) GetBackends(key string) []*Backend {
	backends := make([]*Backend, len(ip.Circles))
	for i, circle := range ip.Circles {
		backends[i] = circle.GetBackend(key)
	}
	return backends
}

func (ip *Proxy) GetAllBackends() []*Backend {
	capacity := 0
	for _, circle := range ip.Circles {
		capacity += len(circle.Backends)
	}
	backends := make([]*Backend, 0, capacity)
	for _, circle := range ip.Circles {
		backends = append(backends, circle.Backends...)
	}
	return backends
}

func (ip *Proxy) GetHealth(stats bool) []interface{} {
	var wg sync.WaitGroup
	health := make([]interface{}, len(ip.Circles))
	for i, c := range ip.Circles {
		wg.Add(1)
		go func(i int, c *Circle) {
			defer wg.Done()
			health[i] = c.GetHealth(stats)
		}(i, c)
	}
	wg.Wait()
	return health
}

func (ip *Proxy) IsForbiddenDB(db string) bool {
	return len(ip.dbSet) > 0 && !ip.dbSet[db]
}

func (ip *Proxy) QueryFlux(w http.ResponseWriter, req *http.Request, qr *QueryRequest) (err error) {
	var bucket, measurement string
	if qr.Query != "" {
		bucket, measurement, err = ScanQuery(qr.Query)
	} else if qr.Spec != nil {
		bucket, measurement, err = ScanSpec(qr.Spec)
	}
	if err != nil {
		return
	}
	if bucket == "" {
		return ErrGetBucket
	} else if ip.IsForbiddenDB(bucket) {
		return fmt.Errorf("database forbidden: %s", bucket)
	}
	if measurement == "" {
		return ErrGetMeasurement
	}
	return QueryFlux(w, req, ip, bucket, measurement)
}

func (ip *Proxy) Query(w http.ResponseWriter, req *http.Request) (body []byte, err error) {
	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		return nil, ErrEmptyQuery
	}

	tokens, check, from := CheckQuery(q)
	if !check {
		return nil, ErrIllegalQL
	}

	checkDb, showDb, alterDb, db := CheckDatabaseFromTokens(tokens)
	if !checkDb {
		db, _ = GetDatabaseFromTokens(tokens)
		if db == "" {
			db = req.FormValue("db")
		}
	}
	if !showDb {
		if db == "" {
			return nil, ErrDatabaseNotFound
		}
		if ip.IsForbiddenDB(db) {
			return nil, fmt.Errorf("database forbidden: %s", db)
		}
	}

	selectOrShow := CheckSelectOrShowFromTokens(tokens)
	if selectOrShow && from {
		return QueryFromQL(w, req, ip, tokens, db)
	} else if selectOrShow && !from {
		return QueryShowQL(w, req, ip, tokens)
	} else if CheckDeleteOrDropMeasurementFromTokens(tokens) {
		return QueryDeleteOrDropQL(w, req, ip, tokens, db)
	} else if alterDb || CheckRetentionPolicyFromTokens(tokens) {
		return QueryAlterQL(w, req, ip)
	}
	return nil, ErrIllegalQL
}

func (ip *Proxy) Write(p []byte, db, rp, precision string) (err error) {
	var (
		pos   int
		block []byte
	)
	for pos < len(p) {
		pos, block = ScanLine(p, pos)
		pos++

		if len(block) == 0 {
			continue
		}
		start := SkipWhitespace(block, 0)
		if start >= len(block) || block[start] == '#' {
			continue
		}
		if block[len(block)-1] == '\n' {
			block = block[:len(block)-1]
		}

		line := make([]byte, len(block[start:]))
		copy(line, block[start:])
		ip.WriteRow(line, db, rp, precision)
	}
	return
}

func (ip *Proxy) WriteRow(line []byte, db, rp, precision string) {
	nanoLine := AppendNano(line, precision)
	mm, err := ScanKey(nanoLine)
	if err != nil {
		log.Printf("scan key error: %s", err)
		return
	}
	if !RapidCheck(nanoLine[len(mm):]) {
		log.Printf("invalid format, db: %s, rp: %s, precision: %s, line: %s", db, rp, precision, string(line))
		return
	}

	key := ip.GetKey(db, mm)
	backends := ip.GetBackends(key)
	if len(backends) == 0 {
		log.Printf("write data error: can't get backends, db: %s, mm: %s", db, mm)
		return
	}

	point := &LinePoint{db, rp, nanoLine}
	for _, be := range backends {
		err = be.WritePoint(point)
		if err != nil {
			log.Printf("write data to buffer error: %s, url: %s, db: %s, rp: %s, precision: %s, line: %s", err, be.Url, db, rp, precision, string(line))
		}
	}
}

func (ip *Proxy) WritePoints(points []models.Point, db, rp string) error {
	var err error
	for _, pt := range points {
		mm := string(pt.Name())
		key := ip.GetKey(db, mm)
		backends := ip.GetBackends(key)
		if len(backends) == 0 {
			log.Printf("write point error: can't get backends, db: %s, mm: %s", db, mm)
			err = ErrEmptyBackends
			continue
		}

		point := &LinePoint{db, rp, []byte(pt.String())}
		for _, be := range backends {
			err = be.WritePoint(point)
			if err != nil {
				log.Printf("write point to buffer error: %s, url: %s, db: %s, rp: %s, point: %s", err, be.Url, db, rp, pt.String())
			}
		}
	}
	return err
}

func (ip *Proxy) ReadProm(w http.ResponseWriter, req *http.Request, db, metric string) (err error) {
	return ReadProm(w, req, ip, db, metric)
}

func (ip *Proxy) Close() {
	for _, c := range ip.Circles {
		c.Close()
	}
}
