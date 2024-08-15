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
)

type Proxy struct {
	Circles []*Circle
	dbrps   map[string][]string
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
		dbrps:   make(map[string][]string),
		sTpl:    newShardTpl(cfg.ShardKey),
	}
	for idx, circfg := range cfg.Circles {
		ip.Circles[idx] = NewCircle(circfg, cfg, idx)
	}
	for key, value := range cfg.DBRP.Mapping {
		ip.dbrps[key] = strings.Split(value, cfg.DBRP.Separator)
	}
	rand.New(rand.NewSource(time.Now().UnixNano()))
	return
}

func (ip *Proxy) GetKey(org, bk, mm string) string {
	return ip.sTpl.GetKey(org, bk, mm)
}

func (ip *Proxy) DBRP2OrgBucket(db, rp string) (string, string, error) {
	dbrp := strings.TrimRight(fmt.Sprintf("%s/%s", db, rp), "/")
	if v, ok := ip.dbrps[dbrp]; ok {
		return v[0], v[1], nil
	}
	return "", "", ErrDBRPNotMapping
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

func (ip *Proxy) GetHealth() []interface{} {
	var wg sync.WaitGroup
	health := make([]interface{}, len(ip.Circles))
	for i, c := range ip.Circles {
		wg.Add(1)
		go func(i int, c *Circle) {
			defer wg.Done()
			health[i] = c.GetHealth()
		}(i, c)
	}
	wg.Wait()
	return health
}

func (ip *Proxy) QueryFlux(w http.ResponseWriter, req *http.Request, org string, qr *QueryRequest) (err error) {
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
	}
	if measurement == "" {
		return ErrGetMeasurement
	}
	return QueryFlux(w, req, ip, org, bucket, measurement)
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

	db := req.FormValue("db")
	if db == "" {
		db, _ = GetDatabaseFromTokens(tokens)
	}
	if !CheckShowDatabasesFromTokens(tokens) {
		if db == "" {
			return nil, ErrDatabaseNotFound
		}
	}
	rp := req.FormValue("rp")
	if rp == "" {
		rp, _ = GetRetentionPolicyFromTokens(tokens)
	}

	selectOrShow := CheckSelectOrShowFromTokens(tokens)
	if selectOrShow && from {
		return QueryFromQL(w, req, ip, tokens, db, rp)
	} else if selectOrShow && !from {
		return QueryShowQL(w, req, ip, tokens)
	} else if CheckDeleteOrDropMeasurementFromTokens(tokens) {
		return QueryDeleteOrDropQL(w, req, ip, tokens, db, rp)
	}
	return nil, ErrIllegalQL
}

func (ip *Proxy) Write(p []byte, org, bucket, precision string) (err error) {
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
		ip.WriteRow(line, org, bucket, precision)
	}
	return
}

func (ip *Proxy) WriteRow(line []byte, org, bucket, precision string) {
	nanoLine := AppendNano(line, precision)
	measurement, err := ScanKey(nanoLine)
	if err != nil {
		log.Printf("scan key error: %s", err)
		return
	}
	if !RapidCheck(nanoLine[len(measurement):]) {
		log.Printf("invalid format, org: %s, bucket: %s, precision: %s, line: %s", org, bucket, precision, string(line))
		return
	}

	key := ip.GetKey(org, bucket, measurement)
	backends := ip.GetBackends(key)
	if len(backends) == 0 {
		log.Printf("write data error: can't get backends, org: %s, bucket: %s, measurement: %s", org, bucket, measurement)
		return
	}

	point := &LinePoint{org, bucket, nanoLine}
	for _, be := range backends {
		err = be.WritePoint(point)
		if err != nil {
			log.Printf("write data to buffer error: %s, url: %s, org: %s, bucket: %s, precision: %s, line: %s", err, be.Url, org, bucket, precision, string(line))
		}
	}
}

func (ip *Proxy) Close() {
	for _, c := range ip.Circles {
		c.Close()
	}
}
