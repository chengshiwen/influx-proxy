// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package transfer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/influxdata/influxdb1-client/models"
	"github.com/panjf2000/ants/v2"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	FieldTypes    = []string{"float", "integer", "string", "boolean"}
	RetryCount    = 10
	RetryInterval = 15
	DefaultWorker = 5
	DefaultBatch  = 20000
	DefaultTick   = int64(0)
	tlog          = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
)

type QueryResult struct {
	Series models.Rows
	Err    error
}

type Transfer struct {
	username     string
	password     string
	authEncrypt  bool
	httpsEnabled bool

	pool         *ants.Pool
	tlogDir      string
	CircleStates []*CircleState
	getKeyFn     func(db, meas string) string
	Worker       int
	Batch        int
	Tick         int64
	Resyncing    bool
	HaAddrs      []string
}

func NewTransfer(cfg *backend.ProxyConfig, circles []*backend.Circle, getKeyFn func(string, string) string) (tx *Transfer) {
	tx = &Transfer{
		tlogDir:      cfg.TLogDir,
		CircleStates: make([]*CircleState, len(cfg.Circles)),
		getKeyFn:     getKeyFn,
		Worker:       DefaultWorker,
		Batch:        DefaultBatch,
		Tick:         DefaultTick,
	}
	for idx, circfg := range cfg.Circles {
		tx.CircleStates[idx] = NewCircleState(circfg, circles[idx])
	}
	return
}

func (tx *Transfer) resetCircleStates() {
	for _, cs := range tx.CircleStates {
		cs.ResetStates()
	}
}

func (tx *Transfer) resetBasicParam() {
	tx.Worker = DefaultWorker
	tx.Batch = DefaultBatch
	tx.Tick = DefaultTick
}

func (tx *Transfer) setLogOutput(name string) {
	logPath := filepath.Join(tx.tlogDir, name)
	if logPath == "" {
		tlog.SetOutput(os.Stdout)
	} else {
		util.MakeDir(tx.tlogDir)
		tlog.SetOutput(&lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     7,
		})
	}
}

func (tx *Transfer) getRetentionPolicies(db string) []string {
	rps := make([]string, 0)
	rpm := make(map[string]bool)
	for _, cs := range tx.CircleStates {
		for _, be := range cs.Backends {
			if be.IsActive() {
				for _, rp := range be.GetRetentionPolicies(db) {
					if _, ok := rpm[rp]; !ok {
						rps = append(rps, rp)
						rpm[rp] = true
					}
				}
			}
		}
	}
	return rps
}

func (tx *Transfer) getDatabases() []string {
	dbs := make([]string, 0)
	dbm := make(map[string]bool)
	for _, cs := range tx.CircleStates {
		for _, be := range cs.Backends {
			if be.IsActive() {
				for _, db := range be.GetDatabases() {
					if _, ok := dbm[db]; !ok {
						dbs = append(dbs, db)
						dbm[db] = true
					}
				}
			}
		}
	}
	return dbs
}

func (tx *Transfer) createDatabases(dbs []string) ([]string, error) {
	if len(dbs) == 0 {
		dbs = tx.getDatabases()
	}
	if len(dbs) > 0 {
		backends := make([]*backend.Backend, 0)
		for _, cs := range tx.CircleStates {
			backends = append(backends, cs.Backends...)
		}
		// create database
		for _, db := range dbs {
			q := fmt.Sprintf("create database \"%s\"", util.EscapeIdentifier(db))
			req := backend.NewQueryRequest("POST", "", q, "")
			_, _, err := backend.QueryInParallel(backends, req, nil, false)
			if err != nil {
				tlog.Printf("create databases error: %s, db: %s, dbs: %v", err, db, dbs)
				return dbs, err
			}
			// create retention policy
			rps := tx.getRetentionPolicies(db)
			tlog.Printf("create retention policy, db: %s, rps: %v", db, rps)
			for _, rp := range rps {
				q = fmt.Sprintf("create retention policy \"%s\" on \"%s\" duration 0s replication 1", util.EscapeIdentifier(rp), util.EscapeIdentifier(db))
				req = backend.NewQueryRequest("POST", "", q, "")
				_, _, err = backend.QueryInParallel(backends, req, nil, false)
				if err != nil {
					tlog.Printf("create retention policy error: %s, db: %s, rp: %s", err, db, rp)
				}
			}
		}
	} else {
		tlog.Printf("databases are empty in all backends")
	}
	return dbs, nil
}

func getBackendUrls(backends []*backend.Backend) []string {
	backendUrls := make([]string, len(backends))
	for k, be := range backends {
		backendUrls[k] = be.Url
	}
	return backendUrls
}

func reformFieldKeys(fieldKeys map[string][]string) map[string]string {
	// The SELECT statement returns all field values if all values have the same type.
	// If field value types differ across shards, InfluxDB first performs any applicable cast operations and
	// then returns all values with the type that occurs first in the following list: float, integer, string, boolean.
	fieldSet := make(map[string]util.Set, len(fieldKeys))
	for field, types := range fieldKeys {
		fieldSet[field] = util.NewSetFromSlice(types)
	}
	fieldMap := make(map[string]string, len(fieldKeys))
	for field, types := range fieldKeys {
		if len(types) == 1 {
			fieldMap[field] = types[0]
		} else {
			for _, dt := range FieldTypes {
				if fieldSet[field][dt] {
					fieldMap[field] = dt
					break
				}
			}
		}
	}
	return fieldMap
}

func (tx *Transfer) write(ch chan *QueryResult, dsts []*backend.Backend, db, rp, meas string, tagMap util.Set, fieldMap map[string]string) error {
	var buf bytes.Buffer
	var wg sync.WaitGroup
	pool, err := ants.NewPool(len(dsts) * 20)
	if err != nil {
		return err
	}
	defer pool.Release()
	for qr := range ch {
		if qr.Err != nil {
			return qr.Err
		}
		serie := qr.Series[0]
		columns := serie.Columns
		valen := len(serie.Values)
		for idx, value := range serie.Values {
			mtagSet := []string{util.EscapeMeasurement(meas)}
			fieldSet := make([]string, 0)
			for i := 1; i < len(value); i++ {
				k := columns[i]
				v := value[i]
				if tagMap[k] {
					if v != nil {
						mtagSet = append(mtagSet, fmt.Sprintf("%s=%s", util.EscapeTag(k), util.EscapeTag(util.CastString(v))))
					}
				} else if vtype, ok := fieldMap[k]; ok {
					if v != nil {
						if vtype == "float" || vtype == "boolean" {
							fieldSet = append(fieldSet, fmt.Sprintf("%s=%v", util.EscapeTag(k), v))
						} else if vtype == "integer" {
							fieldSet = append(fieldSet, fmt.Sprintf("%s=%vi", util.EscapeTag(k), v))
						} else if vtype == "string" {
							fieldSet = append(fieldSet, fmt.Sprintf("%s=\"%s\"", util.EscapeTag(k), models.EscapeStringField(util.CastString(v))))
						}
					}
				}
			}
			mtagStr := strings.Join(mtagSet, ",")
			fieldStr := strings.Join(fieldSet, ",")
			line := fmt.Sprintf("%s %s %v\n", mtagStr, fieldStr, value[0])
			buf.WriteString(line)
			if (idx+1)%DefaultBatch == 0 || idx+1 == valen {
				p := buf.Bytes()
				for _, dst := range dsts {
					dst := dst
					wg.Add(1)
					pool.Submit(func() {
						defer wg.Done()
						var err error
						for i := 0; i <= RetryCount; i++ {
							if i > 0 {
								time.Sleep(time.Duration(RetryInterval) * time.Second)
								tlog.Printf("transfer write retry: %d, err:%s dst:%s db:%s rp:%s meas:%s", i, err, dst.Url, db, rp, meas)
							}
							err = dst.Write(db, rp, p)
							if err == nil {
								break
							}
						}
						if err != nil {
							tlog.Printf("transfer write error: %s, dst:%s db:%s rp:%s meas:%s", err, dst.Url, db, rp, meas)
						}
					})
				}
				buf = bytes.Buffer{}
			}
		}
	}
	wg.Wait()
	return nil
}

func (tx *Transfer) query(ch chan *QueryResult, src *backend.Backend, db, rp, meas string) {
	defer close(ch)
	var rsp *backend.ChunkedResponse
	var err error
	q := fmt.Sprintf("select * from \"%s\".\"%s\"", util.EscapeIdentifier(rp), util.EscapeIdentifier(meas))
	if tx.Tick > 0 {
		q = fmt.Sprintf("%s where time >= %ds", q, tx.Tick)
	}
	for i := 0; i <= RetryCount; i++ {
		if i > 0 {
			time.Sleep(time.Duration(RetryInterval) * time.Second)
			tlog.Printf("transfer query retry: %d, err:%s src:%s db:%s rp:%s meas:%s batch:%d tick:%d", i, err, src.Url, db, rp, meas, tx.Batch, tx.Tick)
		}
		rsp, err = src.QueryChunk("GET", db, q, "ns", tx.Batch)
		if err == nil {
			break
		}
	}
	if err != nil {
		ch <- &QueryResult{Err: err}
		return
	}
	if rsp == nil {
		return
	}
	defer rsp.Close()

	var r *backend.Response
	for {
		r, err = rsp.NextResponse()
		if err != nil {
			if err == io.EOF {
				// End of the streamed response
				return
			}
			// If we got an error while decoding the response, send that back.
			ch <- &QueryResult{Err: err}
			return
		}
		if r == nil {
			return
		}
		if len(r.Results) > 0 && len(r.Results[0].Series) > 0 {
			ch <- &QueryResult{Series: r.Results[0].Series}
		}
		if r.Err != "" {
			ch <- &QueryResult{Err: errors.New(r.Err)}
			return
		}
	}
}

func (tx *Transfer) transfer(src *backend.Backend, dsts []*backend.Backend, db, rp, meas string) error {
	ch := make(chan *QueryResult, 20)
	go tx.query(ch, src, db, rp, meas)

	var tagMap util.Set
	var fieldMap map[string]string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tagKeys := src.GetTagKeys(db, rp, meas)
		tagMap = util.NewSetFromSlice(tagKeys)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		fieldKeys := src.GetFieldKeys(db, rp, meas)
		fieldMap = reformFieldKeys(fieldKeys)
	}()
	wg.Wait()
	return tx.write(ch, dsts, db, rp, meas, tagMap, fieldMap)
}

func (tx *Transfer) submitTransfer(cs *CircleState, src *backend.Backend, dsts []*backend.Backend, db, meas string) {
	rps := src.GetRetentionPolicies(db)
	for _, rp := range rps {
		rp := rp
		cs.wg.Add(1)
		tx.pool.Submit(func() {
			defer cs.wg.Done()
			err := tx.transfer(src, dsts, db, rp, meas)
			if err == nil {
				tlog.Printf("transfer done, src:%s dst:%v db:%s rp:%s meas:%s batch:%d tick:%d", src.Url, getBackendUrls(dsts), db, rp, meas, tx.Batch, tx.Tick)
			} else {
				tlog.Printf("transfer error: %s, src:%s dst:%v db:%s rp:%s meas:%s batch:%d tick:%d", err, src.Url, getBackendUrls(dsts), db, rp, meas, tx.Batch, tx.Tick)
			}
		})
	}
}

func (tx *Transfer) submitCleanup(cs *CircleState, be *backend.Backend, db, meas string) {
	cs.wg.Add(1)
	tx.pool.Submit(func() {
		defer cs.wg.Done()
		_, err := be.DropMeasurement(db, meas)
		if err == nil {
			tlog.Printf("cleanup done, backend:%s db:%s meas:%s", be.Url, db, meas)
		} else {
			tlog.Printf("cleanup error: %s, backend:%s db:%s meas:%s", err, be.Url, db, meas)
		}
	})
}

func (tx *Transfer) runTransfer(cs *CircleState, be *backend.Backend, dbs []string, fn func(*CircleState, *backend.Backend, string, string, []interface{}) bool, args ...interface{}) {
	defer cs.wg.Done()
	if !be.IsActive() {
		tlog.Printf("backend unavailable: %s", be.Url)
		return
	}

	stats := cs.Stats[be.Url]
	stats.DatabaseTotal = int32(len(dbs))
	measures := make([][]string, len(dbs))
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(i int, db string) {
			defer wg.Done()
			measures[i] = be.GetMeasurements(db)
		}(i, db)
	}
	wg.Wait()
	for i := range measures {
		stats.MeasurementTotal += int32(len(measures[i]))
	}

	for i, db := range dbs {
		for _, meas := range measures[i] {
			require := fn(cs, be, db, meas, args)
			if require {
				atomic.AddInt32(&stats.TransferCount, 1)
			} else {
				atomic.AddInt32(&stats.InPlaceCount, 1)
			}
			atomic.AddInt32(&stats.MeasurementDone, 1)
		}
		atomic.AddInt32(&stats.DatabaseDone, 1)
	}
}

func (tx *Transfer) Rebalance(circleId int, backends []*backend.Backend, dbs []string) { //nolint:all
	tx.setLogOutput("rebalance.log")
	dbs, err := tx.createDatabases(dbs)
	if err != nil || len(dbs) == 0 {
		return
	}
	tx.pool, err = ants.NewPool(tx.Worker)
	if err != nil {
		tlog.Printf("new pool error: %s", err)
		return
	}
	defer tx.pool.Release()
	tlog.Printf("rebalance start: circle %d", circleId)
	cs := tx.CircleStates[circleId]
	tx.resetCircleStates()
	tx.broadcastTransferring(cs, true)
	defer tx.broadcastTransferring(cs, false)

	for _, be := range backends {
		cs.wg.Add(1)
		go tx.runTransfer(cs, be, dbs, tx.runRebalance)
	}
	cs.wg.Wait()
	tx.resetBasicParam()
	tlog.Printf("rebalance done: circle %d", circleId)
}

func (tx *Transfer) runRebalance(cs *CircleState, be *backend.Backend, db string, meas string, _ []interface{}) (require bool) {
	key := tx.getKeyFn(db, meas)
	dst := cs.GetBackend(key)
	require = dst.Url != be.Url
	if require {
		tx.submitTransfer(cs, be, []*backend.Backend{dst}, db, meas)
	}
	return
}

func (tx *Transfer) Recovery(fromCircleId, toCircleId int, backendUrls []string, dbs []string) { //nolint:all
	tx.setLogOutput("recovery.log")
	dbs, err := tx.createDatabases(dbs)
	if err != nil || len(dbs) == 0 {
		return
	}
	tx.pool, err = ants.NewPool(tx.Worker)
	if err != nil {
		tlog.Printf("new pool error: %s", err)
		return
	}
	defer tx.pool.Release()
	tlog.Printf("recovery start: circle from %d to %d", fromCircleId, toCircleId)
	fcs := tx.CircleStates[fromCircleId]
	tcs := tx.CircleStates[toCircleId]
	tx.resetCircleStates()
	tx.broadcastTransferring(tcs, true)
	defer tx.broadcastTransferring(tcs, false)

	backendUrlSet := util.NewSet() //nolint:all
	if len(backendUrls) != 0 {
		for _, u := range backendUrls {
			backendUrlSet.Add(u)
		}
	} else {
		for _, b := range tcs.Backends {
			backendUrlSet.Add(b.Url)
		}
	}
	for _, be := range fcs.Backends {
		fcs.wg.Add(1)
		go tx.runTransfer(fcs, be, dbs, tx.runRecovery, tcs, backendUrlSet)
	}
	fcs.wg.Wait()
	tx.resetBasicParam()
	tlog.Printf("recovery done: circle from %d to %d", fromCircleId, toCircleId)
}

func (tx *Transfer) runRecovery(fcs *CircleState, be *backend.Backend, db string, meas string, args []interface{}) (require bool) {
	tcs := args[0].(*CircleState)
	backendUrlSet := args[1].(util.Set) //nolint:all
	key := tx.getKeyFn(db, meas)
	dst := tcs.GetBackend(key)
	require = backendUrlSet[dst.Url]
	if require {
		tx.submitTransfer(fcs, be, []*backend.Backend{dst}, db, meas)
	}
	return
}

func (tx *Transfer) Resync(dbs []string) {
	tx.setLogOutput("resync.log")
	dbs, err := tx.createDatabases(dbs)
	if err != nil || len(dbs) == 0 {
		return
	}
	tx.pool, err = ants.NewPool(tx.Worker)
	if err != nil {
		tlog.Printf("new pool error: %s", err)
		return
	}
	defer tx.pool.Release()
	tlog.Printf("resync start")
	tx.resetCircleStates()
	tx.broadcastResyncing(true)
	defer tx.broadcastResyncing(false)

	for _, cs := range tx.CircleStates {
		tlog.Printf("resync start: circle %d", cs.CircleId)
		for _, be := range cs.Backends {
			cs.wg.Add(1)
			go tx.runTransfer(cs, be, dbs, tx.runResync)
		}
		cs.wg.Wait()
		tlog.Printf("resync done: circle %d", cs.CircleId)
	}
	tx.resetBasicParam()
	tlog.Printf("resync done")
}

func (tx *Transfer) runResync(cs *CircleState, be *backend.Backend, db string, meas string, _ []interface{}) (require bool) {
	key := tx.getKeyFn(db, meas)
	dsts := make([]*backend.Backend, 0)
	for _, tcs := range tx.CircleStates {
		if tcs.CircleId != cs.CircleId {
			dst := tcs.GetBackend(key)
			dsts = append(dsts, dst)
		}
	}
	require = len(dsts) > 0
	if require {
		tx.submitTransfer(cs, be, dsts, db, meas)
	}
	return
}

func (tx *Transfer) Cleanup(circleId int) { //nolint:all
	tx.setLogOutput("cleanup.log")
	var err error
	tx.pool, err = ants.NewPool(tx.Worker)
	if err != nil {
		tlog.Printf("new pool error: %s", err)
		return
	}
	defer tx.pool.Release()
	tlog.Printf("cleanup start: circle %d", circleId)
	cs := tx.CircleStates[circleId]
	tx.resetCircleStates()
	tx.broadcastTransferring(cs, true)
	defer tx.broadcastTransferring(cs, false)

	for _, be := range cs.Backends {
		dbs := be.GetDatabases()
		if len(dbs) > 0 {
			cs.wg.Add(1)
			go tx.runTransfer(cs, be, dbs, tx.runCleanup)
		}
	}
	cs.wg.Wait()
	tx.resetBasicParam()
	tlog.Printf("cleanup done: circle %d", circleId)
}

func (tx *Transfer) runCleanup(cs *CircleState, be *backend.Backend, db string, meas string, _ []interface{}) (require bool) {
	key := tx.getKeyFn(db, meas)
	dst := cs.GetBackend(key)
	require = dst.Url != be.Url
	if require {
		tlog.Printf("backend:%s db:%s meas:%s require to cleanup", be.Url, db, meas)
		tx.submitCleanup(cs, be, db, meas)
	} else {
		tlog.Printf("backend:%s db:%s meas:%s checked", be.Url, db, meas)
	}
	return
}

func (tx *Transfer) broadcastResyncing(resyncing bool) {
	tx.Resyncing = resyncing
	client := backend.NewClient(tx.httpsEnabled, 10)
	for _, addr := range tx.HaAddrs {
		url := fmt.Sprintf("http://%s/transfer/state?resyncing=%t", addr, resyncing)
		tx.postBroadcast(client, url)
	}
}

func (tx *Transfer) broadcastTransferring(cs *CircleState, transferring bool) {
	cs.Transferring = transferring
	cs.SetTransferIn(transferring)
	client := backend.NewClient(tx.httpsEnabled, 10)
	for _, addr := range tx.HaAddrs {
		url := fmt.Sprintf("http://%s/transfer/state?circle_id=%d&transferring=%t", addr, cs.CircleId, transferring)
		tx.postBroadcast(client, url)
	}
}

func (tx *Transfer) postBroadcast(client *http.Client, url string) {
	if tx.httpsEnabled {
		url = strings.Replace(url, "http", "https", 1)
	}
	req, _ := http.NewRequest("POST", url, nil)
	if tx.username != "" || tx.password != "" {
		backend.SetBasicAuth(req, tx.username, tx.password, tx.authEncrypt)
	}
	client.Do(req)
}
