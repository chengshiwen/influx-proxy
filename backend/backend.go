// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"io"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"golang.org/x/sync/errgroup"
)

type CacheBuffer struct {
	Buffer  *bytes.Buffer
	Counter int
}

type Backend struct {
	*HttpBackend
	fb   *FileBackend
	pool *ants.Pool

	running         atomic.Value
	flushSize       int
	flushTime       int
	rewriteInterval int
	rewriteThreads  int
	rewriteTicker   *time.Ticker
	chWrite         chan *LinePoint
	chTimer         <-chan time.Time
	buffers         map[string]map[string]*CacheBuffer
	wg              sync.WaitGroup
}

func NewBackend(cfg *BackendConfig, pxcfg *ProxyConfig) (ib *Backend) {
	ib = &Backend{
		HttpBackend:     NewHttpBackend(cfg, pxcfg),
		flushSize:       pxcfg.FlushSize,
		flushTime:       pxcfg.FlushTime,
		rewriteInterval: pxcfg.RewriteInterval,
		rewriteThreads:  pxcfg.RewriteThreads,
		rewriteTicker:   time.NewTicker(time.Duration(pxcfg.RewriteInterval) * time.Second),
		chWrite:         make(chan *LinePoint, 16),
		buffers:         make(map[string]map[string]*CacheBuffer),
	}
	ib.running.Store(true)

	var err error
	ib.fb, err = NewFileBackend(cfg.Name, pxcfg.DataDir)
	if err != nil {
		panic(err)
	}
	ib.pool, err = ants.NewPool(pxcfg.ConnPoolSize)
	if err != nil {
		panic(err)
	}

	go ib.worker()
	return
}

func NewSimpleBackend(cfg *BackendConfig) *Backend {
	return &Backend{HttpBackend: NewSimpleHttpBackend(cfg)}
}

func (ib *Backend) worker() {
	for ib.IsRunning() {
		select {
		case p, ok := <-ib.chWrite:
			if !ok {
				// closed
				ib.Flush()
				ib.wg.Wait()
				ib.HttpBackend.Close()
				ib.fb.Close()
				ib.pool.Release()
				return
			}
			ib.WriteBuffer(p)

		case <-ib.chTimer:
			ib.Flush()
			if !ib.IsRunning() {
				ib.wg.Wait()
				ib.HttpBackend.Close()
				ib.fb.Close()
				ib.pool.Release()
				return
			}

		case <-ib.rewriteTicker.C:
			ib.RewriteIdle()
		}
	}
}

func (ib *Backend) WritePoint(point *LinePoint) (err error) {
	if !ib.IsRunning() {
		return io.ErrClosedPipe
	}
	ib.chWrite <- point
	return
}

func (ib *Backend) WriteBuffer(point *LinePoint) (err error) {
	org, bucket, line := point.Org, point.Bucket, point.Line
	// it's thread-safe since ib.buffers is only used (read-write) in ib.worker() goroutine
	if _, ok := ib.buffers[org]; !ok {
		ib.buffers[org] = make(map[string]*CacheBuffer)
	}
	if _, ok := ib.buffers[org][bucket]; !ok {
		ib.buffers[org][bucket] = &CacheBuffer{Buffer: &bytes.Buffer{}}
	}
	cb := ib.buffers[org][bucket]
	cb.Counter++
	if cb.Buffer == nil {
		cb.Buffer = &bytes.Buffer{}
	}
	n, err := cb.Buffer.Write(line)
	if err != nil {
		log.Printf("buffer write error: %s", err)
		return
	}
	if n != len(line) {
		err = io.ErrShortWrite
		log.Printf("buffer write error: %s", err)
		return
	}
	if line[len(line)-1] != '\n' {
		err = cb.Buffer.WriteByte('\n')
		if err != nil {
			log.Printf("buffer write error: %s", err)
			return
		}
	}

	switch {
	case cb.Counter >= ib.flushSize:
		ib.FlushBuffer(org, bucket)
	case ib.chTimer == nil:
		ib.chTimer = time.After(time.Duration(ib.flushTime) * time.Second)
	}
	return
}

func (ib *Backend) FlushBuffer(org, bucket string) {
	cb := ib.buffers[org][bucket]
	if cb.Buffer == nil {
		return
	}
	p := cb.Buffer.Bytes()
	cb.Buffer = nil
	cb.Counter = 0
	if len(p) == 0 {
		return
	}

	ib.wg.Add(1)
	ib.pool.Submit(func() {
		defer ib.wg.Done()
		var buf bytes.Buffer
		err := Compress(&buf, p)
		if err != nil {
			log.Print("compress buffer error: ", err)
			return
		}

		p = buf.Bytes()

		if ib.IsActive() {
			err = ib.WriteCompressed(org, bucket, p)
			switch err {
			case nil:
				return
			case ErrBadRequest:
				log.Printf("bad request, drop all data")
				return
			case ErrNotFound:
				log.Printf("bad backend, drop all data")
				return
			case ErrUnprocessableEntity:
				log.Printf("bad entity, drop partial data")
				return
			default:
				log.Printf("write http error, url: %s, org: %s, bucket: %s, plen: %d", ib.Url, org, bucket, len(p))
			}
		}

		b := bytes.Join([][]byte{[]byte(url.QueryEscape(org)), []byte(url.QueryEscape(bucket)), p}, []byte{' '})
		err = ib.fb.Write(b)
		if err != nil {
			log.Printf("write db and data to file error: %s, org: %s, bucket: %s, plen: %d", err, org, bucket, len(p))
			return
		}
	})
}

func (ib *Backend) Flush() {
	ib.chTimer = nil
	for org := range ib.buffers {
		for bucket := range ib.buffers[org] {
			if ib.buffers[org][bucket].Counter > 0 {
				ib.FlushBuffer(org, bucket)
			}
		}
	}
}

func (ib *Backend) RewriteIdle() {
	if !ib.IsRewriting() && ib.fb.IsData() {
		ib.SetRewriting(true)
		go ib.RewriteLoop()
	}
}

func (ib *Backend) RewriteLoop() {
	defer ib.SetRewriting(false)
	for ib.fb.IsData() {
		if !ib.IsRunning() {
			return
		}
		if !ib.IsActive() {
			time.Sleep(time.Duration(ib.rewriteInterval) * time.Second)
			continue
		}
		err := ib.Rewrite()
		if err != nil {
			time.Sleep(time.Duration(ib.rewriteInterval) * time.Second)
			continue
		}
	}
}

func (ib *Backend) Rewrite() (err error) {
	blocks, err := ib.fb.ReadN(ib.rewriteThreads)
	if err != nil {
		log.Print("rewrite read file error: ", err)
		return
	}
	if len(blocks) == 0 {
		return
	}

	var g errgroup.Group
	for _, b := range blocks {
		b := b
		g.Go(func() error {
			return ib.rewrite(b)
		})
	}

	if err = g.Wait(); err != nil {
		err = ib.fb.RollbackMeta()
		if err != nil {
			log.Printf("rollback meta error: %s", err)
		}
		return
	}

	err = ib.fb.UpdateMeta()
	if err != nil {
		log.Printf("update meta error: %s", err)
	}
	return
}

func (ib *Backend) rewrite(b []byte) (err error) {
	p := bytes.SplitN(b, []byte{' '}, 3)
	if len(p) < 3 {
		log.Print("rewrite read invalid data with length: ", len(p))
		return
	}
	org, err := url.QueryUnescape(string(p[0]))
	if err != nil {
		log.Print("rewrite org unescape error: ", err)
		return
	}
	bucket, err := url.QueryUnescape(string(p[1]))
	if err != nil {
		log.Print("rewrite bucket unescape error: ", err)
		return
	}
	err = ib.WriteCompressed(org, bucket, p[2])

	switch err {
	case nil:
	case ErrBadRequest:
		log.Printf("bad request, drop all data")
		err = nil
	case ErrNotFound:
		log.Printf("bad backend, drop all data")
		err = nil
	case ErrUnprocessableEntity:
		log.Printf("bad entity, drop partial data")
		err = nil
	default:
		log.Printf("rewrite http error, url: %s, org: %s, bucket: %s, plen: %d", ib.Url, org, bucket, len(p[1]))
	}
	return
}

func (ib *Backend) IsRunning() (b bool) {
	return ib.running.Load().(bool)
}

func (ib *Backend) Close() {
	ib.running.Store(false)
	close(ib.chWrite)
}

func (ib *Backend) GetHealth() interface{} {
	health := struct {
		Name      string `json:"name"`
		Url       string `json:"url"` //nolint:all
		Active    bool   `json:"active"`
		Backlog   bool   `json:"backlog"`
		Rewriting bool   `json:"rewriting"`
		WriteOnly bool   `json:"write_only"`
	}{
		Name:      ib.Name,
		Url:       ib.Url,
		Active:    ib.IsActive(),
		Backlog:   ib.fb.IsData(),
		Rewriting: ib.IsRewriting(),
		WriteOnly: ib.IsWriteOnly(),
	}
	return health
}
