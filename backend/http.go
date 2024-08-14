// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrBadRequest          = errors.New("bad request")
	ErrUnauthorized        = errors.New("unauthorized")
	ErrForbidden           = errors.New("forbidden")
	ErrNotFound            = errors.New("not found")
	ErrTooLarge            = errors.New("request too large")
	ErrUnprocessableEntity = errors.New("unprocessable entity")
	ErrTooManyRequests     = errors.New("too many requests")
	ErrInternal            = errors.New("internal error")
	ErrUnavailable         = errors.New("unavailable error")
	ErrUnknown             = errors.New("unknown error")
)

const (
	HeaderQueryOrigin = "Query-Origin"
	QueryParallel     = "Parallel"
)

type QueryResult struct {
	Header http.Header
	Status int
	Body   []byte
	Err    error
}

type HttpBackend struct { //nolint:all
	client     *http.Client
	transport  *http.Transport
	Name       string
	Url        string //nolint:all
	token      string
	interval   int
	running    atomic.Value
	active     atomic.Value
	rewriting  atomic.Value
	transferIn atomic.Value
	writeOnly  bool
}

func NewHttpBackend(cfg *BackendConfig, pxcfg *ProxyConfig) (hb *HttpBackend) { //nolint:all
	hb = NewSimpleHttpBackend(cfg)
	hb.client = NewClient(strings.HasPrefix(cfg.Url, "https"), pxcfg.WriteTimeout)
	hb.interval = pxcfg.CheckInterval
	go hb.CheckActive()
	return
}

func NewSimpleHttpBackend(cfg *BackendConfig) (hb *HttpBackend) { //nolint:all
	hb = &HttpBackend{
		transport: NewTransport(strings.HasPrefix(cfg.Url, "https")),
		Name:      cfg.Name,
		Url:       cfg.Url,
		token:     cfg.Token,
		writeOnly: cfg.WriteOnly,
	}
	hb.running.Store(true)
	hb.active.Store(true)
	hb.rewriting.Store(false)
	hb.transferIn.Store(false)
	return
}

func NewClient(tlsSkip bool, timeout int) *http.Client {
	return &http.Client{Transport: NewTransport(tlsSkip), Timeout: time.Duration(timeout) * time.Second}
}

func NewTransport(tlsSkip bool) *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   time.Second * 30,
			KeepAlive: time.Second * 30,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       time.Second * 90,
		TLSHandshakeTimeout:   time.Second * 10,
		ExpectContinueTimeout: time.Second * 1,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: tlsSkip},
	}
}

func CloneQueryRequest(r *http.Request) *http.Request {
	cr := r.Clone(r.Context())
	cr.Body = io.NopCloser(&bytes.Buffer{})
	return cr
}

func Compress(buf *bytes.Buffer, p []byte) (err error) {
	zip := gzip.NewWriter(buf)
	defer zip.Close()
	n, err := zip.Write(p)
	if err != nil {
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
	}
	return
}

func CopyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Set(k, v)
		}
	}
}

func (hb *HttpBackend) SetAuthorization(req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("Token %s", hb.token))
}

func (hb *HttpBackend) CheckActive() {
	for hb.running.Load().(bool) {
		hb.active.Store(hb.Ping())
		time.Sleep(time.Duration(hb.interval) * time.Second)
	}
}

func (hb *HttpBackend) IsActive() (b bool) {
	return hb.active.Load().(bool)
}

func (hb *HttpBackend) IsRewriting() (b bool) {
	return hb.rewriting.Load().(bool)
}

func (hb *HttpBackend) SetRewriting(b bool) {
	hb.rewriting.Store(b)
}

func (hb *HttpBackend) SetTransferIn(b bool) {
	hb.transferIn.Store(b)
}

func (hb *HttpBackend) IsWriteOnly() (b bool) {
	return hb.writeOnly || hb.transferIn.Load().(bool)
}

func (hb *HttpBackend) Ping() bool {
	resp, err := hb.client.Get(hb.Url + "/ping")
	if err != nil {
		log.Print("http error: ", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		log.Printf("ping status code: %d, the backend is %s", resp.StatusCode, hb.Url)
		return false
	}
	return true
}

func (hb *HttpBackend) Write(org, bucket string, p []byte) (err error) {
	var buf bytes.Buffer
	err = Compress(&buf, p)
	if err != nil {
		log.Print("compress error: ", err)
		return
	}
	return hb.WriteStream(org, bucket, &buf, true)
}

func (hb *HttpBackend) WriteCompressed(org, bucket string, p []byte) (err error) {
	buf := bytes.NewBuffer(p)
	return hb.WriteStream(org, bucket, buf, true)
}

func (hb *HttpBackend) WriteStream(org, bucket string, stream io.Reader, compressed bool) (err error) {
	q := url.Values{}
	q.Set("org", org)
	q.Set("bucket", bucket)
	req, err := http.NewRequest("POST", hb.Url+"/api/v2/write?"+q.Encode(), stream)
	if err != nil {
		log.Print("new request error: ", err)
		return nil
	}
	hb.SetAuthorization(req)
	if compressed {
		req.Header.Add("Content-Encoding", "gzip")
	}

	resp, err := hb.client.Do(req)
	if err != nil {
		log.Print("http error: ", err)
		hb.active.Store(false)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return
	}
	log.Printf("write status code: %d, from: %s", resp.StatusCode, hb.Url)

	respbuf, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Print("readall error: ", err)
		return
	}
	log.Printf("error response: %s", respbuf)

	switch resp.StatusCode {
	case 400:
		err = ErrBadRequest
	case 401:
		err = ErrUnauthorized
	case 403:
		err = ErrForbidden
	case 404:
		err = ErrNotFound
	case 413:
		err = ErrTooLarge
	case 422:
		err = ErrUnprocessableEntity
	case 429:
		err = ErrTooManyRequests
	case 500:
		err = ErrInternal
	case 503:
		err = ErrUnavailable
	default: // mostly tcp connection timeout
		err = ErrUnknown
	}
	return
}

func (hb *HttpBackend) QueryFlux(req *http.Request, w http.ResponseWriter) (err error) {
	q := url.Values{}
	q.Set("org", req.URL.Query().Get("org"))
	hb.SetAuthorization(req)

	req.URL, err = url.Parse(hb.Url + "/api/v2/query?" + q.Encode())
	if err != nil {
		log.Print("internal url parse error: ", err)
		return
	}

	resp, err := hb.transport.RoundTrip(req)
	if err != nil {
		log.Printf("flux query error: %s", err)
		return
	}
	defer resp.Body.Close()

	CopyHeader(w.Header(), resp.Header)

	p, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("flux read body error: %s", err)
		return
	}
	w.WriteHeader(resp.StatusCode)
	_, err = w.Write(p)
	return
}

func (hb *HttpBackend) Query(req *http.Request, w http.ResponseWriter, decompress bool) (qr *QueryResult) {
	qr = &QueryResult{}
	if len(req.Form) == 0 {
		req.Form = url.Values{}
	}
	req.Form.Del("u")
	req.Form.Del("p")
	req.ContentLength = 0
	hb.SetAuthorization(req)

	req.URL, qr.Err = url.Parse(hb.Url + "/query?" + req.Form.Encode())
	if qr.Err != nil {
		log.Print("internal url parse error: ", qr.Err)
		return
	}

	q := strings.TrimSpace(req.FormValue("q"))
	resp, err := hb.transport.RoundTrip(req)
	if err != nil {
		if req.Header.Get(HeaderQueryOrigin) != QueryParallel || err.Error() != "context canceled" {
			qr.Err = err
			log.Printf("query error: %s, the query is %s", err, q)
		}
		return
	}
	defer resp.Body.Close()
	if w != nil {
		CopyHeader(w.Header(), resp.Header)
	}

	respBody := resp.Body
	if decompress && resp.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(resp.Body)
		if err != nil {
			qr.Err = err
			log.Printf("unable to decode gzip body: %s", err)
			return
		}
		defer b.Close()
		respBody = b
	}

	qr.Body, qr.Err = io.ReadAll(respBody)
	if qr.Err != nil {
		log.Printf("read body error: %s, the query is %s", qr.Err, q)
		return
	}
	if resp.StatusCode >= 400 {
		rsp, _ := ResponseFromResponseBytes(qr.Body)
		qr.Err = errors.New(rsp.Message)
	}
	qr.Header = resp.Header
	qr.Status = resp.StatusCode
	return
}

func (hb *HttpBackend) Close() {
	hb.running.Store(false)
	hb.transport.CloseIdleConnections()
}
