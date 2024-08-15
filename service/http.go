// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package service

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ServeMux struct {
	*http.ServeMux
}

func NewServeMux() *ServeMux {
	return &ServeMux{ServeMux: http.NewServeMux()}
}

func (mux *ServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Influxdb-Version", backend.Version)
	w.Header().Add("X-Influxdb-Build", "InfluxDB Proxy")
	mux.ServeMux.ServeHTTP(w, r)
}

type HttpService struct { //nolint:all
	ip              *backend.Proxy
	token           string
	pingAuthEnabled bool
	writeTracing    bool
	queryTracing    bool
	pprofEnabled    bool
}

func NewHttpService(cfg *backend.ProxyConfig) (hs *HttpService) { //nolint:all
	ip := backend.NewProxy(cfg)
	hs = &HttpService{
		ip:              ip,
		token:           cfg.Token,
		pingAuthEnabled: cfg.PingAuthEnabled,
		writeTracing:    cfg.WriteTracing,
		queryTracing:    cfg.QueryTracing,
		pprofEnabled:    cfg.PprofEnabled,
	}
	return
}

func (hs *HttpService) Register(mux *ServeMux) {
	mux.HandleFunc("/ping", hs.HandlerPing)
	mux.HandleFunc("/query", hs.HandlerQuery)
	mux.HandleFunc("/write", hs.HandlerWrite)
	mux.HandleFunc("/api/v2/query", hs.HandlerQueryV2)
	mux.HandleFunc("/api/v2/write", hs.HandlerWriteV2)
	mux.HandleFunc("/health", hs.HandlerHealth)
	mux.HandleFunc("/replica", hs.HandlerReplica)
	mux.HandleFunc("/metrics", hs.HandlerMetrics)
	if hs.pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	}
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	if hs.isAuthEnabled() && hs.pingAuthEnabled && !hs.checkAuth(w, req) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	db := req.FormValue("db")
	q := req.FormValue("q")
	body, err := hs.ip.Query(w, req)
	if err != nil {
		log.Printf("influxql query error: %s, query: %s, db: %s, client: %s", err, q, db, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	hs.WriteBody(w, body)
	if hs.queryTracing {
		log.Printf("influxql query: %s, db: %s, client: %s", q, db, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerQueryV2(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	// use org, ignore orgID
	org := req.URL.Query().Get("org")
	if org == "" {
		hs.WriteError(w, req, http.StatusBadRequest, "org not found")
		return
	}

	var contentType = "application/json"
	if ct := req.Header.Get("Content-Type"); ct != "" {
		contentType = ct
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rbody, err := io.ReadAll(req.Body)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	qr := &backend.QueryRequest{}
	switch mt {
	case "application/vnd.flux":
		qr.Query = string(rbody)
	case "application/json":
		fallthrough
	default:
		if err = json.Unmarshal(rbody, qr); err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("failed parsing request body as JSON; if sending a raw Flux script, set 'Content-Type: application/vnd.flux' in your request headers: %s", err))
			return
		}
	}

	if qr.Query == "" && qr.Spec == nil {
		hs.WriteError(w, req, http.StatusBadRequest, "request body requires either spec or query")
		return
	}
	if qr.Type != "" && qr.Type != "flux" {
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("unknown query type: %s", qr.Type))
		return
	}

	req.Body = io.NopCloser(bytes.NewBuffer(rbody))
	err = hs.ip.QueryFlux(w, req, org, qr)
	if err != nil {
		log.Printf("flux query error: %s, query: %s, spec: %s, org: %s, client: %s", err, qr.Query, qr.Spec, org, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if hs.queryTracing {
		log.Printf("flux query: %s, spec: %s, org: %s, client: %s", qr.Query, qr.Spec, org, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
		if precision == "" {
			precision = "ns"
		}
	default:
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use n, ns, u, ms, s, m or h)", precision))
		return
	}

	db := req.URL.Query().Get("db")
	if db == "" {
		hs.WriteError(w, req, http.StatusBadRequest, "database not found")
		return
	}
	rp := req.URL.Query().Get("rp")

	org, bucket, err := hs.ip.DBRP2OrgBucket(db, rp)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, "db/rp not mapping")
		return
	}

	hs.handlerWrite(org, bucket, precision, w, req)
}

func (hs *HttpService) HandlerWriteV2(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	switch precision {
	case "", "ns", "us", "ms", "s":
		// it's valid
		if precision == "" {
			precision = "ns"
		}
	default:
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision))
		return
	}

	// use org and bucket, ignore orgID
	org := req.URL.Query().Get("org")
	if org == "" {
		hs.WriteError(w, req, http.StatusBadRequest, "org not found")
		return
	}
	bucket := req.URL.Query().Get("bucket")
	if bucket == "" {
		hs.WriteError(w, req, http.StatusBadRequest, "bucket not found")
		return
	}

	hs.handlerWrite(org, bucket, precision, w, req)
}

func (hs *HttpService) handlerWrite(org, bucket, precision string, w http.ResponseWriter, req *http.Request) {
	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := io.ReadAll(body)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	err = hs.ip.Write(p, org, bucket, precision)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
	}
	if hs.writeTracing {
		log.Printf("write line protocol, org: %s, bucket: %s, precision: %s, data: %s, client: %s", org, bucket, precision, p, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}
	resp := map[string]interface{}{
		"name":    "influx-proxy",
		"message": "ready for queries and writes",
		"status":  "pass",
		"checks":  []string{},
		"circles": hs.ip.GetHealth(),
		"version": backend.Version,
		"commit":  backend.GitCommit,
	}
	hs.Write(w, req, http.StatusOK, resp)
}

func (hs *HttpService) HandlerReplica(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	org := req.URL.Query().Get("org")
	bucket := req.URL.Query().Get("bucket")
	measurement := req.URL.Query().Get("measurement")
	if measurement == "" {
		measurement = req.URL.Query().Get("meas")
	}
	if org != "" && bucket != "" && measurement != "" {
		key := backend.GetKey(org, bucket, measurement)
		backends := hs.ip.GetBackends(key)
		data := make([]map[string]interface{}, len(backends))
		for i, b := range backends {
			c := hs.ip.Circles[i]
			data[i] = map[string]interface{}{
				"backend": map[string]string{"name": b.Name, "url": b.Url},
				"circle":  map[string]interface{}{"id": c.CircleId, "name": c.Name},
			}
		}
		hs.Write(w, req, http.StatusOK, data)
	} else {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid org, bucket or measurement")
	}
}

func (hs *HttpService) HandlerMetrics(w http.ResponseWriter, req *http.Request) {
	if hs.isAuthEnabled() && hs.pingAuthEnabled && !hs.checkAuth(w, req) {
		return
	}
	promhttp.Handler().ServeHTTP(w, req)
}

func (hs *HttpService) Write(w http.ResponseWriter, req *http.Request, status int, data interface{}) {
	if status/100 >= 4 {
		hs.WriteError(w, req, status, data.(string))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(data, pretty))
}

func (hs *HttpService) WriteError(w http.ResponseWriter, req *http.Request, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Influxdb-Error", err)
	w.WriteHeader(status)
	rsp := backend.ResponseFromError(err)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(rsp, pretty))
}

func (hs *HttpService) WriteBody(w http.ResponseWriter, body []byte) {
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (hs *HttpService) WriteText(w http.ResponseWriter, status int, text string) {
	w.WriteHeader(status)
	w.Write([]byte(text + "\n"))
}

func (hs *HttpService) checkMethodAndAuth(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	return hs.checkMethod(w, req, methods...) && hs.checkAuth(w, req)
}

func (hs *HttpService) checkMethod(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	for _, method := range methods {
		if req.Method == method {
			return true
		}
	}
	hs.WriteError(w, req, http.StatusMethodNotAllowed, "method not allow")
	return false
}

func (hs *HttpService) checkAuth(w http.ResponseWriter, req *http.Request) bool {
	if !hs.isAuthEnabled() {
		return true
	}
	token := strings.TrimSpace(strings.TrimPrefix(req.Header.Get("Authorization"), "Token "))
	if token == hs.token {
		return true
	}
	hs.WriteError(w, req, http.StatusUnauthorized, "authentication failed")
	return false
}

func (hs *HttpService) isAuthEnabled() bool {
	return hs.token != ""
}
