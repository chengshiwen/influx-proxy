// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/influxdata/influxdb1-client/models"
	jsoniter "github.com/json-iterator/go"
)

// Message represents a user-facing message to be included with the result.
type Message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

// Result represents a resultset returned from a single statement.
// Rows represents a list of rows that can be sorted consistently by name/tag.
type Result struct {
	// StatementID is just the statement's position in the query. It's used
	// to combine statement results if they're being buffered in memory.
	StatementID int         `json:"statement_id"`
	Series      models.Rows `json:"series,omitempty"`
	Messages    []*Message  `json:"messages,omitempty"`
	Partial     bool        `json:"partial,omitempty"`
	Err         string      `json:"error,omitempty"`
}

// Response represents a list of statement results.
type Response struct {
	Results []*Result `json:"results,omitempty"`
	Err     string    `json:"error,omitempty"`
}

func (rsp *Response) Unmarshal(b []byte) (e error) {
	dec := jsoniter.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(rsp)
}

func SeriesFromResponseBytes(b []byte) (series models.Rows, e error) {
	rsp := &Response{}
	e = rsp.Unmarshal(b)
	if e == nil && len(rsp.Results) > 0 && len(rsp.Results[0].Series) > 0 {
		series = rsp.Results[0].Series
	}
	return
}

func ResultsFromResponseBytes(b []byte) (results []*Result, e error) {
	rsp := &Response{}
	e = rsp.Unmarshal(b)
	if e == nil && len(rsp.Results) > 0 {
		results = rsp.Results
	}
	return
}

func ResponseFromResponseBytes(b []byte) (rsp *Response, e error) {
	rsp = &Response{}
	e = rsp.Unmarshal(b)
	return
}

func ResponseFromSeries(series models.Rows) (rsp *Response) {
	r := &Result{
		Series: series,
	}
	rsp = &Response{
		Results: []*Result{r},
	}
	return
}

func ResponseFromResults(results []*Result) (rsp *Response) {
	rsp = &Response{
		Results: results,
	}
	return
}

func ResponseFromError(err string) (rsp *Response) {
	rsp = &Response{
		Err: err,
	}
	return
}

// duplexReader reads responses and writes it to another writer while
// satisfying the reader interface.
type duplexReader struct {
	r io.ReadCloser
	w io.Writer
}

func (r *duplexReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err == nil {
		r.w.Write(p[:n])
	}
	return n, err
}

// Close closes the response.
func (r *duplexReader) Close() error {
	return r.r.Close()
}

// ChunkedResponse represents a response from the server that
// uses chunking to stream the output.
type ChunkedResponse struct {
	dec    *json.Decoder
	duplex *duplexReader
	buf    bytes.Buffer
}

// NewChunkedResponse reads a stream and produces responses from the stream.
func NewChunkedResponse(r io.Reader) *ChunkedResponse {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		rc = io.NopCloser(r)
	}
	resp := &ChunkedResponse{}
	resp.duplex = &duplexReader{r: rc, w: &resp.buf}
	resp.dec = json.NewDecoder(resp.duplex)
	resp.dec.UseNumber()
	return resp
}

// NextResponse reads the next line of the stream and returns a response.
func (r *ChunkedResponse) NextResponse() (*Response, error) {
	var response Response
	if err := r.dec.Decode(&response); err != nil {
		if err == io.EOF {
			return nil, err
		}
		// A decoding error happened. This probably means the server crashed
		// and sent a last-ditch error message to us. Ensure we have read the
		// entirety of the connection to get any remaining error text.
		io.Copy(io.Discard, r.duplex)
		return nil, errors.New(strings.TrimSpace(r.buf.String()))
	}

	r.buf.Reset()
	return &response, nil
}

// Close closes the response.
func (r *ChunkedResponse) Close() error {
	return r.duplex.Close()
}

// chunkReader wraps the gzip reader and body reader.
type chunkReader struct {
	gr *gzip.Reader
	br io.ReadCloser
}

func (r *chunkReader) Read(p []byte) (n int, err error) {
	return r.gr.Read(p)
}

func (r *chunkReader) Close() error {
	if err := r.gr.Close(); err != nil {
		return err
	}
	return r.br.Close()
}
