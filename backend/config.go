// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"errors"
	"log"
	"strings"

	"github.com/chengshiwen/influx-proxy/backend/tls"
	"github.com/chengshiwen/influx-proxy/util"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
)

var (
	Version   = "not build"
	GitCommit = "not build"
	BuildTime = "not build"
)

var (
	HashKeyIdx    = "idx"
	HashKeyExi    = "exi"
	HashKeyName   = "name"
	HashKeyURL    = "url"
	HashKeyVarIdx = "%idx"
	ShardKeyVarDb = "%db"
	ShardKeyVarMm = "%mm"
	ShardKeyDbMm  = "%db,%mm"
)

var (
	ErrEmptyCircles          = errors.New("circles cannot be empty")
	ErrEmptyBackends         = errors.New("backends cannot be empty")
	ErrEmptyBackendName      = errors.New("backend name cannot be empty")
	ErrDuplicatedBackendName = errors.New("backend name duplicated")
	ErrInvalidHashKey        = errors.New("invalid hash_key, require idx, exi, name, url or template containing %idx")
	ErrInvalidShardKey       = errors.New("invalid shard_key, require template containing %db or %mm")
)

type BackendConfig struct { //nolint:all
	Name        string `mapstructure:"name"`
	Url         string `mapstructure:"url"` //nolint:all
	Username    string `mapstructure:"username"`
	Password    string `mapstructure:"password"`
	AuthEncrypt bool   `mapstructure:"auth_encrypt"`
	WriteOnly   bool   `mapstructure:"write_only"`
}

type CircleConfig struct {
	Name     string           `mapstructure:"name"`
	Backends []*BackendConfig `mapstructure:"backends"`
}

type ProxyConfig struct {
	Circles         []*CircleConfig `mapstructure:"circles"`
	ListenAddr      string          `mapstructure:"listen_addr"`
	DBList          []string        `mapstructure:"db_list"`
	DataDir         string          `mapstructure:"data_dir"`
	TLogDir         string          `mapstructure:"tlog_dir"`
	HashKey         string          `mapstructure:"hash_key"`
	ShardKey        string          `mapstructure:"shard_key"`
	FlushSize       int             `mapstructure:"flush_size"`
	FlushTime       int             `mapstructure:"flush_time"`
	CheckInterval   int             `mapstructure:"check_interval"`
	RewriteInterval int             `mapstructure:"rewrite_interval"`
	RewriteThreads  int             `mapstructure:"rewrite_threads"`
	ConnPoolSize    int             `mapstructure:"conn_pool_size"`
	WriteTimeout    int             `mapstructure:"write_timeout"`
	IdleTimeout     int             `mapstructure:"idle_timeout"`
	Username        string          `mapstructure:"username"`
	Password        string          `mapstructure:"password"`
	AuthEncrypt     bool            `mapstructure:"auth_encrypt"`
	PingAuthEnabled bool            `mapstructure:"ping_auth_enabled"`
	WriteTracing    bool            `mapstructure:"write_tracing"`
	QueryTracing    bool            `mapstructure:"query_tracing"`
	PprofEnabled    bool            `mapstructure:"pprof_enabled"`
	HTTPSEnabled    bool            `mapstructure:"https_enabled"`
	HTTPSCert       string          `mapstructure:"https_cert"`
	HTTPSKey        string          `mapstructure:"https_key"`
	TLS             *tls.Config     `mapstructure:"tls"`
}

func NewFileConfig(cfgfile string) (cfg *ProxyConfig, err error) {
	viper.SetConfigFile(cfgfile)
	err = viper.ReadInConfig()
	if err != nil {
		return
	}
	cfg = &ProxyConfig{}
	err = viper.Unmarshal(cfg)
	if err != nil {
		return
	}
	cfg.setDefault()
	err = cfg.checkConfig()
	return
}

func (cfg *ProxyConfig) setDefault() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":7076"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if cfg.TLogDir == "" {
		cfg.TLogDir = "log"
	}
	if cfg.HashKey == "" {
		cfg.HashKey = HashKeyIdx
	}
	if cfg.ShardKey == "" {
		cfg.ShardKey = ShardKeyDbMm
	}
	if cfg.FlushSize <= 0 {
		cfg.FlushSize = 10000
	}
	if cfg.FlushTime <= 0 {
		cfg.FlushTime = 1
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 1
	}
	if cfg.RewriteInterval <= 0 {
		cfg.RewriteInterval = 10
	}
	if cfg.RewriteThreads <= 0 {
		cfg.RewriteThreads = 5
	}
	if cfg.ConnPoolSize <= 0 {
		cfg.ConnPoolSize = 20
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 10
	}
}

func (cfg *ProxyConfig) checkConfig() (err error) {
	if len(cfg.Circles) == 0 {
		return ErrEmptyCircles
	}
	set := util.NewSet()
	for _, circle := range cfg.Circles {
		if len(circle.Backends) == 0 {
			return ErrEmptyBackends
		}
		for _, backend := range circle.Backends {
			if backend.Name == "" {
				return ErrEmptyBackendName
			}
			if set[backend.Name] {
				return ErrDuplicatedBackendName
			}
			set.Add(backend.Name)
		}
	}
	if cfg.HashKey != HashKeyIdx && cfg.HashKey != HashKeyExi && cfg.HashKey != HashKeyName && cfg.HashKey != HashKeyURL && !strings.Contains(cfg.HashKey, HashKeyVarIdx) {
		return ErrInvalidHashKey
	}
	if !strings.Contains(cfg.ShardKey, ShardKeyVarDb) && !strings.Contains(cfg.ShardKey, ShardKeyVarMm) {
		return ErrInvalidShardKey
	}
	if cfg.TLS != nil {
		if err := cfg.TLS.Validate(); err != nil {
			return err
		}
	}
	return
}

func (cfg *ProxyConfig) PrintSummary() {
	log.Printf("%d circles loaded from file", len(cfg.Circles))
	for id, circle := range cfg.Circles {
		log.Printf("circle %d: %d backends loaded", id, len(circle.Backends))
	}
	log.Printf("hash key: %s", cfg.HashKey)
	log.Printf("shard key: %s", cfg.ShardKey)
	if len(cfg.DBList) > 0 {
		log.Printf("db list: %v", cfg.DBList)
	}
	log.Printf("auth: %t, encrypt: %t", cfg.Username != "" || cfg.Password != "", cfg.AuthEncrypt)
}

func (cfg *ProxyConfig) String() string {
	json := jsoniter.Config{TagKey: "mapstructure"}.Froze()
	b, _ := json.Marshal(cfg)
	return string(b)
}
