// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package as

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"math"
	"net/http"
	"net/url"
	"time"
	"io"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const ()

type contextKey string

const stateKey = contextKey("asDB")

type asState struct {
	r *rand.Rand

	buf *bytes.Buffer
}

// AsDB just prints out the requested operations, instead of doing them against a database
type asDB struct {
	verbose bool
}

func (db *asDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := new(asState)
	state.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	state.buf = new(bytes.Buffer)

	return context.WithValue(ctx, stateKey, state)
}

func (db *asDB) CleanupThread(_ context.Context) {

}


func (db *asDB) Close() error {
	return nil
}

func hash(s string) uint32 {
        h := fnv.New32a()
        h.Write([]byte(s))
        return uint32(math.Mod(float64(h.Sum32()), 3))
}

func getServer(key string) string{
	in := []string{"127.0.0.1", "127.0.0.1", "127.0.0.1"}
	return in[hash(key)]
}

func createValue(values map[string][]byte) string {
	fullValue := ""
	for k, v := range values {
		fullValue = fullValue + k + ":" + string(v[:])
	}
	return fullValue
}

func (db *asDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	fullKey := table + "-" + key
	data := url.Values{
		"action": {"Get"},
		"key":    {fullKey},
	}

	resp, err := http.PostForm("http://"+getServer(fullKey)+":5000", data)

	if err != nil {
		panic(err)
		panic("PostForm Error Read")
	} else{
		defer resp.Body.Close()
		io.ReadAll(resp.Body)
		// fmt.Println(body)
	}
	return nil, nil
}

func (db *asDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	panic("The asDB has not implemented the batch operation")
}

func (db *asDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *asDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	fullKey := table + "-" + key
	fullValue := createValue(values)
	insertRecord(fullKey, fullValue)
	return nil
}

func (db *asDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The asDB has not implemented the batch operation")
}

func (db *asDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	fullKey := table + "-" + key
	fullValue := createValue(values)
	insertRecord(fullKey, fullValue)
	return nil
}

func (db *asDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The asDB has not implemented the batch operation")
}

func (db *asDB) Delete(ctx context.Context, table string, key string) error {
	fullKey := table + "-" + key
	insertRecord(fullKey, "NULL")
	return nil
}

func insertRecord(key string, value string) {
	// fmt.Println(key)
	// fmt.Println(value)
	data := url.Values{
		"action": {"Put"},
		"key":    {key},
		"value":  {value},
	}

	resp, err := http.PostForm("http://"+getServer(key)+":5000", data)
	if err != nil {
		panic(err)
		panic("PostForm Error insert")
	}else{
		defer resp.Body.Close()
		io.ReadAll(resp.Body)
		// fmt.Println(body)
	}
}

func (db *asDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	panic("The asDB has not implemented the batch operation")
}

type asDBCreator struct{}

func (asDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(asDB)

	db.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)

	return db, nil
}

func init() {
	ycsb.RegisterDBCreator("as", asDBCreator{})
}
