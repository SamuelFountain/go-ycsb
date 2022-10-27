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

package aa

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
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

const stateKey = contextKey("aaDB")

type aaState struct {
	r *rand.Rand

	buf *bytes.Buffer
}

// AaDB just prints out the requested operations, instead of doing them against a database
type aaDB struct {
	verbose bool
}

func (db *aaDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := new(aaState)
	state.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	state.buf = new(bytes.Buffer)

	return context.WithValue(ctx, stateKey, state)
}

func (db *aaDB) CleanupThread(_ context.Context) {

}

func (db *aaDB) Close() error {
	return nil
}

func createValue(values map[string][]byte) string {
	fullValue := ""
	for k, v := range values {
		fullValue = fullValue + k + ":" + string(v[:])
	}
	return fullValue
}

func (db *aaDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	fullKey := table + "-" + key
	data := url.Values{
		"action": {"Get"},
		"key":    {fullKey},
	}

	resp, err := http.PostForm("http://127.0.0.1:5000", data)
	if err != nil {
		panic("PostForm Error")
	} else{

		defer resp.Body.Close()
		io.ReadAll(resp.Body)
		// fmt.Println(body)
	}
	return nil, nil
}

func (db *aaDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	panic("The aaDB has not implemented the batch operation")
}

func (db *aaDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *aaDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	fullKey := table + "-" + key
	fullValue := createValue(values)
	insertRecord(fullKey, fullValue)
	return nil
}

func (db *aaDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The aaDB has not implemented the batch operation")
}

func (db *aaDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	fullKey := table + "-" + key
	fullValue := createValue(values)
	insertRecord(fullKey, fullValue)
	return nil
}

func (db *aaDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The aaDB has not implemented the batch operation")
}

func (db *aaDB) Delete(ctx context.Context, table string, key string) error {
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

	_, err := http.PostForm("http://127.0.0.1:5000", data)
	if err != nil {
		panic("PostForm Error")
	}
}

func (db *aaDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	panic("The aaDB has not implemented the batch operation")
}

type aaDBCreator struct{}

func (aaDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(aaDB)

	db.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)

	return db, nil
}

func init() {
	ycsb.RegisterDBCreator("aa", aaDBCreator{})
}
