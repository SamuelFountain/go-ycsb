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

package aashard

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const ()

type contextKey string

const stateKey = contextKey("aashardDB")

type aashardState struct {
	r *rand.Rand

	buf *bytes.Buffer
}

// AashardDB just prints out the requested operations, instead of doing them against a database
type aashardDB struct {
	verbose bool
}

func (db *aashardDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := new(aashardState)
	state.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	state.buf = new(bytes.Buffer)

	return context.WithValue(ctx, stateKey, state)
}

func (db *aashardDB) CleanupThread(_ context.Context) {

}

func (db *aashardDB) Close() error {
	return nil
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint32(math.Mod(float64(h.Sum32()), 3))
}

func getServer(key string) string {
	in := [][]string{
		{"167.99.10.224", "146.190.76.226", "146.190.76.211"},
		{"167.99.6.137", "192.241.131.207", "146.190.76.223"},
		{"167.99.8.234", "159.203.185.93", "146.190.76.178"}}
	outIndex := hash(key)
	randomIndex := rand.Intn(len(in[outIndex]))
	pick := in[outIndex][randomIndex]
	return pick
}

func createValue(values map[string][]byte) string {
	fullValue := ""
	for k, v := range values {
		fullValue = fullValue + k + ":" + string(v[:])
	}
	return fullValue
}

func (db *aashardDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	fullKey := table + "-" + key
	data := url.Values{
		"action": {"Get"},
		"key":    {fullKey},
	}

	resp, err := http.PostForm("http://"+getServer(fullKey)+":5000", data)

	if err != nil {
		panic("PostForm Error Read")
	} else {
		defer resp.Body.Close()
		io.ReadAll(resp.Body)
		// fmt.Println(body)
	}
	return nil, nil
}

func (db *aashardDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	panic("The aashardDB has not implemented the batch operation")
}

func (db *aashardDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *aashardDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	fullKey := table + "-" + key
	fullValue := createValue(values)
	insertRecord(fullKey, fullValue)
	return nil
}

func (db *aashardDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The aashardDB has not implemented the batch operation")
}

func (db *aashardDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	fullKey := table + "-" + key
	fullValue := createValue(values)
	insertRecord(fullKey, fullValue)
	return nil
}

func (db *aashardDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The aashardDB has not implemented the batch operation")
}

func (db *aashardDB) Delete(ctx context.Context, table string, key string) error {
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
	} else {
		defer resp.Body.Close()
		io.ReadAll(resp.Body)
		// fmt.Println(body)
	}
}

func (db *aashardDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	panic("The aashardDB has not implemented the batch operation")
}

type aashardDBCreator struct{}

func (aashardDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(aashardDB)

	db.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)

	return db, nil
}

func init() {
	ycsb.RegisterDBCreator("aashard", aashardDBCreator{})
}
