/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package testextension

import (
	"testing"

	"github.com/G-Research/yunikorn-core/pkg/scheduler/objects"
	"github.com/G-Research/yunikorn-core/pkg/webservice"
	"gotest.tools/v3/assert"
)

func TestApplicationSnapshot(t *testing.T) {
	app := objects.NewTestApplication(t)
	want := webservice.GetApplicationDAO(app, app.GetApplicationSummary("rmid"))
	got := app.DAO()
	assert.DeepEqual(t, want, got)
}

func TestQueueSnapshot(t *testing.T) {
	queue := objects.NewTestQueue(t)
	want := queue.GetPartitionQueueDAOInfo(false)
	got := queue.DAO(false)
	assert.DeepEqual(t, want, got)
}
