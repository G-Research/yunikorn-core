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

package placement

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/G-Research/yunikorn-core/pkg/common"
	"github.com/G-Research/yunikorn-core/pkg/common/configs"
	"github.com/G-Research/yunikorn-core/pkg/common/security"
	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
	siCommon "github.com/G-Research/yunikorn-scheduler-interface/lib/go/common"
)

func TestRecoveryRuleInitialise(t *testing.T) {
	conf := configs.PlacementRule{
		Name: "recovery",
	}
	rr := &recoveryRule{}
	err := rr.initialise(conf)
	assert.NilError(t, err, "unexpected error in initialize")
}

func TestRecoveryRulePlace(t *testing.T) {
	rr := &recoveryRule{}
	err := initQueueStructure([]byte(confTestQueue))
	assert.NilError(t, err, "setting up the queue config failed")

	// verify that non-forced app is not recovered
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	tags := make(map[string]string)
	app := newApplication("app1", "default", "ignored", user, tags, nil, "")

	var queue string
	queue, err = rr.placeApplication(app, queueFunc)
	if queue != "" || err != nil {
		t.Errorf("recovery rule did not bypass non-forced application, resolved queue '%s', err %v ", queue, err)
	}

	tags[siCommon.AppTagCreateForce] = "true"
	app = newApplication("app1", "default", "ignored", user, tags, nil, "")
	queue, err = rr.placeApplication(app, queueFunc)
	if queue != common.RecoveryQueueFull || err != nil {
		t.Errorf("recovery rule did not place forced application into recovery queue, resolved queue '%s', err %v ", queue, err)
	}
}

func Test_recoveryRule_ruleDAO(t *testing.T) {
	// nothing should be set as everything is ignored by the rule
	rrDAO := &dao.RuleDAO{Name: "recovery", Parameters: map[string]string{"queue": common.RecoveryQueueFull}}
	rr := &recoveryRule{}
	ruleDAO := rr.ruleDAO()
	assert.DeepEqual(t, rrDAO, ruleDAO)
}
