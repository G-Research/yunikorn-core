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

package template

import (
	"math/rand"
	"strconv"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/G-Research/yunikorn-core/pkg/common/configs"
	"github.com/G-Research/yunikorn-core/pkg/common/resources"
	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
)

func getResourceConf() map[string]string {
	resource := make(map[string]string)
	resource["memory"] = strconv.Itoa(rand.Intn(10000) + 10) //nolint:gosec
	resource["vcore"] = strconv.Itoa(rand.Intn(10000) + 10)  //nolint:gosec
	return resource
}

func getProperties() map[string]string {
	properties := make(map[string]string)
	properties[strconv.Itoa(rand.Intn(10000))] = strconv.Itoa(rand.Intn(10000)) //nolint:gosec
	return properties
}

func getResource(t *testing.T) *resources.Resource {
	r, err := resources.NewResourceFromConf(getResourceConf())
	assert.NilError(t, err, "failed to parse resource: %v", err)
	return r
}

func checkMembers(t *testing.T, template *Template, maxApplications uint64, properties map[string]string, maxResource *resources.Resource, guaranteedResource *resources.Resource) {
	// test inner members
	assert.Equal(t, template.maxApplications, maxApplications)
	assert.DeepEqual(t, template.properties, properties)
	assert.DeepEqual(t, template.maxResource, maxResource)
	assert.DeepEqual(t, template.guaranteedResource, guaranteedResource)

	// test all getters
	assert.Equal(t, template.GetMaxApplications(), maxApplications)
	assert.DeepEqual(t, template.GetProperties(), properties)
	assert.DeepEqual(t, template.GetMaxResource(), maxResource)
	assert.DeepEqual(t, template.GetGuaranteedResource(), guaranteedResource)

	assert.DeepEqual(t, template.GetTemplateInfo(), &dao.TemplateInfo{
		MaxApplications:    template.GetMaxApplications(),
		Properties:         template.GetProperties(),
		MaxResource:        template.maxResource.DAOMap(),
		GuaranteedResource: template.guaranteedResource.DAOMap(),
	})
}

func checkNilTemplate(t *testing.T, template *Template) {
	assert.Assert(t, template == nil)
	assert.Assert(t, template.GetMaxResource() == nil)
	assert.Assert(t, template.GetGuaranteedResource() == nil)
	assert.Assert(t, template.GetTemplateInfo() == nil)
}

func TestNewTemplate(t *testing.T) {
	properties := getProperties()
	guaranteedResource := getResource(t)
	maxResource := getResource(t)
	maxApplications := uint64(1)

	checkMembers(t, newTemplate(maxApplications, properties, maxResource, guaranteedResource), maxApplications, properties, maxResource, guaranteedResource)
}

func TestFromConf(t *testing.T) {
	maxApplications := uint64(1)
	properties := getProperties()
	guaranteedResourceConf := getResourceConf()
	maxResourceConf := getResourceConf()

	// case 0: normal case
	template, err := FromConf(&configs.ChildTemplate{
		MaxApplications: maxApplications,
		Properties:      properties,
		Resources: configs.Resources{
			Max:        maxResourceConf,
			Guaranteed: guaranteedResourceConf,
		},
	})
	assert.NilError(t, err, "failed to create template: %v", err)

	maxResource, err := resources.NewResourceFromConf(maxResourceConf)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	guaranteedResource, err := resources.NewResourceFromConf(guaranteedResourceConf)
	assert.NilError(t, err, "failed to parse resource: %v", err)
	checkMembers(t, template, maxApplications, properties, maxResource, guaranteedResource)

	// case 1: empty map produces nil template
	template, err = FromConf(&configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Max:        make(map[string]string),
			Guaranteed: make(map[string]string),
		},
	})
	assert.NilError(t, err)
	checkNilTemplate(t, template)

	template, err = FromConf(nil)
	assert.NilError(t, err)
	checkNilTemplate(t, template)

	// case 2: empty key-value produces nil template
	emptyProps := make(map[string]string)
	emptyProps[""] = ""
	empty2, err := FromConf(&configs.ChildTemplate{
		Properties: emptyProps,
		Resources: configs.Resources{
			Max:        emptyProps,
			Guaranteed: emptyProps,
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, empty2 == nil)

	// case 3: one item can produce template
	props := make(map[string]string)
	props["k"] = "v"
	validTemplate, err := FromConf(&configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Max:        getResourceConf(),
			Guaranteed: make(map[string]string),
		},
	})
	assert.NilError(t, err)
	assert.Assert(t, validTemplate != nil)

	// case 4: invalid max resource
	template, err = FromConf(&configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Max:        map[string]string{"memory": "500m"},
			Guaranteed: make(map[string]string),
		},
	})
	assert.Assert(t, err != nil)
	checkNilTemplate(t, template)

	// case 5: invalid guaranteed resource
	template, err = FromConf(&configs.ChildTemplate{
		Properties: make(map[string]string),
		Resources: configs.Resources{
			Max:        make(map[string]string),
			Guaranteed: map[string]string{"memory": "500m"},
		},
	})
	assert.Assert(t, err != nil)
	checkNilTemplate(t, template)
}
