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

package ugm

import (
	"strings"

	"github.com/G-Research/yunikorn-core/pkg/common"
	"github.com/G-Research/yunikorn-core/pkg/common/configs"
)

// getParentPath return the path of the parent queue and an empty string if this queue is
// the root queue.
func getParentPath(queuePath string) string {
	idx := strings.LastIndex(queuePath, configs.DOT)
	if idx == -1 {
		return common.Empty
	}
	return queuePath[:idx]
}
