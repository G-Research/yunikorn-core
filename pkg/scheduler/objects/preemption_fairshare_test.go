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

package objects

import (
	"testing"
	"time"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/mock"
	"github.com/apache/yunikorn-core/pkg/plugins"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
	"gotest.tools/v3/assert"
)

// TestTryPreemptionOnQueueFairShare tests fair share preemption between sibling queues.
// This test validates the fundamental fair share calculation and preemption mechanism
// when one queue exceeds its fair share allocation.
//
// Test Scenario:
// root: 1000 CPU, 1000 memory
// └── root.parent: no max inherits from root (fair share preemption policy)
//
//	├── root.parent.child1: guarantee=100 CPU, 100 memory (fair share preemption policy)
//	└── root.parent.child2: guarantee=100 CPU, 100 memory (fair share preemption policy)
//
// Expected Behavior:
// Phase 1: Initial Allocation
// - App1 → child1: Request 1000 CPU, 1000 memory → Gets 1000 CPU, 1000 memory (takes all available)
// - App2 → child2: Request 500 CPU, 500 memory → Gets 0 CPU, 0 memory (no resources available)
//
// Phase 2: Fair Share Calculation and Preemption
// - Fair share calculation: total_allocation_of_parent / active_child = 1000 / 2 = 500 CPU per child
// - child1: Currently using 1000 CPU (over fair share by 500 CPU, but above guarantee of 100 CPU)
// - child2: Currently using 0 CPU (under fair share by 500 CPU, but above guarantee of 100 CPU)
// - Preemption: child1 is overusing by 500 CPU, so 500 CPU is preempted from child1
// - Result: child1 gets 500 CPU, child2 gets 500 CPU (500 CPU transferred from child1 to child2)
func TestTryPreemptionOnQueueFairShare(t *testing.T) {
	// Create nodes with sufficient resources
	node1 := newNode(nodeID1, map[string]resources.Quantity{"cpu": 1000, "memory": 1000, "pods": 2})
	node2 := newNode(nodeID2, map[string]resources.Quantity{"cpu": 1000, "memory": 1000, "pods": 2})
	iterator := getNodeIteratorFn(node1, node2)

	// Create root queue with total capacity
	rootQ, err := createRootQueue(map[string]string{"cpu": "1000", "memory": "1000", "pods": "4"})
	assert.NilError(t, err)

	// Create parent queue with fair share preemption policy (no max set - allows over-allocation)
	parentQ, err := createManagedQueueWithProps(rootQ, "parent", true, nil, map[string]string{"preemption.policy": "fairshare"})
	assert.NilError(t, err)

	// Create child queues with fair share preemption policy
	childQ1, err := createManagedQueueWithProps(parentQ, "child1", false, nil, map[string]string{"cpu": "100", "memory": "100", "preemption.policy": "fairshare"})
	assert.NilError(t, err)
	childQ2, err := createManagedQueueWithProps(parentQ, "child2", false, nil, map[string]string{"cpu": "100", "memory": "100", "preemption.policy": "fairshare"})
	assert.NilError(t, err)

	// Verify that parent queue has fair share preemption policy set
	assert.Equal(t, parentQ.GetPreemptionPolicy(), policies.FairSharePreemptionPolicy, "Parent queue should have fair share preemption policy")

	// Create application 1 in child1 (victim queue)
	app1 := newApplication(appID1, "default", "root.parent.child1")
	app1.SetQueue(childQ1)
	childQ1.applications[appID1] = app1

	// Create allocation asks for app1
	ask1 := newAllocationAsk("alloc1", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	ask1.createTime = time.Now().Add(-1 * time.Minute)
	assert.NilError(t, app1.AddAllocationAsk(ask1))

	ask2 := newAllocationAsk("alloc2", appID1, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	ask2.createTime = time.Now()
	assert.NilError(t, app1.AddAllocationAsk(ask2))

	// Create allocations for app1
	alloc1 := newAllocationWithKey("alloc1", appID1, nodeID1, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	alloc1.createTime = ask1.createTime
	app1.AddAllocation(alloc1)
	assert.Check(t, node1.TryAddAllocation(alloc1), "node alloc1 failed")

	alloc2 := newAllocationWithKey("alloc2", appID1, nodeID2, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	alloc2.createTime = ask2.createTime
	app1.AddAllocation(alloc2)
	assert.Check(t, node2.TryAddAllocation(alloc2), "node alloc2 failed")

	// Update queue allocated resources
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask1.GetAllocatedResource()))
	assert.NilError(t, childQ1.TryIncAllocatedResource(ask2.GetAllocatedResource()))

	// Create application 2 in child2 (preemptor queue)
	app2 := newApplication(appID2, "default", "root.parent.child2")
	app2.SetQueue(childQ2)
	childQ2.applications[appID2] = app2

	// App2 initially gets no resources (all resources are taken by App1)
	// Then App2 requests resources, which should trigger fair share preemption
	ask3 := newAllocationAsk("alloc3", appID2, resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 500, "memory": 500, "pods": 1}))
	assert.NilError(t, app2.AddAllocationAsk(ask3))
	childQ2.incPendingResource(ask3.GetAllocatedResource())

	// Set up headroom and preemptor
	headRoom := resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 1000, "memory": 1000, "pods": 3})
	preemptor := NewPreemptor(app2, headRoom, 30*time.Second, ask3, iterator(), false)

	// Configure mock plugin to allow preemption on node2
	allocs := map[string]string{}
	allocs["alloc3"] = nodeID2

	plugin := mock.NewPreemptionPredicatePlugin(nil, allocs, nil)
	plugins.RegisterSchedulerPlugin(plugin)
	defer plugins.UnregisterSchedulerPlugins()

	// Execute preemption
	result, ok := preemptor.TryPreemption()

	// Verify preemption results
	assert.Assert(t, result != nil, "no result")
	assert.Assert(t, ok, "no victims found")
	assert.Equal(t, "alloc3", result.Request.GetAllocationKey(), "wrong alloc")
	assert.Equal(t, nodeID2, result.NodeID, "wrong node")

	// Verify that the correct allocation was preempted
	// In fair share preemption, we expect alloc2 to be preempted since it's on node2
	// and child1 is over its fair share (1000 CPU + 1000 memory total vs 500 CPU + 500 memory fair share)
	assert.Check(t, !alloc1.IsPreempted(), "alloc1 should not be preempted")
	assert.Check(t, alloc2.IsPreempted(), "alloc2 should be preempted")

	// Verify no allocation failure logs
	assert.Equal(t, len(ask3.GetAllocationLog()), 0)
}