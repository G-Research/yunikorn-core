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

package events

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/G-Research/yunikorn-core/pkg/common"
	"github.com/G-Research/yunikorn-core/pkg/common/resources"
	"github.com/G-Research/yunikorn-core/pkg/events/mock"
	"github.com/G-Research/yunikorn-scheduler-interface/lib/go/si"
)

const nodeID1 = "node-1"

func TestSendNodeAddedEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendNodeAddedEvent(nodeID1, resource, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendNodeAddedEvent(nodeID1, resource, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "Node added to the scheduler", event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendNodeRemovedEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendNodeRemovedEvent(nodeID1, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendNodeRemovedEvent(nodeID1, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "Node removed from the scheduler", event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_DECOMISSION, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestSendAllocationAddedEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})

	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendAllocationAddedEvent(nodeID1, "alloc-0", resource, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendAllocationAddedEvent(nodeID1, "alloc-0", resource, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendAllocationRemovedEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})

	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendAllocationRemovedEvent(nodeID1, "alloc-0", resource, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendAllocationRemovedEvent(nodeID1, "alloc-0", resource, "")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendOccupiedResourceChangedEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendNodeOccupiedResourceChangedEvent(nodeID1, resource, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendNodeOccupiedResourceChangedEvent(nodeID1, resource, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_OCCUPIED, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestSendCapacityChangedEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendNodeCapacityChangedEvent(nodeID1, resource, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendNodeCapacityChangedEvent(nodeID1, resource, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_CAPACITY, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestNodeSchedulableChangedEvent(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendNodeSchedulableChangedEvent(nodeID1, false, "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendNodeSchedulableChangedEvent(nodeID1, false, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "schedulable: false", event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_SCHEDULABLE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))

	eventSystem.Reset()
	ne.SendNodeSchedulableChangedEvent(nodeID1, true, "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event = eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, common.Empty, event.ReferenceID)
	assert.Equal(t, "schedulable: true", event.Message)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_SCHEDULABLE, event.EventChangeDetail)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestNodeReservationEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendReservedEvent(nodeID1, resource, "alloc-0", "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendReservedEvent(nodeID1, resource, "alloc-0", "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_RESERVATION, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}

func TestNodeUnreservationEvent(t *testing.T) {
	resource := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	eventSystem := mock.NewEventSystemDisabled()
	ne := NewNodeEvents(eventSystem)
	ne.SendUnreservedEvent(nodeID1, resource, "alloc-0", "")
	assert.Equal(t, 0, len(eventSystem.Events), "unexpected event")

	eventSystem = mock.NewEventSystem()
	ne = NewNodeEvents(eventSystem)
	ne.SendUnreservedEvent(nodeID1, resource, "alloc-0", "")
	assert.Equal(t, 1, len(eventSystem.Events), "event was not generated")
	event := eventSystem.Events[0]
	assert.Equal(t, nodeID1, event.ObjectID)
	assert.Equal(t, "alloc-0", event.ReferenceID)
	assert.Equal(t, common.Empty, event.Message)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_RESERVATION, event.EventChangeDetail)
	assert.Equal(t, 1, len(event.Resource.Resources))
	protoRes := resources.NewResourceFromProto(event.Resource)
	assert.DeepEqual(t, protoRes, resource)
}
