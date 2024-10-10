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
	"github.com/G-Research/yunikorn-core/pkg/common"
	"github.com/G-Research/yunikorn-core/pkg/common/resources"
	"github.com/G-Research/yunikorn-core/pkg/events"
	"github.com/G-Research/yunikorn-scheduler-interface/lib/go/si"
)

type NodeEvents struct {
	eventSystem events.EventSystem
}

func (n *NodeEvents) SendNodeAddedEvent(nodeID string, capacity *resources.Resource, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, "Node added to the scheduler", common.Empty, si.EventRecord_ADD,
		si.EventRecord_DETAILS_NONE, capacity, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendNodeRemovedEvent(nodeID string, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(
		nodeID,
		"Node removed from the scheduler",
		common.Empty,
		si.EventRecord_REMOVE,
		si.EventRecord_NODE_DECOMISSION,
		nil,
		state,
	)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendAllocationAddedEvent(nodeID, allocKey string, res *resources.Resource, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, allocKey, si.EventRecord_ADD,
		si.EventRecord_NODE_ALLOC, res, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendAllocationRemovedEvent(nodeID, allocKey string, res *resources.Resource, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, allocKey, si.EventRecord_REMOVE,
		si.EventRecord_NODE_ALLOC, res, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendNodeSchedulableChangedEvent(nodeID string, ready bool, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	var reason string
	if ready {
		reason = "schedulable: true"
	} else {
		reason = "schedulable: false"
	}
	event := events.CreateNodeEventRecord(nodeID, reason, common.Empty, si.EventRecord_SET,
		si.EventRecord_NODE_SCHEDULABLE, nil, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendNodeCapacityChangedEvent(nodeID string, total *resources.Resource, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, common.Empty, si.EventRecord_SET,
		si.EventRecord_NODE_CAPACITY, total, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendNodeOccupiedResourceChangedEvent(nodeID string, occupied *resources.Resource, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, common.Empty, si.EventRecord_SET,
		si.EventRecord_NODE_OCCUPIED, occupied, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendReservedEvent(nodeID string, res *resources.Resource, askID string, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, askID, si.EventRecord_ADD,
		si.EventRecord_NODE_RESERVATION, res, state)
	n.eventSystem.AddEvent(event)
}

func (n *NodeEvents) SendUnreservedEvent(nodeID string, res *resources.Resource, askID string, state string) {
	if !n.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateNodeEventRecord(nodeID, common.Empty, askID, si.EventRecord_REMOVE,
		si.EventRecord_NODE_RESERVATION, res, state)
	n.eventSystem.AddEvent(event)
}

func NewNodeEvents(evt events.EventSystem) *NodeEvents {
	return &NodeEvents{
		eventSystem: evt,
	}
}
