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

package scheduler

import (
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/placement"
)

type PartitionSchedulingContext struct {
	Root *SchedulingQueue // start of the scheduling queue hierarchy
	RmID string           // the RM the partition belongs to
	Name string           // name of the partition (logging mainly)

	// Private fields need protection
	partition        *cache.PartitionInfo              // link back to the partition in the cache
	applications     map[string]*SchedulingApplication // applications assigned to this partition
	nodes            map[string]*SchedulingNode        // nodes assigned to this partition
	placementManager *placement.AppPlacementManager    // placement manager for this partition
	partitionManager *PartitionManager                 // manager for this partition
	lock             sync.RWMutex
}

// Create a new partitioning scheduling context.
// the flattened list is generated by a separate call
func newPartitionSchedulingContext(info *cache.PartitionInfo, root *SchedulingQueue) *PartitionSchedulingContext {
	if info == nil || root == nil {
		return nil
	}
	psc := &PartitionSchedulingContext{
		applications: make(map[string]*SchedulingApplication),
		nodes:        make(map[string]*SchedulingNode),
		Root:         root,
		Name:         info.Name,
		RmID:         info.RmID,
		partition:    info,
	}
	psc.placementManager = placement.NewPlacementManager(info)
	return psc
}

// Update the scheduling partition based on the reloaded config.
func (psc *PartitionSchedulingContext) updatePartitionSchedulingContext(info *cache.PartitionInfo) {
	psc.lock.Lock()
	defer psc.lock.Unlock()

	if psc.placementManager.IsInitialised() {
		log.Logger().Info("Updating placement manager rules on config reload")
		err := psc.placementManager.UpdateRules(info.GetRules())
		if err != nil {
			log.Logger().Info("New placement rules not activated, config reload failed", zap.Error(err))
		}
	} else {
		log.Logger().Info("Creating new placement manager on config reload")
		psc.placementManager = placement.NewPlacementManager(info)
	}
	root := psc.Root
	// update the root queue properties
	root.updateSchedulingQueueProperties(info.Root.Properties)
	// update the rest of the queues recursively
	root.updateSchedulingQueueInfo(info.Root.GetCopyOfChildren(), root)
}

// Add a new application to the scheduling partition.
func (psc *PartitionSchedulingContext) AddSchedulingApplication(schedulingApp *SchedulingApplication) error {
	psc.lock.Lock()
	defer psc.lock.Unlock()

	// Add to applications
	appID := schedulingApp.ApplicationInfo.ApplicationID
	if psc.applications[appID] != nil {
		return fmt.Errorf("adding application %s to partition %s, but application already existed", appID, psc.Name)
	}

	// Put app under the scheduling queue, the app has already been placed in the partition cache
	queueName := schedulingApp.ApplicationInfo.QueueName
	if psc.placementManager.IsInitialised() {
		err := psc.placementManager.PlaceApplication(schedulingApp.ApplicationInfo)
		if err != nil {
			return fmt.Errorf("failed to place app in requested queue '%s' for application %s: %v", queueName, appID, err)
		}
		// pull out the queue name from the placement
		queueName = schedulingApp.ApplicationInfo.QueueName
	}
	// we have a queue name either from placement or direct
	schedulingQueue := psc.getQueue(queueName)
	// check if the queue already exist and what we have is a leaf queue with submit access
	if schedulingQueue != nil &&
		(!schedulingQueue.isLeafQueue() || !schedulingQueue.CheckSubmitAccess(schedulingApp.ApplicationInfo.GetUser())) {
		return fmt.Errorf("failed to find queue %s for application %s", schedulingApp.ApplicationInfo.QueueName, appID)
	}
	// with placement rules the hierarchy might not exist so try and create it
	if schedulingQueue == nil {
		psc.createSchedulingQueue(queueName, schedulingApp.ApplicationInfo.GetUser())
		// find the scheduling queue: if it still does not exist we fail the app
		schedulingQueue = psc.getQueue(queueName)
		if schedulingQueue == nil {
			return fmt.Errorf("failed to find queue %s for application %s", schedulingApp.ApplicationInfo.QueueName, appID)
		}
	}

	// all is OK update the app and partition
	schedulingApp.queue = schedulingQueue
	schedulingQueue.AddSchedulingApplication(schedulingApp)
	psc.applications[appID] = schedulingApp

	return nil
}

// Remove the application from the scheduling partition.
func (psc *PartitionSchedulingContext) RemoveSchedulingApplication(appID string) (*SchedulingApplication, error) {
	psc.lock.Lock()
	defer psc.lock.Unlock()

	// Remove from applications map
	if psc.applications[appID] == nil {
		return nil, fmt.Errorf("removing application %s from partition %s, but application does not exist", appID, psc.Name)
	}
	schedulingApp := psc.applications[appID]
	delete(psc.applications, appID)

	// Remove app under queue
	schedulingQueue := psc.getQueue(schedulingApp.ApplicationInfo.QueueName)
	if schedulingQueue == nil {
		// This is not normal
		panic(fmt.Sprintf("Failed to find queue %s for app=%s while removing application", schedulingApp.ApplicationInfo.QueueName, appID))
	}
	schedulingQueue.RemoveSchedulingApplication(schedulingApp)

	return schedulingApp, nil
}

// Get the queue from the structure based on the fully qualified name.
// Wrapper around the unlocked version getQueue()
// Visible by tests
func (psc *PartitionSchedulingContext) GetQueue(name string) *SchedulingQueue {
	psc.lock.RLock()
	defer psc.lock.RUnlock()
	return psc.getQueue(name)
}

// Get the queue from the structure based on the fully qualified name.
// The name is not syntax checked and must be valid.
// Returns nil if the queue is not found otherwise the queue object.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionSchedulingContext lock.
func (psc *PartitionSchedulingContext) getQueue(name string) *SchedulingQueue {
	// start at the root
	queue := psc.Root
	part := strings.Split(strings.ToLower(name), cache.DOT)
	// short circuit the root queue
	if len(part) == 1 {
		return queue
	}
	// walk over the parts going down towards the requested queue
	for i := 1; i < len(part); i++ {
		// if child not found break out and return
		if queue = queue.childrenQueues[part[i]]; queue == nil {
			break
		}
	}
	return queue
}

func (psc *PartitionSchedulingContext) getApplication(appID string) *SchedulingApplication {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return psc.applications[appID]
}

// Create a scheduling queue with full hierarchy. This is called when a new queue is created from a placement rule.
// It will not return anything and cannot "fail". A failure is picked up by the queue not existing after this call.
//
// NOTE: this is a lock free call. It should only be called holding the PartitionSchedulingContext lock.
func (psc *PartitionSchedulingContext) createSchedulingQueue(name string, user security.UserGroup) {
	// find the scheduling furthest down the hierarchy that exists
	schedQueue := name // the scheduling queue that exists
	cacheQueue := ""   // the cache queue that needs to be created (with children)
	parent := psc.getQueue(schedQueue)
	for parent == nil {
		cacheQueue = schedQueue
		schedQueue = name[0:strings.LastIndex(name, cache.DOT)]
		parent = psc.getQueue(schedQueue)
	}
	// found the last known scheduling queue,
	// create the corresponding scheduler queue based on the already created cache queue
	queue := psc.partition.GetQueue(cacheQueue)
	// if the cache queue does not exist we should fail this create
	if queue == nil {
		return
	}
	// Check the ACL before we really create
	// The existing parent scheduling queue is the lowest we need to look at
	if !parent.CheckSubmitAccess(user) {
		log.Logger().Debug("Submit access denied by scheduler on queue",
			zap.String("deniedQueueName", schedQueue),
			zap.String("requestedQueue", name))
		return
	}
	log.Logger().Debug("Creating scheduling queue(s)",
		zap.String("parent", schedQueue),
		zap.String("child", cacheQueue),
		zap.String("fullPath", name))
	NewSchedulingQueueInfo(queue, parent)
}

// Get a scheduling node from the partition by nodeID.
func (psc *PartitionSchedulingContext) getSchedulingNode(nodeID string) *SchedulingNode {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	return psc.nodes[nodeID]
}

// Get a copy of the scheduling nodes from the partition.
func (psc *PartitionSchedulingContext) getSchedulingNodes() []*SchedulingNode {
	psc.lock.RLock()
	defer psc.lock.RUnlock()

	schedulingNodes := make([]*SchedulingNode, len(psc.nodes))
	var i = 0
	for _, node := range psc.nodes {
		// filter out the nodes that are not scheduling
		if node.nodeInfo.IsSchedulable() {
			schedulingNodes[i] = node
			i++
		}
	}
	// only return the part that has really been written (no nil's)
	return schedulingNodes[:i]
}

// Add a new scheduling node triggered on the addition of the cache node.
// This will log if the scheduler is out of sync with the cache.
// As a side effect it will bring the cache and scheduler back into sync.
func (psc *PartitionSchedulingContext) addSchedulingNode(info *cache.NodeInfo) {
	if info == nil {
		return
	}

	psc.lock.Lock()
	defer psc.lock.Unlock()
	// check consistency and reset to make sure it is consistent again
	if _, ok := psc.nodes[info.NodeID]; ok {
		log.Logger().Debug("new node already existed: cache out of sync with scheduler",
			zap.String("nodeID", info.NodeID))
	}
	// add the node, this will also get the sync back between the two lists
	psc.nodes[info.NodeID] = NewSchedulingNode(info)
}

// Remove a scheduling node triggered by the removal of the cache node.
// This will log if the scheduler is out of sync with the cache.
// Should never be called directly as it will bring the scheduler out of sync with the cache.
func (psc *PartitionSchedulingContext) removeSchedulingNode(nodeID string) {
	if nodeID == "" {
		return
	}

	psc.lock.Lock()
	defer psc.lock.Unlock()
	// check consistency just for debug
	if _, ok := psc.nodes[nodeID]; !ok {
		log.Logger().Debug("node to be removed does not exist: cache out of sync with scheduler",
			zap.String("nodeID", nodeID))
	}
	// remove the node, this will also get the sync back between the two lists
	delete(psc.nodes, nodeID)
}
