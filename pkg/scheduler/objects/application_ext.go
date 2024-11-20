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
	"encoding/json"
	"testing"

	"github.com/G-Research/yunikorn-core/pkg/common"
	"github.com/G-Research/yunikorn-core/pkg/locking"
	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/looplab/fsm"
	"github.com/stretchr/testify/require"
)

func (sa *Application) daoSnapshot() string {
	sa.snapshotLock.Lock()
	defer sa.snapshotLock.Unlock()

	if err := json.NewEncoder(&sa.snapshot).Encode(sa.DAO()); err != nil {
		// TODO: log error
		return ""
	}

	val := sa.snapshot.String()
	sa.snapshot.Reset()
	return val
}

func (app *Application) DAO() *dao.ApplicationDAOInfo {
	if app == nil {
		return &dao.ApplicationDAOInfo{}
	}

	resourceUsage := app.usedResource.Clone()
	preemptedUsage := app.preemptedResource.Clone()
	placeHolderUsage := app.placeholderResource.Clone()
	var qID *string
	if app.queue != nil {
		qID = &app.queue.ID
	}

	return &dao.ApplicationDAOInfo{
		ID:                  app.ID,
		ApplicationID:       app.ApplicationID,
		UsedResource:        app.allocatedResource.Clone().DAOMap(),
		MaxUsedResource:     app.maxAllocatedResource.Clone().DAOMap(),
		PendingResource:     app.pending.Clone().DAOMap(),
		Partition:           common.GetPartitionNameWithoutClusterID(app.Partition),
		PartitionID:         app.PartitionID,
		QueueID:             qID,
		QueueName:           app.queuePath,
		SubmissionTime:      app.SubmissionTime.UnixNano(),
		FinishedTime:        common.ZeroTimeInUnixNano(app.finishedTime),
		Requests:            getAllocationAsksDAO(app.getAllRequestsInternal()),
		Allocations:         getAllocationsDAO(app.getAllAllocations()),
		State:               app.CurrentState(),
		User:                app.user.User,
		Groups:              app.user.Groups,
		RejectedMessage:     app.rejectedMessage,
		PlaceholderData:     getPlaceholdersDAO(app.getAllPlaceholderData()),
		StateLog:            getStatesDAO(app.stateLog),
		HasReserved:         len(app.reservations) > 0,
		Reservations:        app.getReservations(),
		MaxRequestPriority:  app.askMaxPriority,
		StartTime:           app.startTime.UnixMilli(),
		ResourceUsage:       resourceUsage,
		PreemptedResource:   preemptedUsage,
		PlaceholderResource: placeHolderUsage,
	}
}

func NewTestApplication(t *testing.T) *Application {
	var app Application
	err := gofakeit.Struct(&app)
	require.NoError(t, err)
	app.RWMutex = locking.RWMutex{}
	app.snapshotLock = locking.Mutex{}
	app.stateMachine = &fsm.FSM{}
	app.rmID = "rmid"
	return &app
}

func (sa *Application) getReservations() []string {
	keys := make([]string, 0)
	for key := range sa.reservations {
		keys = append(keys, key)
	}
	return keys
}

func (sa *Application) getAllAllocations() []*Allocation {
	var allocations []*Allocation
	for _, alloc := range sa.allocations {
		allocations = append(allocations, alloc)
	}
	return allocations
}

func (sa *Application) getAllPlaceholderData() []*PlaceholderData {
	var placeholders []*PlaceholderData
	for _, taskGroup := range sa.placeholderData {
		placeholders = append(placeholders, taskGroup)
	}
	return placeholders
}

func getPlaceholdersDAO(entries []*PlaceholderData) []*dao.PlaceholderDAOInfo {
	phsDAO := make([]*dao.PlaceholderDAOInfo, 0, len(entries))
	for _, entry := range entries {
		phsDAO = append(phsDAO, entry.DAO())
	}
	return phsDAO
}

func (ph *PlaceholderData) DAO() *dao.PlaceholderDAOInfo {
	phDAO := &dao.PlaceholderDAOInfo{
		TaskGroupName: ph.TaskGroupName,
		Count:         ph.Count,
		MinResource:   ph.MinResource.DAOMap(),
		Replaced:      ph.Replaced,
		TimedOut:      ph.TimedOut,
	}
	return phDAO
}

func getStatesDAO(entries []*StateLogEntry) []*dao.StateDAOInfo {
	statesDAO := make([]*dao.StateDAOInfo, 0, len(entries))
	for _, entry := range entries {
		statesDAO = append(statesDAO, entry.DAO())
	}
	return statesDAO
}

func (entry *StateLogEntry) DAO() *dao.StateDAOInfo {
	state := &dao.StateDAOInfo{
		Time:             entry.Time.UnixNano(),
		ApplicationState: entry.ApplicationState,
	}
	return state
}
