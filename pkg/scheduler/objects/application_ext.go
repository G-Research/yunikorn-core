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
