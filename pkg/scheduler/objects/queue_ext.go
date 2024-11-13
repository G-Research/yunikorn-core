package objects

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/G-Research/yunikorn-core/pkg/common/configs"
	"github.com/G-Research/yunikorn-core/pkg/common/resources"
	"github.com/G-Research/yunikorn-core/pkg/locking"
	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/looplab/fsm"
	"github.com/stretchr/testify/require"
)

func (sq *Queue) daoSnapshot() string {
	sq.snapshotLock.Lock()
	defer sq.snapshotLock.Unlock()

	if err := json.NewEncoder(&sq.snapshot).Encode(sq.DAO(false)); err != nil {
		// TODO: log error
		return ""
	}
	val := sq.snapshot.String()
	sq.snapshot.Reset()
	return val
}

func (sq *Queue) DAO(include bool) dao.PartitionQueueDAOInfo {
	queueInfo := dao.PartitionQueueDAOInfo{}
	children := sq.getCopyOfChildren()
	if include {
		queueInfo.Children = make([]dao.PartitionQueueDAOInfo, 0, len(children))
		for _, child := range children {
			queueInfo.Children = append(queueInfo.Children, child.DAO(true))
		}
	}
	// we have held the read lock so following method should not take lock again.
	queueInfo.HeadRoom = sq.getHeadRoomLocked().DAOMap()
	for _, child := range children {
		queueInfo.ChildNames = append(queueInfo.ChildNames, child.QueuePath)
	}
	queueInfo.ID = sq.ID
	queueInfo.QueueName = sq.QueuePath
	queueInfo.PartitionID = sq.PartitionID
	queueInfo.Status = sq.stateMachine.Current()
	queueInfo.PendingResource = sq.pending.DAOMap()
	queueInfo.MaxResource = sq.maxResource.DAOMap()
	queueInfo.GuaranteedResource = sq.guaranteedResource.DAOMap()
	queueInfo.AllocatedResource = sq.allocatedResource.DAOMap()
	queueInfo.PreemptingResource = sq.preemptingResource.DAOMap()
	queueInfo.IsLeaf = sq.isLeaf
	queueInfo.IsManaged = sq.isManaged
	queueInfo.CurrentPriority = sq.getCurrentPriority()
	queueInfo.TemplateInfo = sq.template.GetTemplateInfo()
	queueInfo.AbsUsedCapacity = resources.CalculateAbsUsedCapacity(
		sq.maxResource, sq.allocatedResource).DAOMap()
	queueInfo.Properties = make(map[string]string)
	for k, v := range sq.properties {
		queueInfo.Properties[k] = v
	}
	if sq.parent != nil {
		queueInfo.Parent = sq.QueuePath[:strings.LastIndex(sq.QueuePath, configs.DOT)]
		parentID := sq.parent.ID
		queueInfo.ParentID = &parentID
	}
	queueInfo.MaxRunningApps = sq.maxRunningApps
	queueInfo.RunningApps = sq.runningApps
	queueInfo.AllocatingAcceptedApps = make([]string, 0)
	for appID, result := range sq.allocatingAcceptedApps {
		if result {
			queueInfo.AllocatingAcceptedApps = append(queueInfo.AllocatingAcceptedApps, appID)
		}
	}
	return queueInfo
}

func (sq *Queue) getCopyOfChildren() map[string]*Queue {
	childCopy := make(map[string]*Queue)
	for k, v := range sq.children {
		childCopy[k] = v
	}
	return childCopy
}

func (sq *Queue) getHeadRoomLocked() *resources.Resource {
	var parentHeadRoom *resources.Resource
	if sq.parent != nil {
		parentHeadRoom = sq.parent.getHeadRoomLocked()
	}
	return sq.internalHeadRoomNoLock(parentHeadRoom)
}

func NewTestQueue(t *testing.T) *Queue {
	var q Queue
	err := gofakeit.Struct(&q)
	require.NoError(t, err)
	q.RWMutex = locking.RWMutex{}
	q.snapshotLock = locking.Mutex{}
	q.stateMachine = &fsm.FSM{}
	return &q
}

func (sq *Queue) internalHeadRoomNoLock(parentHeadRoom *resources.Resource) *resources.Resource {
	headRoom := sq.maxResource

	// if we have no max set headroom is always the same as the parent
	if headRoom == nil {
		return parentHeadRoom
	}

	// calculate what we have left over after removing all allocation
	// ignore unlimited resource types (ie the ones not defined in max)
	headRoom = resources.SubOnlyExisting(headRoom, sq.allocatedResource)

	// check the minimum of the two: parentHeadRoom is nil for root
	if parentHeadRoom == nil {
		return headRoom
	}
	// take the minimum value of *all* resource types defined
	return resources.ComponentWiseMin(headRoom, parentHeadRoom)
}
