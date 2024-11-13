package objects

import (
	"encoding/json"

	"github.com/G-Research/yunikorn-core/pkg/common/resources"
	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
)

func (node *Node) dao() *dao.NodeDAOInfo {
	return &dao.NodeDAOInfo{
		ID:                 node.ID,
		NodeID:             node.NodeID,
		HostName:           node.Hostname,
		RackName:           node.Rackname,
		PartitionID:        node.PartitionID,
		Attributes:         node.GetAttributes(),
		Capacity:           node.totalResource.Clone().DAOMap(),
		Occupied:           node.occupiedResource.Clone().DAOMap(),
		Allocated:          node.allocatedResource.Clone().DAOMap(),
		Available:          node.availableResource.Clone().DAOMap(),
		Utilized:           node.getUtilizedResource().DAOMap(),
		Allocations:        getAllocationsDAO(node.getAllocations(false)),
		ForeignAllocations: getForeignAllocationsDAO(node.getAllocations(true)),
		Schedulable:        node.schedulable,
		IsReserved:         len(node.reservations) > 0,
		Reservations:       node.getReservationKeys(),
	}
}

func (node *Node) daoSnapshot() string {
	node.snapshotLock.Lock()
	defer node.snapshotLock.Unlock()

	if err := json.NewEncoder(&node.snapshot).Encode(node.dao()); err != nil {
		// TODO: handle error
		return ""
	}
	val := node.snapshot.String()
	node.snapshot.Reset()
	return val
}

func (sn *Node) getReservationKeys() []string {
	keys := make([]string, 0)
	for key := range sn.reservations {
		keys = append(keys, key)
	}
	return keys
}

// Get the utilized resource on this node.
func (sn *Node) getUtilizedResource() *resources.Resource {
	total := sn.totalResource.Clone()
	resourceAllocated := sn.allocatedResource.Clone()
	utilizedResource := make(map[string]resources.Quantity)

	for name := range resourceAllocated.Resources {
		if total.Resources[name] > 0 {
			utilizedResource[name] = resources.CalculateAbsUsedCapacity(total, resourceAllocated).Resources[name]
		}
	}
	return &resources.Resource{Resources: utilizedResource}
}
