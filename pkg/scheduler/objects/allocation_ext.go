package objects

import (
	"sort"
	"strconv"

	"github.com/G-Research/yunikorn-core/pkg/webservice/dao"
)

func (a *Allocation) AskDAO() *dao.AllocationAskDAOInfo {
	return &dao.AllocationAskDAOInfo{
		AllocationKey:       a.GetAllocationKey(),
		AllocationTags:      a.GetTagsClone(),
		RequestTime:         a.GetCreateTime().UnixNano(),
		ResourcePerAlloc:    a.GetAllocatedResource().DAOMap(),
		Priority:            strconv.Itoa(int(a.GetPriority())),
		RequiredNodeID:      a.GetRequiredNode(),
		ApplicationID:       a.GetApplicationID(),
		Placeholder:         a.IsPlaceholder(),
		TaskGroupName:       a.GetTaskGroup(),
		AllocationLog:       getAllocationLogsDAO(a.GetAllocationLog()),
		TriggeredPreemption: a.HasTriggeredPreemption(),
		Originator:          a.IsOriginator(),
		SchedulingAttempted: a.IsSchedulingAttempted(),
		TriggeredScaleUp:    a.HasTriggeredScaleUp(),
	}
}

func (alloc *Allocation) DAO() *dao.AllocationDAOInfo {
	var requestTime int64
	if alloc.IsPlaceholderUsed() {
		requestTime = alloc.GetPlaceholderCreateTime().UnixNano()
	} else {
		requestTime = alloc.GetCreateTime().UnixNano()
	}
	allocTime := alloc.GetCreateTime().UnixNano()
	allocDAO := &dao.AllocationDAOInfo{
		AllocationKey:    alloc.GetAllocationKey(),
		AllocationTags:   alloc.GetTagsClone(),
		RequestTime:      requestTime,
		AllocationTime:   allocTime,
		AllocationDelay:  allocTime - requestTime,
		ResourcePerAlloc: alloc.GetAllocatedResource().DAOMap(),
		PlaceholderUsed:  alloc.IsPlaceholderUsed(),
		Placeholder:      alloc.IsPlaceholder(),
		TaskGroupName:    alloc.GetTaskGroup(),
		Priority:         strconv.Itoa(int(alloc.GetPriority())),
		NodeID:           alloc.GetNodeID(),
		ApplicationID:    alloc.GetApplicationID(),
		Preempted:        alloc.IsPreempted(),
		Originator:       alloc.IsOriginator(),
	}
	return allocDAO
}

func getAllocationsDAO(allocations []*Allocation) []*dao.AllocationDAOInfo {
	allocsDAO := make([]*dao.AllocationDAOInfo, 0, len(allocations))
	for _, alloc := range allocations {
		allocsDAO = append(allocsDAO, alloc.DAO())
	}
	return allocsDAO
}

func getAllocationAsksDAO(asks []*Allocation) []*dao.AllocationAskDAOInfo {
	asksDAO := make([]*dao.AllocationAskDAOInfo, 0, len(asks))
	for _, ask := range asks {
		if !ask.IsAllocated() {
			asksDAO = append(asksDAO, ask.AskDAO())
		}
	}
	return asksDAO
}

func getAllocationLogsDAO(logEntries []*AllocationLogEntry) []*dao.AllocationAskLogDAOInfo {
	logsDAO := make([]*dao.AllocationAskLogDAOInfo, len(logEntries))
	sort.SliceStable(logEntries, func(i, j int) bool {
		return logEntries[i].LastOccurrence.Before(logEntries[j].LastOccurrence)
	})
	for i, entry := range logEntries {
		logsDAO[i] = &dao.AllocationAskLogDAOInfo{
			Message:        entry.Message,
			LastOccurrence: entry.LastOccurrence.UnixNano(),
			Count:          entry.Count,
		}
	}
	return logsDAO
}

func getForeignAllocationsDAO(allocations []*Allocation) []*dao.ForeignAllocationDAOInfo {
	allocsDAO := make([]*dao.ForeignAllocationDAOInfo, 0, len(allocations))
	for _, alloc := range allocations {
		allocsDAO = append(allocsDAO, alloc.ForeignAllocationDAO())
	}
	return allocsDAO
}

func (alloc *Allocation) ForeignAllocationDAO() *dao.ForeignAllocationDAOInfo {
	allocTime := alloc.GetCreateTime().UnixNano()
	allocDAO := &dao.ForeignAllocationDAOInfo{
		AllocationKey:    alloc.GetAllocationKey(),
		AllocationTime:   allocTime,
		ResourcePerAlloc: alloc.GetAllocatedResource().DAOMap(),
		Priority:         strconv.Itoa(int(alloc.GetPriority())),
		NodeID:           alloc.GetNodeID(),
		Preemptable:      alloc.IsPreemptable(),
	}
	return allocDAO
}
