**DESCRIPTION**
This repository is derived from the upstream github.com/apache/yunikorn-core scheduler, and
its functional differences are mostly in the areas of adding unique identifiers to various
scheduler objects (and their external DAO representations) for the purpose of the Unicorn
History Server to be able to accurately track, store, and recall relationships between the
various objects to enable data-exploration of previously deployed applications, spanning
across time.

The Unicorn History Server makes heavy use of the various ID values attached to the objects (described
in detail below), to compute usage, distinguish between similarly-named object (e.g. queues, applications,
etc).

There are no changes whatsoever to the scheduling behavior of yunikorn-core in this fork, only additions/
enhancements for object tracking. It's intended that this repo will follow and integrate upstream code
enhancements and fixes, with minimal behavioral differences.

**CHANGE DETAILS**
- `si.EventRecord` (defined in the github.com/G-Research/yunikorn-scheduler-interface repository) 
now has a `state` member, which is a string holding a JSON-encoded representation of the the
last state of a yunikorn scheduler object

- The various `Create{Queue,App,Request,...}EventRecord()` funcs now have `state` (of type *string*)
as additional required parameter.

- The scheduler objects (in pkg/scheduler/objects/) Application, Queue, Node, and PartitionContext
all now have a `ID` member, which a string containing a formatted ULID (https://github.com/oklog/ulid)
value. Every individual object has a distinct ID value, generated at the object's creation time.

- The scheduler objects Application, Queue, and Node all now have a `PartitionID` member, to accurately
identify the partition the object resided within.

- The ApplicationDAOInfo object (in pkg/webservice/dao) now also has a (QueueID *string) member, to 
identify which queue the application was deployed into.

- The PartitionQueueDAOInfo object now has a `ParentID *string` field, which contains the `ID`
value of that queue's parent queue. If it is the root queue, this field will be nil.

- The Allocation scheduler object now has two new methods for getting external DAO representations of 
the Allocation and affiliated Allocation Asks:
	func (alloc *Allocation) DAO() *dao.AllocationDAOInfo
    func (a *Allocation) AskDAO() *dao.AllocationAskDAOInfo

- During publishing of events for object creation/modification, a snapshot of the DAO representation
of the object is created and stored in the `state` member of the generated DAO.
