package entity

type Node struct {
	QueueIDs map[string]*QueueState
}

type QueueState struct {
	TaskAssigned uint64
	Capacity     uint64
}
