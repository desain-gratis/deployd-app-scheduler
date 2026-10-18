package entity

import "time"

type Node struct {
	// last ping of this node
	LastPing time.Time
	QueueIDs map[string]*QueueState
}

type QueueState struct {
	TaskAssigned uint64
	Capacity     uint64
}
