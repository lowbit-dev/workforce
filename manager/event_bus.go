package manager

import "lowbit.dev/topic"

type EventBus struct {
	Cluster       *topic.Topic[ClusterEvent]
	Resource      *topic.Topic[ResourceEvent]
	Workers       *topic.Topic[WorkerEvent]
	Queue         *topic.Topic[QueueEvent]
	Jobs          *topic.Topic[JobEvent]
	Task          *topic.Topic[TaskStore]
	ArtifactEvent *topic.Topic[ArtifactEvent]
}

type ClusterEvent struct{}
type ResourceEvent struct{}
type WorkerEvent struct{}
type QueueEvent struct{}
type JobEvent struct{}
type TaskEvent struct{}
type ArtifactEvent struct{}
