package pipeline

import "github.com/pingcap/errors"

var (
	// ErrTaskExists is returned when creating a task that already exists.
	ErrTaskExists = errors.Normalize("task already exists", errors.RFCCodeText("DS:Task:Exists"))

	// ErrTaskNotFound is returned when a task is not found.
	ErrTaskNotFound = errors.Normalize("task not found", errors.RFCCodeText("DS:Task:NotFound"))

	// ErrTaskRunning is returned when trying to delete a running task.
	ErrTaskRunning = errors.Normalize("task is running", errors.RFCCodeText("DS:Task:Running"))

	// ErrTaskNotRunning is returned when operations require a running task.
	ErrTaskNotRunning = errors.Normalize("task not running", errors.RFCCodeText("DS:Task:NotRunning"))

	// ErrLeadershipLost is returned when leadership is lost during operation.
	ErrLeadershipLost = errors.Normalize("leadership lost", errors.RFCCodeText("DS:Task:LeadershipLost"))

	// ErrNodeNotFound is returned when a node is not found.
	ErrNodeNotFound = errors.Normalize("node not found", errors.RFCCodeText("DS:Node:NotFound"))
)
