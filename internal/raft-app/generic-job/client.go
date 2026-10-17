package genericjob

import (
	"context"
	"encoding/json"
	"fmt"

	runneretcd "github.com/desain-gratis/common/lib/raft/runner-etcd"
)

type Client struct {
	raftClient *runneretcd.RaftContext
}

func NewClient(raftClient *runneretcd.RaftContext) *Client {
	return &Client{
		raftClient: raftClient,
	}
}

func (c *Client) ID() uint64 {
	return c.raftClient.ID()
}

// Propose convenient function
func (c *Client) Propose(ctx context.Context, request any) (any, error) {
	var command Command

	switch req := request.(type) {
	case CreateTask:
		command = Command_User_CreateTask
	case InitiateTaskExecution:
		command = Command_Leader_InitiateTask
	case UpdateTask:
		command = Command_Leader_UpdateTask
	case RequestRegenerateTaskInstances:
		command = Command_Leader_RequestRegenerateTaskExecution
	case CreateTaskExecution:
		command = Command_Leader_CreateTaskExecution
	case UpdateTaskExecution:
		command = Command_Worker_UpdateTaskExecution
	case RequestWork:
		command = Command_Worker_RequestWork
	case Ping:
		command = Command_Worker_Ping
	default:
		return nil, fmt.Errorf("unsupported type %T", req)
	}

	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	cmdWrap := CommandWrapper{Name: command, Value: payload}

	result, err := c.raftClient.Propose(ctx, cmdWrap)
	if err != nil {
		return nil, err
	}

	return result, nil
}
