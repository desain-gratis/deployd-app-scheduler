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

// Propose convenient function
func (c *Client) Propose(ctx context.Context, request any) (any, error) {
	var command Command

	switch req := request.(type) {
	case ScheduleTask:
		command = Command_User_ScheduleTask
	case InitiateTaskExecution:
		command = Command_Leader_InitiateTask
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
