package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/desain-gratis/common/lib/notifier"
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

// TODO: move to it's own dedicated raft-app client that can be composed with other raft app
// (the same / similar to "multi")
func (c *Client) Subscribe(ctx context.Context) (notifier.Subscription, error) {
	cmdWrap := CommandWrapper{Name: Command_Subscribe, Value: nil}

	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second) // IMPORTANT to avoid stuck; TODO: ALL USE TIMEOUT FOR DG
	defer cancel()

	result, err := c.raftClient.Propose(ctx2, cmdWrap)
	if err != nil {
		return nil, err
	}

	subs, ok := result.(notifier.Subscription)
	if !ok {
		return nil, fmt.Errorf("fatal: not a subscription")
	}

	return subs, nil
}

// Propose convenient function
func (c *Client) Propose(ctx context.Context, request any) (any, error) {
	var command Command

	switch req := request.(type) {
	case CreateRecurringTask:
		command = Command_User_CreateRecurringTask
	case UpdateRecurringTask:
		command = Command_Leader_UpdateRecurringTask
	case RequestRegenerateTaskInstances:
		command = Command_Leader_GenerateRecurringTaskExecution
	case CreateTaskExecution:
		command = Command_CreateTaskExecution
	case UpdateTaskExecution:
		command = Command_Worker_UpdateTaskExecution
	case UpdateAvailability:
		command = Command_Worker_UpdateAvailability
	default:
		return nil, fmt.Errorf("unsupported type %T", req)
	}

	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	cmdWrap := CommandWrapper{Name: command, Value: payload}

	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second) // IMPORTANT to avoid stuck; TODO: ALL USE TIMEOUT FOR DG
	defer cancel()

	result, err := c.raftClient.Propose(ctx2, cmdWrap)
	if err != nil {
		return nil, err
	}

	return result, nil
}
