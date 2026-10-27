package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

var _ TaskExecutionStorage = (*teStorageChDB)(nil)

type teStorageChDB struct {
	db *sql.DB
}

func NewClickhouseTaskExecutionStorage(db *sql.DB) *teStorageChDB {
	return &teStorageChDB{
		db: db,
	}
}

func (t *teStorageChDB) Init(ctx context.Context) error {
	if _, err := t.db.ExecContext(ctx, `
		CREATE DATABASE IF NOT EXISTS scheduler;
	`); err != nil {
		return err
	}

	_, err := t.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scheduler.task_execution
		(
			event_id                 UInt64,
			namespace                String,
			queue_id                 String,
			id                       String,
			scheduled_run_at         DateTime64(1, 'UTC'),
			execution_mode           LowCardinality(String),
			user_location            String,
			execution_status         LowCardinality(String),
			assignment_status        LowCardinality(String),
			assignment_assigned_node UInt64,
			data                     JSON
		)
		ENGINE = MergeTree
		ORDER BY (namespace, scheduled_run_at, queue_id);
	`)
	return err
}

type metadata struct {
	EventID uint64
}

func (t *teStorageChDB) Post(
	ctx context.Context,
	te *entity.TaskExecution,
	meta any,
) (*entity.TaskExecution, error) {
	if te == nil {
		return nil, fmt.Errorf("task execution is nil")
	}

	m, ok := meta.(metadata)
	if !ok {
		return nil, fmt.Errorf("invalid metadata type: %T", meta)
	}

	if te.ScheduledRunAt.IsZero() {
		return nil, fmt.Errorf("scheduled_run_at must not be zero")
	}

	if te.AssignmentState == nil {
		te.AssignmentState = &entity.AssignmentState{}
	}

	data, err := json.Marshal(te)
	if err != nil {
		return nil, err
	}

	query := `
		INSERT INTO scheduler.task_execution (
			event_id,
			namespace,
			queue_id,
			id,
			scheduled_run_at,
			user_location,
			execution_mode,
			execution_status,
			assignment_status,
			assignment_assigned_node,
			data
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`

	_, err = t.db.ExecContext(
		ctx,
		query,
		m.EventID,
		te.Ns,
		te.QueueID,
		te.Id,
		te.ScheduledRunAt.UTC(),
		te.UserTime.Location().String(),
		string(te.ExecutionMode),
		string(te.Status),
		string(te.AssignmentStatus),
		te.AssignmentState.AssignedTo,
		string(data),
	)
	if err != nil {
		return nil, err
	}

	return te, nil
}

// Get individual task execution.
func (t *teStorageChDB) GetByID(
	ctx context.Context,
	namespace string,
	queueID, id string,
) (*entity.TaskExecution, error) {
	return nil, fmt.Errorf("not implemented")
}

// GetAll reads the latest event for each task execution.
func (t *teStorageChDB) GetAll(ctx context.Context) error {
	query := `
		SELECT
			event_id,
			namespace,
			queue_id,
			id,
			scheduled_run_at,
			execution_mode,
			user_location,
			execution_status,
			assignment_status,
			assignment_assigned_node,
			toString(data) AS data
		FROM
		(
			SELECT *
			FROM scheduler.task_execution
			ORDER BY event_id DESC
			LIMIT 1 BY namespace, queue_id, id
		)
		ORDER BY (namespace, scheduled_run_at, queue_id) -- for scheduled task execution, i think can use this;
		-- we can always create another table to optimize
	`

	rows, err := t.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			eventID          uint64
			namespace        string
			queueID          string
			id               string
			scheduledRunAt   sql.NullTime
			executionMode    string
			userLocation     string
			executionStatus  string
			assignmentStatus string
			assignedNode     uint64
			data             string
		)

		if err := rows.Scan(
			&eventID,
			&namespace,
			&queueID,
			&id,
			&scheduledRunAt,
			&executionMode,
			&userLocation,
			&executionStatus,
			&assignmentStatus,
			&assignedNode,
			&data,
		); err != nil {
			return err
		}

		fmt.Printf(
			"eventID=%d namespace=%s queueID=%s id=%s scheduledRunAt=%v "+
				"executionMode=%s userLocation=%s executionStatus=%s "+
				"assignmentStatus=%s assignedNode=%d data=%s\n",
			eventID,
			namespace,
			queueID,
			id,
			scheduledRunAt,
			executionMode,
			userLocation,
			executionStatus,
			assignmentStatus,
			assignedNode,
			data,
		)
	}

	return rows.Err()
}
