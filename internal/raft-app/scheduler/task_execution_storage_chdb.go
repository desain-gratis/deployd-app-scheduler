package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

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

func (t *teStorageChDB) DGCompat() *teStorageDGAdapter {
	return &teStorageDGAdapter{
		teStorageChDB: t,
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
			data                     String -- JSON type has serde problem in golang (failed to parse format)
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
	m, ok := meta.(metadata)
	if !ok {
		return nil, fmt.Errorf("invalid metadata type: %T", meta)
	}

	if te.ScheduledRunAt.IsZero() {
		return nil, fmt.Errorf("scheduled_run_at must not be zero")
	}

	data, err := json.Marshal(te)
	if err != nil {
		return nil, err
	}

	// just for the query; no need on the serialization
	// todo: maybe we can just put it up afterall..
	if te.AssignmentState == nil {
		te.AssignmentState = &entity.AssignmentState{}
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
	query := `
		SELECT toString(data)
		FROM scheduler.task_execution
		WHERE namespace = ?
			AND queue_id = ?
			AND id = ?
		ORDER BY event_id DESC
		LIMIT 1
	`

	var data string
	err := t.db.QueryRowContext(
		ctx,
		query,
		namespace,
		queueID,
		id,
	).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf(
				"task execution not found: namespace=%s queue_id=%s id=%s",
				namespace,
				queueID,
				id,
			)
		}
		return nil, err
	}

	var te entity.TaskExecution
	if err := json.Unmarshal([]byte(data), &te); err != nil {
		return nil, fmt.Errorf("unmarshal task execution: %w", err)
	}

	return &te, nil
}

// GetAll reads the latest event for each task execution.
// TODO: a bit compliated, maybe can simplify
func (t *teStorageChDB) GetAll(
	ctx context.Context,
	namespace string,
) (chan *entity.TaskExecution, chan error) {
	taskCh := make(chan *entity.TaskExecution)
	errCh := make(chan error, 1)

	go func() {
		defer close(taskCh)
		defer close(errCh)

		query := `
			SELECT toString(data) AS data
			FROM
			(
				SELECT *
				FROM scheduler.task_execution
				WHERE namespace = ?
				ORDER BY event_id DESC
				LIMIT 1 BY namespace, queue_id, id
			)
			ORDER BY (namespace, scheduled_run_at, queue_id)
		`

		rows, err := t.db.QueryContext(ctx, query, namespace)
		if err != nil {
			errCh <- err
			return
		}
		defer rows.Close()

		for rows.Next() {
			var data string

			if err := rows.Scan(&data); err != nil {
				errCh <- err
				return
			}

			var te entity.TaskExecution
			if err := json.Unmarshal([]byte(data), &te); err != nil {
				errCh <- fmt.Errorf("unmarshal task execution: %w", err)
				return
			}

			select {
			case taskCh <- &te:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}

		if err := rows.Err(); err != nil {
			errCh <- err
		}
	}()

	return taskCh, errCh
}

func (t *teStorageChDB) GetPendingTasksCount(
	ctx context.Context,
	assignedID uint64,
	before time.Time,
) (int, error) {
	query := `
		SELECT count()
		FROM
		(
			SELECT *
			FROM scheduler.task_execution
			ORDER BY event_id DESC
			LIMIT 1 BY namespace, queue_id, id
		)
		WHERE
			assignment_assigned_node = ?
			AND scheduled_run_at < ?
			AND execution_status = 'PENDING'
	`

	var count uint64
	err := t.db.QueryRowContext(
		ctx,
		query,
		assignedID,
		before.UTC(),
	).Scan(&count)
	if err != nil {
		return 0, err
	}

	return int(count), nil
}
