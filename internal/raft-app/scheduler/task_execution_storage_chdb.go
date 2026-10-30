package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

var _ TaskExecutionStorage = (*teStorageChDB)(nil)

type teStorageChDB struct {
	db *sql.DB
}

type metadata struct {
	EventID uint64
}

type TaskExecutionFilter struct {
	Before           *time.Time
	AssignmentStatus *entity.AssignmentStatus
	ExecutionStatus  []entity.ExecutionStatus
	AssignedNode     []uint64
}

var (
	// indexColumn used to specify individual tasks
	queryIndexColumn = []string{"namespace", "queue_id", "id"}
)

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
			namespace                LowCardinality(String),
			queue_id                 LowCardinality(String),
			id                       String,
			scheduled_run_at         DateTime64(1, 'UTC'),
			execution_mode           LowCardinality(String),
			user_location            LowCardinality(String),
			execution_status         LowCardinality(String),
			assignment_status        LowCardinality(String),
			assignment_assigned_node UInt64,
			data                     String -- JSON type has serde problem in golang (failed to parse format)
		)
		ENGINE = MergeTree
		ORDER BY (namespace, queue_id, scheduled_run_at);
	`)
	return err
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
func (t *teStorageChDB) GetAll(
	ctx context.Context,
	indexes ...string,
) (chan *entity.TaskExecution, chan error) {
	return t.GetAllWithFilter(ctx, TaskExecutionFilter{}, indexes...)
}

// GetAllWithFilter reads the latest event for each task execution,
// optionally filtering by scheduled time, execution status, assignment status,
// and assigned node.
func (t *teStorageChDB) GetAllWithFilter(
	ctx context.Context,
	filter TaskExecutionFilter,
	indexes ...string,
) (chan *entity.TaskExecution, chan error) {
	taskCh := make(chan *entity.TaskExecution)
	errCh := make(chan error, 1)

	go func() {
		defer close(taskCh)
		defer close(errCh)

		whereStatement, whereArgs, err := GenerateSQLWhere(queryIndexColumn, indexes...)
		if err != nil {
			errCh <- err
			return
		}

		query := `
			SELECT toString(data) AS data
			FROM
			(
				SELECT *
				FROM scheduler.task_execution
				` + whereStatement + `
				ORDER BY event_id DESC
				LIMIT 1 BY namespace, queue_id, id
			)
			WHERE 1 = 1
		`

		if filter.Before != nil {
			query += " AND scheduled_run_at < ?"
			whereArgs = append(whereArgs, filter.Before.UTC())
		}

		if len(filter.ExecutionStatus) > 0 {
			query += " AND execution_status IN (" +
				strings.TrimSuffix(strings.Repeat("?,", len(filter.ExecutionStatus)), ",") +
				")"

			for _, status := range filter.ExecutionStatus {
				whereArgs = append(whereArgs, string(status))
			}
		}

		if filter.AssignmentStatus != nil {
			query += " AND assignment_status = ?"
			whereArgs = append(whereArgs, string(*filter.AssignmentStatus))
		}

		if len(filter.AssignedNode) > 0 {
			query += " AND assignment_assigned_node IN (" +
				strings.TrimSuffix(strings.Repeat("?,", len(filter.AssignedNode)), ",") +
				")"

			for _, nodeID := range filter.AssignedNode {
				whereArgs = append(whereArgs, nodeID)
			}
		}

		query += " ORDER BY (namespace, queue_id, scheduled_run_at)"

		rows, err := t.db.QueryContext(ctx, query, whereArgs...)
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
