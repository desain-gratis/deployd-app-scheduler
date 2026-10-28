package scheduler

import (
	"context"
	"fmt"

	"github.com/desain-gratis/common/delivery/mycontent-api/mycontent"
	"github.com/desain-gratis/deployd-app-scheduler/src/entity"
)

var _ mycontent.Usecase[*entity.TaskExecution] = (*teStorageDGAdapter)(nil)

// desain gratis mycontent wrapper for task execution storage
type teStorageDGAdapter struct {
	*teStorageChDB
}

func (t *teStorageDGAdapter) Post(ctx context.Context, data *entity.TaskExecution, meta any) (*entity.TaskExecution, error) {
	return nil, fmt.Errorf("not supported")
}

// Get all of your resource for your user ID here
func (t *teStorageDGAdapter) Get(ctx context.Context, namespace string, refIDs []string, ID string) ([]*entity.TaskExecution, error) {
	// todo if refIDs & ID specified we reject...
	// or we implement GetAll based on the ordering

	result := make([]*entity.TaskExecution, 0)

	indexes := make([]string, 0, len(refIDs)+2)
	if namespace != "" {
		indexes = append(indexes, namespace)
	}
	if len(refIDs) > 0 {
		indexes = append(indexes, refIDs...)
	}
	if ID != "" {
		indexes = append(indexes, ID)
	}

	taskCh, errCh := t.teStorageChDB.GetAll(ctx, indexes...)
	for taskCh != nil || errCh != nil {
		select {
		case task, ok := <-taskCh:
			if !ok {
				taskCh = nil
				continue
			}

			result = append(result, task)

		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// Stream response
func (t *teStorageDGAdapter) Stream(ctx context.Context, namespace string, refIDs []string, ID string) (<-chan *entity.TaskExecution, error) {
	return nil, fmt.Errorf("not supported yet") // TODO: support
}

// Delete your resource here
// the implementation can check whether there are linked resource or not
func (t *teStorageDGAdapter) Delete(ctx context.Context, namespace string, refIDs []string, ID string) (*entity.TaskExecution, error) {
	return nil, fmt.Errorf("not supported")
}
