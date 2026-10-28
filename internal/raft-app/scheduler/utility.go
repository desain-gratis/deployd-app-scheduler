package scheduler

import (
	"fmt"
	"strings"
)

// GenerateWhere allows for hierarchical indexing
func GenerateSQLWhere(indexColumns []string, cols ...string) (string, []any, error) {
	if len(cols) == 0 || (len(cols) == 1 && cols[0] == "*") {
		return "", nil, nil
	}

	if len(cols) > len(indexColumns) {
		return "", nil, fmt.Errorf(
			"too many index values: got %d, want at most %d",
			len(cols), len(indexColumns),
		)
	}

	var where strings.Builder
	args := make([]any, 0, len(cols))

	for i, col := range cols {
		if col == "" || col == "*" {
			return "", nil, fmt.Errorf("invalid index value at position %d -> \"%v\"", i, col)
		}

		if i > 0 {
			where.WriteString(" AND ")
		}

		where.WriteString(indexColumns[i])
		where.WriteString(" = ?")
		args = append(args, col)
	}

	return "WHERE " + where.String(), args, nil
}
