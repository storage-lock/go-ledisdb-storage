package ledisdb_storage

import "fmt"

var (
	ErrKeyPrefixEmpty       = fmt.Errorf("KeyPrefix can not empty")
	ErrConnectionManagerNil = fmt.Errorf("ConnectionManager can not nil")
)
