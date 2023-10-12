package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty                 = errors.New("the key is empty")
	ErrIndexUpdateFailed          = errors.New("failed to update index")
	ErrKeyNotFound                = errors.New("key not found in database")
	ErrDataFileNotFound           = errors.New("data file is not found")
	ErrDataFileDirectoryCorrupted = errors.New("data file directory may be corrupted")
	ErrExceedMaxBatchNum          = errors.New("exceed the max batch num")
	ErrMergeIsProgress            = errors.New("a merge is in progress, try again later")
	ErrDataBaseIsBeingUsed        = errors.New("database directory is being used")
	ErrMergeRatioUnreached        = errors.New("the merge ratio do not reach the threshold in options")
	ErrNotHaveEnoughSpaceForMerge = errors.New("not enough disc space for merge")
)
