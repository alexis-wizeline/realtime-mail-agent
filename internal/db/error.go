package db

import (
	"errors"
	"fmt"
)

var (
	EmptyWorkerName  = errors.New("worker name canot be empty to claim jobs")
	LockedTimeIsZero = errors.New("locked until can not be zero")
	emptyConnString  = errors.New("connection string is empty")
)

type DbComposedErr struct {
	message string
	err     error
}

func (d *DbComposedErr) Error() string {
	return fmt.Sprintf("%s, db error: %s", d.message, d.err)
}

type DbQueryError struct {
	QueryName string
	Err       error
}

func (d *DbQueryError) Error() string {
	return fmt.Sprintf("%s failed, with: %s", d.QueryName, d.Err.Error())
}

func (d *DbQueryError) Unwrap() error {
	return d.Err
}
