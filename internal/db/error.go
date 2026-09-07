package db

import "fmt"

type DbQueryError struct {
	QueryName string
	Err       error
}

func (d *DbQueryError) Error() string {
	return fmt.Sprintf("%s failed, with: %s", d.QueryName, d.Err.Error())
}
