package internal

import (
	"database/sql"
	"encoding/json"
	"reflect"
)

var (
	stringTy = reflect.TypeFor[string]()
	intTy    = reflect.TypeFor[int64]()
	floatTy  = reflect.TypeFor[float64]()
	boolTy   = reflect.TypeFor[bool]()
)

func RowsToJson(rows *sql.Rows) ([]byte, error) {
	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return nil, err
	}

	count := len(columnTypes)
	finalRows := []interface{}{}

	for rows.Next() {
		scanArgs := make([]interface{}, count)
		for i, v := range columnTypes {
			scanTy := v.ScanType()
			if scanTy.ConvertibleTo(stringTy) {
				scanArgs[i] = new(sql.NullString)
			} else if scanTy.ConvertibleTo(floatTy) {
				scanArgs[i] = new(sql.NullFloat64)
			} else if scanTy.ConvertibleTo(intTy) {
				scanArgs[i] = new(sql.NullInt64)
			} else if scanTy.ConvertibleTo(boolTy) {
				scanArgs[i] = new(sql.NullBool)
			} else {
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		finalRows = append(finalRows, scanArgs)
	}

	return json.Marshal(finalRows)
}
