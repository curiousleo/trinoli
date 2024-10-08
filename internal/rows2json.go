package internal

import (
	"database/sql"
)

// var (
// 	stringTy = reflect.TypeFor[string]()
// 	intTy    = reflect.TypeFor[int64]()
// 	floatTy  = reflect.TypeFor[float64]()
// 	boolTy   = reflect.TypeFor[bool]()
// )

func allNulls(columnTypes []*sql.ColumnType) []interface{} {
	scanArgs := make([]interface{}, len(columnTypes))
	for i := range columnTypes {
		scanArgs[i] = new(interface{})
		// scanTy := v.ScanType()
		// if scanTy.ConvertibleTo(stringTy) {
		// 	scanArgs[i] = new(sql.NullString)
		// } else if scanTy.ConvertibleTo(floatTy) {
		// 	scanArgs[i] = new(sql.NullFloat64)
		// } else if scanTy.ConvertibleTo(intTy) {
		// 	scanArgs[i] = new(sql.NullInt64)
		// } else if scanTy.ConvertibleTo(boolTy) {
		// 	scanArgs[i] = new(sql.NullBool)
		// } else {
		// 	scanArgs[i] = new(sql.NullString)
		// }
	}
	return scanArgs
}

func RowsToJson(rows *sql.Rows) ([]Column, [][]any, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	n := len(columnTypes)
	allNulls := allNulls(columnTypes)
	data := [][]interface{}{}

	for rows.Next() {
		var scanArgs []interface{}
		scanArgs = append(scanArgs, allNulls...)
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, nil, err
		}

		data = append(data, scanArgs)
	}

	columns := make([]Column, n)
	for i := range columnTypes {
		columnType := columnTypes[i]
		columns[i] = Column{
			Name: columnType.Name(),
			Type: columnType.DatabaseTypeName(),
		}
	}

	return columns, data, nil
}
