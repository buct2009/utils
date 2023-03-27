package database

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
)

func (m *MySQL) StreamQueryRows(ctx context.Context, send func(row []interface{}) error, querySQL string, args ...interface{}) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query rows on %s:%d failed <-- %s", m.IP, m.Port, err.Error())
		}
	}()

	session, err := m.OpenSession(ctx)
	defer func() {
		if session != nil {
			_ = session.Close()
		}
	}()
	if err != nil {
		return err
	}

	rawRows, err := session.QueryContext(ctx, querySQL, args...)
	defer func() {
		if rawRows != nil {
			_ = rawRows.Close()
		}
	}()
	if err != nil {
		return
	}

	colTypes, err := rawRows.ColumnTypes()
	if err != nil {
		return
	}

	fields := make([]Field, 0, len(colTypes))
	for _, colType := range colTypes {
		fields = append(fields, Field{Name: colType.Name(), Type: getDataType(colType.DatabaseTypeName())})
	}

	for rawRows.Next() {
		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}
		row := getRecordFromReceiver(receiver, fields)
		err = send(row)
		if err != nil {
			return
		}
	}

	err = rawRows.Err()
	return
}

// QueryRowsWithContext 执行MySQL Query语句，返回多条数据
func (m *MySQL) QueryRowsWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	queryRows *Rows, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query rows on %s:%d failed <-- %s", m.IP, m.Port, err.Error())
		}
	}()

	session, err := m.OpenSession(ctx)
	defer func() {
		if session != nil {
			_ = session.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	rawRows, err := session.QueryContext(ctx, querySQL, args...)
	defer func() {
		if rawRows != nil {
			_ = rawRows.Close()
		}
	}()
	if err != nil {
		return
	}

	colTypes, err := rawRows.ColumnTypes()
	if err != nil {
		return
	}

	fields := make([]Field, 0, len(colTypes))
	for _, colType := range colTypes {
		fields = append(fields, Field{Name: colType.Name(), Type: getDataType(colType.DatabaseTypeName())})
	}

	queryRows = newQueryRows()
	queryRows.Fields = fields
	for rawRows.Next() {
		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

// QueryRowWithContext 执行MySQL Query语句，返回１条或０条数据
func (m *MySQL) QueryRowWithContext(ctx context.Context, stmt string, args ...interface{}) (
	row *Row, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := m.QueryRowsWithContext(ctx, stmt, args...)
	if err != nil || queryRows == nil {
		return
	}

	if len(queryRows.Records) < 1 {
		return
	}

	row = newQueryRow()
	row.Fields = queryRows.Fields
	row.Record = queryRows.Records[0]

	return
}

// QueryRows 执行MySQL Query语句，返回多条数据
func QueryRows(ctx context.Context, conn *sql.Conn, querySQL string, args ...interface{}) (
	queryRows *Rows, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query rows in TX failed <-- %s", err.Error())
		}
	}()

	rawRows, err := conn.QueryContext(ctx, querySQL, args...)
	// rawRows, err := db.Query(stmt)
	defer func() {
		if rawRows != nil {
			_ = rawRows.Close()
		}
	}()
	if err != nil {
		return
	}

	colTypes, err := rawRows.ColumnTypes()
	if err != nil {
		return
	}

	fields := make([]Field, 0, len(colTypes))
	for _, colType := range colTypes {
		fields = append(fields, Field{Name: colType.Name(), Type: getDataType(colType.DatabaseTypeName())})
	}

	queryRows = newQueryRows()
	queryRows.Fields = fields
	for rawRows.Next() {
		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

// QueryRowsInTx 执行MySQL Query语句，返回多条数据
func QueryRowsInTx(ctx context.Context, tx *sql.Tx, querySQL string, args ...interface{}) (
	queryRows *Rows, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query rows in TX failed <-- %s", err.Error())
		}
	}()

	rawRows, err := tx.QueryContext(ctx, querySQL, args...)
	// rawRows, err := db.Query(stmt)
	defer func() {
		if rawRows != nil {
			_ = rawRows.Close()
		}
	}()
	if err != nil {
		return
	}

	colTypes, err := rawRows.ColumnTypes()
	if err != nil {
		return
	}

	fields := make([]Field, 0, len(colTypes))
	for _, colType := range colTypes {
		fields = append(fields, Field{Name: colType.Name(), Type: getDataType(colType.DatabaseTypeName())})
	}

	queryRows = newQueryRows()
	queryRows.Fields = fields
	for rawRows.Next() {
		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

// QueryRow 执行MySQL Query语句，返回多条数据
func QueryRow(ctx context.Context, conn *sql.Conn, querySQL string, args ...interface{}) (
	row *Row, err error) {
	rows, err := QueryRows(ctx, conn, querySQL, args...)
	if err != nil {
		return
	}

	if len(rows.Records) < 1 {
		return
	}

	row = newQueryRow()
	row.Fields = rows.Fields
	row.Record = rows.Records[0]
	return
}

// QueryRowInTx 执行MySQL Query语句，返回多条数据
func QueryRowInTx(ctx context.Context, tx *sql.Tx, querySQL string, args ...interface{}) (
	row *Row, err error) {
	rows, err := QueryRowsInTx(ctx, tx, querySQL, args...)
	if err != nil {
		return
	}

	if len(rows.Records) < 1 {
		return
	}

	row = newQueryRow()
	row.Fields = rows.Fields
	row.Record = rows.Records[0]
	return
}

func getRecordFromReceiver(receiver []interface{}, fields []Field) (record []interface{}) {
	record = make([]interface{}, len(fields))
	for idx := 0; idx < len(fields); idx++ {
		field := fields[idx]
		value := receiver[idx]
		switch field.Type {
		case "string":
			{
				nullVal := value.(*sql.NullString)
				if nullVal.Valid {
					record[idx] = nullVal.String
				}
			}
		case "int32":
			{
				nullVal := value.(*sql.NullInt64)
				if nullVal.Valid {
					record[idx] = nullVal.Int64
				}
			}
		case "int64":
			{
				nullVal := value.(*sql.NullString)
				record[idx] = nil
				if nullVal.Valid {
					if nullVal.String[0] == '-' {
						intVal, err := strconv.ParseInt(nullVal.String, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("parse int64 value from '%s' failed <-- %s",
								nullVal.String, err.Error()))
						}
						record[idx] = intVal

					} else {
						uintVal, err := strconv.ParseUint(nullVal.String, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("parse uint64 value from '%s' failed <-- %s",
								nullVal.String, err.Error()))
						}
						if uintVal < 9223372036854775808 {
							record[idx] = int64(uintVal)
						} else {
							record[idx] = uintVal
						}
					}
				}
			}
		case "float64":
			{
				nullVal := value.(*sql.NullFloat64)
				if nullVal.Valid {
					record[idx] = nullVal.Float64
				}
			}
		case "float32":
			{
				nullVal := value.(*sql.NullFloat64)
				if nullVal.Valid {
					record[idx] = float32(nullVal.Float64)
				}
			}
		case "bool":
			{
				nullVal := value.(*sql.NullBool)
				if nullVal.Valid {
					record[idx] = nullVal.Bool
				}
			}
		case "binary":
			{
				rawVal := value.(*sql.RawBytes)
				if rawVal != nil && *rawVal != nil {
					val := make([]byte, len(*rawVal))
					copy(val, *rawVal)
					record[idx] = val
				}
			}
		default:
			{
				nullVal := value.(*sql.NullString)
				if nullVal.Valid {
					record[idx] = nullVal.String
				}
			}
		}
	}
	return
}
