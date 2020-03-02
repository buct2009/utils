package database

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

const (
	dbTypeMysql = "mysql"
)

// Host  主机
type Host struct {
	IP     string `json:"ip"`
	Domain string `json:"domain"`
	Port   int    `json:"port"`
}

// UnanimityHost  id标示的主机
type UnanimityHost struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (uh *UnanimityHost) String() string {
	return fmt.Sprintf("%s:%d", uh.Host, uh.Port)
}

// UnanimityHostWithDomains   带域名的id标示的主机
type UnanimityHostWithDomains struct {
	UnanimityHost
	IP      string   `json:"ip"`
	Domains []string `json:"domains"`
}

// Field 字段
type Field struct {
	Name string
	Type string
}

// FieldType Common type include "STRING", "FLOAT", "INT", "BOOL"
func (f *Field) FieldType() string {
	return f.Type
}

// QueryRow 查询单行数据
type QueryRow struct {
	Fields []Field
	Record map[string]interface{}
}

// QueryRows 查询多行数据
type QueryRows struct {
	Fields  []Field
	Records []map[string]interface{}
}

func newQueryRow() *QueryRow {
	queryRow := new(QueryRow)
	queryRow.Fields = make([]Field, 0)
	queryRow.Record = make(map[string]interface{})
	return queryRow
}

func newQueryRows() *QueryRows {
	queryRows := new(QueryRows)
	queryRows.Fields = make([]Field, 0)
	queryRows.Records = make([]map[string]interface{}, 0)
	return queryRows
}

// MySQL Mysql主机实例
type MySQL struct {
	Host
	UserName       string
	Passwd         string
	DatabaseType   string
	DBName         string
	ConnectTimeout int
	QueryTimeout   int
	stmtDB         *sql.DB
}

// NewMySQL 创建MySQL数据库
func NewMySQL(
	ip string, port int, userName, passwd, dbName string) (mysql *MySQL, err error) {
	mysql = new(MySQL)
	mysql.DatabaseType = dbTypeMysql
	mysql.QueryTimeout = 5
	mysql.IP = ip
	mysql.Port = port
	mysql.UserName = userName
	mysql.Passwd = passwd
	mysql.DBName = dbName

	db, err := sql.Open(mysql.DatabaseType, mysql.fillConnStr())
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Second * 30)
	mysql.stmtDB = db
	return
}

// Close 关闭数据库连接
func (m *MySQL) Close() (err error) {
	if m.stmtDB != nil {
		return m.stmtDB.Close()
	}
	return
}

// GetConnection 获取数据库连接
func (m *MySQL) OpenSession(ctx context.Context) (session *sql.Conn, err error) {
	return m.stmtDB.Conn(ctx)
}

// QueryRows 执行MySQL Query语句，返回多条数据
func (m *MySQL) QueryRows(querySQL string) (queryRows *QueryRows, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query rows on %s:%d failed <-- %s", m.IP, m.Port, err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	session, err := m.OpenSession(ctx)
	if session != nil {
		defer session.Close()
	}
	if err != nil {
		return nil, err
	}

	rawRows, err := session.QueryContext(ctx, querySQL)
	// rawRows, err := db.Query(stmt)
	if rawRows != nil {
		defer rawRows.Close()
	}
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
		receiver := createReceiver(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordFromReceiver(receiver, fields))
	}
	return
}

func createReceiver(fields []Field) (receiver []interface{}) {
	receiver = make([]interface{}, 0, len(fields))
	for _, field := range fields {
		switch field.Type {
		case "string":
			{
				var val sql.NullString
				receiver = append(receiver, &val)
			}
		case "int64":
			{
				var val sql.NullInt64
				receiver = append(receiver, &val)
			}
		case "float64":
			{
				var val sql.NullFloat64
				receiver = append(receiver, &val)
			}
		case "bool":
			{
				var val sql.NullBool
				receiver = append(receiver, &val)
			}
		default:
			var val sql.NullString
			receiver = append(receiver, &val)
		}
	}

	return
}

func getRecordFromReceiver(receiver []interface{}, fields []Field) (record map[string]interface{}) {
	record = make(map[string]interface{})
	for idx := 0; idx < len(fields); idx++ {
		field := fields[idx]
		value := receiver[idx]
		switch field.Type {
		case "string":
			{
				nullVal := value.(*sql.NullString)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = nullVal.String
				}
			}
		case "int64":
			{
				nullVal := value.(*sql.NullInt64)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = nullVal.Int64
				}
			}
		case "float64":
			{
				nullVal := value.(*sql.NullFloat64)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = nullVal.Float64
				}
			}
		case "bool":
			{
				nullVal := value.(*sql.NullBool)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = nullVal.Bool
				}
			}
		default:
			nullVal := value.(*sql.NullString)
			record[field.Name] = nil
			if nullVal.Valid {
				record[field.Name] = nullVal.String
			}
		}
	}
	return
}

func getDataType(dbColType string) (colType string) {
	var columnTypeDict = map[string]string{
		"VARCHAR":  "string",
		"TEXT":     "string",
		"NVARCHAR": "string",
		"DATETIME": "string",
		"DECIMAL":  "float64",
		"BOOL":     "bool",
		"INT":      "int64",
		"BIGINT":   "int64",
	}

	colType, ok := columnTypeDict[dbColType]
	if ok {
		return
	}

	colType = "string"
	return
}

// QueryRow 执行MySQL Query语句，返回１条或０条数据
func (m *MySQL) QueryRow(stmt string) (row *QueryRow, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := m.QueryRows(stmt)
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

func (m *MySQL) fillConnStr() string {
	dbServerInfoStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		m.UserName, m.Passwd, m.IP, m.Port, m.DBName)
	if m.ConnectTimeout > 0 {
		dbServerInfoStr = fmt.Sprintf("%s?timeout=%ds&readTimeout=%ds&writeTimeout=%ds",
			dbServerInfoStr, m.ConnectTimeout, m.QueryTimeout, m.QueryTimeout)
	}

	return dbServerInfoStr
}