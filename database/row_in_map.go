package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/zr-hebo/utils/concurrent"
	"github.com/zr-hebo/utils/container"
)

const (
	dbTypeMysql       = "mysql"
	overflowIn64Value = 9223372036854775808
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

// RowInMap 查询单行数据
type RowInMap struct {
	Fields []Field
	Record map[string]interface{}
}

// RowInOrderedMap 查询单行数据
type RowInOrderedMap struct {
	Fields []Field
	Record *container.OrderedMap
}

// Row 查询单行数据
type Row struct {
	Fields []Field
	Record []interface{}
}

// RowsInMap 查询多行数据
type RowsInMap struct {
	Fields  []Field
	Records []map[string]interface{}
}

// RowsInOrderedMap 查询多行数据
type RowsInOrderedMap struct {
	Fields  []Field
	Records []*container.OrderedMap
}

// Rows 查询多行数据
type Rows struct {
	Fields  []Field
	Records [][]interface{}
}

func newQueryRowInMap() *RowInMap {
	queryRow := new(RowInMap)
	queryRow.Fields = make([]Field, 0)
	queryRow.Record = make(map[string]interface{})
	return queryRow
}

func newQueryRowInOrderedMap() *RowInOrderedMap {
	queryRow := new(RowInOrderedMap)
	queryRow.Fields = make([]Field, 0)
	queryRow.Record = container.NewOrderedMap()
	return queryRow
}

func newQueryRow() *Row {
	queryRow := new(Row)
	queryRow.Fields = make([]Field, 0)
	queryRow.Record = make([]interface{}, 0)
	return queryRow
}

func newQueryRowsInMap() *RowsInMap {
	queryRows := new(RowsInMap)
	queryRows.Fields = make([]Field, 0)
	queryRows.Records = make([]map[string]interface{}, 0)
	return queryRows
}

func newQueryRowsInOrderedMap() *RowsInOrderedMap {
	queryRows := new(RowsInOrderedMap)
	queryRows.Fields = make([]Field, 0)
	queryRows.Records = make([]*container.OrderedMap, 0, 8)
	return queryRows
}

func newQueryRows() *Rows {
	queryRows := new(Rows)
	queryRows.Fields = make([]Field, 0)
	queryRows.Records = make([][]interface{}, 0, 8)
	return queryRows
}

// MySQL Mysql主机实例
type MySQL struct {
	Host
	UserName        string
	Passwd          string
	DatabaseType    string
	DBName          string
	MultiStatements bool
	MaxLifetime     int
	MaxIdleTime     int
	QueryTimeout    int
	// https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_execution_time
	// the unit is millisecond
	MaxExecutionTime int
	UseSSL           bool
	MaxAllowedPacket int
	maxIdleConns     int
	maxOpenConns     int
	// https://github.com/go-sql-driver/mysql#interpolateparams
	InterpolateParams bool
	// https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html
	IsolationLevel string
	Compress       bool

	retryTimes    int
	retryInterval time.Duration

	connectionLock sync.Mutex
	rawDB          *sql.DB
}

// NewMySQL 创建MySQL数据库
func NewMySQL(
	ip string, port int, userName, passwd, dbName string) (mysql *MySQL, err error) {
	mysql = new(MySQL)
	mysql.DatabaseType = dbTypeMysql
	mysql.QueryTimeout = 30
	mysql.IP = ip
	mysql.Port = port
	mysql.UserName = userName
	mysql.Passwd = passwd
	mysql.DBName = dbName
	mysql.retryTimes = 1
	mysql.retryInterval = time.Second

	return
}

// NewMySQLWithTimeout 创建MySQL数据库
func NewMySQLWithTimeout(
	ip string, port int, userName, passwd, dbName string, timeout int) (mysql *MySQL, err error) {
	mysql = new(MySQL)
	mysql.DatabaseType = dbTypeMysql
	mysql.QueryTimeout = timeout
	mysql.IP = ip
	mysql.Port = port
	mysql.UserName = userName
	mysql.Passwd = passwd
	mysql.DBName = dbName
	mysql.retryTimes = 1
	mysql.retryInterval = time.Second

	return
}

func (m *MySQL) SetRetryTimes(retryTimes int) {
	m.retryTimes = retryTimes
}

func (m *MySQL) SetRetryInterval(interval time.Duration) {
	m.retryInterval = interval
}

// SetConnMaxLifetime 设置连接超时时间
func (m *MySQL) SetConnMaxLifetime(lifetime int) {
	m.MaxLifetime = lifetime
}

// SetMaxIdleConns 设置最大空闲连接
func (m *MySQL) SetMaxIdleConns(n int) {
	m.maxIdleConns = n
}

// SetMaxOpenConns 设置最大连接数
func (m *MySQL) SetMaxOpenConns(n int) {
	m.maxOpenConns = n
}

func (m *MySQL) SetMaxExecutionTime(maxExecutionTime int) {
	m.MaxExecutionTime = maxExecutionTime
}

// Close 关闭数据库连接
func (m *MySQL) Close() (err error) {
	if m.rawDB != nil {
		err = m.rawDB.Close()
		m.rawDB = nil
	}
	return
}

// RawDB 获取数据库连接
func (m *MySQL) RawDB() (db *sql.DB, err error) {
	m.connectionLock.Lock()
	defer m.connectionLock.Unlock()

	if m.rawDB == nil {
		var conn *sql.DB
		conn, err = sql.Open(m.DatabaseType, m.fillConnStr())
		if err != nil {
			return nil, err
		}

		if m.MaxLifetime != 0 {
			conn.SetConnMaxLifetime(time.Second * time.Duration(m.MaxLifetime))
		}
		if m.MaxIdleTime != 0 {
			conn.SetConnMaxIdleTime(time.Second * time.Duration(m.MaxIdleTime))
		}
		if m.maxOpenConns != 0 {
			conn.SetMaxOpenConns(m.maxOpenConns)
		}
		if m.maxIdleConns != 0 {
			conn.SetMaxIdleConns(m.maxIdleConns)
		}
		m.rawDB = conn
	}

	db = m.rawDB
	return
}

// OpenSession 获取数据库连接
func (m *MySQL) OpenSession(ctx context.Context) (session *sql.Conn, err error) {
	rawDB, err := m.RawDB()
	if err != nil {
		return
	}

	session, err = rawDB.Conn(ctx)

	return
}

func (m *MySQL) QueryRowsInMap(querySQL string, args ...interface{}) (queryRows *RowsInMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		queryRows, err = m.queryRowsInMap(querySQL, args...)
		if err == nil {
			return
		}

		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(context.Background(), m.retryInterval)
		}
	}

	return
}

// QueryRowsInMap 执行 MySQL Query语句，返回多条数据
func (m *MySQL) queryRowsInMap(querySQL string, args ...interface{}) (queryRows *RowsInMap, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.QueryTimeout)*time.Second)
	defer cancel()
	return m.queryRowsInMapWithContext(ctx, querySQL, args...)
}

func (m *MySQL) QueryRowsInOrderedMap(querySQL string, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		queryRows, err = m.queryRowsInOrderedMap(querySQL, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(context.Background(), m.retryInterval)
		}
	}

	return
}

// QueryRowsInOrderedMap 执行 MySQL Query语句，返回多条数据
func (m *MySQL) queryRowsInOrderedMap(querySQL string, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.QueryTimeout)*time.Second)
	defer cancel()
	return m.queryRowsInOrderedMapWithContext(ctx, querySQL, args...)
}

func (m *MySQL) QueryRowsInMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	queryRows *RowsInMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = m.queryRowsInMapWithContext(ctx, querySQL, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(ctx, m.retryInterval)
		}
	}

	return
}

// QueryRowsInMapWithContext 执行 MySQL Query语句，返回多条数据
func (m *MySQL) queryRowsInMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	queryRows *RowsInMap, err error) {
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

	queryRows, err = QueryRowsWithMap(ctx, session, querySQL, args...)
	return
}

func (m *MySQL) QueryRowsInOrderedMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = m.queryRowsInOrderedMapWithContext(ctx, querySQL, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(ctx, m.retryInterval)
		}
	}

	return
}

// QueryRowsInOrderedMapWithContext 执行 MySQL Query语句，返回多条数据
func (m *MySQL) queryRowsInOrderedMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
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

	queryRows, err = QueryRowsWithOrderedMap(ctx, session, querySQL, args...)
	return
}

func QueryRowsInMapWithRetry(ctx context.Context, conn *sql.Conn, querySQL string, retryTimes int, args ...interface{}) (queryRows *RowsInMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = QueryRowsWithMap(ctx, conn, querySQL, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowsWithMap 执行 MySQL Query语句，返回多条数据
func QueryRowsWithMap(ctx context.Context, conn *sql.Conn, querySQL string, args ...interface{}) (
	queryRows *RowsInMap, err error) {
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

	queryRows = newQueryRowsInMap()
	queryRows.Fields = fields
	for rawRows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordInMapFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

func QueryRowsWithMapInTxWithRetry(ctx context.Context, tx *sql.Tx, querySQL string, retryTimes int, args ...interface{}) (
	queryRows *RowsInMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = QueryRowsWithMapInTx(ctx, tx, querySQL, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowsWithMapInTx 执行 MySQL Query语句，返回多条数据
func QueryRowsWithMapInTx(ctx context.Context, tx *sql.Tx, querySQL string, args ...interface{}) (
	queryRows *RowsInMap, err error) {
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

	queryRows = newQueryRowsInMap()
	queryRows.Fields = fields
	for rawRows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordInMapFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

func QueryRowsWithOrderedMapWithRetry(ctx context.Context, conn *sql.Conn, querySQL string, retryTimes int, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = QueryRowsWithOrderedMap(ctx, conn, querySQL, args...)
		if err == nil {
			return
		}

		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowsWithOrderedMap 执行 MySQL Query语句，返回多条数据
func QueryRowsWithOrderedMap(ctx context.Context, conn *sql.Conn, querySQL string, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
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

	queryRows = newQueryRowsInOrderedMap()
	queryRows.Fields = fields
	for rawRows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordInOrderedMapFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

func QueryRowsWithOrderedMapInTxWithRetry(ctx context.Context, tx *sql.Tx, querySQL string, retryTimes int, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = QueryRowsWithOrderedMapInTx(ctx, tx, querySQL, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowsWithOrderedMapInTx 执行 MySQL Query语句，返回多条数据
func QueryRowsWithOrderedMapInTx(ctx context.Context, tx *sql.Tx, querySQL string, args ...interface{}) (
	queryRows *RowsInOrderedMap, err error) {
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

	queryRows = newQueryRowsInOrderedMap()
	queryRows.Fields = fields
	for rawRows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		receiver := createReceivers(fields)
		err = rawRows.Scan(receiver...)
		if err != nil {
			err = fmt.Errorf("scan rows failed <-- %s", err.Error())
			return
		}

		queryRows.Records = append(queryRows.Records, getRecordInOrderedMapFromReceiver(receiver, fields))
	}

	err = rawRows.Err()
	return
}

func QueryRowWithMapWithRetry(ctx context.Context, conn *sql.Conn, stmt string, retryTimes int, args ...interface{}) (
	queryRows *RowInMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		queryRows, err = QueryRowWithMap(ctx, conn, stmt, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowWithMap 执行 MySQL Query语句，返回１条或０条数据
func QueryRowWithMap(ctx context.Context, conn *sql.Conn, stmt string, args ...interface{}) (
	row *RowInMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := QueryRowsWithMap(ctx, conn, stmt, args...)
	if err != nil || queryRows == nil {
		return
	}

	if len(queryRows.Records) < 1 {
		return
	}

	row = newQueryRowInMap()
	row.Fields = queryRows.Fields
	row.Record = queryRows.Records[0]
	return
}

func QueryRowWithMapInTxWithRetry(ctx context.Context, tx *sql.Tx, stmt string, retryTimes int, args ...interface{}) (
	row *RowInMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		row, err = QueryRowWithMapInTx(ctx, tx, stmt, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowWithMapInTx 执行 MySQL Query语句，返回１条或０条数据
func QueryRowWithMapInTx(ctx context.Context, tx *sql.Tx, stmt string, args ...interface{}) (
	row *RowInMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := QueryRowsWithMapInTx(ctx, tx, stmt, args...)
	if err != nil || queryRows == nil {
		return
	}

	if len(queryRows.Records) < 1 {
		return
	}

	row = newQueryRowInMap()
	row.Fields = queryRows.Fields
	row.Record = queryRows.Records[0]

	return
}

func QueryRowWithOrderedMapWithRetry(ctx context.Context, conn *sql.Conn, stmt string, retryTimes int, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		row, err = QueryRowWithOrderedMap(ctx, conn, stmt, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowWithOrderedMap 执行 MySQL Query语句，返回１条或０条数据
func QueryRowWithOrderedMap(ctx context.Context, conn *sql.Conn, stmt string, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := QueryRowsWithOrderedMap(ctx, conn, stmt, args...)
	if err != nil || queryRows == nil {
		return
	}

	if len(queryRows.Records) < 1 {
		return
	}

	row = newQueryRowInOrderedMap()
	row.Fields = queryRows.Fields
	row.Record = queryRows.Records[0]
	return
}

func QueryRowWithOrderedMapInTxWithRetry(ctx context.Context, tx *sql.Tx, stmt string, retryTimes int, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		row, err = QueryRowWithOrderedMapInTx(ctx, tx, stmt, args...)
		if err == nil {
			return
		}
		if i != retryTimes-1 {
			concurrent.WaitWithDuration(ctx, time.Second*1)
		}
	}

	return
}

// QueryRowWithOrderedMapInTx 执行 MySQL Query语句，返回１条或０条数据
func QueryRowWithOrderedMapInTx(ctx context.Context, tx *sql.Tx, stmt string, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := QueryRowsWithOrderedMapInTx(ctx, tx, stmt, args...)
	if err != nil || queryRows == nil {
		return
	}

	if len(queryRows.Records) < 1 {
		return
	}

	row = newQueryRowInOrderedMap()
	row.Fields = queryRows.Fields
	row.Record = queryRows.Records[0]
	return
}

func (m *MySQL) QueryRowInMap(stmt string, args ...interface{}) (
	row *RowInMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		row, err = m.queryRowInMap(stmt, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(context.Background(), m.retryInterval)
		}
	}

	return
}

// RowInMap 执行 MySQL Query语句，返回１条或０条数据
func (m *MySQL) queryRowInMap(querySQL string, args ...interface{}) (row *RowInMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.QueryTimeout)*time.Second)
	defer cancel()
	return m.queryRowInMapWithContext(ctx, querySQL, args...)
}

func (m *MySQL) QueryRowInOrderedMap(querySQL string, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		row, err = m.queryRowInOrderedMap(querySQL, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(context.Background(), m.retryInterval)
		}
	}

	return
}

// RowInOrderedMap 执行 MySQL Query语句，返回１条或０条数据
func (m *MySQL) queryRowInOrderedMap(querySQL string, args ...interface{}) (row *RowInOrderedMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.QueryTimeout)*time.Second)
	defer cancel()
	return m.queryRowInOrderedMapWithContext(ctx, querySQL, args...)
}

func (m *MySQL) QueryRowInMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	row *RowInMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		row, err = m.queryRowInMapWithContext(ctx, querySQL, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(ctx, m.retryInterval)
		}
	}

	return
}

// QueryRowInMapWithContext 执行 MySQL Query语句，返回１条或０条数据
func (m *MySQL) queryRowInMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	row *RowInMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	queryRows, err := m.queryRowsInMapWithContext(ctx, querySQL, args...)
	if err != nil || queryRows == nil {
		return
	}

	if len(queryRows.Records) < 1 {
		return
	}

	row = newQueryRowInMap()
	row.Fields = queryRows.Fields
	row.Record = queryRows.Records[0]

	return
}

func (m *MySQL) QueryRowInOrderedMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	for i := 0; i < m.retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		row, err = m.queryRowInOrderedMapWithContext(ctx, querySQL, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(ctx, m.retryInterval)
		}
	}

	return
}

// QueryRowInOrderedMapWithContext 执行 MySQL Query语句，返回１条或０条数据
func (m *MySQL) queryRowInOrderedMapWithContext(ctx context.Context, querySQL string, args ...interface{}) (
	row *RowInOrderedMap, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("query row failed <-- %s", err.Error())
		}
	}()

	resp, err := m.queryRowsInOrderedMapWithContext(ctx, querySQL, args...)
	if err != nil || resp == nil {
		return
	}

	if len(resp.Records) < 1 {
		return
	}

	row = newQueryRowInOrderedMap()
	row.Fields = resp.Fields
	row.Record = resp.Records[0]
	return
}

func (m *MySQL) fetchRowsInMapAsync(ctx context.Context,
	recordChan chan map[string]interface{}, fields []Field, querySQL string, args ...interface{}) {
	var err error
	defer func() {
		if err != nil {
			panic(err.Error())
		}
		close(recordChan)
	}()

	session, err := m.OpenSession(ctx)
	defer func() {
		if session != nil {
			_ = session.Close()
		}
	}()
	if err != nil {
		return
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

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("async query context canceled <-- %s\n", ctx.Err().Error())
			return

		default:
			if rawRows.Next() {
				receiver := createReceivers(fields)
				err = rawRows.Scan(receiver...)
				if err != nil {
					panic(fmt.Sprintf("scan rows failed <-- %s", err.Error()))
				}

				recordChan <- getRecordInMapFromReceiver(receiver, fields)

			} else {
				err = rawRows.Err()
				if err != nil {
					panic(fmt.Sprintf("async query failed <-- %s", err.Error()))
				}
				return
			}
		}
	}
}

func createReceivers(fields []Field) (receivers []interface{}) {
	receivers = make([]interface{}, 0, len(fields))
	for _, field := range fields {
		switch field.Type {
		case "string":
			{
				var val sql.NullString
				receivers = append(receivers, &val)
			}
		case "int32":
			{
				var val sql.NullInt64
				receivers = append(receivers, &val)
			}
		case "int64":
			{
				var val sql.NullString
				receivers = append(receivers, &val)
			}
		case "float32":
			{
				var val sql.NullFloat64
				receivers = append(receivers, &val)
			}
		case "float64":
			{
				var val sql.NullFloat64
				receivers = append(receivers, &val)
			}
		case "bool":
			{
				var val sql.NullBool
				receivers = append(receivers, &val)
			}
		case "binary":
			{
				var val sql.RawBytes
				receivers = append(receivers, &val)
			}
		default:
			var val sql.NullString
			receivers = append(receivers, &val)
		}
	}

	return
}

func getRecordInMapFromReceiver(receiver []interface{}, fields []Field) (record map[string]interface{}) {
	record = make(map[string]interface{}, len(fields))
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
		case "int32":
			{
				nullVal := value.(*sql.NullInt64)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = nullVal.Int64
				}
			}
		case "int64":
			{
				nullVal := value.(*sql.NullString)
				record[field.Name] = nil
				if nullVal.Valid {
					if nullVal.String[0] == '-' {
						intVal, err := strconv.ParseInt(nullVal.String, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("parse int64 value from '%s' failed <-- %s",
								nullVal.String, err.Error()))
						}
						record[field.Name] = intVal

					} else {
						uintVal, err := strconv.ParseUint(nullVal.String, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("parse uint64 value from '%s' failed <-- %s",
								nullVal.String, err.Error()))
						}
						if uintVal < overflowIn64Value {
							record[field.Name] = int64(uintVal)
						} else {
							record[field.Name] = uintVal
						}
					}
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
		case "float32":
			{
				nullVal := value.(*sql.NullFloat64)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = float32(nullVal.Float64)
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
		case "binary":
			{
				rawVal := value.(*sql.RawBytes)
				record[field.Name] = nil
				if rawVal != nil && *rawVal != nil {
					val := make([]byte, len(*rawVal))
					copy(val, *rawVal)
					record[field.Name] = val
				}
			}
		default:
			{
				nullVal := value.(*sql.NullString)
				record[field.Name] = nil
				if nullVal.Valid {
					record[field.Name] = nullVal.String
				}
			}
		}
	}
	return
}

func getRecordInOrderedMapFromReceiver(receiver []interface{}, fields []Field) (record *container.OrderedMap) {
	record = container.NewOrderedMapWithSize(len(fields))
	for idx := 0; idx < len(fields); idx++ {
		field := fields[idx]
		value := receiver[idx]
		switch field.Type {
		case "string":
			{
				nullVal := value.(*sql.NullString)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					record.Set(field.Name, nullVal.String)
				}
			}
		case "int32":
			{
				nullVal := value.(*sql.NullInt64)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					record.Set(field.Name, nullVal.Int64)
				}
			}
		case "int64":
			{
				nullVal := value.(*sql.NullString)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					if nullVal.String[0] == '-' {
						intVal, err := strconv.ParseInt(nullVal.String, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("parse int64 value from '%s' failed <-- %s",
								nullVal.String, err.Error()))
						}
						record.Set(field.Name, intVal)

					} else {
						uintVal, err := strconv.ParseUint(nullVal.String, 10, 64)
						if err != nil {
							panic(fmt.Sprintf("parse uint64 value from '%s' failed <-- %s",
								nullVal.String, err.Error()))
						}
						if uintVal < overflowIn64Value {
							record.Set(field.Name, int64(uintVal))

						} else {
							record.Set(field.Name, uintVal)

						}
					}
				}
			}
		case "float64":
			{
				nullVal := value.(*sql.NullFloat64)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					record.Set(field.Name, nullVal.Float64)
				}
			}
		case "float32":
			{
				nullVal := value.(*sql.NullFloat64)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					record.Set(field.Name, nullVal.Float64)
				}
			}
		case "bool":
			{
				nullVal := value.(*sql.NullBool)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					record.Set(field.Name, nullVal.Bool)
				}
			}
		case "binary":
			{
				rawVal := value.(*sql.RawBytes)
				record.Set(field.Name, nil)
				if rawVal != nil && *rawVal != nil {
					val := make([]byte, len(*rawVal))
					copy(val, *rawVal)
					record.Set(field.Name, val)
				}
			}
		default:
			{
				nullVal := value.(*sql.NullString)
				record.Set(field.Name, nil)
				if nullVal.Valid {
					record.Set(field.Name, nullVal.String)
				}
			}
		}
	}
	return
}

var columnTypeDict = map[string]string{
	"CHAR":       "string",
	"VARCHAR":    "string",
	"NVARCHAR":   "string",
	"DATE":       "string",
	"TIME":       "string",
	"YEAR":       "string",
	"DATETIME":   "string",
	"TIMESTAMP":  "string",
	"DECIMAL":    "string",
	"FLOAT":      "float32",
	"DOUBLE":     "float64",
	"BOOL":       "bool",
	"TINYINT":    "int32",
	"SMALLINT":   "int32",
	"MEDIUMINT":  "int32",
	"INT":        "int32",
	"INTEGER":    "int32",
	"BIGINT":     "int64",
	"BINARY":     "binary",
	"BIT":        "binary",
	"VARBINARY":  "binary",
	"BLOB":       "binary",
	"TINYBLOB":   "binary",
	"MEDIUMBLOB": "binary",
	"LONGBLOB":   "binary",
	"TEXT":       "string",
	"TINYTEXT":   "string",
	"MEDIUMTEXT": "string",
	"LONGTEXT":   "string",
}

func getDataType(dbColType string) (colType string) {
	colType, ok := columnTypeDict[dbColType]
	if ok {
		return
	}

	colType = "string"
	return
}

func (m *MySQL) fillConnStr() string {
	zone := url.QueryEscape("+08:00")
	loc := url.QueryEscape("Asia/Shanghai")
	dbServerInfoStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?multiStatements=%v&interpolateParams=%v&maxAllowedPacket=%d&time_zone='%s'&loc='%s'",
		m.UserName, m.Passwd, m.IP, m.Port, m.DBName, m.MultiStatements, m.InterpolateParams, m.MaxAllowedPacket, zone, loc)
	if m.QueryTimeout > 0 {
		dbServerInfoStr = fmt.Sprintf("%s&timeout=3s&readTimeout=%ds&writeTimeout=%ds",
			dbServerInfoStr, m.QueryTimeout, m.QueryTimeout)
	}
	if m.MaxExecutionTime > 0 {
		dbServerInfoStr = fmt.Sprintf("%s&max_execution_time=%d", dbServerInfoStr, m.MaxExecutionTime)
	}
	if m.UseSSL {
		dbServerInfoStr = fmt.Sprintf("%s&tls=skip-verify", dbServerInfoStr)
	}
	if m.Compress {
		dbServerInfoStr = fmt.Sprintf("%s&compress=true", dbServerInfoStr)
	}
	if m.IsolationLevel != "" {
		dbServerInfoStr = fmt.Sprintf("%s&tx_isolation=%s", dbServerInfoStr, m.IsolationLevel)
	}

	return dbServerInfoStr
}

func (m *MySQL) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	for i := 0; i < m.retryTimes; i++ {
		result, err = m.exec(query, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(context.Background(), m.retryInterval)
		}
	}

	return
}

// Exec 执行 MySQL dml语句，返回执行结果
func (m *MySQL) exec(query string, args ...interface{}) (sql.Result, error) {
	session, err := m.OpenSession(context.Background())
	if session != nil {
		defer session.Close()
	}
	if err != nil {
		return nil, err
	}
	return session.ExecContext(context.Background(), query, args...)
}

func (m *MySQL) ExecContext(ctx context.Context, query string, args ...interface{}) (result sql.Result, err error) {
	for i := 0; i < m.retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		result, err = m.execContext(ctx, query, args...)
		if err == nil {
			return
		}
		if i != m.retryTimes-1 {
			concurrent.WaitWithDuration(ctx, m.retryInterval)
		}
	}
	return
}

// ExecContext 执行MySQL dml语句，返回执行结果
func (m *MySQL) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	session, err := m.OpenSession(ctx)
	defer func() {
		if session != nil {
			_ = session.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	return session.ExecContext(ctx, query, args...)
}

func (m *MySQL) StreamQueryRowInMap(
	ctx context.Context, rowDealer func(map[string]interface{}) error, querySQL string, args ...interface{}) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("stream query rows on %s:%d failed <-- %s", m.IP, m.Port, err.Error())
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

		rowInMap := getRecordInMapFromReceiver(receiver, fields)
		err = rowDealer(rowInMap)
		if err != nil {
			return
		}

		err = rawRows.Err()
		if err != nil {
			return
		}
	}
	return
}
