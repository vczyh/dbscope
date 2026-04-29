package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
)

// Schema resolves column names for a given schema.table.
type Schema interface {
	ColumnNames(schema, table string) ([]string, error)
}

// MySQLSchema fetches column names from a live MySQL server.
type MySQLSchema struct {
	conn *client.Conn
}

func NewMySQLSchema(host string, port uint16, user, password string) (*MySQLSchema, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	c, err := client.Connect(addr, user, password, "")
	if err != nil {
		return nil, err
	}
	return &MySQLSchema{conn: c}, nil
}

func (m *MySQLSchema) ColumnNames(schema, table string) ([]string, error) {
	r, err := m.conn.Execute(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
		schema, table,
	)
	if err != nil {
		return nil, err
	}
	cols := make([]string, r.RowNumber())
	for i := 0; i < r.RowNumber(); i++ {
		s, _ := r.GetString(i, 0)
		cols[i] = s
	}
	return cols, nil
}

func (m *MySQLSchema) Close() error {
	if m.conn == nil {
		return nil
	}
	return m.conn.Close()
}
