// sql: sql code fraction for persistentint
//
// Copyright 2022 Aterier UEDA
// Author: Takeyuki UEDA

package persistentint

import (
	"fmt"
	"time"

	dbhandle "github.com/UedaTakeyuki/dbhandle2"
	"github.com/UedaTakeyuki/erapse"
	qb "github.com/UedaTakeyuki/query"
)

const sqlCreateTableForCounter = `CREATE TABLE IF NOT EXISTS %s (
	ID       VARCHAR(16) PRIMARY KEY, 
	Value    INT,
	CHECK (JSON_VALID(attr))
	)`

func (i *PersistentInt64) createDB() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// make Query struct
	query := dbhandle.Query{"", make(map[dbhandle.DBtype]interface{})}
	createDBquery := fmt.Sprintf(sqlCreateTableForCounter, i.tname)
	query.QueryStr[dbhandle.Mariadb] = createDBquery
	query.QueryStr[dbhandle.SQLite] = createDBquery + ` WITHOUT ROWID`

	// exec query
	errStr := "create table failed." // err string in case
	err = i.db.ExecIfNotTableExist(i.cname, i.dbArrayName, query, errStr)

	return
}

func (i *PersistentInt64) readDB() (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// make Query string
	var querybuilder qb.Query
	querybuilder.SetTableName(i.tname)
	queryStr := querybuilder.Select([]interface{}{"Value"}).Where(qb.Equal("ID", i.cname)).QueryString()

	// make Query struct
	/*	query := new(dbhandle.Query)
		query.DefaultQueryStr = queryStr */

	errStr := fmt.Sprintf("id = %v", i.cname)
	err = i.db.QueryRow(i.dbArrayName, queryStr, errStr, &value)

	return
}

func (i *PersistentInt64) saveDB() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// make Query string
	var querybuilder qb.Query
	querybuilder.SetTableName(i.tname)
	queryStr := querybuilder.Update([]qb.Param{{Name: "Value", Value: i.Value}}).Where(qb.Equal("ID", i.cname)).QueryString()

	// make Query struct
	query := new(dbhandle.Query)
	query.DefaultQueryStr = queryStr

	errStr := fmt.Sprintf("id = %v", i.cname)
	err = i.db.Exec(i.dbArrayName, &query, errStr)

	return
}
