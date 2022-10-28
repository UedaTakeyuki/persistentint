// persistentint
//
// Copyright 2020 Aterier UEDA
// Author: Takeyuki UEDA

package persistentint

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"

	// v1.1

	"reflect"
	"time"

	"github.com/UedaTakeyuki/dbhandle2"
	"github.com/UedaTakeyuki/erapse"
)

// PersistentInt
type PersistentInt64 struct {
	Value int64
	path  string
	// v1.1 start
	// for db
	db       *dbhandle2.dbhandle2 // db handle
	usingDBs []dbhandle2.DBtype   // array of db type of using
	tname    string               // table name
	cname    string               // column name
	fname    string               // json field name
	// v1.1 end
	mu sync.Mutex
}

func NewPersistentInt64(path string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
	p.path = path
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.ParseInt(string(filebuffs), 10, 64)

	return
}

// v1.1 start
func NewPersistentIntWithDB64(db *dbhandle2.dbhandle2, tname string, cname string, fname string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
	//	p.path = path
	p.db = db
	p.usingDBs = []dbhandle2.DBtype{dbhandle2.SQLite, dbhandle2.Mariadb, dbhandle2.FireStore}
	p.tname = tname
	p.cname = cname
	p.fname = fname
	//	filebuffs, err := ioutil.ReadFile(p.path)
	//	p.Value, err = strconv.Atoi(string(filebuffs))
	p.Value, err = p.readDB()

	return
}

// read from db, save all
func NewPersistentIntWithDBAndPath64(db *dbhandle2.dbhandle2, tname string, cname string, fname string, path string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
	p.path = path
	p.db = db
	p.usingDBs = []dbhandle2.DBtype{dbhandle2.SQLite, dbhandle2.Mariadb, dbhandle2.FireStore}
	p.tname = tname
	p.cname = cname
	p.fname = fname
	//	filebuffs, err := ioutil.ReadFile(p.path)
	//	p.Value, err = strconv.Atoi(string(filebuffs))
	p.Value, err = p.readDB()

	return
}

// read from path, save all
func NewPersistentIntWithPATHAndDB64(path string, db *dbhandle2.dbhandle2, tname string, cname string, fname string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
	p.path = path
	p.db = db
	p.usingDBs = []dbhandle2.DBtype{dbhandle2.SQLite, dbhandle2.Mariadb, dbhandle2.FireStore}
	p.tname = tname
	p.cname = cname
	p.fname = fname
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.ParseInt(string(filebuffs), 10, 64)
	//	p.Value, err = strconv.Atoi(string(filebuffs))
	//	p.Value, err = p.readDB()

	return
}

// read from path, save all
func NewPersistentIntWithPATHAndDBUsing64(path string, db *dbhandle2.dbhandle2, tname string, cname string, fname string, usingDBs []dbhandle2.DBtype) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
	p.path = path
	p.db = db
	p.usingDBs = usingDBs
	p.tname = tname
	p.cname = cname
	p.fname = fname
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.ParseInt(string(filebuffs), 10, 64)
	//	p.Value, err = strconv.Atoi(string(filebuffs))
	//	p.Value, err = p.readDB()

	return
}

func (i PersistentInt64) saveDB() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	funcs := [...]func(chan dbhandle2.ExitStatus) error{i.sqliteSave, i.mariadbSave, i.firebaseSave}

	c := make(chan dbhandle2.ExitStatus, 6)
	defer close(c)

	for _, db := range i.usingDBs {
		go funcs[db](c)
	}
	err = dbhandle2.SaveUpdateErrorHandler(i.usingDBs, fmt.Sprintf("table counter"), c)

	/*
		var errStr string

		if i.db.SQLiteHandle.SQLiteptr != nil {
			if err := i.sqliteSave(); err != nil {
				errStr += err.Error()
				log.Println(err)
			}
		}
		if i.db.Mariadbhandle2.Mariadbptr != nil {
			if err := i.mariadbSave(); err != nil {
				errStr += err.Error()
				log.Println(err)
			}
		}
		if i.db.FirebaseHandle.Client != nil {
			if err := i.firebaseSave(); err != nil {
				errStr += err.Error()
				log.Panicln(err)
			}
		}
	*/
	return
}

func (i PersistentInt64) sqliteSave(c chan dbhandle2.ExitStatus) (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT OR REPLACE INTO "%s" ("ID", "Attr") VALUES ("%s", JSON_SET(case json_valid("Attr") when "1" then "Attr" else '{}' end, "$.%s", "%d"))`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.SQLiteHandle.Exec(query)
	c <- dbhandle2.ExitStatus{WhichDB: dbhandle2.SQLite, Err: err}
	return
}

func (i PersistentInt64) mariadbSave(c chan dbhandle2.ExitStatus) (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT INTO %s (ID, Attr) VALUES ("%s", JSON_SET(case json_valid("Attr") when "1" then "Attr" else '{}' end, "$.%s", "%d")) ON DUPLICATE KEY UPDATE ID = values(id), Attr=values(Attr)`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.Mariadbhandle2.Exec(query)
	c <- dbhandle2.ExitStatus{WhichDB: dbhandle2.Mariadb, Err: err}
	return
}

func (i PersistentInt64) firebaseSave(c chan dbhandle2.ExitStatus) (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	err = i.db.FirebaseHandle.Set(i.tname, i.cname, map[string]interface{}{
		i.fname: i.Value,
	})
	c <- dbhandle2.ExitStatus{WhichDB: dbhandle2.FireStore, Err: err}
	return
}

func (i PersistentInt64) readDB() (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	funcs := [...]func() (int64, error){i.sqliteRead, i.mariadbRead, i.firebaseRead}

	for _, db := range i.usingDBs {
		if value, err = funcs[db](); err == nil {
			return
		} else {
			dbhandle2.LogInconsistent.Println(fmt.Sprintf("err: db = %s", dbhandle2.Const2dbmsName(db)))
			dbhandle2.LogInconsistent.Println(err)
		}
	}
	return
}

func (i PersistentInt64) sqliteRead() (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	query := fmt.Sprintf(`SELECT  json_extract(attr, "$.%s") FROM %s WHERE id="%s"`, i.fname, i.tname, i.cname)
	if err = i.db.SQLiteHandle.QueryRow(query, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func (i PersistentInt64) mariadbRead() (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	query := fmt.Sprintf(`SELECT  json_extract(attr, "$.%s") FROM %s WHERE id="%s"`, i.fname, i.tname, i.cname)
	if err = i.db.Mariadbhandle2.QueryRow(query, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func (i PersistentInt64) firebaseRead() (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	dsnap, err := i.db.FirebaseHandle.Get(i.tname, i.cname)
	if err == nil {
		return
	}
	m := dsnap.Data()
	log.Println("m", reflect.TypeOf(m))
	//	value = m.(map[string]interface {})[i.fname].(int)
	value = 1

	return
}

// v1.1 end

func (i PersistentInt64) Save() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	var pathErr error
	var dbErr error
	// v1.1 start
	if i.path != "" {
		pathErr = ioutil.WriteFile(i.path, []byte(strconv.FormatInt(i.Value, 10)), os.FileMode(0600))
	}
	if i.db != nil {
		dbErr = i.saveDB()
	}
	var errStr string
	if pathErr != nil {
		errStr += pathErr.Error()
	}
	if dbErr != nil {
		errStr += dbErr.Error()
	}
	if errStr != "" {
		err = errors.New(errStr)
	}
	// v1.1 end
	return err
}

func (i *PersistentInt64) Inc() (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value++
	value = i.Value
	err = i.Save()
	return
}

func (i *PersistentInt64) Add(j int64) (value int64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value += j
	value = i.Value
	err = i.Save()
	return
}
