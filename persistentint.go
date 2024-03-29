// persistentint
//
// Copyright 2020 Aterier UEDA
// Author: Takeyuki UEDA

package persistentint

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"

	// v1.1

	"errors"
	"time"

	"github.com/UedaTakeyuki/dbhandle2"
	"github.com/UedaTakeyuki/erapse"
)

// PersistentInt
type PersistentInt struct {
	value int
	path  string
	// v1.1 start
	// for db
	db *dbhandle2.DBHandle // db handle
	//usingDBs []dbhandle.DBtype  // array of db type of using
	tname string // table name
	cname string // column name
	//fname    string             // json field name
	// v1.1 end
	mu sync.Mutex
}

func NewPersistentInt(path string) (p *PersistentInt, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt)
	p.path = path
	filebuffs, err := ioutil.ReadFile(p.path)
	p.value, err = strconv.Atoi(string(filebuffs))

	return
}

// v1.1 start
func NewPersistentIntWithDB(db *dbhandle2.DBHandle, tname string, cname string /*, fname string*/) (p *PersistentInt, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt)
	//	p.path = path
	p.db = db
	//p.usingDBs = []dbhandle.DBtype{dbhandle.SQLite, dbhandle.Mariadb, dbhandle.FireStore}
	p.tname = tname
	p.cname = cname
	//p.fname = fname
	//	filebuffs, err := ioutil.ReadFile(p.path)
	//	p.Value, err = strconv.Atoi(string(filebuffs))
	if err = p.createDB(); err != nil {
		log.Println(err)
	}
	p.value, err = p.readDB()

	return
}

// read from db, save all
func NewPersistentIntWithDBAndPath(db *dbhandle2.DBHandle, tname string, cname string /*, fname string*/, path string) (p *PersistentInt, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt)
	p.path = path
	p.db = db
	//p.usingDBs = []dbhandle.DBtype{dbhandle.SQLite, dbhandle.Mariadb, dbhandle.FireStore}
	p.tname = tname
	p.cname = cname
	//p.fname = fname
	//	filebuffs, err := ioutil.ReadFile(p.path)
	//	p.Value, err = strconv.Atoi(string(filebuffs))
	if err = p.createDB(); err != nil {
		log.Println(err)
	}
	p.value, err = p.readDB()

	return
}

// read from path, save all
func NewPersistentIntWithPATHAndDB(path string, db *dbhandle2.DBHandle, tname string, cname string /*, fname string*/) (p *PersistentInt, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt)
	p.path = path
	p.db = db
	//p.usingDBs = []dbhandle.DBtype{dbhandle.SQLite, dbhandle.Mariadb, dbhandle.FireStore}
	p.tname = tname
	p.cname = cname
	//p.fname = fname
	filebuffs, err := ioutil.ReadFile(p.path)
	p.value, err = strconv.Atoi(string(filebuffs))
	//	p.Value, err = p.readDB()
	if err = p.createDB(); err != nil {
		log.Println(err)
	}

	return
}

// read from path, save all
func NewPersistentIntWithPATHAndDBUsing(path string, db *dbhandle2.DBHandle, tname string, cname string /*, fname string*/, usingDBs []dbhandle2.DBtype) (p *PersistentInt, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt)
	p.path = path
	p.db = db
	//p.usingDBs = usingDBs
	p.tname = tname
	p.cname = cname
	//p.fname = fname
	filebuffs, err := ioutil.ReadFile(p.path)
	p.value, err = strconv.Atoi(string(filebuffs))
	if err = p.createDB(); err != nil {
		log.Println(err)
	}
	//	p.Value, err = p.readDB()

	return
}

/*
func (i PersistentInt) saveDB() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	funcs := [...]func(chan dbhandle.ExitStatus) error{i.sqliteSave, i.mariadbSave, i.firebaseSave}

	c := make(chan dbhandle.ExitStatus, 6)
	defer close(c)

	for _, db := range i.usingDBs {
		go funcs[db](c)
	}
	err = dbhandle.SaveUpdateErrorHandler(i.usingDBs, fmt.Sprintf("table counter"), c)

	return
}

func (i PersistentInt) sqliteSave(c chan dbhandle.ExitStatus) (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT OR REPLACE INTO "%s" ("ID", "Attr") VALUES ("%s", JSON_SET(case json_valid("Attr") when "1" then "Attr" else '{}' end, "$.%s", "%d"))`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.SQLiteHandle.Exec(query)
	c <- dbhandle.ExitStatus{WhichDB: dbhandle.SQLite, Err: err}
	return
}

func (i PersistentInt) mariadbSave(c chan dbhandle.ExitStatus) (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT INTO %s (ID, Attr) VALUES ("%s", JSON_SET(case json_valid("Attr") when "1" then "Attr" else '{}' end, "$.%s", "%d")) ON DUPLICATE KEY UPDATE ID = values(id), Attr=values(Attr)`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.MariadbHandle.Exec(query)
	c <- dbhandle.ExitStatus{WhichDB: dbhandle.Mariadb, Err: err}
	return
}

func (i PersistentInt) firebaseSave(c chan dbhandle.ExitStatus) (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	err = i.db.FirebaseHandle.Set(i.tname, i.cname, map[string]interface{}{
		i.fname: i.Value,
	})
	c <- dbhandle.ExitStatus{WhichDB: dbhandle.FireStore, Err: err}
	return
}

func (i PersistentInt) readDB() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	funcs := [...]func() (int, error){i.sqliteRead, i.mariadbRead, i.firebaseRead}

	for _, db := range i.usingDBs {
		if value, err = funcs[db](); err == nil {
			return
		} else {
			dbhandle.LogInconsistent.Println(fmt.Sprintf("err: db = %s", dbhandle.Const2dbmsName(db)))
			dbhandle.LogInconsistent.Println(err)
		}
	}
	return
}

func (i PersistentInt) sqliteRead() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	query := fmt.Sprintf(`SELECT  json_extract(attr, "$.%s") FROM %s WHERE id="%s"`, i.fname, i.tname, i.cname)
	if err = i.db.SQLiteHandle.QueryRow(query, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func (i PersistentInt) mariadbRead() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	query := fmt.Sprintf(`SELECT  json_extract(attr, "$.%s") FROM %s WHERE id="%s"`, i.fname, i.tname, i.cname)
	if err = i.db.MariadbHandle.QueryRow(query, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func (i PersistentInt) firebaseRead() (value int, err error) {
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
*/
// v1.1 end

func (i PersistentInt) Save() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	var pathErr error
	var dbErr error
	// v1.1 start
	if i.path != "" {
		pathErr = ioutil.WriteFile(i.path, []byte(strconv.Itoa(i.value)), os.FileMode(0600))
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

func (i *PersistentInt) Inc() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.value++
	value = i.value
	err = i.Save()
	return
}

func (i *PersistentInt) Add(j int) (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.value += j
	value = i.value
	err = i.Save()
	return
}

func (i *PersistentInt) Set(j int) (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.value = j
	value = i.value
	err = i.Save()
	return
}
