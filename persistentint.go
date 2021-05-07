// persistentint
//
// Copyright 2020 Aterier UEDA
// Author: Takeyuki UEDA

package persistentint

import (
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"log"
	"fmt"
	
	// v1.1	
	"cloud.google.com/go/firestore"
	"errors"
	"github.com/UedaTakeyuki/dbhandle"
	"github.com/UedaTakeyuki/erapse"
	"time"
	"context"
	"reflect"
)

// PersistentInt
type PersistentInt struct {
	Value int
	path  string
	// v1.1 start
	// for db
	db    *dbhandle.DBHandle
	tname string // table name
	cname string // column name
	fname string // json field name
	// v1.1 end
	mu    sync.Mutex
}

func NewPersistentInt(path string) (p *PersistentInt, err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
	p = new(PersistentInt)
	p.path = path
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.Atoi(string(filebuffs))

	return
}

// v1.1 start
func NewPersistentIntWithDB(db *dbhandle.DBHandle, tname string, cname string, fname string) (p *PersistentInt, err error){
	defer erapse.ShowErapsedTIme(time.Now())
	
	p = new(PersistentInt)
//	p.path = path
	p.db = db
	p.tname = tname
	p.cname = cname
	p.fname = fname
//	filebuffs, err := ioutil.ReadFile(p.path)
//	p.Value, err = strconv.Atoi(string(filebuffs))
	p.Value, err = p.readDB()

	return	
}

// read from db, save all
func NewPersistentIntWithDBAndPath(db *dbhandle.DBHandle, tname string, cname string, fname string, path string) (p *PersistentInt, err error){
	defer erapse.ShowErapsedTIme(time.Now())
	
	p = new(PersistentInt)
	p.path = path
	p.db = db
	p.tname = tname
	p.cname = cname
	p.fname = fname
//	filebuffs, err := ioutil.ReadFile(p.path)
//	p.Value, err = strconv.Atoi(string(filebuffs))
	p.Value, err = p.readDB()

	return
}

// read from path, save all
func NewPersistentIntWithPATHAndDB(path string, db *dbhandle.DBHandle, tname string, cname string, fname string) (p *PersistentInt, err error){
	defer erapse.ShowErapsedTIme(time.Now())
	
	p = new(PersistentInt)
	p.path = path
	p.db = db
	p.tname = tname
	p.cname = cname
	p.fname = fname
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.Atoi(string(filebuffs))
//	p.Value, err = p.readDB()
	
	return
}

func (i PersistentInt) saveDB() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
	var errStr string

	if i.db.SQLiteHandle.SQLiteptr != nil {
		if err := i.sqliteSave(); err != nil {
			errStr += err.Error()
			log.Println(err)
		}
	}
	if i.db.MariadbHandle.Mariadbptr != nil {
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
	return
}

func (i PersistentInt) sqliteSave() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT OR REPLACE INTO "%s" ("ID", "Attr") VALUES ("%s", JSON_SET(case json_valid("Attr") when "1" then "Attr" else '{}' end, "$.%s", "%d"))`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.SQLiteHandle.Exec(query)
	return
}

func (i PersistentInt) mariadbSave() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) ON DUPLICATE KEY UPDATE`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.MariadbHandle.Exec(query)
	return
}

func (i PersistentInt) firebaseSave() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	_, err = i.db.FirebaseHandle.Client.Collection(i.tname).Doc(i.cname).Update(context.Background(), []firestore.Update{
		{
			Path:  i.fname,
			Value: i.Value,
		},
	})
	return
}

func (i PersistentInt) readDB() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
	var errStr string

	if i.db.SQLiteHandle.SQLiteptr != nil {
		if value, err = i.sqliteRead(); err != nil {
			errStr += err.Error()
			log.Println(err)
		} else {
			return
		}
	}
	if i.db.MariadbHandle.Mariadbptr != nil {
		if value, err = i.mariadbRead(); err != nil {
			errStr += err.Error()
			log.Println(err)
		} else {
			return
		}
	}
	if i.db.FirebaseHandle.Client != nil {
		if value, err = i.firebaseRead(); err != nil {
			errStr += err.Error()
			log.Panicln(err)
		} else {
			return
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
	
	dsnap, err := i.db.FirebaseHandle.Client.Collection(i.tname).Doc(i.cname).Get(context.Background())
	if err == nil {
		return
	}
	m := dsnap.Data()
	log.Println("m",reflect.TypeOf(m))
//	value = m.(map[string]interface {})[i.fname].(int)
	value = 1

	return
}

// v1.1 end

func (i PersistentInt) Save() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
	var pathErr error
	var dbErr error
	// v1.1 start
	if i.path != "" {
		pathErr = ioutil.WriteFile(i.path, []byte(strconv.Itoa(i.Value)), os.FileMode(0600))
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

	i.Value++
	value = i.Value
	err = i.Save()
	return
}

func (i *PersistentInt) Add(j int) (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())
	
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value += j
	value = i.Value
	err = i.Save()
	return
}
