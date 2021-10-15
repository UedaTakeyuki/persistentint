// persistentint
//
// Copyright 2020 Aterier UEDA
// Author: Takeyuki UEDA

package persistentint

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/UedaTakeyuki/dbhandle"
	"github.com/UedaTakeyuki/erapse"
)

// PersistentInt
type PersistentInt64 struct {
	Value int64
	path  string
	mu    sync.Mutex
}

func NewPersistentInt64(path string) (p *PersistentInt64, err error) {
	p = new(PersistentInt64)
	p.path = path
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.ParseInt(string(filebuffs), 10, 64)

	return
}

// v1.1 start
func NewPersistentIntWithDB64(db *dbhandle.DBHandle, tname string, cname string, fname string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
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
func NewPersistentIntWithDBAndPath64(db *dbhandle.DBHandle, tname string, cname string, fname string, path string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
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
func NewPersistentIntWithPATHAndDB64(path string, db *dbhandle.DBHandle, tname string, cname string, fname string) (p *PersistentInt64, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	p = new(PersistentInt64)
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

func (i PersistentInt64) saveDB() (err error) {
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

func (i PersistentInt64) sqliteSave() (err error) {
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

func (i PersistentInt64) mariadbSave() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	//	query := fmt.Sprintf(`REPLACE INTO "%s" ("ID", "Attr") VALUES (%s, JSON_SET(ATTR, "$.%s", "%d")) WHERE ID="%s"`,
	query := fmt.Sprintf(`INSERT INTO %s (ID, Attr) VALUES ("%s", JSON_SET(case json_valid("Attr") when "1" then "Attr" else '{}' end, "$.%s", "%d")) ON DUPLICATE KEY UPDATE ID = values(id), Attr=values(Attr)`,
		i.tname,
		i.cname,
		i.fname,
		i.Value,
	)
	err = i.db.MariadbHandle.Exec(query)
	return
}

func (i PersistentInt64) firebaseSave() (err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	_, err = i.db.FirebaseHandle.Client.Collection(i.tname).Doc(i.cname).Set(context.Background(), map[string]interface{}{
		i.fname: i.Value,
	}, firestore.MergeAll)
	return
}

func (i PersistentInt64) readDB() (value int, err error) {
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

func (i PersistentInt64) sqliteRead() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	query := fmt.Sprintf(`SELECT  json_extract(attr, "$.%s") FROM %s WHERE id="%s"`, i.fname, i.tname, i.cname)
	if err = i.db.SQLiteHandle.QueryRow(query, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func (i PersistentInt64) mariadbRead() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	query := fmt.Sprintf(`SELECT  json_extract(attr, "$.%s") FROM %s WHERE id="%s"`, i.fname, i.tname, i.cname)
	if err = i.db.MariadbHandle.QueryRow(query, &value); err != nil {
		log.Println(err)
		return
	}
	return
}

func (i PersistentInt64) firebaseRead() (value int, err error) {
	defer erapse.ShowErapsedTIme(time.Now())

	dsnap, err := i.db.FirebaseHandle.Client.Collection(i.tname).Doc(i.cname).Get(context.Background())
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
	err = ioutil.WriteFile(i.path, []byte(strconv.FormatInt(i.Value, 10)), os.FileMode(0600))
	return err
}

func (i *PersistentInt64) Inc() (value int64, err error) {
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value++
	value = i.Value
	err = i.Save()
	return
}

func (i *PersistentInt64) Add(j int64) (value int64, err error) {
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value += j
	value = i.Value
	err = i.Save()
	return
}
