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
	
	// v1.1	
	"errors"
	"github.com/UedaTakeyuki/dbhandle"
)

// PersistentInt
type PersistentInt struct {
	Value int
	path  string
	// v1.1 start
	// for db
	db    dbhandle.DBHandle
	tname string // table name
	cname string // column name
	fname string // json field name
	// v1.1 end
	mu    sync.Mutex
}

func NewPersistentInt(path string) (p *PersistentInt, err error) {
	p = new(PersistentInt)
	p.path = path
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.Atoi(string(filebuffs))

	return
}

// v1.1 start
func NewPersistentIntWithDB(db dbhandle.DBHandle, tname string, cname string, fname string) (p *PersistentInt, err error){
	p = new(PersistentInt)
//	p.path = path
	p.db = db
	p.tname = tname
	p.cname = cname
	p.fname = fname
//	filebuffs, err := ioutil.ReadFile(p.path)
//	p.Value, err = strconv.Atoi(string(filebuffs))
	p.Value = p.readDB()

	return	
}

// read from db, save all
func NewPersistentIntWithDBAndPath(db dbhandle.DBHandle, tname string, cname string, fname string, path string) (p *PersistentInt, err error){
	p = new(PersistentInt)
	p.path = path
	p.db = db
	p.tname = tname
	p.cname = cname
	p.fname = fname
//	filebuffs, err := ioutil.ReadFile(p.path)
//	p.Value, err = strconv.Atoi(string(filebuffs))
	p.Value = p.readDB()
}

// read from path, save all
func NewPersistentIntWithPATHAndDB(path string, db dbhandle.DBHandle, tname string, cname string, fname string) (p *PersistentInt, err error){
	p = new(PersistentInt)
	p.path = path
	p.db = db
	p.tname = tname
	p.cname = cname
	p.fname = fname
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.Atoi(string(filebuffs))
//	p.Value = p.readDB()
}
// v1.1 end

func (i PersistentInt) Save() (err error) {
	var pathErr error
	var dbErr error
	if i.path != nil {
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
	err = errors.New(errStr)
	return err
}

func (i *PersistentInt) Inc() (value int, err error) {
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value++
	value = i.Value
	err = i.Save()
	return
}

func (i *PersistentInt) Add(j int) (value int, err error) {
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value += j
	value = i.Value
	err = i.Save()
	return
}
