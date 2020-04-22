package persistentint

import (
	"io/ioutil"
	"os"
	"strconv"
)

// PersistentInt
type PersistentInt struct {
	Value int
	path  string
}

func NewPersistentInt(path string) (p *PersistentInt, err error) {
	p = new(PersistentInt)
	p.path = path
	filebuffs, err := ioutil.ReadFile(p.path)
	p.Value, err = strconv.Atoi(string(filebuffs))

	return
}

func (i PersistentInt) Save() (err error) {
	ioutil.WriteFile(i.path, []byte(strconv.Itoa(i.Value)), os.FileMode(0600))
	return err
}

func (i *PersistentInt) Inc() (err error) {
	i.Value++
	i.Save()
	return err
}