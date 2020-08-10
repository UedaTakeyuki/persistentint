package persistentint

import (
	"io/ioutil"
	"os"
	"strconv"
	"sync"
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
