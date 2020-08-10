package persistentint

import (
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

// PersistentInt
type PersistentInt struct {
	Value int
	path  string
	mu    sync.Mutex
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
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value++
	i.Save()
	return err
}

func (i *PersistentInt) Add(j int) (err error) {
	// lock
	i.mu.Lock()
	defer i.mu.Unlock()

	i.Value += j
	i.Save()
	return err
}
