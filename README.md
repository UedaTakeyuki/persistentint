# persistentint
Integer value which exists in persistent.

## How to use
```go:
// create new persistent int with creating new file to save it. Initial value is 0.
var CounterA, _ := persistentint.NewPersistentInt("data/CounterA.data") 

// create new persistent int with creating new file to save it. Initial value is 0.
var CounterB, _ := persistentint.NewPersistentInt64("data/CounterB.data") 

// increment counter with mutex lock, updated value is automatically saved to the file which was set as parameter of NewPersistentInt.
CounterA.Inc()

// add number to counter as same as increment mentioned above.
CounterA.Add(10)
```
