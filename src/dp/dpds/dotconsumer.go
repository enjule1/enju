package dpds

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"sync"
	"errors"
)


type DotConsumer interface {
	Init()                                    // Initialize the consumer.
	Prepare() error                           // Prepare for consumption.
	Consume(params ...interface{}) error      // Consumes dot data.
	Commit() error                            // Commit all changes.
	Abort() bool                              // Cleanup and shut down.
	Finalize() bool                           // Cleanup and shut down.
}

type DotConsumerDB struct {
	dbd          *DotBaseDB
	dotChannel   chan *ConsumableDot
	abort        chan int
	waiter       sync.WaitGroup
}


func (dc *DotConsumerDB) Init() {
	dc.dbd = new(DotBaseDB)
	dc.dbd.Init()
	dc.dotChannel = make(chan *ConsumableDot, 20)
	dc.abort = make(chan int)
}

type ConsumableDot struct {
	id       uint64
	parentId uint64
	name     string
	value    string
}

func (dc *DotConsumerDB) Prepare() error {
	dc.waiter.Add(1)
	return nil
}

func (dc *DotConsumerDB) Consume(params ...interface{}) error {
	go func() {
		dc.dotChannel <- &ConsumableDot{params[0].(uint64), params[1].(uint64), params[2].(string), params[3].(string)}
	    dc.waiter.Done()
	}()
	return nil
}

func (dc *DotConsumerDB) Commit() error {
	glog.Error("Begin committing dots.")

	tx, err := dc.dbd.connDB.Begin()

	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("insert into dots (Id, ParentId, Name, Value) values (?, ?, ?, ?)")
	if err != nil {
		defer tx.Rollback()
	    glog.Error("Failed to insert dots: " + err.Error())
		return err
	}

	go func() {
		dc.waiter.Wait()
	    close(dc.dotChannel)
	}()

    done := false
	for !done {
		select {
			case dot := <-dc.dotChannel:
			    if dot == nil {
			        done = true
			    	break
			    }
				glog.Error("Inserting a dot: " + dot.name)
		    	_, err := stmt.Exec(dot.id, dot.parentId, dot.name, dot.value)
		    	if err != nil {
		    		defer tx.Rollback()
			        glog.Error("Failed to insert any.")
		    		return err
		    	}
		    case <-dc.abort:
		            close(dc.dotChannel)
		    		defer tx.Rollback()
		    		return errors.New("Aborted")
		}
	}
    
    tx.Commit()
	glog.Error("Finished committing dots.")

	return nil
}
func (dc *DotConsumerDB) Abort() bool {
	dc.abort<-1
	return true
}

func (dc *DotConsumerDB) Finalize() bool {
	return true
}


type DotConsumerFactory struct {
	dc     DotConsumer // Dot Consumer interface
}

func (dcf DotConsumerFactory) GetInstance() DotConsumer {
	if dcf.dc == nil {
		dcf.dc = new(DotConsumerDB)
		dcf.dc.Init()
    }
    return dcf.dc
}

var dcf DotConsumerFactory

func GetConsumerInstance() DotConsumer {
	return dcf.GetInstance()
}
