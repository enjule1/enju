package dpds

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"errors"
	"github.com/golang/glog"
)


type DotProvider interface {
	Init()                                // Initialize the provider.
	Begin(lower int, upper int) bool      // Begin providing all available dots. 
	HasMore() bool                        // Are more dots available?
	Produce(params ...interface{}) error  // Produces and populates dot data fields.
	Finalize() bool                       // Cleanup and shut down.
}

type DotConsumer interface {
	Init()                                    // Initialize the consumer.
	Consume(params ...interface{}) error      // Consumes dot data.
	Commit() error                            // Commit all changes.
	Finalize() bool                           // Cleanup and shut down.
}

type DotBaseDB struct {
	connDB     *sql.DB   // Connection pool
}

type DotProviderDB struct {
	DotBaseDB
    rows       *sql.Rows // Current row set.
}

type DotConsumerDB struct {
	dbd DotBaseDB
	dotChannel   chan *ConsumableDot
}

func (dp DotBaseDB) Init() {
	if dp.connDB == nil {
		glog.Info("Begin DB connection pool.")
		connDB, err := sql.Open("mysql", "@/enju?charset=utf8")
		dp.connDB = connDB

		if err != nil {
			glog.Error("Couldn't get the database.")
		}
		glog.Info("Got DB connection.")
		glog.Flush()
	}
}

func (dp DotProviderDB) Begin(lower int, upper int) bool {
	rows, err2 := dp.connDB.Query("select id, parentId, name, value from dots where id >= ? and id < ?", lower, upper)
	if err2 != nil {
		glog.Errorf("Couldn't get any dots %s", err2)
		return false
	}
	dp.rows = rows
	
	return true
}

func (dp DotProviderDB) Produce(params ...interface{}) error {
	
	if (len(params) != 4) {
		return errors.New("Expected 4 dot fields for population")
    }

	err := dp.rows.Scan(params[0], params[1], params[2], params[3])

	if err != nil {
		glog.Error("Row read failure.")
		return errors.New("Unable to populate a new dot.")
    }
	return nil
}

func (dp DotProviderDB) HasMore() bool { 
	return dp.rows.Next()
}

func (dp DotProviderDB) Finalize() bool {

	if err := dp.rows.Close(); err != nil {
		// Something wrong...
		glog.Error("Cleanup failure.")
		return false
	}
	
	return true
}

func (dc DotConsumerDB) Init() {
	dc.dbd.Init()
	dc.dotChannel = make(chan *ConsumableDot, 20)
}

type ConsumableDot struct {
	id       int
	parentId int
	name     string
	value    string
}

func (dc DotConsumerDB) Consume(params ...interface{}) error {
	go func() {
	    dc.dotChannel <- &ConsumableDot{params[0].(int), params[1].(int), params[2].(string), params[3].(string)}
	}()
	return nil
}

func (dc DotConsumerDB) Commit() error {
	tx, err := dc.dbd.connDB.Begin()

	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`"insert into dots (id, parentId, name, value) values (?, ?, ?, ?)`)
	if err != nil {
		defer tx.Rollback()
		return err
	}

    for dot := range dc.dotChannel {
    	_, err := stmt.Exec(-1, dot.parentId, dot.name, dot.value)
    	if err != nil {
    		defer tx.Rollback()
    		return err
    	}
    }
    
    tx.Commit()

	return nil
}

func (dc DotConsumerDB) Finalize() bool {
	return true
}

type DotProviderFactory struct {
	dp     DotProvider // Dot Provider interface
}

func (dpf DotProviderFactory) GetInstance() DotProvider {
	if dpf.dp == nil {
		dpf.dp = new(DotProviderDB)
    }
    return dpf.dp
}

var dpf DotProviderFactory

func GetInstance() DotProvider {
	return dpf.GetInstance()
}

