package dpds

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"errors"
	"github.com/golang/glog"
)


type DotProvider interface {
	Init()                                // Initialize the provider.
	Begin(lower int, upper int) bool      // Begin providing dots. 
	HasMore() bool                        // Are more dots available?
	Populate(params ...interface{}) error // Pointer to meta dot.
	Finalize() bool                       // Cleanup and shut down.
}

type DotProviderDB struct {
    connDB     *sql.DB   // Connection pool
    rows       *sql.Rows // Current row set.
}

func (dp DotProviderDB) Init() {
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

func (dp DotProviderDB) Populate(params ...interface{}) error {
	
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

