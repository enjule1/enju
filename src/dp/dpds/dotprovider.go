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

type DotProviderDB struct {
	DotBaseDB
    rows       *sql.Rows // Current row set.
}

func (dp *DotProviderDB) Begin(lower int, upper int) bool {
	rows, err2 := dp.connDB.Query("select id, parentId, name, value from dots where id >= ? and id < ?", lower, upper)
	if err2 != nil {
		glog.Errorf("Couldn't get any dots %s", err2)
		return false
	}
	dp.rows = rows
	
	return true
}

func (dp *DotProviderDB) Produce(params ...interface{}) error {
	
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

func (dp *DotProviderDB) Finalize() bool {

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

func GetProviderInstance() DotProvider {
	return dpf.GetInstance()
}

