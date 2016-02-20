package dpds

import (
	"bytes"
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"strconv"
	"sync"
)

type DotProvider interface {
	Init(dbSource string)                // Initialize the provider.
    GetSource() string                   // Get current data source used to init provider.
	InitFields(tableName string, queryFields []string, valueFields []string, whereFields []string, preCommit func() (canCommit bool, err error), lowerBound int, upperBound int)
    Construct() bool                     // Enable ability to construct a dot provider data store.
    Create() bool                        // Enable ability to insert/create dot provider source.
    Update() bool                        // Enable ability to update a dot.
	Destroy() bool                       // Enable ability to destroy a provider source.
	Begin() bool                         // Begin providing all available dots.
	HasMore() bool                       // Are more dots available?
	Produce(params ...interface{}) error // Produces and populates dot data fields.
	Finalize() bool                      // Cleanup and shut down.
}

type DotProviderDB struct {
	DotBaseDB
	rows        *sql.Rows // Current row set.
	tableName   string    // Name of table to use.
	queryFields []string  // Fields to query.
	valueFields []string  // values of fields to use.
	whereFields []string  // Where fields to use.
	PreCommit func() (canCommit bool, err error) // Precommit function.
	lowerBound  int       // lower bound to use.
	upperBound  int       // upper bound to use.
}

func (dp *DotProviderDB) InitFields(tableName string, queryFields []string, valueFields []string, whereFields []string, preCommit func() (canCommit bool, err error), lowerBound int, upperBound int) {
	dp.tableName = tableName
	dp.queryFields = queryFields
	dp.valueFields = valueFields
	dp.whereFields = whereFields
	dp.PreCommit  = preCommit
	dp.lowerBound = lowerBound
	dp.upperBound = upperBound
}

func writeToBuffer(arrayBuffer []string, buffer *bytes.Buffer, sep string) {
	if len(arrayBuffer) == 0 {
		// Nothing to do here.
		return
	}
	arrayBufferLen := len(arrayBuffer)

	for i := 0; i < arrayBufferLen; i++ {
		buffer.WriteString(arrayBuffer[i])
		if i < arrayBufferLen-1 {
			buffer.WriteString(sep)
		}
	}
}

func (dp *DotProviderDB) Construct() bool {
	var buffer *bytes.Buffer = new(bytes.Buffer)

	buffer.WriteString("CREATE TABLE ")
	buffer.WriteString(dp.tableName)
	buffer.WriteString(" ( ")
	writeToBuffer(dp.queryFields, buffer, ", ")
	buffer.WriteString(" ); ")

    glog.Errorf("Creational Query %s", buffer.String())
    
	_, err2 := dp.connDB.Exec(buffer.String())
	if err2 != nil {
		glog.Errorf("Couldn't get any %s %s", dp.tableName, err2)
		return false
	}

	return true
}

func (dp *DotProviderDB) Destroy() bool {
	var buffer *bytes.Buffer = new(bytes.Buffer)

	buffer.WriteString("DROP TABLE ")
	buffer.WriteString(dp.tableName)
	buffer.WriteString(";")

    glog.Errorf("Destruction Query %s", buffer.String())
    
	_, err2 := dp.connDB.Exec(buffer.String())
	if err2 != nil {
		glog.Errorf("Couldn't get any %s %s", dp.tableName, err2)
		return false
	}

	return true
}

func (dp *DotProviderDB) Create() bool {
	var buffer *bytes.Buffer = new(bytes.Buffer)

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(dp.tableName)
	buffer.WriteString(" ( ")
	writeToBuffer(dp.queryFields, buffer, ", ")
	buffer.WriteString(" ) ")
	buffer.WriteString(" VALUES ( ")
	writeToBuffer(dp.whereFields, buffer, ", ")
	buffer.WriteString(" );")

    glog.Errorf("Query %s %d %d", buffer.String(), dp.lowerBound, dp.upperBound)
    
	rows, err2 := dp.connDB.Query(buffer.String(), dp.lowerBound, dp.upperBound)
	if err2 != nil {
		glog.Errorf("Couldn't get any %s %s", dp.tableName, err2)
		return false
	}
	dp.rows = rows

	return true
}

func (dp *DotProviderDB) Update() bool {
	var buffer *bytes.Buffer = new(bytes.Buffer)

	buffer.WriteString("UPDATE ")
	buffer.WriteString(dp.tableName)
	buffer.WriteString(" SET ")
	writeToBuffer(dp.queryFields, buffer, "=?, ")
    glog.Errorf("Query %s %d %d", buffer.String(), dp.lowerBound, dp.upperBound)
    
    stmt, err2 := dp.connDB.Prepare(buffer.String())
	if err2 != nil {
		glog.Errorf("Couldn't update %s %s", dp.tableName, err2)
		return false
	}
	
	_, err3 := stmt.Exec(dp.valueFields)
    if err3 != nil {
		glog.Errorf("Couldn't update %s %s", dp.tableName, err3)
		return false
	}

	return true
}

func (dp *DotProviderDB) Begin() bool {
	var buffer *bytes.Buffer = new(bytes.Buffer)

	buffer.WriteString("SELECT ")
	writeToBuffer(dp.queryFields, buffer, ", ")
	buffer.WriteString(" FROM ")
	buffer.WriteString(dp.tableName)
	buffer.WriteString(" WHERE ")
	writeToBuffer(dp.whereFields, buffer, " ")

    glog.Errorf("Query %s %d %d", buffer.String(), dp.lowerBound, dp.upperBound)
    
	rows, err2 := dp.connDB.Query(buffer.String(), dp.lowerBound, dp.upperBound)
	if err2 != nil {
		glog.Errorf("Couldn't get any %s %s", dp.tableName, err2)
		return false
	}
	dp.rows = rows

	return true
}

func (dp *DotProviderDB) Produce(params ...interface{}) error {
	if len(params) != len(dp.queryFields) {
		return errors.New("Expected " + strconv.Itoa(len(dp.queryFields)) + " dot fields for population, got: " + strconv.Itoa(len(params)))
	}

	err := dp.rows.Scan(params...)

	if err != nil {
		glog.Errorf("Row read failure: %s", err)
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
	dotProviderMap map[string]chan DotProvider //
}

func (dpf DotProviderFactory) GetInstance(dbSource string) DotProvider {
	if dpf.dotProviderMap == nil {
		dpf.dotProviderMap = make(map[string]chan DotProvider)
	}

	_, hasProvider := dpf.dotProviderMap[dbSource]
	if !hasProvider {
		var once sync.Once

		once.Do(func() {
			glog.Errorf("Initializing pool for %s", dbSource)
			dpf.dotProviderMap[dbSource] = make(chan DotProvider, 20)

			for i := 0; i < 20; i++ {
				var dotProvider interface{}
				dotProvider = &DotProviderDB{}
				dotProvider.(DotProvider).Init(dbSource)
				dpf.dotProviderMap[dbSource] <-  dotProvider.(DotProvider)
			}
			glog.Errorf("Done initializing pool for %s", dbSource)
		})
	}

	dotProviderFound := <-dpf.dotProviderMap[dbSource]

	return dotProviderFound
}

func (dpf DotProviderFactory) ReturnProviderInstance(dotProvider DotProvider) {
	go func() {
		// Don't you want to go to your home?
		dpf.dotProviderMap[dotProvider.GetSource()]<-dotProvider
	}()
}

var dpf DotProviderFactory

func GetProviderInstance(dbSource string) DotProvider {
	return dpf.GetInstance(dbSource)
}

func ReturnProviderInstance(dotProvider DotProvider) {
	dpf.ReturnProviderInstance(dotProvider)
}
