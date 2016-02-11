package dpds

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
)

type Dot struct {
	Id       uint64 `json:"-"`   // ID of dot : 0 is the base root dot.
	ParentId uint64 `json:"-"`   // ID of Parent : 0 is the base root dot.
	Name     string              // Dot's name
	Value    string              // Dot's value
}

type MetaDot struct {
	Dot
	ParentName         string             // Parent's name
	Depth              uint64             // Depth of dot from root.
	Children           uint64             // Number of children.
	RequestDotChannel  chan *RequestDot   // RequestDot channel
	DotRoute           string             // Route for this dot
}

// Process a context
func (md *MetaDot) Process(rd *RequestDot) {
	md.RequestDotChannel<-rd
}

// Definition of the base Dot for db access.
type DotBaseDB struct {
	connDB     *sql.DB   // Connection pool
	dbSource   string    // data source that was used.
}

//
//CREATE TABLE dots ( Id BIGINT(8), ParentId BIGINT(8), Name VARCHAR(255), Value VARCHAR(20000) );
//
func (db *DotBaseDB) Init(dbSource string) {
	if db.connDB == nil {
		glog.Error("Begin DB connection pool.")
		var err error = nil
		user := ""
		password := ""
		database := dbSource
		db.dbSource = dbSource
		db.connDB, err = sql.Open("mysql", user + ":" + password +"@/"+ database + "?charset=utf8")

		if err != nil {
			glog.Error("Couldn't get the database.")
		}
		glog.Error("Got DB connection.")
		glog.Flush()
	}
}

func (db *DotBaseDB) GetSource() string {
	return db.dbSource
}
