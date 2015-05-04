package main

import (
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"gopkg.in/mgo.v2"

	"github.com/vagababov/wine/proto"
)

var (
	dbHostPort = flag.String("host_port", "localhost:27017",
		"Host and port of the MongoDB.")
	dbName = flag.String("db_name", "test",
		"The name of the database to use.")
)

const (
	grapesCollection = "grapes"
)

func openCollection(dbHostPort, dbName, collection string) (*mgo.Session, *mgo.Collection, error) {
	if dbHostPort == "" {
		return nil, nil, errors.New("dbHostPort must not be empty")
	}
	session, err := mgo.Dial(dbHostPort)
	if err != nil {
		return nil, nil, fmt.Errorf("error dialing to database %s: %v", dbHostPort, err)
	}

	// TODO(vagababov): Keep? Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	// TODO(vagababov): make DB a flag.
	if dbName == "" {
		return nil, nil, errors.New("dbName must not be empty")
	}
	if collection == "" {
		return nil, nil, errors.New("collection must not be empty")
	}
	c := session.DB(dbName).C(grapesCollection)
	return session, c, nil
}

func main() {
	flag.Parse()
	session, c, err := openCollection(*dbHostPort, *dbName, grapesCollection)
	if err != nil {
		log.Fatalf("error opening collection %s: %v", grapesCollection, err)
	}
	defer session.Close()

	// First remove all the existing elements.
	if ci, err := c.RemoveAll(nil); err != nil {
		log.Fatalf("error removing existing grapes: %v", err)
	} else {
		fmt.Printf("Removed existing: %+v\n", ci)
	}

	grape := &grapes.Grape{
		Name:  proto.String("Merlot"),
		Color: grapes.Color_BLACK.Enum(),
	}

	if err := c.Insert(grape); err != nil {
		log.Fatalf("error inserting %v: %v", grape, err)
	}
}
