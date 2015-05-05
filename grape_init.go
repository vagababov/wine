package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
	"gopkg.in/mgo.v2"

	"github.com/vagababov/wine/proto"
)

var (
	dbHostPort = flag.String("host_port", "localhost:27017",
		"Host and port of the MongoDB.")
	dbName = flag.String("db_name", "test",
		"The name of the database to use.")
	fileDBPath = flag.String("file_db", "./data/grapes.db",
		"Path to the file containing colon separated entries for "+
			"the grapes to upload to the mongo database.")
)

const (
	grapesCollection = "grapes"
)

func loadGrapeFileDB(fileName string) ([]*grapes.Grape, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("could not open file %s: %v", fileName, err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var ret []*grapes.Grape
	lineNo := 1
	for {
		b, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading file %s: %v", fileName, err)
		} else if err == io.EOF {
			break
		}
		buf := &bytes.Buffer{}
		for len(b) > 0 {
			r, size := utf8.DecodeRune(b)
			buf.WriteRune(r)
			b = b[size:]
		}
		line := buf.String()
		line = strings.Trim(line, "\r\n \t")
		if line == "" || line[0] == '#' {
			lineNo++
			continue
		}
		pieces := strings.Split(line, ":")
		const numFields = 6

		// Check number of elements.
		if len(pieces) != numFields {
			return nil, fmt.Errorf("error on line %d in file %s: number of items %d, expect: %d",
				lineNo, fileName, len(pieces), numFields)
		}

		// Verify color is correct.
		if _, ok := grapes.Color_value[pieces[1]]; !ok {
			return nil, fmt.Errorf("error on line %d in file %s: color '%s' is not recognized",
				lineNo, fileName, pieces[1])
		}

		g := &grapes.Grape{
			Name:     proto.String(pieces[0]),
			Color:    grapes.Color(grapes.Color_value[pieces[1]]).Enum(),
			Parent1:  proto.String(pieces[2]),
			Parent2:  proto.String(pieces[3]),
			Regions:  strings.Split(pieces[4], ","),
			AltNames: strings.Split(pieces[5], ","),
		}
		ret = append(ret, g)
		lineNo++
	}
	return ret, nil
}

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

	data, err := loadGrapeFileDB(*fileDBPath)
	if err != nil {
		log.Fatalf("error loading initial data: %v", err)
	}
	fmt.Printf("Data: %+v", data)

	os.Exit(0)
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
	fmt.Println(proto.MarshalTextString(grape))

	if err := c.Insert(grape); err != nil {
		log.Fatalf("error inserting %v: %v", grape, err)
	}
}
