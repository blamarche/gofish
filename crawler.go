package main

/*
	Usage:
	crawler [ domain_to_queue1 domain_to_queue2 ... ]
*/

import (
	"fmt"
	"flag"
	"os"
	"github.com/steveyen/gkvlite"
	/*
	"net"	
	"strconv"
	"time"
	"strings"
	*/
)

func main() {
	flag.Parse()	
	args := flag.Args()
	
	//open or create db file
	f, err := os.OpenFile("./db.gkvlite", 0666, os.ModeExclusive)
	if err!=nil {
		//fmt.Println("Creating db");
		f, err = os.Create("./db.gkvlite")
	}

	//setup store and collections
	store, err := gkvlite.NewStore(f)
	queue := store.SetCollection("scan-queue", nil)
	log := store.SetCollection("scan-log", nil)
	keywords := store.SetCollection("keyword-index", nil)

	//add any domains from command line to queue
	for i:=0; i<len(args); i++ {
		queue.Set([]byte(args[i]), []byte(args[i]))	
	}
	
	//start procesing the queue (todo: multithread process funcs)
	queue.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
	    fmt.Println(string(i.Key)+" : "+string(i.Val))
	    return true
	})
	
	//write kvstore
	store.Flush()


	_=err
	_=queue
	_=keywords
	_=log
	_=args
}
