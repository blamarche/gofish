package main

import (
	"fmt"
	"flag"
	"strings"
	"os"
	"sort"

	"github.com/steveyen/gkvlite"
	/*
	"net"	
	"strconv"
	"time"
	*/
)


//dirty...
var store *gkvlite.Store


func main() {
	flag.Parse()
	args := flag.Args()
	
	//open db file(s) 
	//todo: multiple db files
	f, err := os.Open("./db.gkvlite")
	if err!=nil {
		fmt.Println("Couldn't open db file")
	}

	//setup store
	store, err = gkvlite.NewStore(f)
	if err!=nil {
		fmt.Println("Fatal: ",err)
		return
	}

	//grab collections
	index := store.SetCollection("keyword-index", nil)

	//parse command line special cases
	if len(args)>1 && handleCommandLine(args) { 
		return
	}

	//go
	if len(args)>0 {
		processSearch(args[0], index)
	} else {
		fmt.Println("No search specified")
	}

}

//start searching
func processSearch(phrase string, index *gkvlite.Collection) {
	keywords := strings.Split(strings.ToLower(phrase), " ")
	results := map[string]int{}

	//todo: additional 'hits' for first keyword, then second, etc
	for i:=0; i<len(keywords); i++ {
		hitstr, err := index.Get([]byte(keywords[i]))
		if err==nil {
			hits:= strings.Split(string(hitstr), "||||")
			//todo: add to results
		}
	}

	results["last"]=1;
	results["first"]=10;
	results["middle"]=4;



	urls := make([]string, 0, len(results))	
	for k, v := range results {
	    urls = append(urls, leftPad2Len(string(v), "0", 3)+":"+k)
	}
	sort.Strings(urls)

	for i:=0; i<len(urls); i++ {
		fmt.Println(urls[i])
	}
}

func leftPad2Len(s string, padStr string, overallLen int) string {
    var padCountInt int
    padCountInt = 1 + ((overallLen-len(padStr))/len(padStr))
    var retStr = strings.Repeat(padStr, padCountInt) + s
    return retStr[(len(retStr)-overallLen):]
}

func handleCommandLine(args []string) bool {
	return false
}

