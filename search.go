package main

import (
	"fmt"
	"flag"
	"strings"
	"os"
	"sort"
	"strconv"
	"io/ioutil"
	"time"

	"github.com/steveyen/gkvlite"
	/*
	"net"	
	*/
)


func main() {
	flag.Parse()
	args := flag.Args()

	files, err := ioutil.ReadDir("./")
	if err!=nil {
		fmt.Println("Fatal:", err)
	}

	//load gkv files
	index := []*gkvlite.Collection{}
	meta := []*gkvlite.Collection{}
	title := []*gkvlite.Collection{}

	for i:=0; i<len(files); i++ {
		if strings.Contains(files[i].Name(), ".gkvlite") && !files[i].IsDir() {
			//open db file(s) 
			f, err := os.Open(files[i].Name())
			if err==nil {
				//get store
				store, err := gkvlite.NewStore(f)
				if err==nil {
					ind := store.SetCollection("keyword-index", nil)
					index = append(index, ind)	

					met := store.SetCollection("meta", nil)
					meta = append(meta, met)	

					ti := store.SetCollection("title", nil)
					title = append(title, ti)	
				}				
			}
		}
	}	

	//parse command line special cases
	if len(args)>1 && handleCommandLine(args) { 
		return
	}

	//go
	if len(args)>0 {
		processSearch(args[0], index, meta, title)
	} else {
		fmt.Println("No search specified")
	}

}

//start searching
func processSearch(phrase string, index []*gkvlite.Collection, meta []*gkvlite.Collection, title []*gkvlite.Collection) {
	start:=time.Now()

	keywords := strings.Split(strings.ToLower(phrase), " ")
	results := map[string]int{}

	for i:=0; i<len(keywords); i++ {
		hitstr := ""
		for _, ind := range index {
			hitstrtmp, err := ind.Get([]byte(keywords[i]))	
			if err==nil {
				hitstr += string(hitstrtmp)
			}
		}
		
		hits:= strings.Split(string(hitstr), "||||")
		for j:=0; j<len(hits)-1; j++ { //-1 for the extra |||| at the end
			_, ok := results[hits[j]]
			if ok {
				results[hits[j]] += 1+len(keywords)-i
			} else {
				results[hits[j]] = 1+len(keywords)-i
			}
		}
	}

	//extract results & sort, need to make a better way of doing this
	urls := make([]string, 0, len(results))	
	for k, v := range results {
		t:=""
		m:=""
		
		for _, ti := range title {
			ttmp, err := ti.Get([]byte(k))	
			if err==nil {
				t = string(ttmp)
				break
			}
		}
		for _, met := range meta {
			mtmp, err := met.Get([]byte(k))	
			if err==nil {
				m = string(mtmp)
				break
			}
		}

	    urls = append(urls, leftPad(strconv.Itoa(v), "0", 3)+"\n"+k+"\n"+t+"\n"+m+"\n")
	}
	sort.Strings(urls)

	//output results
	for i:=len(urls)-1; i>=0; i-- {
		fmt.Println(urls[i])
	}

	end:=time.Now()
    diff:=end.Sub(start)

	fmt.Println("Returned", len(urls), "results")	   
	fmt.Println("Time (ms): "+strconv.FormatFloat(diff.Seconds()*1000.0, 'f', 4, 64))
}

func leftPad(s string, padStr string, overallLen int) string {
    var padCountInt int
    padCountInt = 1 + ((overallLen-len(padStr))/len(padStr))
    var retStr = strings.Repeat(padStr, padCountInt) + s
    return retStr[(len(retStr)-overallLen):]
}

func handleCommandLine(args []string) bool {
	return false
}

