package main

/*
	Usage:
	crawler help
	crawler [command]
	crawler [ domain_to_queue1 domain_to_queue2 ... ]
*/

import (
	"fmt"
	"flag"
	"os"
	"time"
	"strconv"
	"net/http"
	"io/ioutil"
	"github.com/steveyen/gkvlite"
	"code.google.com/p/go.net/html"
	/*
	"strings"
	*/
)

//dirty...
var store *gkvlite.Store

//Main function
func main() {
	flag.Parse()	
	args := flag.Args()
	
	//open or create db file
	f, err := os.OpenFile("./db.gkvlite", 0666, os.ModeExclusive)
	if err!=nil {
		f, err = os.Create("./db.gkvlite")
	}

	//setup store
	store, err = gkvlite.NewStore(f)
	if err!=nil {
		fmt.Println("Fatal: ",err)
		return
	}

	//grab collections
	queue := store.SetCollection("scan-queue", nil)
	log := store.SetCollection("scan-log", nil)
	index := store.SetCollection("keyword-index", nil)

	//parse command line special cases
	if len(args)>0 && handleCommandLine(args, queue, log, index) { 
		return
	}

	//add any extra domains from command line to queue
	for i:=0; i<len(args); i++ {
		queue.Set([]byte(args[i]), []byte(args[i]))	
	}
	
	//start procesing the queue
	processQueue(queue, log, index)
	
	//write kvstore
	store.Flush()
}

//Processes the entire queue top to bottom. 
func processQueue(queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection) {
	fmt.Println("Crawling...")
	
	queue.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
	    //TODO: goroutines!
	    fmt.Println("Indexing: "+string(i.Key))
	    start:=time.Now()

	    //fun time!
	    resp, err := http.Get(string(i.Key))
		if err != nil {
			fmt.Println("Err-Get: ", err)
			//todo: delete url from log & queue if 404, or store in a broken link collection?
		} else {
			
			//if javascript or text, use regex to pull any http://, if html
			//we token using go.net html parser, otherwise skip
			if resp.Header.Get("Content-Type")=="text/html" {
				
				fmt.Println("Scraping html...")
				p := html.NewTokenizer(resp.Body)
			    for { 
			        tokenType := p.Next() 
			        if tokenType == html.ErrorToken {
			            break
			        }       
			        token := p.Token()
			        scrapeToken(token, queue, index)
			    }

			} else if resp.Header.Get("Content-Type")=="application/octet-stream" {

				resp.Body.Close()
				fmt.Println("Binary. Skipping...")
				//todo: remove from queue & log, perhaps add to binaries collection?
			
			} else {
			
				fmt.Println("Using "+resp.Header.Get("Content-Type")+" as text...")
				body, err := ioutil.ReadAll(resp.Body)
				if err!=nil {
					fmt.Println("Err-Read: ", err)
				}
				//TODO: regex to extract urls to queue, probably wont bother with keywords on these
				fmt.Println(string(body))
				defer resp.Body.Close()

			}		
		}
		
		//stats
	    end:=time.Now()
	    diff:=end.Sub(start)

		fmt.Println("Time (ms): "+strconv.FormatFloat(diff.Seconds()*1000.0, 'f', 4, 64))
		fmt.Println("Finished: "+string(i.Key))	    
		fmt.Println()

	    //log serialized time of indexing
	    log.Set(i.Key, []byte(strconv.FormatInt(time.Now().Unix(), 10)))
	    queue.Delete(i.Key)

	    store.Flush()
	    return true
	})
}

//Grabs Urls, keywords from token attributes, data, etc
//adds urls to queue, keywords to index
func scrapeToken(token html.Token, queue *gkvlite.Collection, index *gkvlite.Collection) {
	switch token.Type {
        case html.StartTagToken: // <tag>
        	// type Token struct {
            //     Type     TokenType
            //     DataAtom atom.Atom
            //     Data     string
            //     Attr     []Attribute
            // }
            //
            // type Attribute struct {
            //     Namespace, Key, Val string
            // }
        case html.TextToken: // text
        case html.EndTagToken: // </tag>
        case html.SelfClosingTagToken: // <tag/>
    }
}

//Handle the few command line options logic
func handleCommandLine(args []string, queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection) bool {
	if args[0]=="help" {

		fmt.Println("Usage: crawler [command]\nUsage: crawler [domain domain ...]")
		fmt.Println("Commands: list-queue list-log")
		return true

	} else if args[0]=="list-queue" {
	
		fmt.Println("Current Queue\n--------------")
		queue.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    fmt.Println(string(i.Key))
		    return true
		})
		return true

	} else if args[0]=="list-log" {
		
		fmt.Println("Current Log\n--------------")
		log.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    t, _ := strconv.ParseInt(string(i.Val), 10, 64)
		    gt := time.Unix(t, 0)
		    fmt.Println(string(i.Key)+" : "+gt.String())
		    return true
		})
		return true

	}

	return false
}
