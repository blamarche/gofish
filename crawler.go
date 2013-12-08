package main

/*
	Usage:
	crawler help
	crawler [command]
	crawler [ url_to_queue1 url_to_queue2 ... ]
*/

import (
	"fmt"
	"flag"
	"os"
	"time"
	"strconv"
	"net/http"
	"net/url"
	"io/ioutil"
	"strings"
	"regexp"
	"sync"

	"github.com/steveyen/gkvlite"
	"code.google.com/p/go.net/html"	

	"./websearch"
)

//dirty...
type RespChan chan *http.Response
type StrChan chan string

var store *gkvlite.Store
var all_urls bool

var responses RespChan
var scanurls StrChan
var waitsave sync.WaitGroup

//Main function
func main() {
	flag.Parse()	
	args := flag.Args()

	responses = make(chan *http.Response, 10)
	scanurls = make(chan string, 10)	
	all_urls = false

	/*
	if len(args)==0 || args[0]!="no-compact" {
		compactDb()
	}
	*/

	//open or create db file
	f, err := os.OpenFile("./db.gkv", 0666, os.ModeExclusive)
	if err!=nil {
		f, err = os.Create("./db.gkv")
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
	meta := store.SetCollection("meta", nil)
	title := store.SetCollection("title", nil)

	//todo: root-domain scoring algo

	//parse command line special cases
	if len(args)>0 && handleCommandLine(args, queue, log, index, meta, title) { 
		return
	}

	//add any extra domains from command line to queue
	for i:=0; i<len(args); i++ {
		if (args[i]=="all-urls") {
			all_urls = true
		} else if (args[i]=="start-http") {
			websearch.StartServer()
		} else if (args[i]=="start-https") {
			websearch.StartServerSSL()
		} else {
			queueAndCleanUrl(args[i], queue)
		}
	}
	
	//crawl
	for {
		//check log for sites to recrawl
		queueLog(queue, log)

		//start procesing the queue
		processQueue(queue, log, index, meta, title)
		
		//write kvstore
		store.Flush()

		fmt.Println("Sleeping...")
		time.Sleep(3000 * time.Millisecond)	
	}

}

//Add to queue, items taht havent been crawled recently
func queueLog(queue *gkvlite.Collection, log *gkvlite.Collection) {
	fmt.Println("Checking log...")
	
	log.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
	    datediff := 0.0
		datetmp := i.Val

    	t, err := strconv.ParseInt(string(datetmp), 10, 64)
	    logdate := time.Unix(t, 0)

    	if err==nil {
	    	diff := time.Now().Sub(logdate)
	    	datediff = diff.Hours() / 24.0
	    }

	    if datediff >= 7.0 {
	    	queueAndCleanUrl(string(i.Key), queue)
	    }

	    return true
	})
}

func threadHttpRequester() {
	theurl := <- scanurls //wait for first one
	httpRequester(theurl)
	
	for theurl := range scanurls {
		waitsave.Wait()
		httpRequester(theurl)
	}
}

func httpRequester(theurl string) {
	fmt.Println("Requesting: "+theurl)
	resp, err := http.Get(theurl)
	if err != nil {
		fmt.Println("Err-Get: ", err)
		//todo: delete url from log & queue if 404, or store in a broken link collection?
	} else {
		resp.Request.RequestURI = theurl //hack. docs say not to do this, but its blank otherwise, should custom type it
		responses <- resp
	}
}

func threadResponseProcessor(queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection, meta *gkvlite.Collection, title *gkvlite.Collection) {
	resp := <- responses //wait for first one
	responseProcessor(resp, queue, log, index, meta, title)

	for resp := range responses {
		waitsave.Wait()
		responseProcessor(resp, queue, log, index, meta, title)
	}
}

func responseProcessor(resp *http.Response, queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection, meta *gkvlite.Collection, title *gkvlite.Collection) {
	theurl := resp.Request.RequestURI

	waitsave.Wait()

	fmt.Println("Indexing: "+theurl)
    start:=time.Now()

	//if html we tokenize using go.net html parser
	//if javascript or text, use regex to pull any http://, otherwise skip
	if strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		
		fmt.Println("Scraping html...")
		p := html.NewTokenizer(resp.Body)
	    for { 
	        tokenType := p.Next() 
	        if tokenType == html.ErrorToken {
	            break
	        }       
	        token := p.Token()
	        scrapeToken(token, p, theurl, queue, index, meta, title)
	    }

	} else if strings.Contains(resp.Header.Get("Content-Type"), "application/") {

		resp.Body.Close()
		fmt.Println("Binary. Skipping...")
		//todo: perhaps add to binaries collection?
	
	} else if strings.Contains(resp.Header.Get("Content-Type"), "image/") {

		resp.Body.Close()
		fmt.Println("Binary. Skipping...")	

	} else if strings.Contains(resp.Header.Get("Content-Type"), "text"){
	
		fmt.Println("Using "+resp.Header.Get("Content-Type")+" as text...")
		body, err := ioutil.ReadAll(resp.Body)
		if err!=nil {
			fmt.Println("Err-Read: ", err)
		}
		//TODO: regex to extract urls to queue, probably wont bother with keywords on these
		_=string(body)
		defer resp.Body.Close()
	}

	//stats
    end:=time.Now()
    diff:=end.Sub(start)

	fmt.Println("Time (ms): "+strconv.FormatFloat(diff.Seconds()*1000.0, 'f', 4, 64))
	fmt.Println("Finished: "+theurl)	    
	fmt.Println()

    //log serialized time of indexing
    log.Set([]byte(theurl), []byte(strconv.FormatInt(time.Now().Unix(), 10)))
    queue.Delete([]byte(theurl))
}

func threadSaver() {
	for {
		time.Sleep(10000 * time.Millisecond)
		waitsave.Add(1)
		fmt.Println("Saving db...")
		time.Sleep(500 * time.Millisecond)		
		store.Flush()
		waitsave.Done()		
	}
}

//Processes the entire queue top to bottom. 
func processQueue(queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection, meta *gkvlite.Collection, title *gkvlite.Collection) {
	fmt.Println("Crawling...")

	for i:=0; i<20; i++ {
		go threadHttpRequester()
	}
	go threadResponseProcessor(queue, log, index, meta, title)
	go threadSaver()
	
	queue.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {

		waitsave.Wait()

		//remove trailing folder slash
		theurl:=string(i.Key)
		if strings.LastIndex(theurl, "/")==len(theurl)-1 {
			theurl = strings.TrimRight(theurl, "/")
		}

	    //check log to make sure we havent recently scanned a url before we go get it again (7 days)
	    datediff := 99999.9
		datetmp, err := log.Get(i.Key)

	    if err==nil {
	    	t, err := strconv.ParseInt(string(datetmp), 10, 64)
		    logdate := time.Unix(t, 0)

	    	if err==nil {
		    	diff := time.Now().Sub(logdate)
		    	datediff = diff.Hours() / 24.0
		    } else {
		    	//fmt.Println("ERR: ",err)
		    }
	    } else {
	    	//fmt.Println("ERR: ",err)
	    }

	    if datediff >= 7.0 {
	    	scanurls <- string(i.Key)
			//todo: gofish headers etc
	    	//todo: timeout-based domain blacklist to check before queueing
		} else {
			fmt.Println("Skipping, last indexed", datediff, "days ago")
		}

	    return true
	})
}

//queue a url to be indexed, removing non-relevant parts, etc
func queueAndCleanUrl(theurl string, queue *gkvlite.Collection) string {
	if strings.Contains(theurl, "#") {
		urlpart := strings.Split(theurl, "#")
		theurl = urlpart[0]
	}

	if strings.Contains(theurl, "?") {
		urlpart := strings.Split(theurl, "?")
		theurl = urlpart[0]
	}

	if strings.LastIndex(theurl, "/")==len(theurl)-1 {
		theurl = strings.TrimRight(theurl, "/")
	}

	//strip down to domain/subdomain only unless overridden
	if !all_urls {
		u, err := url.Parse(theurl)
		if err==nil {

			if u.Scheme=="" {
				theurl = "http://"+u.Host	
			} else {
				theurl = u.Scheme+"://"+u.Host
			}
			
			test, _ := queue.Get([]byte(theurl))
			if test==nil {
				fmt.Println("Queueing "+theurl)
				queue.Set([]byte(theurl), []byte(""))
			}
		}
	} else {	
		test, _ := queue.Get([]byte(theurl))
		if test==nil {
			fmt.Println("Queueing "+theurl)
			queue.Set([]byte(theurl), []byte(""))
		}
	}

	return theurl
}

//Grabs Urls, keywords from token attributes, data, etc
//adds urls to queue, keywords to index
func scrapeToken(token html.Token, tokenizer *html.Tokenizer, urlo string, queue *gkvlite.Collection, 
						index *gkvlite.Collection, meta *gkvlite.Collection, title *gkvlite.Collection) {
	switch token.Type {
        case html.StartTagToken: // <tag>
        	if token.Data == "a" {
        		href:=""
        		linktext:=""
        		_=linktext

        		nextType:=tokenizer.Next()
        		if nextType==html.TextToken {
        			linktext=tokenizer.Token().Data
        		}

        		for i:=0; i<len(token.Attr); i++ {
        			if token.Attr[i].Key=="href" {
        				href = token.Attr[i].Val
        				if strings.Contains(href, ":") {
        					if strings.Contains(href, "http") {
        						//queue.Set([]byte(href), []byte(""))
        						queueAndCleanUrl(href, queue)
        						//fmt.Println("Queueing "+cleaned)
        						//addKeywords(href, linktext, index)
        					}
        				} else {
        					u, err := url.Parse(urlo)
        					if err==nil {
        						u, err = u.Parse(href)
        						if err==nil {
	        						//queue.Set([]byte(u.String()), []byte(""))
	        						queueAndCleanUrl(u.String(), queue)
		        					//fmt.Println("Queueing "+cleaned)  
		        					//addKeywords(url+href, linktext, index)
		        				}
        					}
        				}
        			}
        		}

        	} else if token.Data == "meta" {
        		use:=false
        		for i:=0; i<len(token.Attr); i++ {
        			if token.Attr[i].Key=="name" && token.Attr[i].Val=="description" {
        				use=true
        			} else if token.Attr[i].Key=="content" && use {
        				text := token.Attr[i].Val
        				meta.Set([]byte(urlo), []byte(text))
        				addKeywords(urlo, text, index)
        			}

        		}

        	} else if token.Data == "title" || token.Data == "h1" || token.Data == "h2" || token.Data == "strong" {
				nextType:=tokenizer.Next()
        		if nextType==html.TextToken {
        			eltext:=tokenizer.Token().Data
        			if token.Data == "title" {
        				title.Set([]byte(urlo), []byte(eltext))
        			}
        			addKeywords(urlo, eltext, index)
        		}  

        	}

        case html.TextToken: // text
        	if strings.Index(token.Data, "http://")==0 || strings.Index(token.Data, "https://")==0 {
				//queue.Set([]byte(token.Data), []byte(""))
        		queueAndCleanUrl(token.Data, queue)
				//fmt.Println("Queueing "+cleaned)
			} else if strings.Index(token.Data, "www.")==0 {
				//queue.Set([]byte("http://"+token.Data), []byte(""))
				queueAndCleanUrl("http://"+token.Data, queue)
				//fmt.Println("Queueing "+cleaned)
			}

        case html.EndTagToken: // </tag>
        case html.SelfClosingTagToken: // <tag/>
    }
}

//extract and add qualified keywords to index
func addKeywords(urlo string, keywordtext string, index *gkvlite.Collection) {
	//remove non-alphanumerics, non-space chars for spaces
	reg, _ := regexp.Compile("[^a-zA-Z0-9 ]")
	keywordtext = string(reg.ReplaceAll([]byte(keywordtext), []byte(" ")))

	//split and loop
	keywords := strings.Split(strings.ToLower(keywordtext), " ")
	for i:=0; i<len(keywords); i++ {
		if len(keywords[i])>2 {
			switch keywords[i] {
				//ignored keywords
				case "and", "the", "not":

				//got this far? add em
				default:
					//todo: come up with better storage mechanism
					list, err := index.Get([]byte(keywords[i]))
					if err!=nil {
						index.Set([]byte(keywords[i]), []byte(urlo+"||||"))
						fmt.Println("Keyword: "+keywords[i])
					} else {
						urls := strings.Split(string(list), "||||")
						add:= true
						for j:=0; j<len(urls); j++ {
							if urls[j]==urlo {
								add = false
							}
						}
						if add {
							index.Set([]byte(keywords[i]), []byte(string(list)+urlo+"||||"))
							fmt.Println("Keyword: "+keywords[i])
						}
					}
			}
		}
	}
}

//Handle the few command line options logic
func handleCommandLine(args []string, queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection, meta *gkvlite.Collection, title *gkvlite.Collection) bool {
	if args[0]=="help" {

		fmt.Println("Usage: crawler [command]\nUsage: crawler [url url ...]")
		fmt.Println("Defaults - Only crawl domains and subdomain index pages. Use all-urls command to change.")
		fmt.Println("Commands: start-http start-https all-urls compact-db list-queue list-log list-index list-meta list-keywords list-titles clear-queue clear-log")
		return true

	} else if args[0]=="compact-db" {

		compactDb()
		return true

	} else if args[0]=="clear-queue" {

		fmt.Println("Clearing Queue\n--------------")
		store.RemoveCollection("scan-queue")
		store.Flush()
		return true

	} else if args[0]=="clear-log" {

		fmt.Println("Clearing Log\n--------------")
		store.RemoveCollection("scan-log")
		store.Flush()
		return true

	} else if args[0]=="list-queue" {
	
		fmt.Println("Current Queue\n--------------")
		queue.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    fmt.Println(string(i.Key))
		    return true
		})
		return true

  	} else if args[0]=="list-index" {
	
		fmt.Println("Current Index\n--------------")
		index.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    fmt.Println(string(i.Key)+" : "+string(i.Val))
		    return true
		})
		return true

	} else if args[0]=="list-meta" {
	
		fmt.Println("Current Meta\n--------------")
		meta.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    fmt.Println(string(i.Key)+" : "+string(i.Val))
		    return true
		})
		return true

	} else if args[0]=="list-titles" {
	
		fmt.Println("Current Titles\n--------------")
		title.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    fmt.Println(string(i.Key)+" : "+string(i.Val))
		    return true
		})
		return true

	} else if args[0]=="list-keywords" {
	
		fmt.Println("Current Keywords\n--------------")
		index.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
		    fmt.Print(string(i.Key)+" ")
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

//Compact the gkv store
func compactDb() {
	fmt.Print("Compacting db...")
	
	//rename db
	os.Rename("./db.gkv", "./_tmp.gkv")

	//open or create db file
	f, err := os.OpenFile("./_tmp.gkv", 0666, os.ModeExclusive)
	if err!=nil {
		f, err = os.Create("./_tmp.gkv")
	}

	//setup store
	tmpstore, err := gkvlite.NewStore(f)
	if err!=nil {
		fmt.Println("Fatal: ",err)
		return
	}

	f, _ = os.Create("./db.gkv")
	tmpstore.CopyTo(f, 999999)
	fmt.Println("Done.")
}