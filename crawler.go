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

	"github.com/steveyen/gkvlite"
	"code.google.com/p/go.net/html"	
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
	meta := store.SetCollection("meta", nil)
	title := store.SetCollection("title", nil)

	//todo: root-domain scoring algo

	//parse command line special cases
	if len(args)>0 && handleCommandLine(args, queue, log, index, meta, title) { 
		return
	}

	//add any extra domains from command line to queue
	for i:=0; i<len(args); i++ {
		//queue.Set([]byte(args[i]), []byte(args[i]))
		queueAndCleanUrl(args[i], queue)	
	}
	
	if len(args)>0 && args[0]=="forever" {
		for {
			//start procesing the queue
			processQueue(queue, log, index, meta, title)
			
			//write kvstore
			store.Flush()

			fmt.Println("Sleeping...")
			time.Sleep(3000 * time.Millisecond)	
		}
	} else {
		//start procesing the queue
		processQueue(queue, log, index, meta, title)
		
		//write kvstore
		store.Flush()
	}
}

//Processes the entire queue top to bottom. 
func processQueue(queue *gkvlite.Collection, log *gkvlite.Collection, index *gkvlite.Collection, meta *gkvlite.Collection, title *gkvlite.Collection) {
	fmt.Println("Crawling...")
	
	queue.VisitItemsAscend([]byte(""), true, func(i *gkvlite.Item) bool {
	    //TODO: goroutines!
	    fmt.Println("Indexing: "+string(i.Key))
	    start:=time.Now()

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

	    	//todo: gofish headers etc
	    	//todo: timeout-based domain blacklist to check before queueing
		    resp, err := http.Get(string(i.Key))
			if err != nil {
				fmt.Println("Err-Get: ", err)
				//todo: delete url from log & queue if 404, or store in a broken link collection?
			} else {

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

				} else {
				
					fmt.Println("Using "+resp.Header.Get("Content-Type")+" as text...")
					body, err := ioutil.ReadAll(resp.Body)
					if err!=nil {
						fmt.Println("Err-Read: ", err)
					}
					//TODO: regex to extract urls to queue, probably wont bother with keywords on these
					_=string(body)
					defer resp.Body.Close()

				}		
			}
		} else {
			fmt.Println("Skipping, last indexed", datediff, "days ago")
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

	queue.Set([]byte(theurl), []byte(""))
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
        						cleaned := queueAndCleanUrl(href, queue)
        						fmt.Println("Queueing "+cleaned)
        						//addKeywords(href, linktext, index)
        					}
        				} else {
        					u, err := url.Parse(urlo)
        					if err==nil {
        						u, err = u.Parse(href)
        						if err==nil {
	        						//queue.Set([]byte(u.String()), []byte(""))
	        						cleaned := queueAndCleanUrl(u.String(), queue)
		        					fmt.Println("Queueing "+cleaned)  
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

        	} else if token.Data == "title" || token.Data == "h1" {
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
        		cleaned := queueAndCleanUrl(token.Data, queue)
				fmt.Println("Queueing "+cleaned)
			} else if strings.Index(token.Data, "www.")==0 {
				//queue.Set([]byte("http://"+token.Data), []byte(""))
				cleaned := queueAndCleanUrl("http://"+token.Data, queue)
				fmt.Println("Queueing "+cleaned)
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
				case "and", "the":

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
		fmt.Println("Commands: compact-db list-queue list-log list-index list-meta list-keywords list-titles clear-queue clear-log")
		return true

	} else if args[0]=="compact-db" {

		fmt.Println("Compacting db\n--------------")
		f, _ := os.Create("./compacted.gkvlite")
		store.CopyTo(f, 9999999)
		fmt.Println("Saved to compacted.gkvlite\n--------------")
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
