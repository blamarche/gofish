package websearch;

import (
	"net/http"
	"io"
	"log"
	"strings"
	"os"
	"sort"
	"strconv"
	"io/ioutil"
	"time"

	"github.com/steveyen/gkvlite"
)


func StartServer() {
	go listenServer(false)
}

func StartServerSSL() {
	go listenServer(true)
}


//-------------------------------
func listenServer(ssl bool) {
	http.HandleFunc("/", handler)
	
	if ssl {
		err := http.ListenAndServeTLS(":8888", "cert.pem", "key.pem", nil)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		err := http.ListenAndServe(":8888", nil)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func handler(w http.ResponseWriter, req *http.Request) {
	keywords:=""
	if req.Method=="POST" {
		keywords = req.FormValue("search")
	}

	//form
	io.WriteString(w, 
					`<!doctype html>
					<html>
						<head>
							<title>Gofish Search</title>
							<meta name="viewport" content="width=device-width, user-scalable=no">
							<style>
								a {
									text-decoration: none;
								}
								.result {
									padding-top: 10px;
									padding-bottom: 10px;
								}
								.stats {
									border-top: 1px solid black;
									padding-top: 20px;
									margin-top: 20px;
									font-style: italic;
									font-size: 10px;
									color: #777;
								}
							</style>
						</head>
						<body>
							<form action='/' method='post'>
								Search: <input name='search' type='text' value="`+keywords+`" /> <input type='submit' value='Go Fish' />
							</form>
					`)

	//search and get results if applicable
	if req.Method=="POST" {
		doSearch(keywords, &w)
	}

	io.WriteString(w, "</body></html>")
}

func doSearch(keywords string, w *http.ResponseWriter) {
	//not efficient to reload for every search, but this is meant for local use, so not a huge deal
	files, err := ioutil.ReadDir("./")
	if err!=nil {
		log.Fatal(err)
	}

	//load gkv files
	index := []*gkvlite.Collection{}
	meta := []*gkvlite.Collection{}
	title := []*gkvlite.Collection{}

	for i:=0; i<len(files); i++ {
		if strings.Contains(files[i].Name(), ".gkv") && !files[i].IsDir() {
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

	processSearch(keywords, index, meta, title, w)
}

//start searching
func processSearch(phrase string, index []*gkvlite.Collection, meta []*gkvlite.Collection, title []*gkvlite.Collection, w *http.ResponseWriter) {
	start:=time.Now()

	keywords := strings.Split(strings.ToLower(phrase), " ")
	results := map[string]int{}

	//exact keyword matches
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
				results[hits[j]] += len(keywords)-i
			} else {
				results[hits[j]] = len(keywords)-i
			}
		}
	}

	//generate keyword variations if results below a threshold
	//todo: use a language library
	if len(results) < 150 {
		//strip trailing suffixes
		suffixes := []string{"est", "ing", "ate", "ful", "ify", "st", "ty", "ed", "al", "er", "or", "s", "y", ""} //"" placeholder for no suffix
		for i:=0; i<len(keywords); i++ {
			tmp:=keywords[i]
			
			for j:=0; j<len(suffixes)-1; j++ {
				if len(tmp)-len(suffixes[j])>0 && tmp[len(tmp)-len(suffixes[j]):] == suffixes[j] {
					keywords[i] = tmp[0:len(tmp)-len(suffixes[j])]
					break
				}
			}
		}

		//then add all the variations back to the base 
		variants := []string{}
		for i:=0; i<len(keywords); i++ {
			for j:=0; j<len(suffixes); j++ {
				if suffixes[j]!="" && keywords[i][len(keywords[i])-1]==suffixes[j][0] {
					variants = append(variants, keywords[i]+suffixes[j][1:])
				} else {
					variants = append(variants, keywords[i]+suffixes[j])
				}
			}
		}

		//search again
		for i:=0; i<len(variants); i++ {
			hitstr := ""
			for _, ind := range index {
				hitstrtmp, err := ind.Get([]byte(variants[i]))	
				if err==nil {
					hitstr += string(hitstrtmp)
				}
			}
			
			hits:= strings.Split(string(hitstr), "||||")
			for j:=0; j<len(hits)-1; j++ { //-1 for the extra |||| at the end
				_, ok := results[hits[j]]
				if ok {
					results[hits[j]] += 1
				} else {
					results[hits[j]] = 1
				}
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
		parts := strings.Split(urls[i], "\n")
		io.WriteString(*w, 
			`<div class="result">
				<a href="`+parts[1]+`"><strong>`+parts[2]+`</strong><br>`+parts[1]+" :"+parts[0]+`</a>
				<br><span style="color: #333"><i>`+parts[3]+`</i></span>
			</div>
			`)
	}

	end:=time.Now()
    diff:=end.Sub(start)

    io.WriteString(*w, "<div class='stats'>")
	io.WriteString(*w, "Returned " + strconv.Itoa(len(urls)) + " results<br>")	   
	io.WriteString(*w, "Time (ms): "+strconv.FormatFloat(diff.Seconds()*1000.0, 'f', 4, 64))
	io.WriteString(*w, "</div>")
}

func leftPad(s string, padStr string, overallLen int) string {
    var padCountInt int
    padCountInt = 1 + ((overallLen-len(padStr))/len(padStr))
    var retStr = strings.Repeat(padStr, padCountInt) + s
    return retStr[(len(retStr)-overallLen):]
}